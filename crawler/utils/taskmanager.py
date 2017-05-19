
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''


#Ashish
from ConfigParser import NoOptionError
import sys

#from crawler.utils import db_sessionmanager

import os


sys.path.append(os.getcwd().rsplit('/',1)[0])
#from processing.managers import BaseManager, CreatorMethod
import traceback
import pickle,md5
import json
from datetime import datetime

from tgimport import config
import model
from model import *
from task import Task
from connectors.connectionfactory import ConnectionFactory
from database import session

from utils import email_exception
import logging

#from logging import config
#logging.config.fileConfig(taskmaster_shard_path + '/logging.cfg')

#logging.raiseExceptions = 0 #should be set to 0
#read http://docs.python.org/dev/library/logging.html 16.6.8. Exceptions raised during logging

log = logging.getLogger('TaskManager')

from decorators import *

try:
    '''
    Create taskMaster object
    '''
    from multiprocessing.managers import BaseManager

    class QueueManager(BaseManager):
        pass
    QueueManager.register(callable=lambda: tm, typeid='proxyMaster')

    # m = QueueManager(address=(config.get(section='taskmaster_shard', option='host'),
    #                           config.get(section='taskmaster_shard', option='port')),
    #                  authkey='none')
    m = QueueManager(address=('127.0.0.1',
                              int(config.get(section='taskmaster_shard', option='port'))),
                     authkey='none')
    m.connect()
    tm = m.proxyMaster()
except:
    print "taskmaster doesnt seem to be running"
    log.exception("TaskMaster doesn't seem to be running, So taskmaster related utility functions _WON'T WORK_"\
                  "However if you don't need to run any such functions _IGNORE THIS_ error")

@logit(log,'bindModelToDb')
def bindModelToDb(workspace_id=None, dburi=None):
    # ignoring any params and just initialize the engine with default dburi from config file
    dburi = config.get(section="taskmaster_shard", option="dburi")
    model.metadata.bind = dburi

@logit(log, 'putTask')
def __putTask(task, task_identifier):
    """
    takes a task object
    tries to get a connector class based on the instance_data of a conector instance,
    restore the old session_info for a specific url
    call 'put' on the priority queue
    """
    try:
        if not task.connClass:
            task.connClass = __guessConnectorName(task.instance_data, task.workspace_id)
        try:
            tokens = config.get(section='Connector', option='%s_tokens' % str(task.connClass.lower()))
            if tokens:
                task.token = tuple([t.strip() for t in tokens.split(',') if t.strip()])
        except NoOptionError:
            print "Ignoring tokens for %s" % (str(task.connClass.lower()), )
        # connector = session.query(model.Connector).filter_by(name=task.connClass).first()
        connector=session.query(model.Connector).filter_by(name=task.connClass).first()
        if connector:
            task.instance_data.update(json.loads(connector.connector_data))
        ##
        log.debug('got connector class: %s' % task.connClass)
            #TODO-HAS A FLAW - ONLY CHKING THE WORKSPACE ID RATHER THEN THE CONNECTOR INSTANCE ID and CHK FOR CHANGED KEYWORDS

        #I have task.connector_instance_id >> last connector_instance_log_id >> match uri >> session_info
        last_connector_instance_log=session.query(model.ConnectorInstanceLog).\
            filter(model.ConnectorInstanceLog.id != task.connector_instance_log_id).\
            filter(model.ConnectorInstanceLog.connector_instance_id == task.connector_instance_id).\
            order_by(model.ConnectorInstanceLog._tid.desc()).first()

        if last_connector_instance_log:
            res=session.query(model.TaskLog).\
                filter_by(uri=task.instance_data['uri'],
                      connector_instance_log_id=last_connector_instance_log.id,
                      delete_status=False).\
                      order_by(model.TaskLog.completion_time.desc()).first()
            if res:
                log.debug("putting last session info")
                task.session_info=pickle.loads(res.session_info)

        log.debug("trying to create a task log entry in the DB")
        #creating a task and putting the enqueue time
        session.begin()
        task_log = model.TaskLog()
        task_log.enqueue_time = datetime.utcnow()
        task_log.uri = task.instance_data['uri']
        task_log.workspace_id = task.workspace_id
        task_log.connector_instance_log_id = task.connector_instance_log_id
        task_log.level = task.level
        task_log.session_info = pickle.dumps(task.session_info)
        session.save_or_update(task_log)
        session.flush()
        session.commit()
        #task_log_id=task_log.id
        log.info("db entry created for, task_log_id: %s, with enqueue time" % task_log.id)

        log.debug('trying to put a task in the priority queue')
        task.id = task_log.id
        task.instance_data['parent_extracted_entities'] = __putParentSessionInfo(task.connector_instance_id)
        log.debug('task put in the priority queue')
        #session.commit()
        task.task_identifier = task_identifier #have included this attribute which is different from id
                                               #as a part of task which will be used to put/remove a task from bdb
        tm.put((task.priority, task, task_identifier))
    except:
        print traceback.format_exc()
        #email_exception(str(traceback.format_exc()),interval = 600)
        log.exception('failed to get a connector/get session info or failed to create taskLog/ put task in the priority queue')
        log.critical('failed to get a connector/get session info or failed to create taskLog/ put task in the priority queue')
        if session:session.rollback()

################################
# Functions related to create tasks

@logit(log, 'createTasks')
def __createTasks(connector_instances, priority):
    """
    - gets a calculated frequency from the taskfeeder
    - calculates a priority based on the frequency
    - priority=96/frequency+1
    -    frequency|priority
    -      -         1      - such tasks do not come from schedule - online, one time, weekly, monthly
    -      96        2      - 15 mins job
    -      |         |
    -      1         97     - once in a day
    - read scheduled connector instances from db according to the frequency
    - creates connector instance log and conector instance data for each connector instance
    - create task objects
    - putTask
    - if applyKeywords ==False, put keywords=None
    """

    try:
        log.debug("iterating over connector instances, to create tasks")
         #to give the online tasks priority = 1
        for connector_instance in connector_instances:
            try:
                log.debug('trying to create a connector instance log')
                if not __enqueueConnector(connector_instance):
                    continue
                task_identifier = __getTaskIdentifier(connector_instance.workspace_id,
                                     json.loads(connector_instance.instance_data)['uri'] ,
                                                           priority,json.loads(connector_instance.\
                                                                      instance_data).get('instance_filter_words'))
                print task_identifier
                if task_identifier:
                    session.begin()
                    print connector_instance.id
                    connector_instance_log = model.ConnectorInstanceLog()
                    connector_instance_log.connector_instance_id=connector_instance.id
                    print connector_instance_log
                    session.save_or_update(connector_instance_log)
                    session.flush()
                    print "got a connector instance log"
                    log.debug("got a connector instance log")
                    log.debug("trying to recreate connector instance data from connector and instance data")
                    #re-construct the instance_data (merge the connector data in)
                    task= __createTask(connector_instance, connector_instance_log.id,priority)
                    log.debug("task created")
                    print "task created"
                    session.commit() #NOT SURE ABOUT THE LOCATION OF COMMIT
                    log.debug('calling tm putTask')
                    print "calling tm puttask"
                    __putTask(task,task_identifier)
                    print "return from tm puttask"
                    log.debug('return from tm putTask')
                else:
                    log.info('task already enqueued , so not enqueuing again')
            except:
                print traceback.format_exc()
                log.exception('one of the scheduled task failed to be read')
                log.critical('one of the scheduled task failed to be read')
                session.rollback()
        log.debug('all scheduled tasks created, iteration done')
    except:
        log.exception('failed to read schedule')
        log.critical('failed to read schedule')


@logit(log, 'readLinksOut')
def readLinksOut(linksOut):
    """
    -is called by crawler processes when there are links out from a site
    -recieves a list of task objects
    -calls the putTask method for each task object
    """
    try:
        log.debug('trying to unpickle and read the links out (cloned tasks)- pickle.loads')
        linksOut=pickle.loads(linksOut)
        log.debug('linksOut (cloned tasks) read')

        log.debug('puting the linksout in the priority queue')
        #LINK here means task object
        for link in linksOut:
            task_identifier = __getTaskIdentifier(link.workspace_id,
                                                 link.instance_data['uri'] , 
                                                       link.priority,link.instance_data.get('instance_filter_words'))
            if task_identifier:
                __putTask(link,task_identifier)
            else:
                log.info('similar task is already enqueued , so not putting that task')
        log.debug('all linksOut (cloned taks) successfully put in the prioity queue. returning, true')
        return True
    except :
        log.exception('reading links out failed, false')
        return False


@logit(log, 'createTask')
def __createTask(connector_instance, connector_instance_log_id,priority):
    instance_data = __getInstanceData(connector_instance)
    task=Task(instance_data=instance_data,
              keywords=[keyword_obj for keyword_obj in connector_instance.workspace.keywords \
                        if keyword_obj.active_status==True and keyword_obj.delete_status==False],
              priority=priority,
              connector_instance_log_id=connector_instance_log_id,
              workspace_id = connector_instance.workspace.id,
              client_id = connector_instance.workspace.client.id,
              client_name = connector_instance.workspace.client.name,
              connector_instance_id = connector_instance.id,
              dburi = str(model.metadata.bind.url)
              )
    return task

################################


##########################################Fetch Task from taskmaster
@logit(log, 'getTask')
def getTask(requestToken,block=True):
    """
    -get a task out from the queue
    -update the task_log entry with dequeue time
    -return the task to the caller-crawlnode
    """
    try:
        task = tm.get(block,requestToken)
        log.debug('trying to update the corresponding task_log with dequeue time')
        bindModelToDb(workspace_id=task[1].workspace_id)
        session.begin()
        task_log=session.query(model.TaskLog).filter_by(id=task[1].id).one()
        task_log.dequeue_time=datetime.utcnow()
        session.save_or_update(task_log)
        session.flush()
        log.debug("db entry updated for, task_log_id: %s, with a dequeue time." % task[1].id)
        log.debug('returning the task after getting from priority queue and updating dequeue time')
        session.commit()
        return task
    except:
        email_exception(str(traceback.format_exc()),interval = 600)
        log.exception('failed to get a task from the priority queue or failed to update the dequeue time')
        log.critical('failed to get a task from the priority queue or failed to update the dequeue time')
        if session: session.rollback()



@logit(log,'removeTask')
def removeTask(task_identifier):
    tm.removeTask(task_identifier)

############################################################### Utility Functions
@logit(log, '_guessConnectorInstance')
def __guessConnectorName(instance_data,workspace_id):
    """
    """
    log.debug('trying to guess the connector for the url submitted ')
    #to figure out what connector it belongs to
    connectors={}
    results=session.query(model.Connector).all()
    for result in results:
        connectors.setdefault(result.protocol,{})
        connectors[result.protocol][result.url_segment] = (result.name)
    cf=ConnectionFactory()
    connName=cf.getConnector(connectors, instance_data)
    log.debug('connector guessed is: %s' % connName)
    return connName

@logit(log, '__getInstanceData')
def __getInstanceData(connector_instance):
    '''
    It gets the connector_instance_object as a input, and get the workspace_id
    and gets the workspace_meta data and return it
    '''
    instance_data = json.loads(connector_instance.instance_data)
    ##there is no connector data if thats a html connector or generic or rss
    
    if connector_instance.connector:
        instance_data.update(json.loads(connector_instance.connector.connector_data))
        
    if connector_instance.workspace.metadata:
        workspace_data = json.loads(connector_instance.workspace.metadata)
        instance_data.update(workspace_data)
        
    instance_data['name'] = connector_instance.name
    log.info(str(instance_data))
    return instance_data



##Connector instance only put after every x days
@logit(log,'__enqueueConnector')
def __enqueueConnector(connector_instance):
    instance_data = __getInstanceData(connector_instance)
    if instance_data.get('crawl_everyday'):
        log.info("It needs to be crawled daily")
        return True

    ##For now return true anyway, as previously we use to take mod on connector_instance_id(int) is now uuid, so it won't work now
    # connector_name = __guessConnectorName(json.loads(connector_instance.instance_data),\
    #                                       connector_instance.workspace_id)
    # connector = model.session.query(model.Connector).filter_by(name=connector_name).first()
    # if connector:
    #     source_type = json.loads(connector.connector_data).get('source_type')
    #     last_connector_instance_log=model.session.query(model.ConnectorInstanceLog).\
    #         filter(model.ConnectorInstanceLog.connector_instance_id==connector_instance.id).\
    #         order_by(model.ConnectorInstanceLog._tid.desc()).first()
    #     if last_connector_instance_log and source_type and source_type.lower() == 'review' and \
    #             connector_instance.id % 7 != datetime.today().weekday():
    #         log.info('The connector instance : %s is not crawled today for the condition' \
    #                  ' ((mod)%s != %s(weekday))'%(connector_instance.id , \
    #                                                   connector_instance.id % 7 , 
    #                                               datetime.today().weekday()))
    #         return False
    return True

@logit(log, '__getTaskIdentifier')
def __getTaskIdentifier(workspace_id,uri,priority,instance_filter_words):
    try:
        print "__getTaskIdentifier"
        if isinstance(uri,unicode):
            uri = uri.encode('utf-8','ignore')
        else:
            uri = uri.decode('utf-8','ignore').encode('utf-8','ignore')
        if  not instance_filter_words:
            task_identifier = md5.md5(str(workspace_id) + uri + str(priority)).hexdigest()
        else:
            if isinstance(instance_filter_words,unicode):
                instance_filter_words = instance_filter_words.encode('utf-8','ignore')
            else:
                instance_filter_words = instance_filter_words.decode('utf-8','ignore').encode('utf-8','ignore')
            task_identifier = md5.md5(str(workspace_id) + uri + str(priority)+instance_filter_words).hexdigest()
        if not tm.isEnqueued(task_identifier):
            return task_identifier
    except:
        log.info(traceback.format_exc())
    return False

def addFile(title, file_url, workspace_id):
    """Add a new task with a file's URL for CustomDataConnector
    """
    raise Exception('Not Implemented')
    # connector_instance = model.ConnectorInstance.query.filter_by(name='customdataconnector', 
    #                                                                workspace_id=workspace_id).first()

    # # Update the url of the connector_instance's instance_data
    # inst_data = json.loads(connector_instance.instance_data)
    # inst_data['uri'] = file_url
    # inst_data['title'] = title
    # connector_instance.instance_data = json.dumps(inst_data)

    # # Create the task and put it in queue
    # task_identifier = tm.getTaskIdentifier(connector_instance.workspace_id,
    #                                            json.loads(connector_instance.instance_data)['uri'], 1,\
    #                                                json.loads(connector_instance.instance_data)\
    #                                                .get('instance_filter_words'))
    # log.debug("Task Identifier %s" %(task_identifier))
    # if task_identifier:
    #     model.session.begin()
    #     connector_instance_log = model.ConnectorInstanceLog(connector_instance_id=connector_instance.id)
    #     model.session.save_or_update(connector_instance_log)
    #     model.session.flush()
    #     log.debug("got a connector instance log")
    #     log.debug("trying to recreate connector instance data from connector and instance data")
    #     #re-construct the instance_data (merge the connector data in)
    #     task = __createTask(connector_instance, connector_instance_log.id, 1)
    #     log.debug("task created")
    #     model.session.commit() #NOT SURE ABOUT THE LOCATION OF COMMIT
    #     log.debug('calling tm putTask')
    #     __putTask(task, task_identifier)
    #     log.debug('return from tm putTask')
    # else:
    #     log.info('task already enqueued , so not enqueuing again')


##Product Session Information Api
def __putParentSessionInfo(connector_instance_id):
    # query = "select common.parent_extracted_entity_names.name, common.parent_extracted_entity_names.data_type ,"\
    #     "common.parent_extracted_entity_values.value from "\
    #     "common.parent_extracted_entity_values,common.parent_extracted_entity_names where common."\
    #     "parent_extracted_entity_values._tid in (select max(_tid) from common.parent_extracted_entity_values "\
    #     "group by extracted_entity_name_id,connector_instance_id ) and common.parent_extracted_entity_names.id = common."\
    #     "parent_extracted_entity_values.extracted_entity_name_id and common.parent_extracted_entity_values."\
    #     "connector_instance_id = '%s'"
    query = "select common.parent_extracted_entity_names.name, common.parent_extracted_entity_names.data_type ,"\
        "common.parent_extracted_entity_values.value from common.parent_extracted_entity_values, "\
        "common.parent_extracted_entity_names where common.parent_extracted_entity_values._tid in "\
        "(select max(_tid) from common.parent_extracted_entity_values where connector_instance_id "\
        "= '%s' group by extracted_entity_name_id,connector_instance_id ) "\
        "and common.parent_extracted_entity_names.id = common.parent_extracted_entity_values.extracted_entity_name_id "\
        "and common.parent_extracted_entity_values.connector_instance_id = '%s' "\
        "order by common.parent_extracted_entity_values._tid asc;"
    results = model.metadata.bind.execute(query%(connector_instance_id,connector_instance_id))
    parent_session_info = {}
    for name,data_type,value in list(results):
         parent_session_info[(name,data_type)] = value
    log.info('parent_session_info == %s'%str(parent_session_info))
    return parent_session_info


#############################################################################Starting Crawl/Schedule

@logit(log, 'crawlnow')
def crawlNow(**kwds):
    '''
    kwds - workspace_id(s) or (connector_instance_id(s) 
    **and corresponding workspace_id** [Because of db breakup based on workspaces]) 
    '''
    try:
        print "gotcha"
        log.debug("gotcha")
        # ###XOR
        # assert kwds.get('workspace_ids') or (kwds.get('connector_instance_ids') and kwds.get('workpsace_id')) and \
        #     not (kwds.get('workspace_ids') and (kwds.get('connector_instance_ids') and kwds.get('workpsace_id') ))
        # ###
        assert bool(kwds.get('connector_instance_ids') and kwds.get('workspace_id')) ^ bool(kwds.get('workspace_ids')) #Xor operator
        log.debug("trying to get conector instances for %s" % (','.join(['%s:%s' % (str(k),str(v))
                                                                         for k,v in kwds.items()])))
        priority = kwds.get('priority',97) #can pass priority paramater to taskmaster
        if kwds.get('workspace_ids'):
            for workspace_id in kwds['workspace_ids']:
                bindModelToDb(workspace_id=workspace_id)
                connector_instances = session.query(model.ConnectorInstance).filter(
                    model.and_(model.ConnectorInstance.workspace_id == workspace_id,
                               model.ConnectorInstance.active_status=='t',
                               model.ConnectorInstance.delete_status=='f'))
                __createTasks(connector_instances, priority)

        else:
            bindModelToDb(workspace_id=kwds.get('workspace_id'))
            # connector_instances = model.ConnectorInstance.query.filter(\
            #     model.and_(model.ConnectorInstance.id.in_(kwds['connector_instance_ids']),
            #                model.ConnectorInstance.active_status=='t',
            #                model.ConnectorInstance.delete_status=='f'))
            connector_instances = session.query(model.ConnectorInstance).filter(\
                model.and_(model.ConnectorInstance.id.in_(kwds['connector_instance_ids']),
                           model.ConnectorInstance.active_status=='t',
                           model.ConnectorInstance.delete_status=='f'))
            print connector_instances.all()[0].instance_data
            __createTasks(connector_instances, priority)

        return True
    except:
        print traceback.format_exc()
        #email_exception(str(traceback.format_exc()),interval = 600)
        log.exception('crawlNow failed')
        return False


@logit(log,'getQueueSize')
def getQueueSize():
        return tm.qSize()

@logit(log, 'readSchedule')
def readSchedule(freq):
    raise Exception('Not Implemented')
    # log.debug("trying to get conector instances for frequency: %d" % freq)

    # connector_instances = model.session.query(model.ConnectorInstance).filter(model.and_(model.ConnectorInstance.frequency>=freq,
    #                                                                                model.ConnectorInstance.active_status=='t',
    #                                                                                model.ConnectorInstance.delete_status=='f'))
    # log.debug("got %d connector instances for frequency: %d" % (len([each for each in connector_instances]),freq))
    # log.debug(','.join([str(each.id) for each in connector_instances]))
    # priority = 96/freq+1
    # __createTasks(connector_instances, priority)






