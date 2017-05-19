
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#JV
#VAIDHY
#Ashish

import logging

logging.raiseExceptions = 0 #should be set to 0
#read http://docs.python.org/dev/library/logging.html 16.6.8. Exceptions raised during logging

from logging import config
# logging.config.fileConfig('logging.cfg')
log = logging.getLogger('CrawlNode')

from multiprocessing import Process,freeze_support,active_children
#from processing import Process,activeChildren
#from processing import Queue
import traceback
import os,socket
import time
from utils.utils import email_exception
from datetime import datetime
from tgimport import *
from utils import taskmanager
from utils.task import Task

from utils.decorators import *


# token_q=Queue()

# @logit(log, 'callback')
# def callback(result):
#     """
#     for every returning process put a token in the queue object - token_q
#     and return immediately
#     """
#     callback_start = time.time()
#     try:
#         log.debug("Trying to remove %s" %(result))
#         token_q.put(1)
#         taskmanager.removeTask(result)
#         log.debug("Removed %s. Took %f seconds" % (result, time.time() - callback_start))
#     except:
#         email_exception(str(traceback.format_exc()),interval = 600)
#         log.critical('exception in crawlnode.callback: Exception %s' %(str(traceback.format_exc())))
        
        
def getCurrentIPAddress():
    """
    Returns IP address associated with the resolved fqdn (there should a entry of fqdn in /etc/hosts for python)
    """
    return socket.gethostbyname(socket.gethostname())

@logit(log, 'fetchTask')
def fetchTask():
    """
    fetch a task from the master, initialize the connector instance and return it
    """
    try:
        try:
            log.debug('trying to fetch a task from the taskmaster')
            token = getCurrentIPAddress()
            task=taskmanager.getTask(token,True)
            log.debug('got a task from the taskmaster')
        except:
            print "failed to fetch a task from taskmaster"
            log.exception('FAILED to fetch a task from the taskmaster')
            log.critical('FAILED to fetch a task from the taskmaster')
            return

        try:
            log.debug('trying to instantiate a connector')
            task,task_identifier=task[1:] # since the task is a tuple (priority, task,task_identifier)
            connclass=task.connClass
            # import connection class from connectors
            class_ = __import__('connectors.' + connclass.lower(), globals(),
                                locals(), [connclass],-1)
            connectorInstance = class_.__dict__[connclass](task)
            log.debug('returning a connector instance: ' + connclass)
            return connectorInstance,task_identifier
        except:
            print traceback.format_exc()
            print "failed to instantiate a connector"
            #email_exception(str(traceback.format_exc()),interval = 600)
            log.exception('FAILED to instantiate a connector, task id %s' % getattr(task,'id',None))
            log.critical('FAILED to instantiate a connector')
    except:
        log.exception('FAILED to fetchTask')
        log.critical('FAILED to fetchTask')


if __name__ == '__main__':
    """
    -create a proxy for the taskmaster object
    -create a pool
    -infinite loop - sleep 10 seconds empty the token_q and create as many new processes as in token_q
    """
    try:
        print "starting crawlnode"
        n = 1 #tg.config.config.configMap['CrawlNode']['numprocess']
        try:
            while True:
                while len(active_children()) < n:
                    try:
                        log.info('spawning a new process')
                        connectorInstance,task_identifier=fetchTask()
                        print connectorInstance
                        if connectorInstance:
                            process = Process(target=connectorInstance.run,args=(task_identifier,))
                            print "starting"
                            process.start()
                            print "started"
                            log.info('Starting the newly spawned process')
                        else:
                            time.sleep(10)
                    except:
                        print traceback.format_exc()
                        log.exception('exception in creating on of the pool workers')
                        continue
                time.sleep(10)
        except:
            print traceback.format_exc()
            log.exception('exception in creating pool')
            log.critical('exception in creating pool')
    except Exception,e:
        #email_exception(str(traceback.format_exc()),interval = 600)
        log.exception('exception in crawlnode')
        log.critical('exception in crawlnode')


