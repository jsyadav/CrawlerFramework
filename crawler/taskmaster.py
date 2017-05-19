

'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#JV
#VAIDHY
#Ashish

#from processing.managers import BaseManager, CreatorMethod

from multiprocessing.managers import BaseManager

#from knowledgemate import model
from utils.priorityqueue import PriorityQueue
from utils.namedqueue import NamedQueue
#from connectors.connectionfactory import ConnectionFactory

import logging
from tgimport import config
#logging.config.fileConfig('logging_tm.cfg')

logging.raiseExceptions = 0 #should be set to 0
#read http://docs.python.org/dev/library/logging.html 16.6.8. Exceptions raised during logging

log = logging.getLogger('TaskMaster')

from utils.decorators import *

tm = None

class TaskMaster():
    """
    - The master controller for the crawling process
    - Maintains a priority queue for tasks.
    """
    @logit(log, '__init__')
    def __init__(self):
        """
        creates the priority queue that holds the task objects.
        """
        self.tq = PriorityQueue(maxsize=0)
        self.tq.setPersistConfig(persistFile=config.get(section='taskmaster_shard', option='qFile'))
        self.namedQueue = NamedQueue()

    @logit(log,'isenqueued')
    def isEnqueued(self,task_identifier):
        return self.tq.enqueued(task_identifier)
    

    @logit(log, 'qSize')
    def qSize(self):
        """
        returns the priority queue size
        """
        try:
            log.debug('trying to get priority queue size')
            qsize = self.tq.qsize()  + self.namedQueue.qsize()
            log.debug('got priority queue size')
            return qsize
        except:
            log.exception('failed to get the priority queue size')

    
    @logit(log,'put')
    def put(self,elem):
        log.debug('trying to enqueue a task in queue')
        self.tq.put(elem)

    
    @logit(log,'get')
    def get(self, block, requestToken):
        """
        Every crawlnode has to supply requestToken, which can be used to issue task whose token matches requesttoken.
        usecase: If a task can ONLY be performed by crawlnodes running on a particular machine/IP. (Boardreader API)
        They can send this token and matching tasks will only be issued to those crawlnodes

        Algo:
        1) First check if there is any task available for this crawlnode ( if the request token matches)
        2) If not keep on deuqueuing the queue, until
          2.1) A task with matching token is found
          2.2) A task without a token a found (Can be executed by any crawlnode available)
        
       With this entire setup, there is one problem - It's possible that all the nodes are blocked on blocking get() request
       from the main queue, while the namedQueue is not empty. Think of this scenario.
       Queue : () << Crawlnodes are requesting to access element in this order , [C(token1), C(token2)]
       Note that queue is initially empty, and all the crawlnodes are blocked in order given above.

       1) One element is enqueued - (token2 : <task>)
          Queue = (token2 : <task>)
       2) C(token1) dequeues the task, and as token doesn't match it will enqueue it to the namedQueue for the token
          C(token2) is still blocked to the main queue get() call, and will never get the namedQueue task until it's 
          unblocked.
       Workaround: if main Queue is empty, and token don't match (like in case given above)
       enqueue it back to the main queue so that rest of the crawlnodes are given the chance until there is a match.
       

        """
        
        log.debug('trying to get a task from the priority queue')
        selectiveTask = self.namedQueue.get(requestToken)
        if selectiveTask:
            log.info("Returning a type of task, which can only be handled by crawlnode with matching token:%s"%requestToken)
            return selectiveTask

        while True:# Get the next element in queue
            Qtask = self.tq.get(block=block)
            if not Qtask[1].token or requestToken in Qtask[1].token: 
                log.debug('got a task from the prioriy queue for token: %s'%requestToken)
                return Qtask
            elif self.tq.empty(): #The workaround explained above
                self.removeTask(Qtask[-1])
                self.put(Qtask)
            else:
                self.namedQueue.add(Qtask[1].token, Qtask)
                

    @logit(log,'removeTask')
    def removeTask(self,task_identifier):
        log.info("tid : " + str(task_identifier))
        self.tq._remove(task_identifier)


class QueueManager(BaseManager):
    pass
QueueManager.register(callable=lambda:tm, typeid='proxyMaster')
        
def start():
    """
    starts the most important thing in crawler - the master
    """
    try:
        print "entry"
        log.debug('initializing Taskmaster Server')
        address=(config.get(section='taskmaster_shard', option='host'),
                 int(config.get(section='taskmaster_shard', option='port')))
        print "creating queuemanager"
        qm = QueueManager(address=address, authkey='none')
        print "created queuemanager"
        log.debug('TaskMaster Server initialized')
        print "starting server"
        m = qm.get_server()
        m.serve_forever()
        print "quitting..."
    except:
        import traceback
        print traceback.format_exc()
        log.exception('failed to initialize Taskmaster Server')
        log.critical('failed to initialize Taskmaster Server')

if __name__ == '__main__':
    tm = TaskMaster()
    start()





