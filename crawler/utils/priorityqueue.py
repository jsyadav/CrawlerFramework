
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

from Queue import Queue
from heapq import heappush, heappop
import bsddb3
import pickle


class PriorityQueue(Queue):
    # Initialize the queue representation
    def _init(self, maxsize):
        self.maxsize = maxsize
        self.queue = []

    def setPersistConfig(self, persistFile=None):
        self.persist = bool(persistFile)
        if self.persist:
            self.pdb = bsddb3.btopen(persistFile,'c')
            for value in self.pdb.values():
                task = pickle.loads(value)
                heappush(self.queue, task)

    # Put a new item in the queue # item contains (priority,task,task_identifier)
    def _put(self, item):
        if self.persist:
            self.pdb[item[-1]] = pickle.dumps(item)
            self.pdb.sync()
        return heappush(self.queue, item)

    # check if a task is enqueued #param : task_identifier
    def enqueued(self,task_identifier): 
        if self.persist and task_identifier in self.pdb.keys():
            return True
        else:
            return False
        

    # Get an item from the queue
    def _get(self):
        item = heappop(self.queue)
        return item


    def _remove(self,task_identifier):
        if self.persist:
            self.pdb.pop(task_identifier)
            self.pdb.sync()

if __name__ == "__main__":
    q = PriorityQueue()
    q.put((2,"a"))
    q.put((0,"b"))
    q.put((1,"c"))
    q.put((2,"d"))
    q.put((1,"e"))
    while not q.empty():
        print q.get()
