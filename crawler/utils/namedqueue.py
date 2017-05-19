from priorityqueue import PriorityQueue

class NamedQueue(object):
    """It keeps ip_address/token (key): queue (value) pairs
    This can be used to create on the fly queues which can only be accessed by specific crawlnodes having the matching token.
    """

    def __init__(self, maxsize=0):
        self.maxsize = maxsize
        self.__namedQueue = dict()

    def qsize(self):
        return sum([q.qsize() for q in self.__namedQueue.values()])
    
    def listTokens(self):
        return self.__namedQueue.keys()

    def get(self, requestToken):
        """
        Will Return next element in priorityqueue if a queue is present for the given requestToken and is not empty
        Will implcitely return None otherwise
        """
        for tokenSet in self.__namedQueue.keys():
            if requestToken in tokenSet: #If a task can be run by more than one set of  crawlnodes [multiple IP's]
                if not self.__namedQueue[tokenSet].empty():
                    return self.__namedQueue[tokenSet].get(block=False)

    def add(self, task_token, item):
        """
        Create a new priorityQueue for the task_token if not already present
        And add the new task to the Queue for this token then
        """
        #So than order is ignored, and well, calling sorted on tuple returns list??
        task_token = tuple(sorted(tuple(task_token))) 
        #If queue is not already present, create a new queue and associate it with this token
        if not self.__namedQueue.has_key(task_token): 
            pq = PriorityQueue(self.maxsize)
            pq.setPersistConfig(persistFile=None)
            self.__namedQueue[task_token] = pq

        self.__namedQueue[task_token].put(item)
        
