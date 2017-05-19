import sys

import os

sys.path.append(os.path.abspath(__file__).rsplit('/',3)[0])
import re
from threading import Timer
from collections import defaultdict

import logging
log = logging.getLogger('AuthLib')


class ObjectPool(object):
    '''
    Object pool design pattern it supports maintaining pool of 
    available api request handlers( for sites like twitter,facebook etc.)
    
    '''
    
    def __init__(self):
        '''
        Initializes pool with all the objects for which paramaters are provided.
        Could have used a queue for pool for FIFO kind of behaviour,
        but in that case (number_of_active_twitter_tasks) <=  len(pool) always, however in case of list more than one task 
        can share the same handler if required, not possible in case of queue.

        '''
        self.__pool = defaultdict(list)
                    
    # def poolExists(self, pool_key):
    #     return pool_key.lower() in self.__pool.keys()

    def addObjects(self, obj_class, pool_key, **kwargs):
        """
        Adds new objects to the pool
        class_name is provided explicitely, use that name instead as a key, think of generic baseobject class case
        """
        args_section = kwargs.get('args_section')
        if args_section: #Parse app.cfg to get args if any, for this handler
            if isinstance(args_section, basestring): #Is a key to args present in app.cfg
                init_args = BaseObject.parsecfg(args_section)
        else:
            init_args = [{}] #Create one empty dummy handler
                
        if kwargs.has_key('fetch_func'): #If fetch_func is provided, add it to init arguments
            for each_args in init_args:
                each_args['fetch_func'] = kwargs['fetch_func']

        pool_key = pool_key.lower()
        #Create as many handlers as APIKeys are available (length of args), OR 1 if none
        self.__pool[pool_key].extend([obj_class(pool_key, self, **each_args) for each_args in init_args])
        
    def get(self,pool_key):
        """
        It will return next available handler in the pool and will block if none are available to be used.
        This function implements routing behavior, and basically is a proxy between API endpoints and connector client 
        using the object.


        So apis can be used in this form - 
        *api.get().lookup_users(screen_names = screen_names)*

        """
        if not self.__pool.has_key(pool_key):
            raise NoActiveHandlersAvailable("No Active Handlers Found")

        handlers = sorted([handler for handler in self.__pool[pool_key] if handler.isUsable()])

        if handlers:
            return handlers[0]
        else:
            raise NoActiveHandlersAvailable("No Active Handlers Found")

    def recheckRateLimit(self,pool_key):
        """
        This function can be called in case, you want to do a rate-limit check on the handlers
        and mark the ones disabled which are rate-limited.
        """
        for obj in self.__pool.get(pool_key,[]):
            if hasattr(obj,'_getWaitingTime'):
                waitingTime = obj._getWaitingTime()
                if waitingTime > 0:
                    obj.resetStateAfter(waitingTime)
                    

    def __repr__(self):
        """
        Pretty representation
        """
        repr_string = ""
        for k,v  in self.__pool.iteritems():
            repr_string += "Pool : %s, Active handlers : %d, Total Handlers : %d \n"%(k,len([handler for \
                                  handler in v if handler.isUsable()]), len(v))
        return repr_string or "No Pool created"


class BaseObject(object):
    """
    Defines the base object class for the object instances in the pool
    """
    
    def __init__(self, pool_key, parent_pool_ref, *args, **kwargs):
        self.__parent_pool = parent_pool_ref #keep a reference to parent pool container
        self.pool_key = pool_key
        self._enabled = True

    def getParentPool(self):
        return self.__parent_pool
        
    def isUsable(self):
        return self._enabled

    def updateState(self,enable=True):
        """
        It updates state of given api handler, 
        which can be used to set if this object is getting rate-limited and not to be used till next X minutes, etc.
        """
        self._enabled = enable

    def resetStateAfter(self,secs=None):
        """
        It Starts a thread which will disable existing api to be used and reenable handler again after X seconds.
        """
        if not secs:
            secs = self.__getWaitingTime()

        self.updateState(enable=False) #Disable the event
        timedEvent = Timer(secs, self.updateState, kwargs={'enable':True}) #Renable after specified seconds
        timedEvent.setDaemon(True)
        timedEvent.start() #Start the event

    def __fetch__(self,*args,**kwargs):
        """
        To be implemented in derived class
        """
        raise NotImplementedError()

    def fetch(self,*args,**kwargs):
        """
        It is a wrapper around __fetch__ method, which is used to fetch data 
        if you need to directly fetch the uri yourself and are not using some third party library to do it for you.

        if to be used, then __fetch__ has to be implemeted in the derived class.
        if a API call gets rate limited, __fetch__ should raise GotRateLimited(timeout) exception 
        to notify to switch to next available handler
        """
        try:
            return self.__fetch__(*args,**kwargs)
        except GotRateLimited as timeout:
            #Disable this handler, and execute query for next available handler
            print "Got rate Limited :: "
            self.resetStateAfter(timeout.resetAfterSecs) #disable this handler
            self.__parent_pool.get(self.pool_key).fetch(*args,**kwargs) #Get next available handler and execute the query


    @classmethod
    def parsecfg(cls,key):
        """
        Will parse the file, and will return [{<key>:<value> .. ] params used to initialize the objects
        Sample Settings :
        [api_keys]
        [[twitter_ouath]]
        #twitter Oauth secret key
        1.oauth_consumer_key="xxxx"
        1.oauth_consumer_secret="xxxx"
        1.user_access_token_key="xxxx"
        1.user_access_token_secret="xxxx"
        2.oauth_consumer_key="xxxx"
        2.oauth_consumer_secret="xxxx"
        2.user_access_token_key="xxxx"
        2.user_access_token_secret="xxxx"

        [[boardreader]]
        api_time_reset

        In this case I will create 2 instances for oauth handler: 2 * (4 config values required for each instance)
        In this case I will create 2 instances for twitter_search : as per pool_size setting
        """
        # section = config.get(section='api_keys', option=key)
        key_regex = re.compile('[0-9]+\..+')
        

        # if section and section.has_key('api_time_reset'):
        #     return dict(api_time_reset = section['api_time_reset'])
        api_keys = [('1.consumer_key', "zyv8He3BfmrkjIEbxj0sNQ"),
                    ('1.consumer_secret', "HBGtlvzaF8iGMrFQINpkxfHx4GIK7AM9S9hi9Yk"),
                    ('1.access_token_key', "14364093-wPM4mVMtRFAKE3gF77hNHipcUvDtaziSaXduPY21E"),
                    ('1.access_token_secret', "CLLBWA0n5z5uNvUAmLGAcCyxiSWsxEsuqegcoLRHUhY"),
                    ('1.key', "543db2rz4wb6hbs3f467ugjr")]
        # if section and all([key_regex.match(key) for key in section.keys()]):
        args = defaultdict(dict)
        for k,v in api_keys:
            seq_id,arg_key = k.split('.',1)
            args[seq_id][arg_key] = v
        return args.values()
        # else:
        #     raise Exception('Either section not found, or configuration key:value are not of the correct format')

        

class GotRateLimited(Exception):
    """Indicates that no active handlers are available to be used"""
    def __init__(self, resetAfterSecs):
        self.resetAfterSecs = resetAfterSecs

class NoActiveHandlersAvailable(Exception):
    """Indicates that no active handlers are available to be used"""
    def __init__(self, msg):
        self.msg = msg
