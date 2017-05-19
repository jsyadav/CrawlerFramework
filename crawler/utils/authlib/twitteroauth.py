#modified by harsh
from time import time
from urllib2 import HTTPError

from apilib import BaseObject


#from utils.httpconnection import HTTPConnection
import oauth2 as oauth
import json
import tweepy
#from ..utils.decorators import logit
import logging

log = logging.getLogger('TwitterConnector')

class TwitterOauth(BaseObject):
    '''
    Creates API objects using Oauth handlers, example derived class of BaseObject
    '''
    def __init__(self,*args,**kwargs):
        self.consumer_key = kwargs['consumer_key']
        self.consumer_secret = kwargs['consumer_secret']
        self.access_token_key = kwargs['access_token_key']
        self.access_token_secret = kwargs['access_token_secret']
        auth = tweepy.OAuthHandler(kwargs['consumer_key'], kwargs['consumer_secret'])
        auth.set_access_token(kwargs['access_token_key'],  kwargs['access_token_secret'])
        self._api = tweepy.API(auth)
        super(TwitterOauth,self).__init__(*args,**kwargs) #Call parent's class constructor
    
    # @logit(log, 'fetch')
    def fetch(self,uri,*args,**kwargs):
         try:
              
             auth = tweepy.OAuthHandler(self.consumer_key ,self.consumer_secret)
             auth.set_access_token(self.access_token_key, self.access_token_secret)
             #self._api = tweepy.API(auth)
             #consumer_key=tg.config.get(path='keys', key='consumer_key')
             #consumer_secret=tg.config.get(path='keys', key='consumer_secret')
             consumer=oauth.Consumer(self.consumer_key ,self.consumer_secret)
             #access_token_key=tg.config.get(path='keys', key='access_token_key')
             #print "Access_token_key: %s"%self.access_token_key
             log.info("Access_token_key: %s"%self.access_token_key)
             #access_token_secret=tg.config.get(path='keys', key='access_token_secret')
             #print "Access_token_secret: %s"%self.access_token_secret 
             log.info("Access_token_secret: %s"%self.access_token_secret)
             access_token=oauth.Token(self.access_token_key, self.access_token_secret)
             client=oauth.Client(consumer,access_token)
             res=client.request(uri)
             output=json.loads(res[1])
             log.info("Response: %s"%res[0])
#==============================================================================
#              with open("/home/harsh/Desktop/test5.txt", "a") as myfile:
#                      myfile.write("Response: %s\n\n"%res[0])
#                      myfile.close()
#==============================================================================
             #data = urlopen(uri).read()"""
             """conn = HTTPConnection()
             conn.createrequest('http://api.twitter.com/search/tweets.json?q=ipad%20-mini')
             res = conn.fetch(timeout=60).read()
             print 'Response: %s'%res
             output = json.loads(res)"""
             return output
         except HTTPError,e:
             
             print 'Error :%s:%s for  %s'%(e.msg, e.code, uri)
             #raise e
              
    
    def __getattribute__(self,name):
        """
        This is a bit tricky. This is specific to the case in which I am using a 
        a external lib interface for API requests, like for. ex. tweepy for twitter.
        In the usual case, where you just request a URL it's easy to intercept rate-limiting exception 
        raised from __fetch__ method. In this case however any method can be called - search, lookup_users , etc.
        And all of thee methods must be intercepted for rate-limiting exception.
        So doing a exception handling within the *generic* __getattribute__ call.
        Also as this is not common to all the derived classes, can't move this to generic class.
        """
        #print name
        try:
            return BaseObject.__getattribute__(self,name) #Is this a attribute of the object
        except AttributeError,e:
            try:
                return getattr(self._api,name) #or the _api (*tweepy)
            except AttributeError:
                raise e

    def _getWaitingTime(self): #pass api handler as the argument
        """
        Gets number of seconds after which this api will be available again
        return value : 0  handler can be used
                       x > 0  wait for x secnds before start using the API again
        """
        try:
            rateLimitStatus = self._api.rate_limit_status()
            timeLeftToReset = max(rateLimitStatus['reset_time_in_seconds'] - int(time()),10) #In some cases, 
                                                                             # I have seen this become negative, 
                                                                             # while hits remaning is still not reset
            apiRatio = float(rateLimitStatus['remaining_hits']) / rateLimitStatus[u'hourly_limit']
            if apiRatio < 0.1:
                return timeLeftToReset 
            return 0
        except:
            return 0


