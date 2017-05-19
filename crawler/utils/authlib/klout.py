from urllib2 import urlopen,HTTPError

from utils.authlib.apilib import BaseObject
from utils.authlib.apilib import GotRateLimited
import json


class Klout(BaseObject):
    '''
    Creates API objects, example derived class of BaseObject
    '''
    def __init__(self,*args,**kwargs):
        self.key = kwargs['key']
        super(Klout,self).__init__(*args,**kwargs) #Call parent's class constructor


    def __fetch__(self, uri, *args, **kwargs):
        """
        This __fetch__ method is actually called from BaseObject fetch method
        If you see that you have been ratelimited, from this function you need to raise this exception
        raise GotRateLimited(100)
        This will disable current handler for next 100 seconds
        """
        try:
            uri = uri+'&key=%s' % self.key
            data = urlopen(uri).read()
            if json.loads(data)['status'] == 403:
                raise GotRateLimited(100)
            return data
        except HTTPError,e :
            print 'Error :%s:%s for  %s'%(e.msg, e.code, uri)
            raise e
