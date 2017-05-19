from crawler.utils.authlib.apilib import BaseObject


class Generic(BaseObject):
    def __init__(self, *args, **kwargs):
        """
        Parameters required : 
        pool_key : This will be used as a key to access the pool for a particular handler.
        section : This will be the keyname to parse app.cfg file to get keys if any, 
        or can be a list of dicts (one for each handler) to be used as args, 
        which can be used to initialzed that many number of handlers 
        as length of outer list given.
        """
        self._args = kwargs.get('args')
        self.__fetch__ = kwargs['fetch_func'] #Pass callable function as aparameter
        super(Generic,self).__init__(*args,**kwargs) #Call parent's class constructor
        
