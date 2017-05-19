import sys

import memcache
import turbogears as tg
import os

sys.path.append(os.getcwd().rsplit(os.sep,1)[0])
tg.update_config(configfile='../knowledgemate/app.cfg', modulename='knowledgemate.config')

class MemCache:
    def __init__(self,client_uri,client_port,timeout,keep_uncompressed):
        self.client_uri=client_uri
        self.client_port = client_port
        self.timeout = timeout
        self.keep_uncompressed = keep_uncompressed
        self.memc = memcache.Client(['%s:%s'%(self.client_uri,self.client_port)])
    
    def get(self,uri):
        return self.memc.get(uri.encode('utf-8'))

    def set(self,uri,data):
        if self.keep_uncompressed:
            return self.memc.add(uri.encode('utf-8'),data,int(self.timeout))
        else:
            return self.memc.add(uri.encode('utf-8'),data,int(self.timeout),1)
    
    def flushAll(self):
        """
        clear cache
        """
        self.memc.flush_all()
