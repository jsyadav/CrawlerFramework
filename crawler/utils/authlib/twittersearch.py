from urllib2 import urlopen,HTTPError
import logging

from crawler.utils.authlib.apilib import GotRateLimited


log = log = logging.getLogger('TwitterSearchAPIHandler')

def fetch(uri,*args,**kwargs):
    try:
        return urlopen(uri).read()
    except HTTPError,e:
        if e.code == 420 and e.headers.get('Retry-After'):
            waitTill = int(e.headers.get('Retry-After'))
            log.info('Rate limit excedded for Search, disabling for %s seconds'%waitTill)
            raise GotRateLimited(waitTill)
        else:
            raise e
