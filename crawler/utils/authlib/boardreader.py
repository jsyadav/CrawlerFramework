import urllib2
import urllib

from crawler.utils.authlib.apilib import BaseObject
from crawler.utils.authlib.apilib import GotRateLimited
import simplejson


class BoardReader(BaseObject):
    '''
    Creates API objects, example derived class of BaseObject
    '''
    def __init__(self,*args,**kwargs):
        self.key = kwargs['key']
        super(BoardReader, self).__init__(*args, **kwargs) 
        

    def __fetch__(self, term, search_options='forums',fetch_comments=False,**kw):
        """
        This __fetch__ method is actually called from BaseObject fetch method
        If you see that you have been ratelimited, from this function you need to raise this exception
        raise GotRateLimited(100)
        This will disable current handler for next 100 seconds
        """
        try:
            params = {'highlight':0,                      
                     'limit':100,
                     'mode':'full',
                    'match_mode':'extended',                    
                    'filter_language':'en'
                    }
            params['query'] = term
            params.update(kw)
            params['rt'] = 'json'
            params['key'] = self.key
            params['body'] = 'full_text'
            if search_options == 'blogs' and fetch_comments:
                params['blog_comments']='on'
            params = urllib.urlencode(params)
            if search_options=='blogs':                
                req = urllib2.Request('http://api.boardreader.com/v1/Blogs/Search?' + params) 
            elif search_options=='news':
                req = urllib2.Request('http://api.boardreader.com/v1/News/Search?' + params)       
            else:
                req = urllib2.Request('http://api.boardreader.com/v1/Boards/Search?' + params)
            response = urllib2.urlopen(req)
            data = simplejson.loads(response.read())
            if data.get('Error') and data['Error'].get('ErrorCode'):
                if date['Error']['ErrorCode'] == 11:
                    raise GotRateLimited(100)
                else:
                    raise Exception(date['Error']['ErrorMsg'])
            return data
        
        except Exception, e:
            raise e
