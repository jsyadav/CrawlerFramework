#prerna
import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime,timedelta
from cgi import parse_qsl
from urllib import urlencode

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from BeautifulSoup import BeautifulSoup

log = logging.getLogger('BrokerageReviewsConnector')
class BrokerageReviewsConnector(BaseConnector):
    '''
    This will fetch the info for 
    Sample uris is
    http://www.brokeragereviews.org/stock-broker-reviews/interactivebrokers-review.aspx
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of brokeragereviews.org
        """
        try:
            self.__setSoupForCurrentUri()
            self.__task_elements_dict = {
                                'priority':self.task.priority,
                                'level': self.task.level,
                                'last_updated_time':datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ"),
                                'pickup_date':datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ"),
                                'connector_instance_log_id': self.task.connector_instance_log_id,
                                'connector_instance_id':self.task.connector_instance_id,
                                'workspace_id':self.task.workspace_id,
                                'client_id':self.task.client_id,
                                'client_name':self.task.client_name,
                                'versioned':False,
                                'category':self.task.instance_data.get('category',''),
                                'task_log_id':self.task.id }
                                
            posts = self.soup.find('div',id = 'Reviews').findAll('tr')[1:-1]
            if not posts:
                log.info(self.log_msg('No posts found'))
                return False
            log.info(self.log_msg('Total No of Posts found is %d'%len(posts)))
            for post in posts:
                if not self.__addPost(post):
                    return False
            return True   
           
        except:
            log.exception(self.log_msg('can fetched the page for url %s' %self.currenturi))
        return True        
            
    def __addPost(self, post):
        '''It will add the post
        '''
        try:
            page = self.__getData(post)
            if not page:
                return True
            unique_key  = get_hash( {'data' : page['data'], 'title' : page['title']})
            if checkSessionInfo('review', self.session_info_out, unique_key,\
                         self.task.instance_data.get('update'),parent_list\
                                            = [self.currenturi]):
                log.info(self.log_msg('Session info returns True'))
                return False

            result=updateSessionInfo('review', self.session_info_out, unique_key, \
                get_hash( page ),'Review', self.task.instance_data.get('update'),\
                                parent_list=[self.currenturi])
            if not result['updated']:
                log.info(self.log_msg('Update session info returns False'))
                return True
            page['path'] = [self.currenturi] 
            page['parent_path'] = []
            page['uri'] = self.currenturi
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page['entity'] = 'review'
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            log.info(page)
            log.info(self.log_msg('Post Added'))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
        return True  
    
    def __getData(self, post):
        page = {}
        try:    
            page['data'] = '\n'.join([stripHtml(each.renderContents())for each in post.\
                            findAll('span','ReviewText')]).replace('Pros:','').replace('Cons:','')
##            page['data'] = stripHtml(post.find('b',text = re.compile('^Cons:')).\
##                            findParent('span').findNext('span').renderContents())
            page['title'] = ''
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            return       
        try:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('Posted date not found'))
        try:
            page['et_author_name'] = stripHtml(post.find('span','ReviewDate').\
                                        renderContents()).split('by')[-1]
        except:
            log.exception(self.log_msg('author name not found'))
##        try:
##            page['et_pros'] =  stripHtml(post.find('b',text = re.compile('^Pros:')).\
##                                findParent('span').renderContents()).split('Pros:')[-1]
##        except:
##            log.exception(self.log_msg('pros not found'))
##        try:
##            page['et_cons'] =  stripHtml(post.find('b',text = re.compile('^Cons:')).\
##                                findParent('span').renderContents()).split('Cons:')[-1]
##        except:
##            log.exception(self.log_msg('cons not found'))    
                                        
        try:
            page['ei_data_rating'] = int(re.search('\d+stars', post.find('span','ReviewDate').\
                                    findPrevious('img')['src']).group(0).strip('stars'))
        except:
            log.exception(self.log_msg('rating not found'))        
           
         
        return page                                                                                                                                                                                      
    @logit(log, '__setSoupForCurrentUri')                                                                                 
    def __setSoupForCurrentUri(self, data=None, headers={}):
        """It will set soup object for the Current URI
        """
        res = self._getHTML(data=data, headers=headers)
        if res:
            self.rawpage = res['result']
        else:
            log.info(self.log_msg('Page Content cannot be fetched for the url: \
                                                            %s'%self.currenturi))
            raise Exception('Page content not fetched for th url %s'%self.currenturi)
        self._setCurrentPage()                                                 
          
         