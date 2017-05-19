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

log = logging.getLogger('InsiderPagesConnector')
class InsiderPagesConnector(BaseConnector):
    '''
    This will fetch the info for 
    Sample uris is
    http://www.insiderpages.com/b/3722158900/bank-of-america-plano
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of insiderpages.com
        """
        try:
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
            self.__setSoupForCurrentUri()
            posts = self.soup.findAll('div', id = re.compile('inline_review_\d+'))
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
    @logit(log, '__addPosts')        
    def __addPost(self, post):
        '''It will add the post
        '''
        try:
            unique_key  = post['id']
            if checkSessionInfo('review', self.session_info_out, unique_key,\
                         self.task.instance_data.get('update'),parent_list\
                                            = [self.currenturi]):
                log.info(self.log_msg('Session info returns True'))
                return False
            
            page = self.__getData(post)
            if not page:
                return True

            result=updateSessionInfo('review', self.session_info_out, unique_key, \
                get_hash( page ),'Review', self.task.instance_data.get('update'),\
                                parent_list=[self.currenturi])
            if not result['updated']:
                log.info(self.log_msg('Update session info returns False'))
                return True
            page['parent_path'] =[self.task.instance_data['uri']] 
            page['path'] = [self.currenturi, unique_key] 
            page['uri'] = self.currenturi + '#' + unique_key
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page['entity'] = 'review'
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            log.info(page)
            log.info(self.log_msg('Post Added'))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False  
    
    @logit(log, '__getData')
    def __getData(self, post):
        page = {}
        try:
            page['title'] = stripHtml(post.find('span','lrgFnt review_title summary').\
                            renderContents())
        except:
            log.exception(self.log_msg('title not found %s'%self.currenturi))
            page['title'] = ''                    
        try:    
            page['data'] = stripHtml(post.find('p','description').renderContents())
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''
            
        if not page['title'] and not page['data']: 
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False         
        try:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.debug(self.log_msg('Posted date not found'))
        try:
            page['et_author_name'] = stripHtml(post.find('a','reviewer').renderContents())
        except:
            log.exception(self.log_msg('author name not found'))
        try:
           page['ei_data_rating'] = int(post.find('div','user_info').find('abbr')['title'])
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
          
         