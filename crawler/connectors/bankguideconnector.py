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

log = logging.getLogger('BankGuideConnector')
class BankGuideConnector(BaseConnector):
    @logit(log , 'fetch')
    def fetch(self):
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
            
            self.__iteratePosts()    
        except:
            log.exception(self.log_msg('can fetched the page for url %s' %self.currenturi))
        return True        
            
    @logit(log, '__iteratePosts')
    def __iteratePosts(self):   
        
        try:
            posts = self.soup.findAll('div','comment-box')
            if not posts:
                log.info(self.log_msg('No posts found'))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(posts)))
            for post in posts:
                if not self.__addPost(post):
                    return False
            return True    
        except:
            log.exception(self.log_msg('can not  find the data'))
            return False    
            
    def __addPost(self, post):
        '''It will add the post
        '''
        try:
            
            page = self.__getData(post)
            if not page:
                return True
            unique_key  = get_hash( {'data' : page['data'] })
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
            #page['path'].append(unique_key)
            page['uri'] = self.currenturi
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page['entity'] = 'post'
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            log.info(page)
            log.info(self.log_msg('Post Added'))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False  
    
    def __getData(self, post):
        page = {}
        try:
            post_tag = BeautifulSoup(post.__str__().replace('/>>','/>'))
            table_tag = post_tag.find('table')
            if table_tag:
                table_tag.extract()
            try:    
                page['data'] = stripHtml(post_tag.renderContents())
                page['title']= ''
            except:
                log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
                return        
        
            try:
                date_str = stripHtml(table_tag.findAll('strong')[-1].renderContents())
                page['posted_date'] = datetime.strftime(datetime.\
                                        strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",date_str).\
                                        strip(),"%d %B %Y"),"%Y-%m-%dT%H:%M:%SZ")             
            except:
                log.exception(self.log_msg('Posted date not found'))
                page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            try:
                page['et_author_name'] = stripHtml(table_tag.findAll('strong')[0].renderContents())
            except:
                log.exception(self.log_msg('author name not found'))
        except:
            log.exception(self.log_msg('post tag not found'))        
         
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
          
         