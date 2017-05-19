import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime
from urllib import urlencode 

from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('GetHumanConnector')
class GetHumanConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        '''
        try:
            self.__baseuri ='http://gethuman.com'
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
            main_page_soup = copy.copy(self.soup)
            while self.__iteratePosts():
                try:
                    next_page_tag = main_page_soup.find('b',text = re.compile('Next 5 reviews ')).findParent('a')['href']
                    if not next_page_tag:
                        break
                    self.currenturi = self.__baseuri + next_page_tag
                    self.__setSoupForCurrentUri()
                    main_page_soup = copy.copy(self.soup)
                except:
                    log.exception(self.log_msg('can not fetch next page links %s'))
                    break
        except:
            log.exception(self.log_msg('page not fetched'))
        return True
        
    @logit(log, '__iteratePosts')
    def __iteratePosts(self):   
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            links =  [self.__baseuri + each.find('a')['href']for each in self.soup.findAll('tr','clickable review')]  
            if not links:
                log.info(self.log_msg('No posts found'))
                return False
            log.info(self.log_msg('Total No of Posts found is %d'%len(links)))
            #return True
            #posts.reverse()
            for link in links:
                if not self.__addPosts(link):
                    False
            return True    
        except:
            log.exception(self.log_msg('can not  find the data'))
            return False        
        
    def __addPosts(self,link):
        '''It will add the post
        '''
        try:
            self.currenturi = link
            if checkSessionInfo('review', self.session_info_out, self.currenturi, \
                            self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info returns True for uri %s'\
                                                               %self.currenturi))
                return False
            self.__setSoupForCurrentUri()    
            page = self.__getData()
            if not page:
                return True
            result = updateSessionInfo('review', self.session_info_out, 
                self.currenturi,get_hash( page ),'review', self.task.instance_data.get('update'))
            if result['updated']:
                page['path'] = [ self.currenturi]
                page['parent_path'] = []
                page['uri']= self.currenturi 
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                page['entity'] = 'review'
                page.update(self.__task_elements_dict)
                self.pages.append(page)
                log.info(self.log_msg('Page added'))
            else:
                log.info(self.log_msg('Update session info returns False for \
                                                url %s'%self.currenturi))
        except:
            log.exception(self.log_msg('Cannot add the post for the uri %s'\
                                                            %self.currenturi))
            return False 
    
    @logit(log, '__getData')
    def __getData(self):
        page = {}
        table = self.soup.find('table','data review').findAll('tr',recursive=False)
        try:
            
            page['data'] = stripHtml(table[1].renderContents()).\
                                    split('read more reviews like this')[0].strip()
        except:
            log.exception(self.log_msg('data not found'))
            page['data'] = ''
        try:            
            page['title'] = stripHtml(table[0].renderContents()).split('see all reviews')[-1].strip()
                            
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''            
        try:
            info = self.soup.findAll('tr','row words')   
            page['et_customer_service_rating'] = stripHtml(info[0].renderContents()).split(':')[-1].strip()
            page['et_communication_rating'] = stripHtml(info[1].renderContents()).split(':')[-1].strip()  
        except:
            log.exception(self.log_msg('info not found'))
        try:
            page['et_customer_hold_time'] = stripHtml(table[4].renderContents()).\
                                            split('tell us your wait')[0].split('Hold time')[-1].strip()
        except:
            log.exception(self.log_msg('hold time not found'))
            
        try:
            date_str = stripHtml(table[5].renderContents()).split('Date reported')[-1].\
                        split('(')[0].strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%a, %d %b %Y'),"%Y-%m-%dT%H:%M:%SZ")                                                        
        except:
            log.exception(self.log_msg('posted_date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
         
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
         
       