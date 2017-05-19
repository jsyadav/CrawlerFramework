#prerna

import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime, timedelta

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from BeautifulSoup import BeautifulSoup

log = logging.getLogger('TrustPilotConnector')
class TrustPilotConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        sample url: http://www.trustpilot.com/review/www.aeropostale.com
        '''
        try:
            self.genre = "Review"
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
            copycurrenturi = self.currenturi
            #count = 0
            while self.__iteratePosts():
                self.currenturi = copycurrenturi + main_page_soup.find('a', id = 'ShowMoreLink')['href']
                self.__setSoupForCurrentUri()
                main_page_soup = copy.copy(self.soup)
                
##                count+=1
##                if count >= 5: #for pagination
##                    break
        except:
            log.exception(self.log_msg('Exception while add the theread posts \
                                            for the url %s'%self.currenturi))
        return True
    @logit(log, '__iteratePosts')
    def __iteratePosts(self):   
        try:
            posts = self.soup.find('div',id = 'Reviews').findAll('div','tabcontentblock reviewblock')
            if not posts:
                log.info(self.log_msg('No posts found %s'%self.currenturi))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(posts)))
            for post in posts:#use some range for few data  
                if not self.__addPost(post):
                    continue
            return True    
        except:
            log.exception(self.log_msg('can not  find the data %s'%self.currenturi))
            return False  
    
    
    @logit(log, '__addPost')    
    def __addPost(self, post): 
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        page ={}
        try:
           
            unique_key = post.find('div','companyComments')['id'].split('_')[-1]
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                         self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True'))
                return False
            page = self.__getData(post)
            if not page:
                return True 
        except:
            log.exception(self.log_msg('Cannot add the post for the url %s'%\
                                                            self.currenturi))
            return False
        try:
            result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                get_hash( page ),'review', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if not result['updated']:
                log.exception(self.log_msg('Update session info returns False'))
                return True
            page['parent_path'] = [self.task.instance_data['uri']]
            page['path'] =  [self.task.instance_data['uri'], unique_key]
            page['uri'] = self.currenturi + '#' + unique_key 
            page['entity'] = 'review'
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            #log.info(page)
            log.info(self.log_msg('page added  %s'%self.currenturi))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False         
    
    @logit(log, '__getData')
    def __getData(self, post):
        page = {}
        try:
            page['data'] = stripHtml(post.find('div','reviewcontent').find('p').\
                            renderContents())
        except:
            log.exception(self.log_msg('data not found %s'%self.currenturi))
            page['data'] = ''    
        try:    
            page['title'] = stripHtml(post.find('div','reviewcontent').find('h2').\
                            renderContents())
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
        
        if not page['data'] and not page['title']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False
        
        try:
            date_str = stripHtml(post.find('div','ndate').renderContents()).strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%d %B %Y'),\
                                    "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('posted_date nt found %s'%self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        
        # author info     
        try:
            auth_info = BeautifulSoup(str(post))
            review_tag = auth_info.find('span','reviewsCount').findParent('li')
            credit_tag = auth_info.find('span','credibilityCount').findParent('li')
            auth_tag =  auth_info.find('a','username').findParent('li')
            if review_tag: 
                review_tag.extract()
            if credit_tag:  
                credit_tag.extract()
            if auth_tag:      
                auth_tag.extract()
            page['et_author_location'] = stripHtml(auth_info.find('div','profileinfo').\
                                        find('ul').renderContents()) 
        except:
            log.exception(self.log_msg('posted_date nt found %s'%self.currenturi))                                   
        try:
            page['ei_author_post_count'] = int(stripHtml(post.find('span','reviewsCount').\
                                            renderContents()).strip().split(' ')[0])                        
        except:
            log.exception(self.log_msg('author post count not found'))                                  
        try:
            page['et_author_name'] = stripHtml(post.find('a','username').renderContents())
        except:
            log.exception(self.log_msg('author_name not found %s'%self.currenturi))
        try:
            page['ei_author_credibility_count'] = int(stripHtml(post.find('span','credibilityCount').\
                                        renderContents()).strip().split(' ')[0])
        except:
            log.exception(self.log_msg('rating tag not found'))    
        try:
            rating = post.find('div','nstar').find('div')['class']
            rating_col = 'acegi'
            page['ef_rating_overall'] = rating_col.index(rating) + 1
            #'acegi'.index(post.find('div','nstar').find('div')['class']) + 1
        except:
            log.exception(self.log_msg(' rating overall not found')) 
        
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
            
                                 
                     
           
          
        
    