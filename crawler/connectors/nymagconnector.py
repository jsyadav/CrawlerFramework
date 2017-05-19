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

log = logging.getLogger('NyMagConnector')
class NyMagConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        sample url: http://nymag.com/urr/listings/beauty/lia_schorr_spa/?sort=recent
        '''
        try:
            self.genre = "Review"
            self.baseuri = 'http://nymag.com'
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
            #count = 0
            while self.__iteratePosts():
                self.currenturi = main_page_soup.find('li','next').find('a')['href']
                if self.currenturi == '#':
                    log.info('no next page')
                    break
                self.__setSoupForCurrentUri()
                main_page_soup = copy.copy(self.soup)
##                count+=1
##                if count >= 2: #for pagination
##                    break
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'%self.currenturi))
        return True
    @logit(log, '__iteratePosts')
    def __iteratePosts(self):   
        try:
            posts = self.soup.findAll('div','urr-container')
            if not posts:
                log.info(self.log_msg('No posts found %s'%self.currenturi))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(posts)))
            for post in posts:
                if not self.__addPost(post):
                    return False
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
        try:
            page = self.__getData(post)
            if not page:
                return True 
            unique_key = get_hash({'title' : page['title'], 'data': page['data']}) 
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                         self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True'))
                return False
            
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
            page['uri'] = self.currenturi 
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
            page['title'] = stripHtml(post.find('h2').renderContents())
            #log.info(page['title'])
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
        try:
            page['data'] = stripHtml(post.find('p','urr-description').renderContents())
        except:
            log.exception(self.log_msg('data not found'))
            page['data'] = ''
        
        if not page['title'] and not page['data']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False      
            
        try:
            date_str = stripHtml(post.find('p','urr-usr-date').renderContents()).\
                        split('on')[-1].strip()
            page['posted_date'] = datetime.strftime(datetime.\
                                    strptime(date_str,'%m/%d/%Y'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('posted_date nt found %s'%self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            author = stripHtml(post.find('p','urr-usr-date').renderContents()).split('on')[0]
            if author.startswith('By '):
                page['et_author_name'] =  author.replace('By ','')
            else:
                page['et_author_name'] = author    
        except:
            log.exception(self.log_msg('author_name not found %s'%self.currenturi))
        try:
            page['ef_rating_overall'] = float(stripHtml(post.find('span','overall-num').\
                                        renderContents()))
        except:
            log.exception(self.log_msg('rating tag not found'))    
        try:
            rating = [stripHtml(each.renderContents())for each in post.\
                        findAll('li','category-header')]
            for each in rating:
                if each.startswith('Atmosphere:'):
                    page['et_rating_atmosphere'] = each.split(':')[-1].replace('(','')\
                                                    .replace(')','').strip()
                elif each.startswith('Service:'):
                    page['et_rating_service'] = each.split(':')[-1].replace('(','')\
                                                    .replace(')','').strip()
        except:
            log.exception(self.log_msg('rating not found'))
        try:
            page['et_author_go_back_opinion'] = stripHtml(post.find('li','urr-go-back').\
                                    find('strong').renderContents()).replace(',','')
        except:
            log.exception(self.log_msg('author opinion not found'))
        try:
            page['et_data_helpful'] = int(stripHtml(post.find('p','urr-helpful-info').\
                                        renderContents()).splitlines()[-1].\
                                        split('out of')[0].strip())        
        except:
            log.exception(self.log_msg('author opinion not found'))    
            
        
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
            
                                 
                     
           
          
        
    