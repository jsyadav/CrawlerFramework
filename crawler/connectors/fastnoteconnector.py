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

log = logging.getLogger('FastNoteConnector')
class FastNoteConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        sample url: http://www.fastnote.com/wells-fargo-company?note_filters[is_archived]=1
        '''
        try:
            self.genre = "Review"
            self.baseuri = 'http://www.fastnote.com'
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
                self.currenturi = main_page_soup.find('a','page_link next')['href']
                self.__setSoupForCurrentUri()
                main_page_soup = copy.copy(self.soup)
##                count+=1
##                if count >= 2: #for pagination
##                    break
        except:
            log.exception(self.log_msg('Exception while add the theread posts \
                                            for the url %s'%self.currenturi))
        return True
    @logit(log, '__iteratePosts')
    def __iteratePosts(self):   
        try:
            posts = self.soup.find('ul','note-list').findAll('li',recursive = False)
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
            unique_key = post.find('p','archived')['id']
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
            page['title'] = stripHtml(post.find('p','topic').renderContents())
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
        try:
            page['data'] = stripHtml(post.find('p','description').renderContents())
        except:
            log.exception(self.log_msg('data not found'))
            page['data'] = ''
        
        if not page['title'] and not page['data']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False      
        try:
            date_str = stripHtml(post.find('div','status').find('p','time').\
                        renderContents()).strip()
            match_object = re.search('(\d+) (weeks?|months?|years?|days?) ago',date_str)
            no_of_days = 0
            if match_object:
                days_str = match_object.group(2)
                if days_str.startswith('day'):
                    no_of_days = int (match_object.group(1))
                elif days_str.startswith('week'):
                    no_of_days = int (match_object.group(1)) * 7
                elif days_str.startswith('year'):
                    no_of_days = int (match_object.group(1)) * 365
                elif days_str.startswith('month'):
                    no_of_days = int (match_object.group(1)) * 30
                page['posted_date']= datetime.strftime(datetime.now() - timedelta(no_of_days), "%Y-%m-%dT%H:%M:%SZ")
            else:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            log.info(page['posted_date'])    
        except:
            log.exception(self.log_msg('posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        
        try:     
            page['et_data_sentiments'] = stripHtml(post.find('div','status').\
                                    find('p',attrs = {'class':re.compile('note-type\s*')}).\
                                    renderContents())
        except:
            log.exception(self.log_msg('data sentiments not found'))
        try:     
            page['et_author_location'] = stripHtml(post.find('div','status').\
                                    find('p','user-location').renderContents())
        except:
            log.exception(self.log_msg('author_location not found'))    
            
        try:     
            page['ef_rating_agree'] = float(stripHtml(post.\
                                        find('tr',attrs = {'class':re.compile('agree\s*')}).\
                                        find('td','count').renderContents()))
        except:
            log.exception(self.log_msg('rating agree not found'))
        try:     
            page['ef_rating_disagree'] = float(stripHtml(post.\
                                        find('tr',attrs = {'class':re.compile('disagree\s*')}).\
                                        find('td','count').renderContents())) 
        except:
            log.exception(self.log_msg('rating disagree not found'))   
        try:
            page['ef_rating_funny'] = float(stripHtml(post.\
                                    find('tr',attrs = {'class':re.compile('funny\s*')}).\
                                    find('td','count').renderContents()))
        except:
            log.exception(self.log_msg('rating funny not found'))     
        try:     
            page['ef_rating_clever'] = float(stripHtml(post.\
                                    find('tr',attrs = {'class':re.compile('clever \s*')}).\
                                    find('td','count').renderContents()))
        except:
            log.exception(self.log_msg('rating clever not found'))    
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
            
                                 
                     
           
          
        
    