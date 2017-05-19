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

log = logging.getLogger('JoyStiqConnector')
class JoyStiqConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        sample url: http://www.joystiq.com/2010/09/15/xbla-in-brief-space-invaders-infinity-gene-kof-sky-stage-son/
        '''
        try:
            self.genre = "Review"
            self.baseuri = 'http://www.joystiq.com/'
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
            self.__setParentPage()
            self.__iteratePosts()
        except:
            log.exception(self.log_msg('Exception while add the theread posts \
                                            for the url %s'%self.currenturi))
        return True

    @logit(log, '__setParentPage')
    def __setParentPage(self):
        """ this will set parent page info """
        page = {}
        try:
            page['title']  = stripHtml(self.soup.find('span',id = re.compile('ppt\d+')).\
                                renderContents()) 
        except:
            log.exception(self.log_msg('main page title  not found %s'%self.currenturi))
            page['title'] = ''
        try:
            page['data']  = stripHtml(self.soup.find('span',id = re.compile('ppt\d+')).\
                                renderContents())
        except:
            log.exception(self.log_msg('data not found %s'%self.currenturi)) 
            page['data'] = ''  
            
        if not page['title'] and not page['data']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False  
        try:
            date_str = stripHtml(str(self.soup.find('p','byline').find('a').findNext('a').\
                        next.next)).split('on')[-1]
            page['posted_date'] = datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",date_str).\
                                    strip(),"%b %d %Y  %I:%M%p").strftime("%Y-%m-%dT%H:%M:%SZ")
        except:                            
            log.exception(self.log_msg('Posted date not found %s'%self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), \
                                    "%Y-%m-%dT%H:%M:%SZ")
        try:
            page['ei_data_views_count'] = int(stripHtml(self.soup.find('div','comment-count').\
                                            renderContents()).replace(',',''))
        except:
            log.exception(self.log_msg('views count not found %s'%self.currenturi))                                
        try:
            page['et_author_name'] = stripHtml(self.soup.find('a','byline-author').renderContents())
        except:
            log.exception(self.log_msg('game info not found %s'%self.currenturi))
                
        unique_key = self.currenturi
        if checkSessionInfo('review', self.session_info_out, unique_key,\
            self.task.instance_data.get('update')):
                    
            log.info(self.log_msg('Session info returns True for uri %s'\
                                                                           %self.currenturi))
            return False
        try:
            result=updateSessionInfo('review', self.session_info_out, unique_key, \
                    get_hash( page ),'Review', self.task.instance_data.get('update'))
            if not result['updated']:
                log.exception(self.log_msg('Update session info returns False %s'%self.currenturi))
                return True
            page['parent_path'] = [] #parent path empty..recheck why not product page!!
            page['path'] = [self.task.instance_data['uri']]
            page['uri'] = self.currenturi
            page['entity'] = 'Review'
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            #log.info(page)
            log.info(self.log_msg('Post Added %s'%self.currenturi))
            return True        
        except:
            log.exception(self.log_msg('Error while adding session info %s'%self.currenturi))
            return False    

    @logit(log, '__iteratePosts')
    def __iteratePosts(self):   
        try:
            posts = self.soup.findAll('div','comment-inner')
            if not posts:
                log.info(self.log_msg('No posts found %s'%self.currenturi))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(posts)))
            for post in posts[:]:#use some range for few data  
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
            unique_key = get_hash({'posted_date' : page['posted_date'], 'data': page['data']}) 
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
            page['path'] =  [self.task.instance_data['uri'],unique_key]
            page['uri'] = self.currenturi
            page['entity'] = 'review'
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            log.info(page)
            log.info(self.log_msg('page added  %s'%self.currenturi))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False  
    
    @logit(log, '__getData')
    def __getData(self, post):
        page = {}
        try:
            page['title'] = page['data'] =stripHtml(post.find('div','comment-body').renderContents()).\
                            replace('/>>','/>')
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['title'] = page['data'] = ''        
        
        if not page['title'] and not page['data']: 
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False  
        try:
            date_str = stripHtml(str(post.find('h4').next)).split('Posted:')[-1].strip()
            page['posted_date']= datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",date_str),\
                                    "%b %d %Y  %I:%M%p").strftime("%Y-%m-%dT%H:%M:%SZ")   
        except:
            log.exception(self.log_msg('posted_date nt found %s'%self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")                                 
        
        # author info 
        try:
            page['et_author_name'] = stripHtml(post.find('span').renderContents()).\
                                        split('said')[0].strip()
        except:
            log.exception(self.log_msg('author_name not found %s'%self.currenturi))
                                            
        copycurrenturi = self.currenturi
        try:
            auth_link = post.find('span').find('a')['href']
            self.currenturi = auth_link
            self.__setSoupForCurrentUri()  
            try:
                page['et_author_posts_count'] = int(stripHtml(str(self.soup.find('td',text = 'Joystiq').\
                                                findNext('td').next.next.next)).\
                                                split('Comments')[0].replace(',',''))
            except:
                log.exception(self.log_msg('author level  not found %s'%self.currenturi))                                              
            try:     
                date_str = stripHtml(self.soup.find('p','title').renderContents()).split(':')[-1].strip()
                page['edate_author_join_date'] = datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",date_str),\
                                                    "%b %d, %Y").strftime("%Y-%m-%dT%H:%M:%SZ")                                
            except:
                log.exception(self.log_msg('Join date not found %s'%self.currenturi))
                    
                                                        
        except:
            log.exception(self.log_msg('author link not found %s'%self.currenturi))
        self.currenturi = copycurrenturi   
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
            
                                 
                     
           
          
        
    