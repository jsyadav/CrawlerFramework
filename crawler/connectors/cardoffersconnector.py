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

log = logging.getLogger('CardOffersConnector')
class CardOffersConnector(BaseConnector):
    '''
    This will fetch the info for 
    Sample uris is
    http://www.cardoffers.com/Credit-Card-Commentaries/Citi-AAdvantage-MasterCard/
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of cardoffers.com
        """
        try:
            
            self.baseuri = 'http://www.cardoffers.com'
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
            self.__iteratePosts()                    
            next_page_links = [self.baseuri + each['href']for each in self.soup.\
                                find('td', 'NumberMiddleImage').findAll('a')]
            for each_next_page_link in next_page_links:
                self.currenturi = each_next_page_link
                self.__setSoupForCurrentUri()      
                self.__iteratePosts()  
            return True                      
        except:
            log.exception(self.log_msg('can fetched the page for url %s' %self.currenturi))
        return True  
    
    @logit(log, '__iteratePosts')
    def __iteratePosts(self):
        try:
            posts = self.soup.find('td','NumberBottomImage').findParent('tr').\
                    findNext('tr').findNext('tr').findAll('table')
            if not posts:
                log.info(self.log_msg('No posts found'))
                return False
            log.info(self.log_msg('Total No of Posts found is %d'%len(posts)))
            for post in posts:
                if not self.__addPost(post):
                    return False
            return True
        except:
            log.exception(self.log_msg('can not  find the data%s'%self.currenturi)) 
            return False        
    
    @logit(log, '__addPosts')        
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
            return False  
    
    @logit(log, '__getData')
    def __getData(self, post):
        page = {}
        try:
            page['title'] = stripHtml(post.find('td','PopularData1').renderContents()).\
                            replace('"','')
        except:
            log.exception(self.log_msg('title not found %s'%self.currenturi))
            page['title'] = ''                    
        try:    
            page['data'] = stripHtml(post.find('td','PopularData1').findParent('tr').\
                            findNext('tr').renderContents())
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''
            
        if not page['title'] and not page['data']: 
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False         
        
        try:
            date_str = stripHtml(post.find('td',text = re.compile('^Date:')).\
                        __str__()).split('Date:')[-1].strip()
                        
            page['posted_date'] = datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        try:
            page['et_author_name'] = stripHtml(post.find('td',text = re.compile('^Posted by:'))\
                                    .__str__()).split('Posted by:')[-1]
        except:
            log.exception(self.log_msg('author name not found'))
        try:
            page['et_data_most_attractive_feature'] = stripHtml(post.\
                                                    find('span','CardPerkTitle1',text = re.compile('^Most\s*')).\
                                                    findParent('td').renderContents()).split(':')[-1]  
        except:
            log.exception(self.log_msg('most attractive feature not  found')) 
        try:
            page['et_data_least_attractive_feature'] = stripHtml(post.\
                                                    find('span','CardPerkTitle1',text = re.compile('^Least\s*')).\
                                                    findParent('td').renderContents()).split(':')[-1]  
        except:
            log.exception(self.log_msg('most attractive feature not  found'))                                                           
        try:
           page['ei_data_rating_overall'] = int(re.search('\d+',stripHtml(post.\
                                                find('td',text=re.compile('^ Overall: ')).\
                                                next['src'].__str__())).group(0))
        except:
            log.exception(self.log_msg('rating not found'))
        try:
           page['ei_data_rating_benefits'] = int(re.search('\d+',stripHtml(post.\
                                                find('td',text=re.compile('^ Benefits: ')).\
                                                next['src'].__str__())).group(0))
                                                
        except:
            log.exception(self.log_msg('rating not found'))   
        try:
            page['ei_data_rating_service'] = int(re.search('\d+',stripHtml(post.\
                                                find('td',text=re.compile('^ Service: ')).\
                                                next['src'].__str__())).group(0))
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
          
         