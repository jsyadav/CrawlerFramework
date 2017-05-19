#prerna
import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime, timedelta

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from cgi import parse_qsl
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from BeautifulSoup import BeautifulSoup

log = logging.getLogger('StarReviewsConnector')
class StarReviewsConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        sample url: http://www.bankrate.com/financing/credit-cards/warren-to-review-card-agreements/
        '''
        try:
            self.genre = "Review"
            self.baseuri = 'http://www.starreviews.com/bank-of-america.aspx'
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
            headers={'Host':'www.starreviews.com'}
            headers['Referer'] = self.currenturi
            while True:
                self.__iteratePosts()
                try:
                    data = {}
                    data['__EVENTTARGET'] = self.soup.find('a','nextTrigger')['id'].replace('_','$')
                    data['__VIEWSTATE'] =  self.soup.find('input',id= '__VIEWSTATE')['value']
                    self.__setSoupForCurrentUri(data=data,headers=headers)                    
                except:
                    log.exception(self.log_msg('next page not found %s'%self.currenturi)) 
                    break 
            return True      
        except:
            log.exception(self.log_msg('Exception while add the theread posts \
                                            for the url %s'%self.currenturi)) 
        return True

    

    @logit(log, '__iteratePosts')
    def __iteratePosts(self):   
        try:
            post_rows = self.soup.find('div',id = re.compile('\w+MemberReview_UpMemberReview')).\
                    findAll('table',width = '723')[-1].findAll('tr')
            if not post_rows:
                log.info(self.log_msg('No posts found %s'%self.currenturi))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(post_rows)))
            range = len(post_rows)
            c=0
            d=4
            posts = []
            while True:
                new_post = []
                for post_row in post_rows[c:d]:
                    new_post.append(post_row)
                posts.append(BeautifulSoup(new_post.__str__()))
                c += 5
                d += 5
                if d > range:
                    break
            for post in posts:
                self.__addPost(post)    
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
            unique_key = get_hash({'data' : page['data']})
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
            #log.info(page)
            log.info(self.log_msg('page added  %s'%self.currenturi))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False  
    
    @logit(log, '__getData')
    def __getData(self, post):
        page = {}
        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        
        try:
            page['title'] = stripHtml(post.find('td','memreviewhead').renderContents())
        except:
            log.exception(self.log_msg('title not found %s'%self.currenturi))
            page['title'] = ' '
        try:
            page['et_author_name'] = stripHtml(post.find('td','Byline').\
                                        renderContents()).split('who')[0].replace('By','')
        except:
            log.exception(self.log_msg('author_name not found %s'%self.currenturi))    
        try:    
            page['data'] = stripHtml(post.find('td','body11').renderContents())
                      
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
        if not page['title'] and not page['data']: 
            log.info(self.log_msg("Data and title not found for %s,"\
                                " discarding this review"%self.currenturi))
            return False 
        try:
            rating = post.find('td','memreviewhead').find('img')['src']
            page['ei_data_rating'] = int(re.search('\d+',rating.split('/')[-1]).group(0))
        except:
            log.exception(self.log_msg('data rating not found'))    
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
            
                                 
                     
           
          
        
    