#modified by prerna
#prerna
import re, copy
import logging
from urllib2 import urlparse
from datetime import datetime
from urllib import urlencode 

from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.httpconnection import HTTPConnection
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('MyBankTrackerConnector')
class MyBankTrackerConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self): 
        '''This is a fetch method which  fetches the data 
        '''
        try:
            self.baseuri = 'http://www.mybanktracker.com'
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
##            conn = HTTPConnection()
##            headers ={}
##            headers['Accept-encoding'] = ''
##            headers['Host'] = 'www.mybanktracker.com'
            self.__setSoupForCurrentUri()
            parent_uri = self.currenturi
            #main_page_soup = copy.copy(self.soup)
            page_count_list = re.findall('\d+',stripHtml(self.soup.find('p','view-bank-reviews-current-count').\
                                renderContents()))
            if len(page_count_list)>1: 
                x = int(page_count_list[1])
                y = int(page_count_list[0])
                page_count = x/y if x%y==0 else (x/y) + 1 
            else:
                log.info('post found in one page only')
            c=1    
            while self.__iteratePosts():
                try:
##                    next_page_uri = self.soup.find('span',text =re.compile('^Next')).\
##                                    findParent('a')['href']
##                    f = open('mytest.html','w')
##                    f.write(self.soup.prettify())
##                    f.close()
                    if page_count:
                        c +=1
                        if c<=page_count:
                            next_page_uri = parent_uri + '/page:' +str(c)
                            self.currenturi = next_page_uri
                            self.__setSoupForCurrentUri()
                        else:
                            break    
                except:
                    log.exception(self.log_msg('can not fetch next page links %s'%self.currenturi))
                    break
                log.debug(self.log_msg('LINKSOUT: ' + str(len(self.linksOut))))
            return True
        except:
            log.exception(self.log_msg('page not fetched'))
            return False
    
    @logit(log, '__iteratePosts')
    def __iteratePosts(self):   
        """It will Iterate Over the links found in the Current URI
        """
        try:
##            posts = self.soup.find('div','customer-reviews').find('div','pad-top pad-bottom').\
##                    findAll('div',recursive = False)
            posts = self.soup.find('div','customer-reviews').\
                    findAll('div', attrs = {'class':re.compile('customer-review')}, recursive = False)
            if not posts:
                log.info(self.log_msg('No posts found'))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(posts)))
            #posts.reverse()
            for post in posts:
                if not self.__addPost(post):
                    return False
            return True    
        except:
            log.exception(self.log_msg('can not  find the data'))
            return False                 

    @logit(log, '__addPost')  
    def __addPost(self, post): 
        """
        This will take the post tag , and fetch data 
        """
        try:
            page = self.__getData(post)              
            if not page:
                return True           
            unique_key  = get_hash( {'title' : page['title'],'data' : page['data'] })
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
            page['entity'] = 'post'
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            log.info(page)
            log.info(self.log_msg('Post Added'))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False  
               
    
    @logit(log, '__getData')
    def __getData(self,post):
        page = {}
        try:
            date_str = stripHtml(post.find('div','customer-review-date').renderContents()).strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",date_str).\
                                    strip(),"%b %d %Y"),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('posted date  not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        
        try:
            page['title'] = stripHtml(post.find('div','customer-review-review-text').find('h2').\
                            renderContents())
            
        except:
            log.exception(self.log_msg('can not find title')) 
        copycurrenturi = self.currenturi    
        try:
            continue_tag = post.find('a','continue')
            if continue_tag:
                self.currenturi = self.baseuri + continue_tag['href']
                self.__setSoupForCurrentUri()
                page['data'] = stripHtml(self.soup.find('div','review-text-container').\
                                find('span',property ='v:description').renderContents())
            else:
                page['data'] = stripHtml(post.find('div','customer-review-review-text').find('p').\
                            renderContents())
        except:
            log.exception(self.log_msg('can not find data'))  
            page['data'] = page['title']   
        self.currenturi = copycurrenturi    
        
        if not page['title']:
            log.info(self.log_msg("title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False    
        try:
            page['et_author_name'] = stripHtml(post.find('span','customer-review-user-name').\
                                        find('a').renderContents())
        except:
            log.exception(self.log_msg('author name not found'))     
        
        try:
            page['ei_data_rating'] = int(stripHtml(post.find('p','star-rating-text').renderContents()).\
                        split('(')[-1].split('out')[0])
        except:
            log.exception(self.log_msg('review rating not found'))     
            
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
