#prerna
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

log = logging.getLogger('CiaoConnector')
class CiaoConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        sample url: http://www.ciao.co.uk/Reviews/HSBC__55648
        '''
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
            main_page_soup = copy.copy(self.soup)
            #c = 0
            while self.__iteratePosts():
                try:
                    next_page_tag =main_page_soup.find('a',text = 'Next page').parent['href']
                    if not next_page_tag:
                        break
                    self.currenturi = next_page_tag
                    self.__setSoupForCurrentUri()
                    main_page_soup = copy.copy(self.soup)
##                    c +=1
##                    if c >=1:
##                        break
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
            posts = self.soup.findAll('div','CWCiaoKievReviewShort')
            if not posts:
                log.info(self.log_msg('No posts found'))
                return False
            log.info(self.log_msg('Total No of Posts found is %d'%len(posts)))
            for post in posts:
                if not self.__addPosts(post):
                   return False
            return True    
        except:
            log.exception(self.log_msg('can not  find the data'))
            return False        
        
    def __addPosts(self, post):
        '''It will add the post
        '''
        try:
            unique_key = post['id'].split('_')[-1]
            if checkSessionInfo('review', self.session_info_out, unique_key, \
                            self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info returns True for uri %s'\
                                                               %self.currenturi))
                return False
            page = self.__getData(post)
            if not page:
                return True
            result = updateSessionInfo('review', self.session_info_out, 
                unique_key,get_hash( page ),'review', self.task.instance_data.get('update'))
            if result['updated']:
                page['path'] = [ self.currenturi, unique_key]
                page['parent_path'] = []
                if not page.get('uri'):
                    page['uri']= self.currenturi + '#' + unique_key
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
        return True
    
    @logit(log, '__getData')
    def __getData(self, post):
        
        page = {}
        copycurrenturi = self.currenturi
        try:
            data_link = post.find('a',text = 'Read full review')
            if data_link:
                page['uri'] = self.currenturi = data_link.parent['href']
                self.__setSoupForCurrentUri()
                page['data'] = stripHtml(self.soup.find('div',id = 'intelliTXT').\
                            renderContents())
                try:
                    page['et_author_recommendation'] = stripHtml(self.soup.find('span','CWnoWrap').\
                                                renderContents()).replace(':','')          
                except:
                    log.exception(self.log_msg('recommmendation not found'))            
            else:
                page['data'] = stripHtml(post.find('div','CWBingProdPageReviewText').\
                                find('p').renderContents())   
        except:
            log.exception(self.log_msg('data not found'))
            page['data'] = ''
        self.currenturi = copycurrenturi    
        try:
            title_tag = post.find('a','ReviewTitle')
            if title_tag:
                page['title'] = stripHtml(title_tag.renderContents())
            else:
                page['title'] = stripHtml(post.find('p','CWFontCActive quickReviewHead').\
                                renderContents())    
                            
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''            
        try:
            pros =  post.find('strong',text = 'Advantages:')
            if pros:
                page['et_data_pros'] = stripHtml(pros.next.__str__())
            cons = post.find('strong',text = 'Disadvantages:')
            if cons:    
                page['et_data_cons'] = stripHtml(cons.next.__str__())
        except:
            log.info(self.log_msg('pros cons not found')) 
        try:    
            page['et_author_name'] = stripHtml(post.find('span','ReviewInfo').\
                                        renderContents()).split(' ')[0] 
        except:
            log.exception(self.log_msg('author name not found'))
        try:
            date_str = stripHtml(post.find('span','ReviewInfo').renderContents()).split(' ')[1].strip()           
            page['posted_date'] = datetime.strptime(date_str,"%d.%m.%Y").strftime("%Y-%m-%dT%H:%M:%SZ")                                                       
        except:
            log.exception(self.log_msg('posted_date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['ei_rating_overall'] = int(post.find('span','ReviewInfo').\
                                            findPrevious('img')['src'].split('/')[-1].\
                                            split('stars')[-1].split('.')[0])/10    
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
         
       