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

log = logging.getLogger('ShoppingConnector')
class ShoppingConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        sample url: http://www.shopping.com/xMR-Macys.com~MRD-11314
        '''
        try:
            self.genre = "Review"
            self.baseuri = 'http://www.shopping.com'
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
            self.parent_uri = self.currenturi 
            #count = 0
            while self.__iterateLinks():
                self.currenturi = self.baseuri + main_page_soup.find('td','rightSquareArrowTd').\
                                    find('a')['href']
                self.__setSoupForCurrentUri()
                main_page_soup = copy.copy(self.soup)
                self.parent_uri = self.currenturi
##                count+=1
##                if count >= 2: #for pagination
##                    break
        except:
            log.exception(self.log_msg('Exception while add the theread posts \
                                            for the url %s'%self.currenturi))
        return True
    @logit(log, '__iterateLinks')
    def __iterateLinks(self):   
        try:
            posts = [each.findParent('table',width = '100%')for each in self.\
                    soup.findAll('div','pL10 pR10')]
            if not posts:
                log.info(self.log_msg('No posts found %s'%self.currenturi))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(posts)))
            for post in posts:
                full_review = post.find('a',text = re.compile('Read full review'))
                if full_review:
                    full_review_link = full_review.parent['href']
                    if not self.__addLink(full_review_link):
                        return False
                else:
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
        page = {}
        try:
            page['data'] = stripHtml(post.find('div','pL10 pR10 pT5').renderContents()).strip()
            
        except:
            log.exception(self.log_msg('data not found'))
            page['data'] = ''
        try:
            page['title'] = stripHtml(post.find('h2','boxSubTitle').renderContents()).\
                            strip()
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = '' 
        if not page['data'] and not page['title']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False                                             
        try:
            date_str = stripHtml(post.find('div','lightTxt').renderContents()).\
                        split(' ,')[-1].strip()
            page['posted_date']= datetime.strftime(datetime.\
                                    strptime(date_str,'%B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")  
        except:
            log.exception(self.log_msg('posted_date nt found %s'%self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['et_author_name'] = stripHtml(post.find('div','lightTxt').\
                                        find('a').renderContents())
        except:
            log.exception(self.log_msg('author_name not found %s'%self.currenturi))
        try:
            page['ef_rating_overall'] = float(post.find('b',text = re.compile('^Overall Rating:')).\
                                    findNext('img')['alt'].split('/')[0])        
        except:
            log.exception(self.log_msg('author rating not found'))     
        try:
            page['ef_rating_on_time_delivery'] = float(post.find('b',text = re.compile('^On-Time Delivery:')).\
                                    findNext('img')['alt'].split('/')[0])        
        except:
            log.exception(self.log_msg('author rating not found')) 
        try:
            page['ef_rating_customer_service'] = float(post.find('b',text = re.compile('Customer Service:')).\
                                    findNext('img')['alt'].split('/')[0])        
        except:
            log.exception(self.log_msg('author rating not found'))                                     
            
        try:
            unique_key = get_hash({'data': page['data']})  
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
            page['path'] =  [self.task.instance_data['uri']]
            page['uri'] = self.parent_uri
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
    @logit(log, '__addLink')    
    def __addLink(self, link): 
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            self.currenturi = self.baseuri + link
            self.__setSoupForCurrentUri()
            unique_key = self.currenturi 
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                         self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True'))
                return False
            page = self.__getData()
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
            page['path'] =  [self.task.instance_data['uri']]
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
    def __getData(self):
        page = {}
        
        try:
            page['title'] = stripHtml(self.soup.find('h2','boxTitleNB2').renderContents())
        except:
            log.exception(self.log_msg('title not found for the url %s'%self.currenturi))
            page['title'] = '' 
        
        try:
            pros_tag = self.soup.find('div','boxMidRt').findAll('p')    
            if pros_tag:
                [each.extract()for each in pros_tag]
            for each in pros_tag:    
                try:    
                    page['et_data_pros'] = each.find('b',text =re.compile('Pros:')).\
                                            next.__str__()
                except:
                    log.exception(self.log_msg('data pros not found'))
                try:    
                    page['et_data_cons'] = each.find('b',text =re.compile('Cons:')).\
                                            next.__str__()
                except:
                    log.exception(self.log_msg('data cons not found'))
                try:    
                    page['et_data_bottom_line'] = each.find('b',text =re.compile('The Bottom Line:')).\
                                                    next.__str__()
                except:
                    log.exception(self.log_msg('data Bottom Line not found'))
        except:    
            log.exception(self.log_msg('pros tag not found'))
        try:
            data_tag = self.soup.find('div','boxMidRt')     
            author_tag = data_tag.find('div','lightTxt')     
            if author_tag:
                author_tag.extract()
            data = stripHtml(data_tag.renderContents().replace('/>>','/>')) 
            try:
                if data.startswith('Full review'):
                    data = data.replace('Full review','')
                    page['data'] = data
                else:     
                    page['data'] = stripHtml(data_tag.renderContents())
            except:
                log.exception(self.log_msg('title not found for the url %s'%self.currenturi))
                page['data'] = ''
            try:
                date_str = stripHtml(author_tag.renderContents()).\
                        split('written on')[-1].strip()
                page['posted_date']= datetime.strftime(datetime.\
                                    strptime(date_str,'%b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")  
            except:
                log.exception(self.log_msg('posted_date nt found %s'%self.currenturi))
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ") 
                  
        except:
            log.exception(self.log_msg('data tag not found'))        
        
        if not page['title'] and not page['data']: 
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False  
                                      
        
        # author info
        try:
            page['et_author_name'] = stripHtml(self.soup.find('div','pL10 pR10 pT10').\
                                        find('a').renderContents())
        except:
            log.exception(self.log_msg('author_name not found %s'%self.currenturi)) 
        try:
            page['et_author_posts_count'] = int(self.soup.find('div','pT10').\
                                            find('b',text = re.compile('Reviews Written:')).\
                                            next.__str__().replace('<',''))
        except:
            log.exception(self.log_msg('author post count not found %s'%self.currenturi))   
        
        try:
            page['ef_rating_overall'] = float(self.soup.find('td',text = re.compile('Overall Rating:')).\
                                    findNext('img')['alt'].split('/')[0])        
        except:
            log.exception(self.log_msg('author rating not found'))
        try:
            page['ef_rating_on_time_delivery'] = float(self.soup.\
                                            find('td',text = re.compile('On-Time Delivery:')).\
                                            findNext('img')['alt'].split('/')[0])        
        except:
            log.exception(self.log_msg('author rating not found')) 
        try:
            page['ef_rating_customer_service'] = float(self.soup.\
                                                find('td',text = re.compile('Customer Service:')).\
                                                findNext('img')['alt'].split('/')[0])        
        except:
            log.exception(self.log_msg('author rating not found'))
        try:
            page['ef_rating_ease_of_ordering'] = float(self.soup.\
                                                find('td',text = re.compile('Ease of Ordering:')).\
                                                findNext('img')['alt'].split('/')[0])        
        except:
            log.exception(self.log_msg('author rating not found')) 
        try:
            page['ef_rating_selection'] = float(self.soup.find('td',text = re.compile('Selection:')).\
                                    findNext('img')['alt'].split('/')[0])        
        except:
            log.exception(self.log_msg('author rating not found'))                
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
            
                                 
                     
           
          
        
    