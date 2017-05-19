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

log = logging.getLogger('DealTimeConnector')
class DealTimeConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        sample url: http://www1.dealtime.com/Apple-IPhone-3G-8-Gb/reviews
        '''
        try:
            self.genre = "Review"
            self.baseuri = 'http://www.dealtime.com/'
            #params = dict(type='movies',id=1000,page=100)
            #self.baseuri%params
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
            #self.__iterateLinks()
            #count = 0
            while self.__iterateLinks():
                self.currenturi = self.baseuri + main_page_soup.find('span','selected').\
                                    findNext('a')['href'].replace(' ','')
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
            page_data = {}
            left_tag = self.soup.findAll('div','reviewLeftCol')
            right_tag = self.soup.findAll('div','reviewRightColTL')
            zipped = [ BeautifulSoup('\n'.join([x.__str__() for x in list(each)]))\
                        for each in  zip(left_tag,right_tag)]
            if not zipped:
                log.info(self.log_msg('No posts found %s'%self.currenturi))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(zipped)))
            for zipp in zipped:#use some range for few data  
                full_review = zipp.find('a','readMore2')
                if full_review:
                    try:
                        page_data['ef_rating_on_time_delivery'] = float(zipp.\
                            find('b',text = re.compile('^On-Time Delivery:')).\
                                        findNext('img')['alt'].split('/')[0])        
                    except:
                        log.exception(self.log_msg('author rating not found')) 
                    try:
                        page_data['ef_rating_customer_service'] = float(zipp.\
                            find('b',text = re.compile('Customer Service:')).\
                                    findNext('img')['alt'].split('/')[0])        
                    except:
                        log.exception(self.log_msg('author rating not found'))   
                    full_review_link = full_review['href'].replace(' ','')
                    if not self.__addLink(full_review_link, page_data):
                        return False
                else:
                    if not self.__addPost(zipp):
                        return False    
            return True    
        except:
            log.exception(self.log_msg('can not  find the data %s'%self.currenturi))
            return False  
    
    @logit(log, '__addPost')    
    def __addPost(self, zipp): 
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        page = {}
        try:
            page['data'] = stripHtml(zipp.find('div','briefReviewWrapper').renderContents())
            page['title'] = ''
        except:
            log.exception(self.log_msg('title data not found'))
            return
        try:
            date_str = stripHtml(zipp.find('div','reviewLeftCol').find('span','reviewDate').\
                        renderContents()).strip()
            page['posted_date']= datetime.strftime(datetime.\
                                    strptime(date_str,'%B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")  
        except:
            log.exception(self.log_msg('posted_date nt found %s'%self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['et_author_name'] = stripHtml(zipp.find('div','reviewLeftCol').find('a').\
                                        renderContents())
        except:
            log.exception(self.log_msg('author_name not found %s'%self.currenturi))
        try:
            page['ef_data_rating'] = float(zipp.find('b',text = re.compile('^Author')).\
                                    findNext('img')['title'].split('/')[0])        
        except:
            log.exception(self.log_msg('author rating not found'))  
        try:
            page['ef_rating_on_time_delivery'] = float(zipp.find('b',text = re.compile('^On-Time Delivery:')).\
                                    findNext('img')['alt'].split('/')[0])        
        except:
            log.exception(self.log_msg('author rating not found')) 
        try:
            page['ef_rating_customer_service'] = float(zipp.find('b',text = re.compile('Customer Service:')).\
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
    @logit(log, '__addPost')    
    def __addLink(self, link, page_data): 
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
            page.update(page_data)
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
            title_tag = self.soup.find('div','reviewRightColTM').find('h1')
            if title_tag:
                title_tag.extract()
            page['title'] = stripHtml(title_tag.renderContents())
        except:
            log.exception(self.log_msg('title not found for the url %s'%self.currenturi))
            page['title'] = '' 
        try:
            rating_tag = self.soup.find('div','reviewRightColTM').find('table')
            if rating_tag:
                rating_tag.extract()
            page['ef_data_rating'] = float(rating_tag.find('img')['title'].split('/')[0])
        except:
            log.exception(self.log_msg('rating  not found %s'%self.currenturi))    
        try:
            unwanted_tag = self.soup.find('div','reviewRightColTM').\
                            find('b',text = re.compile('Full Review'))
            log.info(unwanted_tag)
            if unwanted_tag:
                unwanted_tag.extract()                 
        except:
            log.exception(self.log_msg('unw tag not found')) 
        try:
            pros_tag = self.soup.find('div','reviewRightColTM').findAll('p')    
            log.info(pros_tag)
            if pros_tag:
                [each.extract()for each in pros_tag]
            for each in pros_tag:    
                try:    
                    page['et_data_pros'] = each.find('b',text =re.compile('Pros:')).next.__str__()
                except:
                    log.exception(self.log_msg('data pros not found'))
                try:    
                    page['et_data_cons'] = each.find('b',text =re.compile('Cons:')).next.__str__()
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
            page['data'] = stripHtml(self.soup.find('div','reviewRightColTM').renderContents())
        except:
            log.exception(self.log_msg('title not found for the url %s'%self.currenturi))
            page['data'] = ''
        
        if not page['title'] and not page['data']: 
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False  
        try:
            date_str = stripHtml(self.soup.find('div','reviewLeftCol').find('span','reviewDate').\
                        renderContents()).strip()
            page['posted_date']= datetime.strftime(datetime.strptime(date_str,'%b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")  
        except:
            log.exception(self.log_msg('posted_date nt found %s'%self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")                                 
        
        # author info 
        try:
            page['et_author_name'] = stripHtml(self.soup.find('div','reviewLeftCol').\
                                        find('a').renderContents())
        except:
            log.exception(self.log_msg('author_name not found %s'%self.currenturi))
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
            
                                 
                     
           
          
        
    