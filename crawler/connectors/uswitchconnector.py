import re
from urllib2 import urlparse
import logging
from datetime import datetime,timedelta
from cgi import parse_qsl
from urllib import urlencode
from BeautifulSoup import BeautifulSoup
from urllib2 import urlopen
from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('UswitchConnector')
class UswitchConnector(BaseConnector):
    @logit(log , 'fetch')
    def fetch(self):
        ''' sample url: 
        http://www.uswitch.com/credit-cards/bank-of-ireland-uk/
        '''
        

        try:
            baseuri = 'http://www.uswitch.com/credit-cards'
            headers = {}
            headers = {'Accept-encoding':''}          
            self.soup = BeautifulSoup(urlopen(self.currenturi).read())
            f=open('test2.html', 'w')
            f.write(self.soup.prettify())
            f.close()
            self.task_elements_dict = {
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
            self.__setParentPage()
            try:
                review_link = baseuri + self.soup.\
                                find('a',id=re.compile('\s*_customerReviewsShowMore'))['href'].split('.')[-1]          
                currenturi = review_link
                self.soup = BeautifulSoup(urlopen(currenturi).read())
                
                self.__getData()
            except:
                log.exception(self.log_msg('review link not found'))    
            
        except:
            log.exception(self.log_msg('no fetched')) 
    
    @logit(log,'__setParentPage')
    def __setParentPage(self):
        """
        """
        page = {}
        try: 
            page['title'] = page['data'] = stripHtml(self.soup.find('h1','underline').renderContents())
            log.info(page['title'])
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg(page['posted_date']))
        except:
            log.exception(self.log_msg('main page title  not found'))
            return False  
        unique_key = get_hash({'title': page['title']})
        if checkSessionInfo('review', self.session_info_out, unique_key,\
            self.task.instance_data.get('update')):
                    
            log.info(self.log_msg('Session info returns True for uri %s'\
                                                                           %self.currenturi))
            return False
        try:
            result=updateSessionInfo('review', self.session_info_out, unique_key, \
                    get_hash( page ),'review', self.task.instance_data.get('update'))
            if not result['updated']:
                log.exception(self.log_msg('Update session info returns False'))
                return True
            page['parent_path'] =[]
            page['path'] = [unique_key]
            page['path'].append(unique_key)
            page['uri'] = self.currenturi
            page['entity'] = 'review'
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page.update(self.task_elements_dict)
            self.pages.append(page)
            log.info(page)
            log.info(self.log_msg('Post Added'))
            return True        
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False  
          
              
    @logit(log , '__getData')    
    def __getData(self):
        
        try:
            data_info = self.soup.findAll('div','us-panel generic reviewsummary')
            log.info(len(data_info))
            for each in data_info[1:]:
                category = stripHtml(each.find('div','inner-header').renderContents()).split('(')[0].strip()
                log.info(category)
                reviews = each.findAll('tr')
                #try:
                for review in reviews:
                    page = {}
                    try:
                        page['et_category'] = category
                        page['data'] =page['title'] = stripHtml(review.renderContents())
                        page['uri'] = self.currenturi
                    except:
                        log.exception(self.log_msg('data and title not found'))
                        page['data'] = page['title'] =  '' 
                    if page['title'] == 'No data found to display':
                        log.info(self.log_msg("no review avaliable"))
                        return False            
                    
                    try:
                        page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
       
                    except:
                        log.exception(self.log_msg('Posted date not found'))
                
                    unique_key = get_hash({'title': page['title']})
                    if checkSessionInfo('review', self.session_info_out, unique_key,\
                         self.task.instance_data.get('update')):
                    
                        log.info(self.log_msg('Session info returns True for uri %s'\
                                                                           %self.currenturi))
                        continue
                    try:
                        result=updateSessionInfo('review', self.session_info_out, unique_key, \
                                get_hash( page ),'review', self.task.instance_data.get('update'))
                        if not result['updated']:
                            log.exception(self.log_msg('Update session info returns False'))
                            return True
                        page['parent_path'] =[]
                        page['path'] = [unique_key]
                        page['path'].append(unique_key)
                        #page['uri'] = unique_key
                        page['entity'] = 'review'
                        page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
                        page.update(self.task_elements_dict)
                        self.pages.append(page)
                        log.info(page)
                        log.info(self.log_msg('Post Added'))
                        
                    except:
                        log.exception(self.log_msg('Error while adding session info'))
                    

                #except:
                 #   log.exception(self.log_msg('review not found'))     
        except:
            log.exception(self.log_msg('no data found'))                                                                                                                                                                                      
        
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
          
         