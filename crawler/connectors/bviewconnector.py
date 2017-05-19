import re # unused import
import copy # unused 
import logging

from urllib2 import urlparse
from datetime import datetime# remove time delta
from cgi import parse_qsl # unused
from urllib import urlencode #un  used

from tgimport import tg #unused
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('BViewConnector')
class BViewConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self): # Doc String
        '''This is a fetch method which  fetches the data 
        '''
        
            #baseuri = 'http://www.bview.co.uk/listing/0593245/Amazoncouk-in-SL1'
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
        self.__getParentPage()
        try:
            review = self.soup.find('ul','reviews')
            review_list = review.findAll('li','hreview ')
            for each in review_list:
                try:
                    self.__addLink(each)
                except:
                    log.exception(self.log_msg('Cannot add the review ')) 
                     
        except:
            log.exception(self.log_msg('can not find the data %s'))
            return True
    
    def __addLink(self,each):
        try:
            unique_key = get_hash({'title':stripHtml(each.find('h4','summary').renderContents()),\
                        'data': stripHtml(each.find('p','description').renderContents())})
            #unique_key = get_hash({'title' : page['title'],'data' : page['data']})
            if checkSessionInfo('review', self.session_info_out, unique_key, \
                            self.task.instance_data.get('update'),parent_list\
                                          = [self.currenturi]):
                log.info(self.log_msg('Session info returns True for uri %s'\
                                                               %unique_key))
                return False
            #self.__setSoupForCurrentUri()    
            page = self.__getData(each)
            #page.update(page_data)
            #self.pages.append(page)
            #log.info(self.log_msg('Page added'))
            if not page:
                return True
            result = updateSessionInfo('review', self.session_info_out, 
                unique_key,get_hash( page ),'forum', self.task.instance_data.get('update')\
                ,parent_list=[self.currenturi])
            if result['updated']:
                page['path'] = [ self.currenturi]
                page['parent_path'] = []
                page['uri']= self.currenturi 
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                page['entity'] = 'post'
                log.info(page)
                page.update(self.__task_elements_dict)
                self.pages.append(page)
                log.info(self.log_msg('Page added'))
            else:
                log.info(self.log_msg('Update session info returns False for \
                                                url %s'%self.currenturi))
        except:
            log.exception(self.log_msg('Cannot add the post for the uri %s'\
                                                            %self.currenturi))
            return False 
            
    @logit(log,'__getParentPage')
    def __getParentPage(self):
        page = {}
        try:
            unique_key = self.currenturi
            if checkSessionInfo('review', self.session_info_out, unique_key, \
                            self.task.instance_data.get('update'),parent_list\
                                          = [self.currenturi]):
                log.info(self.log_msg('Session info returns True for uri %s'\
                                                               %unique_key))
                return False
            try:
                page['data'] = page['title'] = stripHtml(self.soup.find('h1','fn org').renderContents())
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ") # Try blok
            except:
                log.exception(self.log_msg('can not find title')) 
                return 
##            try:
##                page['data'] = stripHtml(self.soup.find('h3',text ='About').findParent('div').findNext('div','content').renderContents())    
##            except:
##                log.exception(self.log_msg('can not find title')) 
##                page['data'] = ''    

            try:
                page['et_parent_organization'] = stripHtml(self.soup.find('h1','fn org').renderContents())
            except:
                log.exception(self.log_msg('can not find parent org'))    
            try:
                page['et_parent_org_category'] = stripHtml(self.soup.find('a','category').renderContents())
            except:
                log.exception(self.log_msg('can not find parent org category'))     
            try:
                page['et_parent_org_review']= stripHtml(self.soup.find('a','reviewCount').renderContents())
            except:
                log.exception(self.log_msg('can not find parent org reviewocunt'))  
    ##        try:
    ##            page['et_parent_org_rating']=
    ##        except:
    ##            log.exception(self.log_msg('can not find parent org category'))      
            try:
                page['et_parent_org_street_address'] = stripHtml(self.soup.find('span','street-address').renderContents())
            except:
                log.exception(self.log_msg('can not find parent org street address'))
            try:
                page['et_parent_org_locality'] = stripHtml(self.soup.find('span','locality').renderContents())
            except:
                log.exception(self.log_msg('can not find parent org locality'))
            try:
                page['et_parent_org_region'] = stripHtml(self.soup.find('span','region').renderContents()) #Check 
            except:
                log.exception(self.log_msg('can not find parent org street region'))
            try:
                page['et_parent_org_postal-code'] = stripHtml(self.soup.find('span','postal-code').renderContents())
            except:
                log.exception(self.log_msg('can not find parent org postal-code'))            
                         
            try:
                page['et_parent_org_telphone_no'] = stripHtml(self.soup.find('strong','work').renderContents())     
            except:
                log.exception(self.log_msg('can not find parent org phone_no'))
            #self.pages.append(page)    
            if not page:
                    return True
            result = updateSessionInfo('review', self.session_info_out, 
                unique_key,get_hash( page ),'forum', self.task.instance_data.get('update')\
                ,parent_list=[self.currenturi])
            if result['updated']:
                page['path'] = [ self.currenturi]
                page['parent_path'] = []
                page['uri']= self.currenturi 
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                page['entity'] = 'post'
                log.info(page)
                page.update(self.__task_elements_dict)
                self.pages.append(page)
                log.info(self.log_msg('Page added'))
            else:
                log.info(self.log_msg('Update session info returns False for \
                                                url %s'%self.currenturi))    
        except:
            log.exception(self.log_msg('Cannot add the post for the uri %s'\
                                                            %self.currenturi))
            return False 

        #self.pages.append(page)      

                           
    @logit(log, '__getData')
    def __getData(self,each):
        page = {}
        try:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
##            date_str = stripHtml(self.soup.find('span','smallBody dtreviewed').renderContents())
##            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('posted date  not found'))
            #page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['data'] = stripHtml(each.find('p','description').renderContents())
        except:
            log.exception(self.log_msg('data not found'))
            page['data'] = ' ' 
        try:
            page['title'] = stripHtml(each.find('h4','summary').renderContents())
        except:
            log.exception(self.log_msg('title not found')) 
            page['title'] = ''    
                 
        try:
            page['et_review_summary'] = stripHtml(each.find('h4','summary').renderContents())
        except:
            log.exception(self.log_msg('summary not found')) 
        try:
            date_str = stripHtml(each.find('span','smallBody dtreviewed').renderContents())
            page['et_reviewed_date'] = datetime.strftime(datetime.strptime(date_str,'%b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('reviewed date  not found'))     
            
        try:
            page['et_review_description'] = stripHtml(each.find('p','description').renderContents())
        except:
            log.exception(self.log_msg('review description not found'))     
           
        try:
            page['et_author_name'] = stripHtml(each.find('a','fn').renderContents())
        except:
            log.exception(self.log_msg('author name not found'))     
            
        try:
            page['et_author_location'] = stripHtml(each.find('span','smallBody locality').renderContents())
        except:
            log.exception(self.log_msg('authoe location not found'))     
            
        try:
            page['et_review_quality'] = stripHtml(each.find('span',text = 'Quality').findNext('div').renderContents())
        except:
            log.exception(self.log_msg('review quality not found'))     
            
        try:
            page['et_review_reliability'] = stripHtml(each.find('span',text = 'Reliability').findNext('div').renderContents())
        except:
            log.exception(self.log_msg('review reliability not found'))                              
           
        try:
            page['et_review_rating'] = stripHtml(each.find('span',text = 'Value').findNext('div').renderContents())
        except:
            log.exception(self.log_msg('review value not found')) 
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
