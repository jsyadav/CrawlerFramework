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


log = logging.getLogger('GatesFoundationConnector')
class GatesFoundationConnector(BaseConnector):
    @logit(log , 'fetch')
    def fetch(self):
        self.__baseuri = 'http://www.gatesfoundation.org'
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
        headers = {}
        headers['Referer'] = self.currenturi
        headers['Host'] = 'www.gatesfoundation.org'
        while True:
            try:
                try:
                    log.info(self.log_msg(stripHtml(self.soup.find('div','grantresult').renderContents())))
                except:
                    log.info(self.log_msg('No grat found'))
                
                if not self.__iterateLinks():
                    log.info(self.log_msg('No more links found'))
                    break
                next_page = self.soup.find('a', 'nexton')
                if not next_page:
                    log.info(self.log_msg('Processed all the grants, return True'))
                    break
                data = dict([(x['id'],x['value']) for x in self.soup.find('form', attrs={'name':'aspnetForm'}).findAll('input',id=True,value=True) if x['id'].startswith('__')])
                data['__EVENTTARGET'] = 'ctl00$PlaceHolderMain$SearchControl$bottomPager$nextButton'
                self.__setSoupForCurrentUri(headers=headers, data=data)
            except:
                log.exception(self.log_msg('Exception in fetch for the url %s'\
                                                                %self.currenturi))
        return True
    @logit(log, '__iteratelinks')
    def __iterateLinks(self):
        try:
            
            links = list(set([each.find('a')['href'] for each in self.soup.findAll('div','grantresult')]))
            if not links:
                log.info(self.log_msg('No links found'))
                return False
            log.debug(self.log_msg('Total No of links found is %d'%len(links))) 
            for link in links:
                self.__addLink(link)
        except:
            log.exception(self.log_msg('can not find the data'))
            return False
        return True
    
             
    @logit(log, '__addLink')
    def __addLink(self,link):
        self.__baseuri = 'http://www.gatesfoundation.org'
        try:
            unique_key = link
            self.currenturi = self.__baseuri + unique_key
            
            log.info(self.log_msg('current uri: %s'%self.currenturi))
            if checkSessionInfo('review', self.session_info_out, unique_key, \
                            self.task.instance_data.get('update'),parent_list\
                                          = [self.currenturi]):
                log.info(self.log_msg('Session info returns True for uri %s'\
                                                               %unique_key))
                return False
            self.__setSoupForCurrentUri()    
            page = self.__getData()
            #self.pages.append(page)
            #log.info(self.log_msg('Page added'))
            if not page:
                return True
            result = updateSessionInfo('review', self.session_info_out, 
                unique_key,get_hash( page ),'forum', self.task.instance_data.get\
                    ('update'),parent_list=[self.currenturi])
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
        return True          
            
    @logit(log, '__getData')
    def __getData(self):
        
        page={}
        try:
            #page['uri'] = self.currenturi
            page['title'] = stripHtml(self.soup.find('div', 'granttitle').renderContents())
        except:
            log.exception(self.log_msg('Title not found'))
            page['title'] = ''
        details = self.soup.findAll('div','grantdetail grantdetailgap')
        try:
            date_str = stripHtml(details[0].renderContents())
            if date_str.startswith('Date:'):
                date_str = '01 ' + date_str.replace('Date:',"").strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,\
                                              '%d %B %Y'),"%Y-%m-%dT%H:%M:%SZ")
                                              
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-\
                                                            %m-%dT%H:%M:%SZ")
        try:
            purpose = stripHtml(details[1].renderContents())
            if purpose.startswith('Purpose:'):
                purpose = purpose.replace('Purpose:',"").strip()
            page['data'] = purpose   
            
        except:
            log.info(self.log_msg('data not found'))
            
        try:
            amount = stripHtml(details[2].renderContents())
            if amount.startswith('Amount:'):
                amount = amount.replace('Amount:',"").strip()
            page['et_amount'] = amount
        except:
            log.exception(self.log_msg('amount count not found')) 
        try:
            term = stripHtml(details[3].renderContents())
            if term.startswith('Term:'):
                term = term.replace('Term:',"").strip()
            page['et_term']  = term
        except:
            log.exception(self.log_msg('terms count not found'))  
        try:
            topic = stripHtml(details[4].renderContents())
            if topic.startswith('Topic:'):
                topic = topic.replace('Topic:',"").strip()
            page['et_topic'] = topic    
        except:
            log.exception(self.log_msg('topic count not found'))  
        try:  
            region_served= stripHtml(details[5].renderContents())
            if region_served.startswith('Region Served:'):
                region_served =region_served .replace('Region Served:',"").strip()
            page['et_region_served'] = region_served    
        except:
            log.exception(self.log_msg('region count not found'))
        
        try:  
            program = stripHtml(details[6].renderContents())
            if program.startswith('Program:'):
                page['et_program'] =program.replace('Program:',"").strip()
        except:
            log.exception(self.log_msg('region count not found'))    
        try:  
            grantee_location = stripHtml(details[7].renderContents())
            if grantee_location.startswith('Grantee Location:'):
                page['et_grantee_location'] =grantee_location.replace('Grantee Location:',"").strip()
            
        except:
            log.exception(self.log_msg('location count not found'))
                
        try:  
            grantee_website = stripHtml(self.soup.find('div','grantdetail').renderContents())
            if grantee_website.startswith('Grantee Web site:'):
                page['et_grantee_website'] =grantee_website.replace('Grantee Web site:',"").strip()
            
        except:
            log.exception(self.log_msg('website count not found'))  
               
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