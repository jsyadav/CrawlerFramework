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

log = logging.getLogger('UsptoConnector')
class UsptoConnector(BaseConnector):
    @logit(log , 'fetch')
    def fetch(self):
        try:
            baseuri= 'http://patft.uspto.gov'
            if '&Query=' in self.currenturi:
                self.__setSoupForCurrentUri()
                while True:
                    try:
                        self.__iterateLinks()
                        nextpagelink = self.soup.find('img', alt = "[NEXT_LIST]").findParent('a')['href']
                        self.currenturi = baseuri + nextpagelink 
                        self.__setSoupForCurrentUri()
                    except:
                        log.info(self.log_msg('Fetched all content, no more page naviagation'))
                        break
            else:
                self.__addLink()          
        except:
            log.info(self.log_msg('Exception  in Fetch'))
        return True 
        
    
    @logit(log, '__iterateLinks')
    def __iterateLinks(self):
        baseuri= 'http://patft.uspto.gov'
        try:
            links = [baseuri + each.find('a')['href'] for each in self.soup.find('td',text = re.compile('PAT. NO.')).findParent('table').findAll('tr')[1:]]
            if not links:
                log.info(self.log_msg('No links found'))
                return False
            log.debug(self.log_msg('Total No of links found is %d'%len(links))) 
            for link in links[:]:
                temp_task = self.task.clone()
                temp_task.instance_data['uri'] = link
                self.linksOut.append(temp_task)
            log.exception(self.log_msg('LINKSOUT: ' + str(len(self.linksOut))))
            #self.linksOut = [] 
        except:
            log.exception(self.log_msg('can not add the links'))
        return True
    
    @logit(log, '__addLink')
    def __addLink(self):
        task_elements_dict = {
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
        try:
            if checkSessionInfo('review', self.session_info_out, self.currenturi, \
                            self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info returns True for uri %s'\
                                                               %self.currenturi))
                return False
            self.__setSoupForCurrentUri()    
            page = self.__getData()
            if not page:
                return True
            result = updateSessionInfo('review', self.session_info_out, 
                self.currenturi,get_hash( page ),'post', self.task.instance_data.get('update'))
            if result['updated']:
                page['path'] = [ self.currenturi]
                page['parent_path'] = []
                page['uri']= self.currenturi 
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                page['entity'] = 'post'
                page.update(task_elements_dict)
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
            page['title'] = stripHtml(self.soup.find('font', {'size':'+1'}).renderContents())
        except:
            log.exception(self.log_msg('Title not found'))
            page['title'] = ''
           
        try:  
            page['data'] = stripHtml(self.soup.find('p').renderContents())
        except:
            log.exception(self.log_msg('data not found'))
            page['data'] = ''
        
        metainfotags = metainfotags = self.soup.findAll('table',{'width':'100%'})    
        cols = metainfotags[1].findAll('td')
        try: 
            date_str = stripHtml(cols[3].renderContents())  
            page['posted_date'] =  datetime.strftime(datetime.\
                        strptime(date_str,'%B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")  
            
        except:
            log.exception(self.log_msg('posted date not found'))
            
        try:  
            page['et_patent_abstract'] = stripHtml(self.soup.find('p').renderContents())
        except:
            log.exception(self.log_msg('abstract not found'))
        
        patno = re.compile('\d+')   
        try:  
            page['et_patent_no'] = patno.findall(self.soup.find('title').contents[0])[0]  
        except:
            log.exception(self.log_msg('patent no not found'))
        try:  
            page['et_patent_url'] = self.currenturi
        except:
            log.exception(self.log_msg('url not found'))    
        
        try:
            date_str = stripHtml(cols[3].renderContents())  
            page['et_patent_date'] = datetime.strftime(datetime.\
                        strptime(date_str,'%B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ") 
        except:
            log.exception(self.log_msg(' date not found'))
            
        try:  
            inventors = stripHtml(self.soup.find('td',text = re.compile('Inventors:')).\
                                    findNext('td').renderContents())
                                  
            page['et_patent_inventor_first_name'] = [each.split(';')[-1]for each in re.sub('\(.*?\)','',inventors).split(',')]
            page['et_patent_inventor_last_name'] = [each.split(';')[-2]for each in re.sub('\(.*?\)','',inventors).split(',')]                       
        
        except:
            log.exception(self.log_msg('inventor not found'))
            
        try:  
            page['et_patent_assignee'] = stripHtml(self.soup.find('td',text = re.compile('Assignee:')).\
                                    findNext('td').renderContents())
        except:
            log.exception(self.log_msg('assignee not found'))
            
        try:  
            page['et_patent_application_no'] = stripHtml(self.soup.find('td',text = re.compile('Appl. No.:')).\
                                            findNext('td').renderContents()) 
        except:
            log.exception(self.log_msg('application not found'))
            
        try: 
            date_str = stripHtml(self.soup.find('td',text = re.compile('Filed:')).\
                        findNext('td').renderContents()) 
            page['et_patent_filed_date'] = datetime.strftime(datetime.strptime(date_str,'%B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ") 
        except:
            log.exception(self.log_msg('filed not found'))                                                      
        try:
            page['et_patent_current_u.s._class'] = stripHtml(self.soup.find('td',text = re.compile('Current U.S. Class:'))\
                                                .findNext('td').renderContents())
        except:
            log.exception(self.log_msg('US class not found'))
            
        try:
            page['et_patent_current_international_class'] = stripHtml(self.soup.find('td',text = re.compile('Current International Class:'))\
                                                .findNext('td').renderContents())
        except:
            log.exception(self.log_msg('Current International Class not found'))
            
##        try:
##            page['et_field_of_search'] = stripHtml(self.soup.find('td',text = re.compile('Field of Search:'))\
##                                            .findNext('td').renderContents())
##        except:
##            log.exception(self.log_msg('Field of Search not found'))
            
        try:
            page['et_patent_primary_examiner'] = stripHtml(self.soup.\
                                            find('i',text = re.compile('Primary Examiner:')).\
                                            next.__str__())
        except:
            log.exception(self.log_msg('not found'))
            
        try:
            page['et_patent_assistance_examiner'] = stripHtml(self.soup.\
                                            find('i',text = re.compile('Assistant Examiner:')).\
                                            next.__str__())
        except:
            log.exception(self.log_msg('not found'))
         
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
            raise Exception('Page content not fetched for the url %s'%self.currenturi)
        self._setCurrentPage()                             