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


log = logging.getLogger('ProjectReporterConnector')
class ProjectReporterConnector(BaseConnector):
    @logit(log , 'fetch')
    def fetch(self):
        self.__baseuri = 'http://projectreporter.nih.gov/'
        if 'reporter_searchresults' in self.currenturi:
            self.__createTasksForLinks()
        else:
            self.__addLink()        
        return True
    
    @logit(log, '__createTasksForThreads')
    def __createTasksForLinks(self):
        
        """
        This will create Tasks for the threads found on the given url
        The # of Tasks are limited by Config Variable
        """
        try:            
            self.__setSoupForCurrentUri()
            self.__iterateLinks()
            main_page_soup = copy.copy(self.soup)
            headers = {}
            headers['Referer'] = self.currenturi
            headers['Host'] = 'projectreporter.nih.gov'
            temp_count = 0
            while True:
                try:
##                    try:
##                        log.info(self.log_msg(stripHtml(self.soup.find('a', title='Sort By Project Number').renderContents())))
##                    except:
##                        log.info(self.log_msg('Not  found'))
                    if not self.__iterateLinks():
                        log.info(self.log_msg('No more links found'))
                        break
                        
                    data = dict([(x['id'],x['value']) for x in main_page_soup.findAll('input',id=True,value=True)\
                            if x['id'].startswith('sr_')])
                    params = main_page_soup.find('img', title='Next').findParent('a')['href'].split('(')[-1][:-2].split(',')
                    data['sr_startrow'] = params[0]
                    data['sr_pagenum'] = params[1]
                    self.currenturi = self.task.instance_data['uri']
                    self.__setSoupForCurrentUri(headers=headers, data=data)
                    main_page_soup = copy.copy(self.soup)
##                    temp_count += 1
##                    if temp_count>=200:
##                        break
                except:
                    log.exception(self.log_msg('not found')) 
                    break
            log.exception(self.log_msg('LINKSOUT: ' + str(len(self.linksOut))))
            #self.linksOut = [] 
                
    ##                f=open('test.html', 'w')
    ##                f.write(self.soup.prettify())
    ##                f.close()
            #return True
        except:
            log.exception(self.log_msg('Exception while creating tasks for the url %s'\
                                                         %self.currenturi))    
            #return False
                
    @logit(log, '__iteratelinks')
    def __iterateLinks(self):
        try:
            linkinfo =  self.soup.find('a', title='Sort By Project Number').findParent('table').findAll('tr', recursive=False)
             
            if not linkinfo:
                log.info(self.log_msg('No Links found'))
                return False
            #data_links = [each.findAll('td',recursive = 'False')[6].find('a')['href'] for each in linkinfo[1:]]                            
            for link in linkinfo[:]:
                data_link = self.__baseuri + link.findAll('td')[6].find('a')['href']
                temp_task = self.task.clone()
                temp_task.instance_data['uri'] = data_link
                self.linksOut.append(temp_task)
        except:
            log.exception(self.log_msg('can not add the links'))
        return True
                
    
    @logit(log, '__addLink')
    def __addLink(self):
##        self.__baseuri = 'http://projectreporter.nih.gov/'
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
        try:
            unique_key = self.currenturi
            self.__setSoupForCurrentUri()
            #self.currenturi = self.__baseuri + data_link
            prev_uri = self.currenturi #= self.__baseuri + data_link
            prev_soup = copy.copy(self.soup)
            log.info(self.log_msg('current uri: %s'%self.currenturi))
            if checkSessionInfo('review', self.session_info_out, unique_key, \
                            self.task.instance_data.get('update'),parent_list\
                                          = [self.currenturi]):
                log.info(self.log_msg('Session info returns True for uri %s'\
                                                               %unique_key))
                return False
            #self.__setSoupForCurrentUri()    
            page = self.__getData()
            self.currenturi = self.currenturi.replace('description','details')
            #self.currenturi = self.__baseuri + data_link.replace('description','details')
            self.__setSoupForCurrentUri()
            
            page['et_mainrecord_pi_email'] = stripHtml(self.soup.find('b',text =re.compile('Email:')).findNext('a').renderContents())
            page['et_mainrecord_pi_title'] = stripHtml(self.soup.find('b',text =re.compile('Email:')).findParent('span').renderContents()).split('Title:')[-1].strip()
            page['et_grant_other_pi'] = stripHtml(self.soup.find('span',text =re.compile('Other PI Information:')).findNext('tr').find('span').renderContents())    
            page['et_grant_congressional_district'] = stripHtml(self.soup.find('span',text =re.compile('Congressional District:')).findParent('tr').findNext('tr').find('span').renderContents()).split('District:')[-1].strip()
            page['et_grant_state_code'] =stripHtml(self.soup.find('span',text =re.compile('Congressional District:')).findParent('tr').findNext('tr').find('span').renderContents()).split('District:')[-2].replace('State Code:','').strip()
            page['et_grant_rfa/pa'] =stripHtml(self.soup.find('span',text =re.compile('RFA/PA:')).findParent('td').findNext('td').find('span').renderContents())
            page['et_grant_duns_number'] =stripHtml(self.soup.find('span',text =re.compile('DUNS Number:')).findParent('td').findNext('td').find('span').renderContents())
            page['et_grant_cfda_code'] = stripHtml(self.soup.find('span',text =re.compile('CFDA Code:')).findParent('td').findNext('td').find('span').renderContents())
            #page['et_institution_or_affiliation_country'] = stripHtml(self.soup.find('span',text ='Organization:').findParent('tr').findNext('tr').renderContents()).split(',')[-4] 
            page['et_institution_or_affiliation_country'] = 'united state'
            page['et_institution_or_affiliation_address_line_1'] = stripHtml(self.soup.find('span',text ='Organization:').findParent('tr').findNext('tr').renderContents()).split(',')[-4]
            page['et_institution_or_affiliation_address_line_2'] = stripHtml(self.soup.find('span',text ='Organization:').findParent('tr').findNext('tr').renderContents()).split(',')[-3]
            page['et_institution_or_affiliation_city'] = stripHtml(self.soup.find('span',text ='Organization:').findParent('tr').findNext('tr').renderContents()).split(',')[-2]
            page['et_institution_or_affiliation_state'] = stripHtml(self.soup.find('span',text ='Organization:').findParent('tr').findNext('tr').renderContents()).split(',')[-1].strip().split(' ')[-2]
            page['et_institution_or_affiliation_zip'] = stripHtml(self.soup.find('span',text ='Organization:').findParent('tr').findNext('tr').renderContents()).split(',')[-1].strip().split(' ')[-1]
            page['et_grant_study_section'] = stripHtml(self.soup.find('span',text =re.compile('Study Section:')).findParent('td').findNext('td').find('span').renderContents())
            page['et_grant_irg'] = stripHtml(self.soup.find('span',text =re.compile('Study Section:')).findParent('td').findNext('td').find('span').renderContents()).split('(')[-1].split(')')[-2]
            date_str = stripHtml(self.soup.find('span',text =re.compile('Project Start Date:')).findParent('td').findNext('td').find('span').renderContents())
            page['edate_mainrecord_project_start_date'] = datetime.strftime(datetime.strptime(date_str,'%d-%b-%Y'),"%Y-%m-%dT%H:%M:%SZ") 
            date_str = stripHtml(self.soup.find('span',text =re.compile('Project End Date:')).findParent('td').findNext('td').find('span').renderContents()) 
            page['edate_mainrecord_project_end_date'] = datetime.strftime(datetime.strptime(date_str,'%d-%b-%Y'),"%Y-%m-%dT%H:%M:%SZ") 
            date_str = stripHtml(self.soup.find('span',text =re.compile('Budget Start Date:')).findParent('td').findNext('td').find('span').renderContents())
            page['edate_grant_budget_start_date'] = datetime.strftime(datetime.strptime(date_str,'%d-%b-%Y'),"%Y-%m-%dT%H:%M:%SZ") 
            date_str = stripHtml(self.soup.find('span',text =re.compile('Budget End Date:')).findParent('td').findNext('td').find('span').renderContents())
            page['edate_grant_budget_end_date'] = datetime.strftime(datetime.strptime(date_str,'%d-%b-%Y'),"%Y-%m-%dT%H:%M:%SZ") 
            date_str = stripHtml(self.soup.find('span',text =re.compile('Award Notice Date:')).findParent('td').renderContents()).split()[-1]
            page['et_grant_award_notice_date'] = datetime.strftime(datetime.strptime(date_str,'%d-%b-%Y'),"%Y-%m-%dT%H:%M:%SZ") 
            #page['et_grant_award_notice_date'] = stripHtml(self.soup.find('span',text =re.compile('Award Notice Date:')).findParent('td').renderContents()).split()[-1]
            page['et_grant_project_funding_ic'] = stripHtml(self.soup.find('td',text =re.compile('Funding IC')).findParent('tr').findNext('tr').findAll('td',recursive = False)[1].renderContents())
            page['et_mainrecord_icd'] = stripHtml(self.soup.find('span',text =re.compile('Administering Institutes or Centers:')).findParent('b').findNext('table').renderContents())
            page['et_mainrecord_fiscal_year'] = stripHtml(self.soup.find('td',text =re.compile('Funding IC')).findParent('tr').findNext('tr').findAll('td',recursive = False)[0].renderContents())
            page['et_grant_project_total_cost'] = stripHtml(self.soup.find('td',text =re.compile('Funding IC')).findParent('tr').findNext('tr').findAll('td',recursive = False)[2].renderContents())
            page['et_grant_url']  = self.currenturi
            try:
                page['uri'] = self.currenturi
            except: 
                log.exception(self.log_msg('uri not found'))
            self.currenturi = prev_uri
            self.soup = prev_soup
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
        return True 
        

            
    @logit(log, '__getData')
    def __getData(self):
        
        page={}
        try:  
            page['title'] = stripHtml(self.soup.find('span',text= re.compile('\s*Title:')).\
                                findNext('span').renderContents())

        except:
            log.exception(self.log_msg('Title not found'))
            page['title'] = ''  
        try:
            page['data'] = stripHtml(self.soup.find('span',text= re.compile('\s*Abstract Text:')).\
                                            findNext('span').renderContents()) 
        except:
            log.exception(self.log_msg('data not found'))  
            page['data'] =''    
            
        try:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('posted date not found'))         
    
        try:
            page['et_mainrecord_grant_number'] = stripHtml(self.soup.find('span',text= re.compile('\s*Project Number')).\
                                        findNext('span').renderContents())
     
        except:
            log.exception(self.log_msg('Project no not found')) 
        try:  
            page['et_mainrecord_project_title'] = stripHtml(self.soup.find('span',text= re.compile('\s*Title:')).\
                                findNext('span').renderContents())

        except:
            log.exception(self.log_msg('Project Title not found'))    
        try:
            page['et_grant_subproject_no'] = stripHtml(self.soup.find('span',text = re.compile('Sub-Project ID:')).findNext('span').renderContents())
        except:
            log.exception(self.log_msg('sub projrct not found'))
        try:
            page['et_pi_first_name'] = stripHtml(self.soup.\
                                        find('span',text= re.compile('\s*Contact Principal Investigator:')).\
                                        findNext('span').renderContents()).split(',')[-2]
            page['et_pi_last_name'] = stripHtml(self.soup.\
                                        find('span',text= re.compile('\s*Contact Principal Investigator:')).\
                                        findNext('span').renderContents()).split(',')[-1]                            
        except:
            log.exception(self.log_msg('investigator not found'))
        
        
        try:
            page['et_mainrecord_sponsor'] = stripHtml(self.soup.find('span',text= re.compile('\s*Awardee Organization:')).\
                                            findNext('span').renderContents())
        except:
            log.exception(self.log_msg('award organization not found')) 
            
        
        try:
            page['et_mainrecord_abstract'] = stripHtml(self.soup.find('span',text= re.compile('\s*Abstract Text:')).\
                                            findNext('span').renderContents()) 
        except:
            log.exception(self.log_msg('abstract not found'))  
            page['data'] =''   
        try:
            page['et_grant_nih_spending_category'] =  stripHtml(self.soup.find('span',text= re.compile('\s*NIH Spending Category:')).\
                                            findNext('span').renderContents())  
        except:
            log.exception(self.log_msg('category not found'))
        
        try:
            page['et_mainrecord_thesaurus_terms'] = stripHtml(self.soup.find('span',text= re.compile('\s*Project Terms:')).findNext('span').renderContents()).split(';')
        except:
            log.exception(self.log_msg('project terms not found'))   
            
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
     