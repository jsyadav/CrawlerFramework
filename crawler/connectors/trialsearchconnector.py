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

log = logging.getLogger('TrialSearchConnector')
class TrialSearchConnector(BaseConnector):
    @logit(log , 'fetch')
    def fetch(self):
        baseuri = 'http://apps.who.int/trialsearch/crawl/'
        if 'crawl0' in self.currenturi:
            self.__setSoupForCurrentUri()
            linkinfo =  self.soup.find('table',id='DataList1').findAll('tr',recursive = False)
            links = [baseuri + each.find('td',recursive = 'False').find('a')['href'] for each in linkinfo]   
            if not links:
                log.info(self.log_msg('No Links found'))
                return False
            for link in links[:]: 
                self.currenturi = link
                self.__setSoupForCurrentUri()
                for eachlink in [ x['href'] for x in self.soup.find('table',id= 'DataList1').findAll('a')][:]:
                    temp_task = self.task.clone()
                    log.info(eachlink)
                    temp_task.instance_data['uri'] = eachlink
                    self.linksOut.append(temp_task)
##                    self.__createTasksForLinks()     
                log.exception(self.log_msg('LINKSOUT: ' + str(len(self.linksOut))))
                #self.linksOut = [] 
        else:
            self.__addInfo()        
        return True        
                
##            except:
##            log.exception(self.log_msg('can not find the data %s'))
##            return False
##        return True
##            self.__createTasksForLinks()
        
##            log.exception(self.log_msg('LINKSOUT: ' + str(len(self.linksOut))))
##            self.linksOut = [] 
                
    @logit(log, '__addInfo')
    def __addInfo(self):
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
            #self.currenturi = nextlink
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
                unique_key,get_hash( page ),'forum', self.task.instance_data.get('update')\
                ,parent_list=[self.currenturi])
            if result['updated']:
                page['path'] = [ self.currenturi]
                page['parent_path'] = []
                page['uri']= self.currenturi 
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                page['entity'] = 'post'
                #log.info(page)
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
        
        page = {}
        
        try:
            page['title'] = stripHtml(self.soup.find('td',text =re.compile('Public title')).findNext('td').renderContents())
            
        except:
            log.exception(self.log_msg('can not find title'))
        
        try:
            page['data'] = stripHtml(self.soup.find('td',text =re.compile('Public title')).findNext('td').renderContents())
            
        except:
            log.exception(self.log_msg('can not find data'))   
         
        try:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")  
        except:
            log.exception(self.log_msg('can not find posted date'))      
            
        try:
            page['et_trial_register'] = stripHtml(self.soup.find('td',text =re.compile('Register:')).findNext('td').renderContents())
        except:
            log.exception(self.log_msg('can not find register'))
            
        try:
            date_str = stripHtml(self.soup.find('td',text =re.compile('Last refreshed on:')).findNext('td').renderContents())
            page['edate_mainrecord_last_update'] = datetime.strftime(datetime.strptime(date_str,'%d %B %Y'),"%Y-%m-%dT%H:%M:%SZ")
            
        except:
            log.exception(self.log_msg('can not find last update')) 
            
        try:
            page['et_mainrecord_nctid'] = stripHtml(self.soup.find('td',text =re.compile('Main ID:')).findNext('td').renderContents())
            
        except:
            log.exception(self.log_msg('can not find'))
            
        try:
            date_str = stripHtml(self.soup.find('td',text =re.compile('Date of registration:')).findNext('td').renderContents())
            if '-' in date_str:
                split_char = '-'
            elif '/' in date_str:
                split_char = '/'
            if len(date_str.split(split_char)[-1])==4:
                char_order = ['%d', '%m', '%Y']
            else:
                char_order = ['%Y', '%m', '%d']
            page['edate_mainrecord_trial_registration_date'] = datetime.strftime(datetime.strptime(date_str, split_char.join(char_order)), "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('can not find registration date'))
            
            
        try:
            page['et_mainrecord_lead_sponsor'] = stripHtml(self.soup.find('td',text =re.compile('Primary sponsor:')).findNext('td').renderContents())
            
        except:
            log.exception(self.log_msg('can not find Primary sponsor'))
        
        try:
            page['et_mainrecord_brief_title'] = stripHtml(self.soup.find('td',text =re.compile('Public title')).findNext('td').renderContents())
            
        except:
            log.exception(self.log_msg('can not find title'))    
        
        try:
            page['et_mainrecord_official_title'] = stripHtml(self.soup.find('td',text =re.compile('Scientific title:')).findNext('td').renderContents())
            
        except:
            log.exception(self.log_msg('can not find scientific title'))
        
        try:
            date_str = stripHtml(self.soup.find('td',text =re.compile('Date of first enrolment:')).findNext('td').renderContents())
            if '-' in date_str:
                split_char = '-'
            elif '/' in date_str:
                split_char = '/'
            if len(date_str.split(split_char)[-1])==4:
                char_order = ['%d', '%m', '%Y']
            else:
                char_order = ['%Y', '%m', '%d']
            page['edate_mainrecord_enrolment'] = datetime.strftime(datetime.strptime(date_str, split_char.join(char_order)), "%Y-%m-%dT%H:%M:%SZ")
##        try:
####            date_str = stripHtml(self.soup.find('td',text =re.compile('Date of first enrolment:')).findNext('td').renderContents())
####            page['et_mainrecord_enrolment']  = datetime.strftime(datetime.strptime(date_str,'%d/%m/%Y'),"%Y-%m-%dT%H:%M:%SZ")
##            page['et_mainrecord_enrolment']  = stripHtml(self.soup.find('td',text =re.compile('Date of first enrolment:')).findNext('td').renderContents())
        except:
            log.exception(self.log_msg('can not find Date of first enrolment'))
        
        try:
            page['et_trial_target_sample_size'] = stripHtml(self.soup.find('td',text =re.compile('Target sample size:')).findNext('td').renderContents())
            
        except:
            log.exception(self.log_msg('can not find Target sample size'))
        try:
            page['et_mainrecord_recruitment_status'] = stripHtml(self.soup.find('td',text =re.compile('Recruitment status:')).findNext('td').renderContents())
            
        except:
            log.exception(self.log_msg('can not find Recruitment status'))
        
        try:
            page['et_mainrecord_url'] = stripHtml(self.soup.find('td',text =re.compile('URL:')).findNext('td').renderContents())
            
        except:
            log.exception(self.log_msg('can not find URL'))
        
        try:
            page['et_mainrecord_study_type'] = stripHtml(self.soup.find('td',text =re.compile('Study type:')).findNext('td').renderContents())
            
        except:
            log.exception(self.log_msg('can not find Study type'))
            
        
        try:
            page['et_mainrecord_study_design'] = stripHtml(self.soup.find('td',text =re.compile('Study design:')).findNext('td').renderContents()) 
            
        except:
            log.exception(self.log_msg('can not find Study design'))
            
       
        try:
            page['et_trial_countries_of_recruitment'] = stripHtml(self.soup.find('span',text =re.compile('Countries of recruitment')).findParent('tr').findNext('tr').find('span').renderContents())
            
            
        except:
            log.exception(self.log_msg('can not find countries for recruitment'))
            
        try:
            contactinfo = self.soup.find('table',id = re.compile('DataList4')).findAll('tr',recursive = False)[1]
            datainfo = [x for x in contactinfo.findAll('td',recursive = False) if stripHtml(x.renderContents())]
            if datainfo:
                tab =datainfo[0].find('table').findAll('tr') 
                try:
                    page['et_mainrecord_responsible_contact_person_first_name'] = stripHtml(tab[0].find('span',id =re.compile('\w*FirstnameLabel')).renderContents())
                    page['et_mainrecord_responsible_contact_person_last_name'] = stripHtml(tab[0].find('span',id =re.compile('\w*LastnameLabel')).renderContents())
                except:
                    log.exception(self.log_msg('can no find contact person name1'))
                try:
                    page['et_trial_responsible_contact_person_address'] = stripHtml(tab[1].find('td',text =re.compile('Address:')).findNext('td').renderContents())
            
                except:
                    log.exception(self.log_msg('can not find contact address'))
                        
                try:
                    page['et_trial_responsible_contact_person_telephone_no'] = stripHtml(tab[2].find('span',id =re.compile('\w*TelephoneLabel')).renderContents())
            
                except:
                    log.exception(self.log_msg('can not find contact Telephone no1'))
            
                try:
                    page['et_trial_responsible_contact_person_email'] = stripHtml(tab[3].find('span',id =re.compile('\w*EmailLabel')).renderContents())
                    
                except:
                    log.exception(self.log_msg('can not find contact email1'))
                    
                try:
                    page['et_trial_responsible_contact_person_affiliation'] = stripHtml(tab[4].find('span',id =re.compile('\w*AffiliationLabel')).renderContents())
                    log.info(page['et_trial_responsible_contact_person_affiliation'])
                except:
                    log.exception(self.log_msg('can not find contact affiliation1'))
                    
                    
            if len(datainfo)>=2:
                tab =datainfo[1].find('table').findAll('tr') 
                try:
                    page['et_mainrecord_research_contact_person_first_name'] = stripHtml(tab[0].find('span',id =re.compile('\w*FirstnameLabel')).renderContents())
                    #log.info(page['et_mainrecord_research_contact_person_first_name'])
                    page['et_mainrecord_research_contact_person_last_name'] = stripHtml(tab[0].find('span',id =re.compile('\w*LastnameLabel')).renderContents())
                except:
                    log.exception(self.log_msg('can no find contact person name1'))
                try:
                    page['et_trial_research_contact_person_address'] = stripHtml(tab[1].find('span',id =re.compile('\w*AddressLabel')).renderContents())
            
                except:
                    log.exception(self.log_msg('can not find contact address'))
                
                try:
                    page['et_trial_research_contact_person_telephone_no'] = stripHtml(tab[2].find('span',id =re.compile('\w*TelephoneLabel')).renderContents())
            
                except:
                    log.exception(self.log_msg('can not find contact Telephone no2'))
            
                try:
                    page['et_trial_research_contact_person_email'] = stripHtml(tab[3].find('span',id =re.compile('\w*EmailLabel')).renderContents())
                    
                except:
                    log.exception(self.log_msg('can not find contact email2'))
                    
                try:
                    page['et_trial_research_contact_person_affiliation'] = stripHtml(tab[4].find('span',id =re.compile('\w*AffiliationLabel')).renderContents())
                    #log.info(page['et_contact_affiliation2'])
                except:
                    log.exception(self.log_msg('can not find contact affiliation2'))
            
            
        except:
            log.exception(self.log_msg('can no find contact info'))
        
        try:
            page['et_mainrecord_patient_inclusion'] = stripHtml(self.soup.find('span',id =re.compile('\w*Inclusion_criteriaLabel')).renderContents()) 
            
        except:
            log.exception(self.log_msg('can not find inclusion criteria not found'))
          
        try:
            page['et_mainrecord_patient_exclusion'] = stripHtml(self.soup.find('span',id =re.compile('\w*Exclusion_criteriaLabel')).renderContents()) 
            
        except:
            log.exception(self.log_msg('can not find  exclusion criteria not found'))  
        
        try:
            page['et_mainrecord_min_age'] = stripHtml(self.soup.find('span',id =re.compile('\w*Label8')).renderContents()) 
        except:
            log.exception(self.log_msg('can not find min age'))
        
        try:
            page['et_mainrecord_max_age'] = stripHtml(self.soup.find('span',id =re.compile('\w*Label11')).renderContents()) 
            
        except:
            log.exception(self.log_msg('can not find max age'))
        
##        try:
##            page['et_mainrecord_gender'] = stripHtml(self.soup.find('span',id =re.compile('\w*Label8')).renderContents()) 
##            
##        except:
##            log.exception(self.log_msg('can not find gender'))
        try:
            page['et_mainrecord_condition_summary'] = stripHtml(self.soup.find('span',id =re.compile('\w*Condition_FreeTextLabel')).renderContents())
            
        except:
            log.exception(self.log_msg('can not find health condition'))
        try:
            intervention = self.soup.find('table', id=re.compile('DataList10')).findAll('tr')[1:]

            page['et_mainrecord_interventions'] = [stripHtml(each.find('span').renderContents()) for each in intervention]
        except:
            log.exception(self.log_msg('can not find interventions'))
        
        try:
            prioutcomes = self.soup.find('table', id=re.compile('DataList12')).findAll('tr')[1:]
            page['et_mainrecord_primary_outcomes'] = [stripHtml(each.find('span').renderContents()) for each in prioutcomes]
        except:
            log.exception(self.log_msg('can not find primary outcomes'))
        
        try:
            secoutcomes = self.soup.find('table', id=re.compile('DataList14')).findAll('tr')[1:] 
            page['et_mainrecord_secondary_outcomes'] = [stripHtml(each.find('span').renderContents()) for each in secoutcomes]
        except:
            log.exception(self.log_msg('can not find sec outcomes'))
        
        try:
            secid =  self.soup.find('table', id=re.compile('DataList16')).findAll('tr')[1:] 
            page['et_trial_secondary_id'] = [stripHtml(each.find('span').renderContents()) for each in secid]
            log.info(page['et_trial_secondary_id'])
        
        except:
            log.exception(self.log_msg('can not find sec id'))
        
        try:
            src_support = self.soup.find('table', id=re.compile('DataList18')).findAll('tr')[1:]
            page['et_trial_source_of_monetary_support'] = [stripHtml(each.find('span').renderContents()) for each in src_support]
        except:
            log.exception(self.log_msg('can not find source for support'))       
        
        try:
            secsponsor =  self.soup.find('table', id=re.compile('DataList20')).findAll('tr')[1:] 
            page['et_mainrecord_secondary_sponsor'] = [stripHtml(each.find('span').renderContents()) for each in secsponsor]
            
        except:
            log.exception(self.log_msg('can not find sec sponser'))
        
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
        
        