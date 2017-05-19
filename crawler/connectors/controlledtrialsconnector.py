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

log = logging.getLogger('ControlledTrialsConnector')
class ControlledTrialsConnector(BaseConnector):
    @logit(log , 'fetch')
    def fetch(self):
        '''The starting url is http://www.controlled-trials.com/mrct/search/51/50/1/desc//7065/
        '''
        self.__baseuri = 'http://www.controlled-trials.com'
        self.__setSoupForCurrentUri()
        #temp_count = 0
        if '/search/' in self.currenturi:
            while True:
                try:
                    task_urls = [ self.__baseuri + x.a['href'] for x in self.soup.findAll('td', id='WhiteText') if x.find('a', href=re.compile('/mrct/'))]
                    for each_url in  task_urls:
                        temp_task = self.task.clone()
                        temp_task.instance_data['uri'] = each_url
                        self.linksOut.append(temp_task)
                    log.info(self.log_msg('Total # of Tasks added is %d'%len(self.linksOut)))
                    next_page_tag = self.soup.find('a', title='Next Page of Results',href=True)
                    if not next_page_tag:
                        break
                    self.currenturi = self.__baseuri + next_page_tag['href']
                    self.__setSoupForCurrentUri()
##                    temp_count += 1
##                    if temp_count>=5:
##                        break
                except:
                    log.exception(self.log_msg('cannot find the taks urls'))
                    break
            #self.linksOut = []
            return True
        else:
            return self.__addPosts()
        
            
        
    def __addPosts(self):
        '''It will add the post
        '''
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
        page = {}
        try:
            entity_names= {'Source of record':'et_trial_source_of_record',\
                            'ISRCTN':'et_mainrecord_nctid','ClinicalTrials.gov identifier':'et_mainrecord_nctid',\
                            'Local reference number(s)':\
                            'et_trial_local_ref_number','Serial number at source':\
                            'et_mainrecord_organization_protocol_id_number',\
                            'Protocol/serial number':'et_mainrecord_organization_protocol_id_number',\
                            'Study ID numbers':'et_mainrecord_organization_protocol_id_number',\
                            'Public title':'et_mainrecord_brief_title','Title of trial/grant title':'et_mainrecord_brief_title',\
                            'Scientific title':'et_mainrecord_official_title','Official title':'et_mainrecord_official_title',\
                            'Current status of trial':'et_trial_trial_status',\
                            'Trial status':'et_trial_trial_status','Status of trial':'et_trial_trial_status',\
                            'Acronym':'et_trial_acronym',\
                            'Disease/condition/study domain':'et_mainrecord_condition_summary',\
                            'Condition(s)':'et_mainrecord_condition_summary','Disease or condition':'et_mainrecord_condition_summary',\
                            'Study hypothesis':'et_trial_study_hypothesis','Topic/hypothesis/clinical objectives':'et_trial_study_hypothesis',\
                            'Design/methodology':'et_mainrecord_study_design','Study design':'et_mainrecord_study_design',\
                            'Study type and design':'et_mainrecord_study_design','Research ethics review':'et_mainrecord_research_ethics_review',\
                            'Ethics approval':'et_mainrecord_research_ethics_review','Ethics committee approval':'et_mainrecord_research_ethics_review',\
                            'Eligibility criteria - inclusion':'et_mainrecord_patient_inclusion','Participants - inclusion criteria':'et_mainrecord_patient_inclusion',\
                            'Eligibility criteria - exclusion':'et_mainrecord_patient_exclusion','Participants - exclusion criteria':'et_mainrecord_patient_exclusion',\
                            'Countries of recruitment':'et_trial_recruitment_countries','Countries of trial':'et_trial_recruitment_countries',\
                            'Sample size':'et_trial_sample_size','Target recruitment number':'et_trial_sample_size','Target number of participants':'et_trial_sample_size',\
                            'Study start':'et_mainrecord_start_date','Trial start date':'et_mainrecord_start_date','Start date':'et_mainrecord_start_date','Anticipated start date':'et_mainrecord_start_date',\
                            'Trial end date':'et_mainrecord_end_date','Anticipated end date':'et_mainrecord_end_date',\
                            'Intervention(s)':'et_mainrecord_intervention','Groups/interventions':'et_mainrecord_intervention',\
                            'Methodology/interventions':'et_mainrecord_intervention','Interventions':'et_mainrecord_intervention',\
                            'Primary outcome measure(s)':'et_mainrecord_primary_outcome','Primary outcome(s)':'et_mainrecord_primary_outcome',\
                            'Primary outcome':'et_mainrecord_primary_outcome',\
                            'Secondary outcome':'et_mainrecord_secondary_outcome','Secondary outcome(s)':'et_mainrecord_secondary_outcome',\
                            'Secondary outcome measure(s)':'et_mainrecord_secondary_outcome',\
                            'Sources of funding':'et_mainrecord_source','Sponsor name':'et_mainrecord_lead_sponsor',\
                            'Sponsor':'et_mainrecord_lead_sponsor',\
                            'Contact name':'et_mainrecord_contact_name',\
                            'Sponsor details':'et_trail_sponsor_detail','Contact name(s)':'et_mainrecord_contact_name',\
                            'Contact details':'et_trail_contact_details','Last edited':'et_mainrecord_last_update',\
                            'Last updated':'et_mainrecord_last_update','Minimum age':'et_mainrecord_min_age',\
                            'Maximum age':'et_mainrecord_max_age','Gender':'et_mainrecord_gender',\
                            'Sponsors and collaborators':'et_mainrecord_collaborators','Sponsors and collaborators':'et_mainrecord_lead_sponsor',\
                            'Link to the ClinicalTrials.gov record':'et_trial_link','Phase':'et_mainrecord_current_phase',\
                            'Information provided by':'et_trial_information_provider',\
                            'Locations':'et_trial_locations','Record first received':'et_trial_record_first_received',\
                            'Purpose':'et_mainrecord_study_purpose','Download date':'et_trail_download_date',\
                            'Last refreshed in the mRCT':'et_trial_last_refreshed','Further information':'et_trial_further_info',\
                            'Wellcome Trust reference':'et_trial_trust_reference','Date updated in mRCT':'et_trial_last_refreshed',\
                            "Other organisations' reference numbers":'et_trial_other_organization_reference_no',\
                            'Multicentre?':'et_trial_ismulticenter','Main country location':'et_trial_recruitment_countries',\
                            'Paediatric?':'et_trial_ispaediatric','Adult?':'et_trial_isadult','Date ISRCTN assigned':'et_mainrecord_trial_registration_date',\
                            'Senior?':'et_trial_issenior','Patient information material':'et_trial_patient_information',\
                            'End of recruitment date':'et_trial_recruitment_end_date','End of follow-up date':'et_trial_follow-up_end_date',\
                            'Findings':'et_trial_findings','Publications':'et_trial_publications','Eligibility criteria':''}
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            data_info =[each.findParent('tr')for each in self.soup.findAll('td',id ='FieldName')]
            #log.info(len(data_info))
            for each in data_info:
                #log.info(each)
                field_name = stripHtml(each.find('td',id='FieldName').renderContents())
                try:
                    if  not field_name in entity_names.keys():
                        field_name = 'et_' + field_name.lower().replace(' ','_').replace('/','_')
                        page[field_name] = stripHtml(each.find('td',id='FieldValue').renderContents())
                        log.info(page[field_name])
                        continue
                    if field_name == 'Eligibility criteria':
                        try:
                            eli_criteria =  stripHtml(each.find('td',id='FieldValue').renderContents()).split('Exclusion Criteria:')
                            page['et_mainrecord_patient_inclusion'] = eli_criteria[-2]
                            page['et_mainrecord_patient_exclusion'] = eli_criteria[-1]
                        except: 
                            log.exception(self.log_msg('Eligibility criteria not found'))     
                    elif 'start' in field_name:
                        date_str = stripHtml(each.find('td',id='FieldValue').renderContents())
                        try:
                            if date_str.startswith('A')or date_str.startswith('M') or date_str.startswith('J')\
                             or date_str.startswith('D')or date_str.startswith('S') or date_str.startswith('N')\
                            or date_str.startswith('F')or date_str.startswith('O'):
                            
                                date_str = '01 ' + date_str
                                page[entity_names[field_name]] = datetime.strftime(datetime.strptime(date_str,'%d %B %Y'),"%Y-%m-%dT%H:%M:%SZ")          
                                log.info(page[entity_names[field_name]])
                            else:
                                page[entity_names[field_name]] = datetime.strftime(datetime.strptime(date_str,'%d/%m/%Y'),"%Y-%m-%dT%H:%M:%SZ")    
                                #log.info(page[entity_names[field_name]])
                                #log.info(date_str)
                        except: 
                            log.exception(self.log_msg('start date not found'))
                    elif 'end date' in field_name:
                        date_str = stripHtml(each.find('td',id='FieldValue').renderContents())
                        try: 
                            page[entity_names[field_name]] = datetime.strftime(datetime.strptime(date_str,'%d/%m/%Y'),"%Y-%m-%dT%H:%M:%SZ")       
                        except: 
                            log.exception(self.log_msg('End date not found'))
                    elif field_name == 'Last updated'or field_name == 'Last edited':
                        try:
                            date_str = stripHtml(each.find('td',id='FieldValue').renderContents())
                            try:
                                if ':' in date_str:
                                    page[entity_names[field_name]] = datetime.strftime(datetime.strptime(date_str,'%d/%m/%Y %H:%M:%S'),"%Y-%m-%dT%H:%M:%SZ")
                                elif 'on' in date_str:
                                    date_str = date_str.split('on')[-1].replace('.','')
                                    page[entity_names[field_name]] = datetime.strftime(datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",date_str).strip(),"%d %B %Y"),"%Y-%m-%dT%H:%M:%SZ")
                                elif ',' in date_str :
                                    date_str = date_str.replace(',','')                        
                                    page[entity_names[field_name]] = datetime.strftime(datetime.strptime(date_str,'%B %d %Y'),"%Y-%m-%dT%H:%M:%SZ")       
                                else:
                                    page[entity_names[field_name]] = date_str
                                #log.info(date_str)    
                            except:
                                #log.info(date_str)  
                                log.exception(self.log_msg('last update date not found'))
                                #log.info(date_str)  
                        except:
                            #log.info(field_name)
                            log.exception(self.log_msg('error in last update'))
                                            
                    elif field_name =='Date ISRCTN assigned':
                        try:
                            date_str = stripHtml(each.find('td',id='FieldValue').renderContents())
                            if ':' in date_str:
                                page[entity_names[field_name]] = datetime.strftime(datetime.strptime(date_str,'%d/%m/%Y %H:%M:%S'),"%Y-%m-%dT%H:%M:%SZ")
                            else:
                                page[entity_names[field_name]] = datetime.strftime(datetime.strptime(date_str,'%d/%m/%Y'),"%Y-%m-%dT%H:%M:%SZ")        
                        except: 
                            log.exception(self.log_msg('registration date not'))
                    else:
                        page[entity_names[field_name]] = stripHtml(each.find('td',id='FieldValue').renderContents())
    ##                page['posted_date']=datetime.strftime(datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",posted_date_str).strip(),"%B %d %Y"),\
    ##                                                                  "%Y-%m-%dT%H:%M:%SZ")    
                    
                    if  'hypothesis' in field_name:
                        try:
                            page['data'] = stripHtml(each.find('td',id='FieldValue').renderContents())
                        except:
                            log.exception(self.log_msg('data not found'))
                            page['data'] = 'N/A'
                    else:        
                        page['data'] = 'N/A'    
                        
##                    if 'Public title' or 'grant title' in field_name:
##                        try:
##                            page['title'] = stripHtml(each.find('td',id='FieldValue').renderContents())
##                            log.info(page['title'])
                    try:
                        page['title'] = page['et_mainrecord_brief_title']
                        #log.info(page['title'])  
                    except:
                        log.exception(self.log_msg('title not found'))
                        page['title'] = ''
                        log.info(page['title'])
                except:
                    log.exception(self.log_msg('field name not found'))            
                #log.info(page)                                
        except:
            log.exception(self.log_msg('can not find data')) 
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
         
       
