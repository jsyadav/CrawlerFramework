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

log = logging.getLogger('UminConnector')
class UminConnector(BaseConnector):
    @logit(log , 'fetch')
    def fetch(self):
        self.__baseuri = 'http://upload.umin.ac.jp/cgi-open-bin/ctr'
        if 'function=search' in self.currenturi:
            self.__createTasksForLinks()
        else:
            self.__addLink()        
        return False
        
    @logit(log, '__createTasksForThreads')
    def __createTasksForLinks(self):
        
        """
        This will create Tasks for the threads found on the given url
        The # of Tasks are limited by Config Variable
        """
        try:            
            self.__setSoupForCurrentUri()
            self.__iterateLinks()
            parent_links = list(set([x.parent['href'][1:] for x in self.soup.find('font', \
                    text=re.compile('Studies searched')).findParent('p')\
                    .findAll('a', text=re.compile('^\s*\d+\s*$'),href=True)\
                     if x.parent.get('href')]))
            #print len(parent_links)
            #print len(list(set(parent_links)))
            #self.linksOut = []
            for each_parent_link in parent_links:
                self.currenturi = self.__baseuri + each_parent_link
                self.__setSoupForCurrentUri()      
                self.__iterateLinks()  
            #log.info(self.log_msg('LINKSOUT: ' + str(len(self.linksOut))))
            return True  
        except:
            log.exception(self.log_msg('Exception while creating tasks for the url %s'\
                                                         %self.currenturi)) 
            return False                
    
    @logit(log, '__iteratelinks')
    def __iterateLinks(self):
        try:
            #linkinfo = self.soup.findAll('tr')[4:]
            #links = [each.findAll('td')[-2].find('a')['href'][1:] for each in linkinfo if each]
            links = [ self.__baseuri + each.find('a',text = 'Detail').parent['href'][1:] for each in self.soup.findAll('tr')[4:] if each]
            if not links:
                log.info(self.log_msg('No links found'))
                return False
            log.debug(self.log_msg('Total No of links found is %d'%len(links))) 
            for link in links:
                temp_task = self.task.clone()
                temp_task.instance_data['uri'] = link
                self.linksOut.append(temp_task)
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
            page['title'] = stripHtml(self.soup.find('h1').renderContents()) 
        except:
            log.exception(self.log_msg('Title not found'))
            page['title'] = ''   
        
        try:
            rect_st = self.soup.find('td',text= 'Recruitment status').findParent('tr')
            page['et_project_recruitment_status'] = stripHtml(rect_st.renderContents()).splitlines()[-1] 
        except:
            log.info(self.log_msg("recruitment status not found")) 
            
        try:
            info =self.soup.find('td',text= 'Unique trial Number').findParent('tr')
            page['et_project_unique_number'] = [x.strip() for x in stripHtml(info.renderContents())\
                                    .splitlines() if x.strip()][-1]
        except: 
            log.info(self.log_msg(" unique number not found"))
        
        try:
            info = self.soup.find('td',text= 'Title of the study').findParent('tr')
            page['et_project_brief_title'] = [x.strip() for x in stripHtml(info.renderContents())\
                                    .splitlines() if x.strip()][-1] 
        except:
            log.info(self.log_msg("study title not found"))
         
        try:
            page['et_project_official_title'] = stripHtml(self.soup.find('td',text= 'Official scientific title of the study').\
                            findNext('td').renderContents())
        except:
            log.info(self.log_msg('data not found'))   
            
        try:
            info = self.soup.find('td',text= 'Date of formal registration').findParent('tr') 
            date_str = [x.strip() for x in stripHtml(info.renderContents()).splitlines()\
                          if x.strip()][-1]  
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%Y/%m/%d'),"%Y-%m-%dT%H:%M:%SZ") 
        except:
            log.exception(self.log_msg('post date not found %s'%self.currenturi)) 
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")  
        
        try:
            info = self.soup.find('td',text= 'Date and time of last update').findParent('tr')
            date_str = [x.strip() for x in stripHtml(info.renderContents()).splitlines()if x.strip()][-1]  
            page['et_project_last_update'] = datetime.strftime(datetime.strptime(date_str,'%Y/%m/%d %H:%M:%S'),"%Y-%m-%dT%H:%M:%SZ")                                            
                                                                                  
        except:
                          
            log.info(self.log_msg('last update not found %s'%self.currenturi))
            
        try:
            page['data'] = stripHtml(self.soup.find('td',text= 'Official scientific title of the study').\
                            findNext('td').renderContents())
        except:
            log.info(self.log_msg('data not found'))
            
        try:
            page['et_project_region'] = stripHtml(self.soup.find('td',text= 'Region').\
                                        findNext('td').renderContents()) 
        except:
            log.info(self.log_msg('region  not found'))
        try:
            page['et_project_condition'] = stripHtml(self.soup.find('b', text='Condition').\
                                    findParent('table').findAll('tr',recursive='False')[2].\
                                    find('td',text = 'Condition').findNext('td').renderContents())
        except:
            log.info(self.log_msg('condition not found'))
        try:
            info = self.soup.find('td',text= 'Classification by specialty').findNext('td')    
            page['et_project_specialty_classification'] = [x.strip() for x in stripHtml(info.renderContents()).\
                                    splitlines() if x.strip()]
        except:
            log.info(self.log_msg('specialty not found'))  

        try:
            page['et_project_malignancy_classification'] = stripHtml(self.soup.\
                                    find('td',text= 'Classification by malignancy').\
                                    findNext('td').renderContents())      
        except:
            log.info(self.log_msg('malignancy not found'))  

            
        try:
            page['et_project_genomic_information'] = stripHtml(self.soup.\
                                        find('td',text= 'Genomic information').\
                                        findNext('td').renderContents())
            
        except:
            log.info(self.log_msg('Genomic information not found'))
            
        try:
            page['et_project_narrative_objective1'] = stripHtml(self.soup.\
                                    find('td',text= 'Narrative objectives1').\
                                        findNext('td').renderContents())
        except:
            log.info(self.log_msg('Narrative objective1 not found')) 
           
        try:
            page['et_project_basic_objectives2'] = stripHtml(self.soup.\
                                            find('td',text= 'Basic objectives2').\
                                                findNext('td').renderContents())
        except:
            log.info(self.log_msg('objective2 not found'))
        
        try:
            page['et_project_basic_objectives_others'] = stripHtml(self.soup.\
                                find('td',text= 'Basic objectives -Others').\
                                findNext('td').renderContents())
        except:
            log.info(self.log_msg('objective3 not found'))  
               
        try:
            page['et_project_trial_characteristics1'] = stripHtml(self.soup.\
                                    find('td',text= 'Trial characteristics_1').\
                                        findNext('td').renderContents())
        except:
            log.info(self.log_msg('characteristics1 not found'))     

        try:
            page['et_project_trial_characteristics2'] = stripHtml(self.soup.\
                                        find('td',text='Trial characteristics_2').\
                                        findNext('td').renderContents())
        except:
            log.info(self.log_msg('characteristics2 not found'))
        
        try:
            page['et_project_outcomes_primary'] = stripHtml(self.soup.find('td',text='Primary outcomes').\
                                            findNext('td').renderContents())
        except:
            log.info(self.log_msg('primary outcomes not found'))
        
        try:
            info = self.soup.find('td',text='Key secondary outcomes').findNext('td')
            page['et_project_outcomes_secondary'] = [x.strip() for x in stripHtml(info.renderContents())\
                                                    .splitlines() if x.strip()]
        except:
            log.info(self.log_msg('key outcomes not found'))
        
        try:
            page['et_project_study_type'] = stripHtml(self.soup.find('td',text='Study type').\
                                    findNext('td').renderContents())
        except:
            log.info(self.log_msg('Study type not found'))
        
        try:
            page['et_project_basic_design'] = stripHtml(self.soup.find('td',text='Basic design').\
                                        findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Basic design not found'))
          
        try:
            page['et_project_randomization'] = stripHtml(self.soup.find('td',text='Randomization').\
                                        findNext('td').renderContents()) 	              
        except:
            log.info(self.log_msg('randomization  not found'))
          
        try:
            page['et_project_randomization_unit'] = stripHtml(self.soup.\
                                    find('td',text='Randomization unit').\
                                    findNext('td').renderContents())
        except:
            log.info(self.log_msg('Randomization unit not found'))

        try:
            page['et_project_blinding'] = stripHtml(self.soup.find('td',text='Blinding').\
                                                findNext('td').renderContents())
        except:
            log.info(self.log_msg('Blinding not found'))
 
        try:   
            page['et_project_control'] = stripHtml(self.soup.find('td',text='Control').\
                                                findNext('td').renderContents())
        except:
            log.info(self.log_msg('Control not found'))
        
        try:
            page['et_project_stratification'] =  stripHtml(self.soup.find('td',text='Stratification').\
                                        findNext('td').renderContents())
        except:
            log.info(self.log_msg('Stratification  not found'))
        try:
            page['et_project_dynamic_allocation'] = stripHtml(self.soup.\
                                            find('td',text='Dynamic allocation').\
                                            findNext('td').renderContents())
        except:
            log.info(self.log_msg('Dynamic allocation not found'))
        
        try:        
            page['et_project_institution_consideration'] = stripHtml(self.soup.\
                                    find('td',text='Institution consideration').\
                                    findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Institution consideration not found'))
        
        try:
            page['et_project_blocking'] = stripHtml(self.soup.find('td',text='Blocking').\
                                                findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Study type not found'))
        
        try:
            page['et_project_concealment'] = stripHtml(self.soup.find('td',text='Concealment').\
                                                findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Concealment not found'))
            
        try:
            page['et_project_no_of_arms'] = stripHtml(self.soup.find('td',text='No. of arms').\
                                                findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Study type not found'))
        
        try:
            page['et_project_intervention_purpose']= stripHtml(self.soup.\
                                        find('td',text='Purpose of intervention').\
                                            findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Purpose of intervention not found'))
            
        try:
            page['et_project_intervention_type'] = stripHtml(self.soup.\
                                        find('td',text='Type of intervention').\
                                        findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Type of intervention not found')) 
            
        try:
            page['et_project_interventions_control1'] = stripHtml(self.soup.\
                                    find('td',text='Interventions/Control_1').\
                                    findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Interventions/Control_1 not found')) 
            
        try:
            page['et_project_interventions_control2'] = stripHtml(self.soup.\
                                    find('td',text='Interventions/Control_2').\
                                    findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Interventions/Control_2 not found')) 
            
        try:
            page['et_project_interventions_control3'] = stripHtml(self.soup.\
                                        find('td',text='Interventions/Control_3').\
                                        findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Interventions/Control_3 not found'))  
         
        try:
            page['et_project_interventions_control4'] = stripHtml(self.soup.\
                                        find('td',text='Interventions/Control_4').\
                                        findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Interventions/Control_4 not found'))     
        
        try:
            page['et_project_interventions_control5'] = stripHtml(self.soup.\
                                    find('td',text='Interventions/Control_5').\
                                    findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Interventions/Control_5 not found'))  
            
        try:
            page['et_project_interventions_control6'] = stripHtml(self.soup.\
                                    find('td',text='Interventions/Control_6').\
                                    findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Interventions/Control_6 not found'))  
            
        try:
            page['et_project_interventions_control7'] = stripHtml(self.soup.\
                                    find('td',text='Interventions/Control_7').\
                                    findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Interventions/Control_7 not found'))  
            
        try:
            page['et_project_interventions_control8'] = stripHtml(self.soup.\
                                    find('td',text='Interventions/Control_8').\
                                                findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Interventions/Control_8 not found'))  
        
        try:
            page['et_project_interventions_control9'] = stripHtml(self.soup.\
                                        find('td',text='Interventions/Control_9').\
                                        findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Interventions/Control_9 not found'))  
            
        try:
            page['et_project_interventions_control10'] = stripHtml(self.soup.\
                                        find('td',text='Interventions/Control_10').\
                                        findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Interventions/Control_10 not found'))  
                                   
        try:
            page['et_project_min_age'] = stripHtml(self.soup.find('td',text='Age-lower limit').\
                                        findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Age-lower limit not found')) 
            
        try:
            page['et_project_max_age'] = stripHtml(self.soup.find('td',text='Age-upper limit').\
                                            findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Age-upper limit not found')) 
         
        try:
            page['et_project_gender'] = stripHtml(self.soup.find('td',text='Gender').\
                                    findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Gender not found')) 
            
        try:
            page['et_project_inclusion_criteria'] = stripHtml(self.soup.\
                                    find('td',text='Key inclusion criteria').\
                                    findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Key inclusion criteria not found')) 
            
        try:
            page['et_project_exclusion_criteria'] = stripHtml(self.soup.\
                                    find('td',text='Key exclusion criteria').\
                                    findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Key exclusion criteria not found')) 
            
        try:
            page['et_project_target_sample_size'] = stripHtml(self.soup.\
                                    find('td',text='Target sample size').\
                                    findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Target sample size not found')) 
          
        
            
        try:  
            research_info = self.soup.find('b', text='Research contact person').findParent('table')
            
            page['et_project_principal_investigator_name'] = stripHtml(research_info.\
                                                     findAll('tr',recursive='False')[2].\
                                                     find('td',text = 'Name of lead principal investigator').\
                                                     findNext('td').renderContents())
            page['et_project_research_organization'] =  stripHtml(research_info.\
                                                findAll('tr',recursive='False')[3].\
                                                find('td',text = 'Organization').\
                                                findNext('td').renderContents())

            page['et_project_research_division_name'] = stripHtml(research_info.\
                                                findAll('tr',recursive='False')[4].\
                                                find('td',text = 'Division name').\
                                                findNext('td').renderContents())
       
            page['et_project_research_contact_address'] = stripHtml(research_info.\
                                                  findAll('tr',recursive='False')[5].\
                                                  find('td',text ='Address').\
                                                 findNext('td').renderContents())
        except:    
            log.info(self.log_msg('research contact info  not found')) 
          
        try:
            public_contact = self.soup.find('b', text='Public contact ').findParent('table')
            
            page['et_project_public_contact_person'] = stripHtml(public_contact.\
                                                findAll('tr',recursive='False')[2].\
                                                find('td',text = 'Name of contact person').\
                                                findNext('td').renderContents())
            page['et_project_public_organization'] =    stripHtml(public_contact.\
                                                findAll('tr',recursive='False')[3].\
                                                find('td',text = 'Organization').\
                                                findNext('td').renderContents())

            page['et_project_public_division_name'] = stripHtml(public_contact.\
                                                findAll('tr',recursive='False')[4].\
                                                find('td',text = 'Division name').\
                                                findNext('td').renderContents())
                                                
            page['et_project_public_contact_address'] = stripHtml(public_contact.\
                                                  findAll('tr',recursive='False')[5].\
                                                  find('td',text ='Address').\
                                                 findNext('td').renderContents())                                    
       
            page['et_project_public_telephone'] = stripHtml(public_contact.\
                                                  findAll('tr',recursive='False')[6].\
                                                  find('td',text ='TEL').\
                                                 findNext('td').renderContents())  
        except: 
            log.info(self.log_msg('public contact info not found')) 
           
        try:
            page['et_project_homepage_url'] = stripHtml(self.soup.find('td',text='Homepage URL').\
                                        findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Homepage URL not found'))  
            
        try:
            page['et_project_email'] = stripHtml(self.soup.\
                        find('td',text='Email').findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Email not found'))  
        
        try:
            page['et_project_primary_sponsor'] = stripHtml(self.soup.\
                                    find('td',text='Name of primary sponsor').\
                                    findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Name of primary sponsor not found'))   
            
        try:
            page['et_project_funding_source'] = stripHtml(self.soup.\
                                    find('td',text='Source of funding').\
                                    findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Source of funding not found'))             
            
        try:
            page['et_project_organization_category'] = stripHtml(self.soup.\
                                find('td',text='Category of Org.').findNext('td').\
                                renderContents())
        except:    
            log.info(self.log_msg('Homepage URL not found')) 
                 
        try:
            page['et_project_funding_nation'] = stripHtml(self.soup.\
                            find('td',text='Nation of funding').findNext('td').\
                                renderContents())
        except:    
            log.info(self.log_msg('Nation of funding not found'))
            
        try:
            page['et_project_secondary_sponsor(s)'] = stripHtml(self.soup.\
                        find('td',text='Name of secondary sponsor(s)').\
                        findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Name of secondary sponsor(s) not found'))  
            
        try:
            page['et_project_secondary_funder(s)'] = stripHtml(self.soup.\
                            find('td',text=re.compile('Name of secondary.*')).\
                            findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Name of secondary funder(s) not found'))  
            
        try:
            page['et_project_research_ethics_review'] = stripHtml(self.soup.\
                            find('td',text='Research ethics review').findParent('table').\
                            findAll('tr',recursive='False')[2].\
                             find('td',text = 'Research ethics review').\
                            findNext('td').renderContents())

        except:    
            log.info(self.log_msg('Research ethics review not found'))  
            
        try:
            secid = [x for x in self.soup.findAll('b',text='Secondary IDs') \
                    if len(x.findParent('tr').findAll('td'))>1][0].\
                    findParent('tr').findAll('td')[-1]
            page['et_project_secondary_ids'] = stripHtml(secid.renderContents())
        except:    
            log.info(self.log_msg('secondary Ids not found'))  
            
        try:
            page['et_project_study_id1'] = stripHtml(self.soup.find('b',text='Study ID_1').\
                                findNext('td').renderContents())
            #log.info(page['et_study_id_1'])
        except:    
            log.info(self.log_msg('Study ID_1 not found'))  
            
        try:
            page['et_project_org_issuing_international_id1'] = stripHtml(self.soup.\
                                find('b',text='Org. issuing International ID_1').\
                                findNext('td').renderContents())
            #log.info(page['et_org_issuing_international_id1'])
        except:    
            log.exception(self.log_msg('Org issuing International ID_1 not found'))  
            
        try:
            page['et_project_study_id2'] = stripHtml(self.soup.find('b',text='Study ID_2').\
                                    findNext('td').renderContents())
        except:    
            log.exception(self.log_msg('Study ID_2 not found'))  
            
        try:
            page['et_project_org._issuing_international_id2'] = stripHtml(self.soup.\
                                find('b',text='Org. issuing International ID_2').\
                                findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Org. issuing International ID_2 not found'))  
            
        try:
            page['et_project_ind_to_mhlw'] = stripHtml(self.soup.find('b',text='IND to MHLW').\
                                    findNext('td').renderContents())
        except:    
            log.info(self.log_msg('IND to MHLW not found'))  
            
        try:
            page['et_project_institutions'] = stripHtml(self.soup.find('td',text='Institutions').\
                                findParent('table').findAll('tr',recursive='False')[2].\
                                find('td',text = 'Institutions').findNext('td').renderContents())

        except:  
            log.info(self.log_msg('Name of secondary sponsor(s) not found'))  
            
        try:
            date_str = stripHtml(self.soup.\
                                    find('td',text='Date of protocol fixation').\
                                    findNext('td').renderContents())
            page['et_project_protocol_fixation_date'] = datetime.strftime(datetime.strptime(date_str,'%Y/%m/%d'),"%Y-%m-%dT%H:%M:%SZ")                        
        except:    
            log.info(self.log_msg('Date of protocol fixation not found'))  
            
        try:
            date_str = stripHtml(self.soup.find('td',text='Anticipated trial start date').\
                                    findNext('td').renderContents()) 
            if date_str:                       
                date_str = date_str + '/01'
            page['et_project_anticipated_trial_start_date'] = datetime.strftime(datetime.strptime(date_str,'%Y/%m/%d'),"%Y-%m-%dT%H:%M:%SZ")
        except:    
            log.info(self.log_msg('Anticipated trial start date not found'))  
            
        try:
            date_str = stripHtml(self.soup.find('td',text='Last follow-up date').\
                                            findNext('td').renderContents()) 
            if date_str:
                date_str = date_str + '/01'
            page['et_project_last_follow_up_date'] = datetime.strftime(datetime.strptime(date_str,'%Y/%m/%d'),"%Y-%m-%dT%H:%M:%SZ") 
        except:    
            log.info(self.log_msg('Last follow-up date not found'))  
            
        try:
            date_str = stripHtml(self.soup.find('td',text='Date of closure to data entry').\
                                findNext('td').renderContents()) 
            if date_str:
                date_str = date_str + '/01'                    
            page['et_project_closure_to_data_entry_date'] = datetime.strftime(datetime.strptime(date_str,'%Y/%m/%d'),"%Y-%m-%dT%H:%M:%SZ")    
        except:    
            log.info(self.log_msg('Date of closure to data entry not found'))  
            
        try:
            date_str = stripHtml(self.soup.find('td',text='Date trial data considered complete"').\
                            findNext('td').renderContents()) 
            if date_str:
                date_str = date_str + '/01'                
            page['et_project_trial_data_considered_complete_date'] =  datetime.strftime(datetime.strptime(date_str,'%Y/%m/%d'),"%Y-%m-%dT%H:%M:%SZ")    
        except:    
            log.info(self.log_msg('Date trial data considered complete not found'))  
         
        try:
            date_str = stripHtml(self.soup.find('td',text='Date analysis concluded').\
                                                findNext('td').renderContents()) 
            if date_str:
                date_str = date_str + '/01'                                    
            page['et_project_analysis_concluded_date'] = datetime.strftime(datetime.strptime(date_str,'%Y/%m/%d'),"%Y-%m-%dT%H:%M:%SZ")                 
        except:    
            log.info(self.log_msg('Name of secondary sponsor(s) not found'))  
            
        try:
            page['et_project_url_releasing_protocol'] = stripHtml(self.soup.\
                                                find('td',text='URL releasing protocol').\
                                                findNext('td').renderContents())
        except:    
            log.info(self.log_msg('URL releasing protocol not found'))  
            
        try:
            page['et_project_publication_results'] = stripHtml(self.soup.\
                                                find('td',text='Publication of results').\
                                                findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Publication of results not found'))  
            
        try:
            page['et_project_url_releasing_results'] = stripHtml(self.soup.\
                                                find('td',text='URL releasing results').\
                                                findNext('td').renderContents())
        except:    
            log.info(self.log_msg('URL releasing results not found'))  
            
        try:
            page['et_project_results'] = stripHtml(self.soup.find('td',text='Results').\
                                findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Results  not found'))  
            
        try:
            page['et_project_other_related_information'] = stripHtml(self.soup.\
                                                find('td',text='Other related information').\
                                                findNext('td').renderContents())
        except:    
            log.info(self.log_msg('Other related information not found'))  
        
          
        try:
            page['et_project_url(Japanese)'] = stripHtml(self.soup.find('td',text= 'URL(Japanese)').\
                                        findNext('td').renderContents())
        except:    
            log.info(self.log_msg('URL(Japanese) not found'))  
            
        try:
            page['et_project_url(English)'] = stripHtml(self.soup.find('td',text= 'URL(English)').\
                                      findNext('td').renderContents())
        except:    
            log.info(self.log_msg('URL(English) not found'))  
                
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