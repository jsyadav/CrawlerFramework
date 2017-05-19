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
from BeautifulSoup import BeautifulStoneSoup

log = logging.getLogger('OneUpConnector')
class OneUpConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        sample url: http://www.1up.com/do/gameOverview?cId=3168176&sec=REVIEW
        '''
        try:
            self.genre = "Review"
            self.baseuri = 'http://www.1up.com'
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
            self.__iterateLinks()
        except:
            log.exception(self.log_msg('Exception while add the theread posts \
                                            for the url %s'%self.currenturi))
        return True

    @logit(log, '__iterateLinks')
    def __iterateLinks(self):   
        try:
            links = [self.baseuri + each.find('div','floatleft').find('a')['href']for each in self.soup.findAll('div','content_item')]
            if not links:
                log.info(self.log_msg('No posts found %s'%self.currenturi))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(links)))
            for link in links:#use some range for few data  
                if not self.__addPost(link):
                    return False 
            return True    
        except:
            log.exception(self.log_msg('can not  find the data %s'%self.currenturi))
            return False     
    
    @logit(log, '__addPost')    
    def __addPost(self, link): 
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        self.currenturi = link
        self.__setStoneSoupForCurrentUri()
        try:
            page = self.__getData()
            if not page:
                return True 
            if checkSessionInfo(self.genre, self.session_info_out, self.currenturi,\
                         self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True'))
                return False
        except:
            log.exception(self.log_msg('Cannot add the post for the url %s'%\
                                                            self.currenturi))
            return False
        try:
            result=updateSessionInfo(self.genre, self.session_info_out, self.currenturi, \
                get_hash( page ),'review', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if not result['updated']:
                log.exception(self.log_msg('Update session info returns False'))
                return True
            page['parent_path'] = [self.task.instance_data['uri']]
            page['path'] =  [self.task.instance_data['uri'], self.currenturi]
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
            page['title'] = stripHtml(self.soup.find('div','floatleft').find('h2').\
                            renderContents())
        except:
            log.exception(self.log_msg('title not found for the url %s'%self.currenturi))
            page['title'] = ''  
        try:
            page['data'] = stripHtml(self.soup.find('span','articleText').renderContents())
        except:
            log.exception(self.log_msg('data not found%s'%self.currenturi))                             
        
        if not page['title'] and not page['data']: 
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False  
        try:
            date_str = stripHtml(self.soup.find('p','blurb').renderContents()).\
                        split('.')[0].strip()
            page['posted_date'] = datetime.strptime(date_str,"%Y-%m-%d %H:%M:%S").\
                                    strftime("%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('posted_date nt found %s'%self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['et_game_name'] = stripHtml(self.soup.find('div',id='userReview').\
                                    findNext('h1').renderContents())
        except:
            log.exception(self.log_msg('game  title not found'))                                                                 
        
        # author info 
        try:
            page['et_author_score'] = stripHtml(self.soup.find('div','floatleft').\
                                        find('h4').renderContents())
        except:
            log.exception(self.log_msg('author score not found'))    
        try:
            page['et_author_name'] = stripHtml(self.soup.find('div','floatleft').\
                                    find('a').renderContents())
        except:
            log.exception(self.log_msg('author_name not found %s'%self.currenturi))
                                            
        copycurrenturi = self.currenturi
        try:
            self.currenturi = self.baseuri + self.soup.find('div','floatleft').\
                                find('a')['href']
            self.__setSoupForCurrentUri()  
            try:
                page['et_author_full_name'] = stripHtml(self.soup.find('div',id ='userInfo').\
                                                findNext('h1').renderContents())
            except:
                log.exception(self.log_msg('author full name not found'))
            try:
                try:
                    field =[stripHtml(each.renderContents())for each in self.soup.\
                           findAll('dt')]
                except:
                    log.exception(self.log_msg('author field not found'))
                try:            
                    value = [stripHtml(each.renderContents())for each in self.soup.\
                            findAll('dd')]
                except:
                    log.exception(self.log_msg('autor value not found'))
                info = dict([('et_author_' + x.lower().replace(' ','_')[:-1],y) for x,y in zip(field,value)])
                try:
                    age = info.pop('et_author_age')
                    info['ei_author_age'] = int(age)
                except:
                    log.exception(self.log_msg('age not found'))
                
                points = info.pop('et_author_total_points')
                info['ei_author_total_points'] = int(points.replace(',',''))
                
                last_visit = info.pop('et_author_last_visit').replace('PDT','').replace('PST','')
                info['edate_author_last_visit'] = datetime.strptime(last_visit,"%a %b %d %H:%M:%S  %Y").\
                                                strftime("%Y-%m-%dT%H:%M:%SZ") 
                log.info(info['edate_author_last_visit'])                
                                
                page.update(info)                
                                
            except:
                log.exception(self.log_msg('author info not found'))                    
            try:
                page['et_author_personal_website'] = self.soup.find('a',text = 'Personal Website',href=True).\
                                                        parent['href']   
            except:
                log.exception(self.log_msg('personal website not found'))                                                                                 
            try:
                page['et_author_social_aim_name)'] = stripHtml(self.soup.find('li','social_aim').\
                                            renderContents()).split(':')[-1].strip()
            except:
                log.exception(self.log_msg('author aim  not found %s'%self.currenturi)) 
            try:
                page['ei_author_social_icq)'] = int(stripHtml(self.soup.find('li','social_icq').\
                                                renderContents()).split(':')[-1].\
                                                strip().replace(',',''))
            except:
                log.exception(self.log_msg('author social icq not found %s'%self.currenturi))  
                                                                   
        except:
            log.exception(self.log_msg('author link not found %s'%self.currenturi))
        self.currenturi = copycurrenturi   
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
    
    @logit(log, '__setStoneSoupForCurrentUri')                                                                                 
    def __setStoneSoupForCurrentUri(self, data=None, headers={}):
        """It will set soup object for the Current URI
        """
        res = self._getHTML(data=data, headers=headers)
        if res:
            self.rawpage = res['result']
        else:
            log.info(self.log_msg('Page Content cannot be fetched for the url: \
                                                            %s'%self.currenturi))
            raise Exception('Page content not fetched for th url %s'%self.currenturi)
        #self._setCurrentPage()
        self.soup = BeautifulStoneSoup(self.rawpage)                  
            
                                 
                     
          
        
    