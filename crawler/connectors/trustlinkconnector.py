'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved
This software is confidential and proprietary information of Serendio Software.
It is disclosed pursuant to a non-disclosure agreement between the recipient
and Serendio. This source code is provided for informational purposes only, and
Serendio makes no warranties, either express or implied, in this. Information 
in this program, including URL and other Internet website references, is 
subject to change without notice. The entire risk of the use or the results of 
the use of this program  with the user. Complying with all applicable 
copyright laws is the responsibility of the user. Without limiting the rights 
under copyright, no part of this program may be reproduced, stored in, or 
introduced into a retrieval system, or distributed or transmitted in any form 
or by any means (electronic, mechanical, photocopying, recording, on a website,
or otherwise) or for any purpose, without the express written permission of 
Serendio Software.
Serendio may have patents, patent applications, trademarks, copyrights, or 
other intellectual property rights covering subject matter in this program. 
Except as expressly provided in any written license agreement from Serendio, 
the furnishing of this program does not give you any license to these patents, 
trademarks, copyrights, or other intellectual property.
'''
#prerna
import re, copy, logging
from urllib2 import urlparse, urlopen
from urllib import urlencode
from datetime import datetime

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging.getLogger('TrustLinkConnector')
class TrustLinkConnector(BaseConnector):
    '''
    This will fetch the info for 
    Sample url is
    http://www.trustlink.org/Reviews/Bank-of-America-Home-Loans-205653148
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of trustlink.org
        """
        try:
            self.__genre = 'review'
            self.baseuri = 'http://www.trustlink.org'
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
            headers={'Host':'www.trustlink.org'}
            headers['Referer'] = self.currenturi
            
            while True:
                main_page_soup = copy.copy(self.soup)
                self.__iteratePosts()
                next_page_row = main_page_soup.find('table',id = re.compile('ctl\d+_ctl\d+_Main_MainColumn_dgReviews')).\
                                        findAll('tr', recursive = False)[-1]
                try:
                    data = {}
                    try:
                        data['__EVENTTARGET'] =next_page_row.find('span').findParent('td').findNext('td').find('a')['href'].split('(')[-1].split(',')[0][1:-1]
                    except:
                        log.exception(self.log_msg('event'))                        
                    data['__EVENTARGUMENT'] = next_page_row.find('span').\
                                                findParent('td').findNext('td').\
                                                find('a')['href'].split(',')[-1].replace(')','')[1:-1]
                    log.info(data)
                    data['__VIEWSTATE'] =  main_page_soup.find('input',id = '__VIEWSTATE')['value']
                    self.__setSoupForCurrentUri(data=data,headers=headers)
                    main_page_soup = copy.copy(self.soup)
                    
                except:
                    log.exception(self.log_msg('next page not found %s'%self.currenturi)) 
                    break   
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'%self.currenturi))
        return True
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the links found in the Current URI
        """
        try:
            posts = self.soup.find('table',id = re.compile('ctl\d+_ctl\d+_Main_MainColumn_dgReviews')).\
                    findAll('tr', recursive = False)[:-1]
            if not posts:
                log.info(self.log_msg('No posts found in url %s'%self.currenturi))
                return False
            for post in posts:
                if not self.__addPost(post):
                    log.debug(self.log_msg('Post not added to self.pages for url\
                                                            %s'%self.currenturi))
                    break
            return True
        except:
            log.exception(self.log_msg('Reviews are not found for url %s'%self.currenturi))
            return True
    
    @logit(log, '__addPost')
    def __addPost(self, post):
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:    
            page = self.__getData(post)
            if not page:
                log.info(self.log_msg('page contains empty data, getdata \
                                    returns  False for uri %s'%self.currenturi))
                return True
            unique_key = get_hash({'data' : page['data']})
            if checkSessionInfo(self.__genre, self.session_info_out, \
                        unique_key, self.task.instance_data.get('update'),\
                        parent_list = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'% \
                                                                unique_key))
                return False
            result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                get_hash( page ),'review', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if result['updated']:
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [self.task.instance_data['uri']]
                page['uri'] = self.currenturi 
                page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
                page ['entity'] = 'review'
                page.update(self.__task_elements_dict)
                self.pages.append(page)
                #log.info(page)
            else:
                log.exception(self.log_msg('Update session info returns False for \
                                                    url %s'%self.currenturi))
        except:
            log.exception(self.log_msg('Cannot add the post for the uri %s'%self.currenturi))
        return True
    
    @logit(log, '__getData')
    def __getData(self, post):
        """ This will return the page dictionry
        """
        page = {}
        try:
            date_str = stripHtml(post.find('span',id = re.compile('\w+lblDatePosted')).\
                        renderContents()).split(' ')[-1].strip()
            page['posted_date']= datetime.strptime(date_str,'%m/%d/%Y').strftime("%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('posted_date not be found in %s'% self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['data'] = stripHtml(post.find('span',id = re.compile('\w+lblReviewText')).\
                            renderContents())
            page['title'] ='' 
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            return
        try:
            page['ei_data_rating'] = int(len(post.findAll('span','smallratingStar smallfilledRatingStar')))
        except:
            log.exception(self.log('reviews couont not found'))                                             
        try:
            page['ei_author_posts_count'] = int(stripHtml(post.find('span',id = re.compile('\w+lblNumReviews')).\
                                                renderContents()).split(' ')[0].replace(',',''))
        except:
            log.exception(self.log('hits couont not found'))  
        try:
            page['et_author_name'] = stripHtml(post.find('div','user').renderContents())
        except:
            log.exception(self.log_msg('auth name not found'))
        copycurrenturi = self.currenturi    
        try:        
            auth_link = post.find('div','user').find('a')
            if auth_link:
                self.currenturi = self.baseuri + auth_link['href']
                self.__setSoupForCurrentUri()
                try:
                    page['et_author_age_range'] = stripHtml(self.soup.find('span',id = re.compile('\w+lblAge')).\
                                                    renderContents())
                except:
                    log.exception(self.log_msg('author age range not found')) 
                try:
                    page['et_author_gender'] = stripHtml(self.soup.find('span',id = re.compile('\w+lblGender')).\
                                                renderContents())
                except:
                    log.exception(self.log_msg('author gender not found')) 
                try:
                    page['et_author_marital_status'] = stripHtml(self.soup.\
                                                        find('span',id = re.compile('\w+lblMaritalStatus')).\
                                                        renderContents())
                except:
                    log.exception(self.log_msg('author marital status not found')) 
                try:
                    page['et_author_householdsize'] = stripHtml(self.soup.find('span',id = re.compile('\w+lblHouseholdSize')).\
                                                renderContents())
                except:
                    log.exception(self.log_msg('author household not found')) 
                try:
                    page['et_author_location'] = stripHtml(self.soup.find('span',id = re.compile('\w+lblLocation')).\
                                                renderContents())
                except:
                    log.exception(self.log_msg('author location not found')) 
                try:
                    page['et_author_occupation'] = stripHtml(self.soup.find('span',id = re.compile('\w+lblOccupation')).\
                                                renderContents())
                except:
                    log.exception(self.log_msg('author occuption not found')) 
                try:
                    page['et_author_education'] = stripHtml(self.soup.find('span',id = re.compile('\w+lblEducation')).\
                                                renderContents())
                except:
                    log.exception(self.log_msg('author education not found')) 
                try:
                    date_str = '01 ' + stripHtml(self.soup.find('span',id = re.compile('\w+lblTLJoinDate')).\
                                                renderContents())
                    page['et_author_joined_date'] = datetime.strptime(date_str,'%d %B %Y').\
                                                    strftime("%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.exception(self.log_msg('author joined date not found')) 
                    page['et_author_joined_date'] = date_str
                try:
                    page['et_author_profile_website'] = stripHtml(self.soup.find('span',id = re.compile('\w+lblProfileWebsite')).\
                                                renderContents())
                except:
                    log.exception(self.log_msg('author ProfileWebsite  not found'))     
                                        
        except:
            log.exception(self.log_msg('auth_info not found'))
        self.currenturi = copycurrenturi                
        return page            
    
    @logit(log,'__setSoupForCurrentUri')
    def __setSoupForCurrentUri(self, data=None, headers={}):
        """It will set soup object for the Current URI
        """
        res = self._getHTML(data=data, headers=headers)
        if res:
            self.rawpage = res['result']
        else:
            raise Exception('Page content not fetched for th url %s'%self.currenturi)
        self._setCurrentPage()