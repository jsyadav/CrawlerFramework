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
#fixed on 19june

import re, copy, logging
from urllib2 import urlparse, urlopen
from urllib import urlencode
from datetime import datetime

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging.getLogger('PissedConsumerConnector')
class PissedConsumerConnector(BaseConnector):
    '''
    This will fetch the info for 
    Sample uris is
    http://hsbc.pissedconsumer.com/
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of pissedconsumer.com
        """
        try:
            self.__genre = 'review'
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
            main_page_soup = copy.copy(self.soup)
            #c = 0
            while True:
                try:
                    if not self.__iterateLinks():
                        log.info(self.log_msg('No more links found'))
                        break
                    self.currenturi = main_page_soup.find('a','pagenavMore',title = 'Next')['href']
                    self.__setSoupForCurrentUri() 
                    main_page_soup = copy.copy(self.soup)
                    #c += 1
                    #if c >1:
                     #   break
                except:
                    log.exception(self.log_msg('next page not found %s'%self.currenturi)) 
                    break   
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'%self.currenturi))
        return True
        
    @logit(log , '__iterateLinks')
    def __iterateLinks(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            links = [each.find('td','reviewrow').find('a')['href']for each in self.\
                    soup.find('table','tbl').findAll('tr','hreview')[1:-2]]
            if not links:
                log.info(self.log_msg('No posts found in url %s'%self.currenturi))
                return False
            for link in links:
                if not self.__addPost(link):
                    log.debug(self.log_msg('Post not added to self.pages for url\
                                                            %s'%self.currenturi))
                    break
            return True
        except:
            log.exception(self.log_msg('Reviews are not found for url %s'%self.currenturi))
            return True
    
    @logit(log, '__addPost')
    def __addPost(self, link):
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:    
            self.currenturi = link
            #unique_key = get_hash({'data' : page['data'],'title' : page['title']})
            if checkSessionInfo(self.__genre, self.session_info_out, \
                        self.currenturi, self.task.instance_data.get('update'),\
                        parent_list = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'% \
                                                                unique_key))
                return False
            self.__setSoupForCurrentUri()
            page = self.__getData()
            if not page:
                log.info(self.log_msg('page contains empty data, getdata \
                                    returns  False for uri %s'%self.currenturi))
                return True
            result = updateSessionInfo(self.__genre, self.session_info_out, self.currenturi, \
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
                log.info(page)
            else:
                log.info(self.log_msg('Update session info returns False for \
                                                    url %s'%self.currenturi))
        except:
            log.exception(self.log_msg('Cannot add the post for the uri %s'%self.currenturi))
        return True
    
    @logit(log, '__getData')
    def __getData(self):
        """ This will return the page dictionry
        """
        page = {}
        info = self.soup.find('div','complaintBody')
        try:
            date_str = stripHtml(info.find('td',text ='Posted On:').findNext('td').\
                        renderContents()).strip()
            page['posted_date']= datetime.strptime(date_str,"%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('posted_date not be found in %s'% self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['title'] = stripHtml(info.find('h1','stitle').renderContents())
        except:
            log.exception(self.log_msg('post title  not found in %s'% self.currenturi))
            page['title'] = ' '
        try:
            data_tag = info.find('div','postContent')
            script_tag = data_tag.findAll('script')
            if script_tag:
                [x.extract() for x in script_tag]
            table_tag = data_tag.findAll('table')
            if table_tag:
                [x.extract() for x in table_tag]
            extra_tag = data_tag.find('div',id = 'loading-layer')
            if extra_tag:
                extra_tag.extract()    
            page['data'] = stripHtml(data_tag.renderContents())
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''  
        
        if not page['title'] and not page['data']: 
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False
        try:
            page['ei_data_counts'] = int(stripHtml(info.find('td',text ='Review #:').\
                                        findNext('td').renderContents()).replace(',','')) 
        except:
            log.exception(self.log('reviews couont not found'))                                             
        try:
            page['ei_data_hits_count'] = int(stripHtml(info.find('td',text ='Total hits:').\
                                            findNext('td').renderContents()).replace(',',''))
        except:
            log.exception(self.log('hits couont not found'))  
        try:
            page['ei_data_comments_count'] = int(stripHtml(info.find('td',text ='Review #:').\
                                            findNext('td').renderContents()).replace(',',''))
        except:
            log.exception(self.log('hits couont not found'))                                        
        try:
            page['et_author_name'] = stripHtml(info.find('td',text ='Posted by:').\
                                        findNext('td').renderContents())
        except:
            log.exception(self.log_msg('auth name not found'))
        copycurrenturi = self.currenturi    
        try:        
            auth_link = info.find('td',text ='Posted by:').findNext('td').find('a')
            if auth_link:
                self.currenturi = auth_link['href']
                self.__setSoupForCurrentUri()
                try:
                    date_str = stripHtml(self.soup.find('td',text= 'Joined:').\
                                findNext('td').renderContents()).strip()
                    page['edate_author_joined_date'] = datetime.strptime(date_str,"%b %d, %Y").\
                                                strftime("%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.exception(self.log_msg('author joined date not found'))   
             
                try:
                    page['ei_author_posts_count'] = int(stripHtml(self.soup.\
                                                    find('td',text= 'Posted Reviews:').\
                                                    findNext('td').renderContents()).\
                                                    replace(',',''))
                except:
                    log.exception(self.log_msg('author posts count not found'))
                try:
                    date_str = stripHtml(self.soup.find('td',text= 'Last seen Online:').\
                                findNext('td').renderContents()).strip()
                    page['edate_author_last_online'] = datetime.strptime(date_str,"%b %d, %Y").\
                                                        strftime("%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.exception(self.log_msg('author last online not found')) 
                try:
                    page['et_author_companies_discussed'] = stripHtml(self.soup.\
                                                            find('td',text= 'Companies Discussed:').\
                                                            findNext('td').renderContents())
                except:
                    log.exception(self.log_msg('author companies not found')) 
                try:
                    page['et_author_areas_of_interest'] = stripHtml(self.soup.\
                                                            find('td',text= 'Areas of interest:').\
                                                            findNext('td').renderContents())
                except:
                    log.exception(self.log_msg('author areas of intrest not found')) 
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