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
log = logging.getLogger('RipOffReportConnector')
class RipOffReportConnector(BaseConnector):
    '''
    This will fetch the info for 
    Sample uris is
    http://www.ripoffreport.com/Search/Body/bank-of-america.aspx
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of ripoffreport.com
        """
        try:
            self.baseuri = 'http://www.ripoffreport.com'
            self.__setSoupForCurrentUri()
            #c =0 # for limiting the page limit 
            main_page_soup = copy.copy(self.soup)
            
            while self.__processLinkUrl():
                try:
                    self.currenturi = main_page_soup.find('li', 'next').find('a',text =re.compile('^Next')).\
                                        parent['href']
                    self.__setSoupForCurrentUri()
                    main_page_soup = copy.copy(self.soup)
##                    c+= 1
##                    if c >=5:
##                        break 
                except:
                    log.exception(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                    break 
        except:
            log.exception(self.log_msg('Exception while creating tasks for the url %s'\
                                                         %self.currenturi)) 
        return True
    
    @logit(log, '__processLinkUrl')
    def __processLinkUrl(self):
        """
        It will fetch each links
        """
        links = [each.find('a')['href']for each in self.soup.findAll('span','searchItem title')]
        for link in links:
            self.__addLinksAndPosts(link)     
        return True
    
    @logit(log, '__addLinksAndPosts')
    def __addLinksAndPosts(self, link): 
        """
        This will add the link info from setParentPage method and 
        Add the posts  addPosts mehtod
        """
        try:
            self.currenturi = self.baseuri + link
            self.genre = "Review"
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
            self.__setParentPage()
            #if self.task.instance_data.get('pick_comments'):
            posts = self.soup.findAll('div','article_update')
            for post in posts:
                if not self.__addPost(post):
                    log.debug(self.log_msg('Post not added to self.pages for url\
                                                            %s'%self.currenturi))
                    return False
            return True
        except:
            log.exception(self.log_msg('Exception while add the theread posts \
                                            for the url %s'%self.currenturi))
            return False
    
    @logit(log, '__setParentPage')
    def __setParentPage(self):
        page ={}
        
        if checkSessionInfo(self.genre, self.session_info_out, \
                    self.currenturi, self.task.instance_data.get('update'),\
                    parent_list = [self.task.instance_data['uri']]):
            log.info(self.log_msg('Session info returns True for uri %s'% \
                                                                self.currenturi))
            return False
        try:
            date_str = stripHtml(self.soup.find('b', text =re.compile('^Submitted:')).\
                        next.__str__())
            page['posted_date']= datetime.strptime(date_str,"%A, %B %d, %Y").strftime("%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('posted_date not be found in %s'% self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            title_tag = self.soup.find('h1', attrs = {'class' : re.compile('reportTitle \s*')})
            unwanted_tag = title_tag.find('a')
            if unwanted_tag:
                unwanted_tag.extract()
            page['title'] = stripHtml(title_tag.renderContents())
        except:
            log.exception(self.log_msg('post title  not found in %s'% self.currenturi))
            page['title'] = ' '
        try:
            page['data'] = stripHtml(self.soup.find('div', 'reportText konaBody').\
                            renderContents())
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''  
        
        if not page['title'] and not page['data']: 
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False
        try:
            date_str = stripHtml(self.soup.find('div',  attrs = {'class' : re.compile('reportAddress\s*')}).renderContents()).\
                        splitlines()[-1].split(':')[-1].strip()
            page['edate_last_posted_date'] = datetime.strptime(date_str,"%A, %B %d, %Y").\
                                                strftime("%Y-%m-%dT%H:%M:%SZ")            
        except:
            log.exception(self.log_msg('last poosted date not found'))                                             
        try:
            page['et_author_name'] = stripHtml(self.soup.find('h3', attrs = {'class' : re.compile('reported_by\s*')}).\
                                        renderContents()).split(':')[-1].strip().\
                                        split('(')[0].strip()
        except:
            log.exception(self.log_msg('auth name not found'))
        try:
                
            page['et_author_address'] = stripHtml(self.soup.find('h3', attrs = {'class' :re.compile('reported_by\s*')}).\
                                        renderContents()).split(':')[-1].strip().\
                                        split('(')[-1].replace(')','') 
        except:
            log.exception(self.log_msg('auth_address not found'))                                   
                                 
        try:
            page['et_category_name'] = stripHtml(self.soup.find('b', text = re.compile('^Category:')).\
                                    findParent('h3').renderContents()).split(':')[-1]
        except:
            log.exception(self.log_msg('category not found'))
        try:
            page['et_product_name'] = stripHtml(self.soup.find('h2', attrs = {'class' : re.compile('pageTitle\s*')}).\
                                    renderContents()).split(':')[-1].strip()
        except:
            log.exception(self.log_msg('bank name not found'))
        try:
            page['et_bank_address'] = stripHtml(self.soup.find('div', attrs = {'class' : re.compile('reportAddress\s*')}).\
                                        renderContents()).split('Category:')[0]

        except:
            log.exception(self.log_msg('bank name not found'))     
        try:    
            result = updateSessionInfo(self.genre, self.session_info_out, self.currenturi, \
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
        
        
    @logit(log, '__addPost')
    def __addPost(self, post):
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:    
            unique_key = post.find('h2','updateReportTitle').find('a')['id']
            if checkSessionInfo(self.genre, self.session_info_out, \
                        unique_key, self.task.instance_data.get('update'),\
                        parent_list = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'% \
                                                                unique_key))
                return False
            page = self.__getData(post)
            if not page:
                log.info(self.log_msg('page contains empty data, getdata \
                                    returns  False for uri %s'%self.currenturi))
                return True
            result = updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                get_hash( page ),'review', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if result['updated']:
                page['parent_path'] = [self.task.instance_data['uri'], unique_key]
                page['path'] = [self.task.instance_data['uri'], unique_key]
                page['uri'] = self.currenturi + '#' + unique_key
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
    def __getData(self, post):
        """ This will return the page dictionry
        """
        page = {}
        try:
            date_str  = stripHtml(post.find('span','report_posted').renderContents()).\
                        split(':')[-1].strip()
            page['edate_last_posted_date']= datetime.strptime(date_str,"%A, %B %d, %Y").\
                                    strftime("%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('last posted_date not be found in %s'% self.currenturi))
            
        try:
            page['title'] =  stripHtml(post.find('h2','updateReportTitle').renderContents())
        except:
            log.exception(self.log_msg('post title  not found in %s'% self.currenturi))
            page['title'] = ' '
        try:
            page['data'] = stripHtml(post.find('div','reportTextCol2 KonaBody').\
                            renderContents())
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''  
        
        if not page['title'] and not page['data']: 
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False
        try:
            date_str = stripHtml(post.find('span','report_submitted').\
                                    renderContents()).split(':')[-1].strip()
            page['posted_date'] = datetime.strptime(date_str,"%A, %B %d, %Y").\
                                                strftime("%Y-%m-%dT%H:%M:%SZ")            
        except:
            log.exception(self.log_msg('posted_date not be found in %s'% self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")                                           
        try:
            page['et_author_name'] = stripHtml(post.find('h2','updateReportTitle').\
                                        findNext('h3').renderContents()).split('-')[0]
                                        
        except:
            log.exception(self.log_msg('auth name not found'))
        try:
            page['et_author_address'] = stripHtml(post.find('h2','updateReportTitle').\
                                        findNext('h3').renderContents()).split('-')[-1].\
                                        replace('(','').replace(')','')
        except:
            log.exception(self.log_msg('auth address not found'))    
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