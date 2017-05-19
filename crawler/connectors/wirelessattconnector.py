'''
Copyright (c)2008-2010 Serendio Software Private Limited
All Rights Reserved
This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
import copy
import logging
import urllib2
from cgi import parse_qsl
from urllib2 import urlparse
from urllib import urlencode
from datetime import datetime,timedelta

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('WirelessATTConnector')
class WirelessATTConnector(BaseConnector):
    '''Forum for wireless.att.com
    ''' 
    @logit(log , 'fetch')
    def fetch(self):
        """
        This will add the Thread info from setParentPage method and 
        Add the posts of the thread in addPosts mehtod
        """
        try:
            self.__setSoupForCurrentUri()
            self.currenturi = self.soup.find('iframe', id='BVFrame')['src'].split('?')[0]
            self.__setSoupForCurrentUri()
            self.__genre = "Review"
            self.__task_elements_dict = {
                            'priority':self.task.priority,
                            'level': self.task.level,
                            'last_updated_time':datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ"),
                            'pickup_date':datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ"),
                            'connector_instance_log_id': self.task.connector_instance_log_id,
                            'connector_instance_id':self.task.connector_instance_id,
                            'workspace_id':self.task.workspace_id,
                            'client_id':self.task.client_id,
                            'client_name':self.task.client_name,
                            'versioned':False,
                            'category':self.task.instance_data.get('category',''),
                            'task_log_id':self.task.id }
            current_page = 1
            while self.__iteratePosts():
                try:
                    current_page += 1
                    self.currenturi = [each['href'] for each in [x.find('a') for x in self.soup.find('div', 'BVPageBasedPaginationContainer').findAll('span', id='pageLink')] if int(stripHtml(each.renderContents()))==current_page][0]
                    self.__setSoupForCurrentUri()
                except:
                    log.info(self.log_msg('No Previous URL found for url \
                                                        %s'%self.currenturi))
                    break
        except:
            log.exception(self.log_msg('Exception while fetching posts'))
        return True
    
    @logit(log, '__createTasksForThreads')
    def __createTasksForThreads(self):
        """
        This will create Tasks for the threads found on the given url
        The # of Tasks are limited by Config Variable
        """
        self.__total_threads_count = 0
        self.__last_timestamp = datetime( 1980,1,1 )
        self.__setSoupForCurrentUri()
        self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                            'everythingberry_maxthreads'))
        
        while self.__getThreads():
            try:
                self.currenturi = self.soup.find('img', alt='Next').parent['href']
                self.__setSoupForCurrentUri()
            except:
                log.info(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                break
        if self.__links_to_process:
            updateSessionInfo('Search', self.session_info_out,\
                    self.__last_timestamp , None, 'ForumThreadsPage', \
                    self.task.instance_data.get('update'))
        log.info(self.log_msg('# of tasks added is %d'%len(self.linksOut)))
        return True
    
    @logit(log, '__getThreads')
    def __getThreads(self):
            """
            It will fetch each thread and its associate infomarmation
            and add the tasks
            """
            threads = self.soup.find('ol', id='threads').findAll('li', id=re.compile('thread'))
            if not threads:
                log.info(self.log_msg('No threads are found for url %s'%\
                                                            self.currenturi))
                return False
            for thread in threads:
                self.__total_threads_count += 1
                if self.__total_threads_count > self.__max_threads_count:
                    log.info(self.log_msg('Reaching maximum post,Return false \
                                            from the url %s'%self.currenturi))
                    return False
                try:
                    date_str  = stripHtml(thread.findAll('dd')[-1].renderContents())
                    thread_time  = self.__getDateObj(date_str,'%m-%d-%Y %I:%M %p')
                except:
                    log.exception(self.log_msg('Cannot fetch the date for the url\
                                                            %s'%self.currenturi))
                    continue
                if checkSessionInfo('Search', self.session_info_out, thread_time,\
                                        self.task.instance_data.get('update')):
                        log.info(self.log_msg('Session info Returns True for url %s'%self.currenturi))
                        return False
                self.__last_timestamp = max(thread_time , self.__last_timestamp )
                try:
                    self.__links_to_process.append(thread.find('h3', 'threadtitle').a['href'])
                except:
                    log.exception(self.log_msg('Cannot find the thread url \
                                            in the uri %s'%self.currenturi))
                    continue
            return True
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = self.soup.findAll('table', 'BVReviewDisplay')
            if not posts:
                log.info(self.log_msg('No posts found'))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(posts)))
            for post in posts:
                if not self.__addPost(post):
                    log.info(self.log_msg('Post not added to self.pages for url\
                                                            %s'%self.currenturi))
                    return False
            return True
        except:
            log.exception(self.log_msg('Reviews are not found for url %s'%self.currenturi))
            return False
    
    @logit(log, '__addPost')
    def __addPost(self, post):
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            unique_key = dict(parse_qsl(post.findParent('tr').find('a', 'BVSocialBookmarkingSharingLink')['href'].split('?')[-1]))['url']
            if checkSessionInfo(self.__genre, self.session_info_out, unique_key,\
                                        self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info returns True for uri %s'%unique_key))
                return False
            page = self.__getData(post)
            if not page:
                log.info(self.log_msg('page contains empty data, getdata \
                                    returns  False for uri %s'%self.currenturi))
                return True
            result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                get_hash( page ),'forum', self.task.instance_data.get('update'))
            if result['updated']:
                page['parent_path'] = []
                page['path'] = [unique_key]
                page['uri'] = self.currenturi
                page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
                log.info(page)
                page.update(self.__task_elements_dict)
                self.pages.append(page)
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
        page = {'entity':'review'}
        try:
            page['data'] = stripHtml(post.find('td', 'BVcontent').renderContents())
            page['title'] = ''
        except:
            log.info(self.log_msg('Data not found'))
            page['data'] = ''
        if not page['data']: 
            log.info(self.log_msg("Data is not found for %s,discarding this Post"%(permalink)))
            return False 
        try:
            page['et_author_name'] = stripHtml(post.find('span', 'BVReviewer').renderContents()).replace('By  ','')
        except:
            log.info(self.log_msg('author name not found'))
        try:
            date_str= stripHtml(post.find('span', 'BVdateCreated').renderContents())[1:].strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str, '%B %d, %Y'), "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            
        return page
    
    @logit(log,'__getDateObj')
    def __getDateObj(self, date_str_tag, format):
        '''
        It will get the HTML Tag as Input Or date_str as input 
        and returns posted_date
        '''
        date_obj = datetime.utcnow()
        if not date_str_tag:
            log.info('No date str in avbl')
            return date_obj
        try:
            date_str = stripHtml(str(date_str_tag))
            if date_str.startswith('Today') or date_str.startswith('Yesterday'):
                day_str = re.split('[\s,]',date_str)[0]
                day_dict = {'Today':0, 'Yesterday':1}
                date_str = (datetime.strftime(datetime.utcnow()-timedelta(days=day_dict\
                    [day_str]),"%m-%d-%Y") + date_str.replace(day_str, '')).strip()                                
            date_str = re.sub('\s+', ' ', date_str)
            date_obj = datetime.strptime(date_str,format)
        except:
            log.exception(self.log_msg('Posted date not found for the url %s'%self.currenturi))
        return date_obj
            
    @logit(log,'__setSoupForCurrentUri')
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
    
    @logit(log, '__removeSessionId')
    def __removeSessionId(self, uri):
        '''This will remove the session id from current url
        '''
        try:
            url_sep = uri.split('?')
            info_dict = dict(parse_qsl(url_sep[-1]))
            if 's' in info_dict.keys():
                info_dict.pop('s')
            uri = url_sep[0]
            if info_dict:
                uri = uri + '?' + urlencode(info_dict)
        except:
            log.info(self.log_msg('Cannot remove the Session id in url %s'%uri))
            #log.info(uri)
        return uri
    