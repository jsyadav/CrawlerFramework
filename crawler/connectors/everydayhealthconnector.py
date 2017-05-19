'''
Copyright (c)2008-2010 Serendio Software Private Limited
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
#Skumar

import re
import logging
from urllib2 import urlparse
from urllib import urlencode
from datetime import datetime

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('EverydayHealthConnector')
class EverydayHealthConnector(BaseConnector):
    '''
    This will fetch the info for everydayhealth.com 
    Sample uris is
    http://www.everydayhealth.com/forums/dental-health/watch-your-mouth
    '''
        
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of everydayhealth.com
        """
        try:
            self.__genre = 'review'
            self.__baseuri = 'http://www.everydayhealth.com'
            self.__setSoupForCurrentUri()
            if not '/topic/' in self.currenturi:
                return self.__createTasksForThreads()
            else:
                return self.__addThreadAndPosts()
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'%self.currenturi))
            return False
        
    @logit(log, '__addThreadAndPosts')
    def __addThreadAndPosts(self):
        """
        This will add the Thread info from setParentPage method and 
        Add the posts of the thread in addPosts mehtod
        """
        self.__hierarchy = []
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
        self.__setParentPage()
        self.__goToLastPage()
        while self.__iteratePosts():
            try:
                self.currenturi = self.__baseuri + self.soup.find('a', 'prev_page')['href']
                self.__setSoupForCurrentUri()
            except:
                log.exception(self.log_msg('No Previous URL found for url \
                                                    %s'%self.currenturi))
                break
        try:
            question_post = self.soup.find('div', 'msgPost')
            self.__addPost(question_post, True)
        except:
            log.exception(self.log_msg('Question post not not found in url %s'%self.currenturi))
            return False
        return True
        
    @logit(log, '__createTasksForThreads')
    def __createTasksForThreads(self):
        """
        This will create Tasks for the threads found on the given url
        The # of Tasks are limited by Config Variable
        """
        self.__current_thread_count = 0
        self.__last_timestamp = datetime(1980, 1, 1)
        self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                            'everydayhealth_maxthreads'))
        while self.__getThreads():
            try:
                self.currenturi = self.__baseuri + self.soup.find('a', 'next_page')['href']
                self.__setSoupForCurrentUri()
            except:
                log.exception(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                break
        log.info('Total # of tasks found is %d'%len(self.linksOut))
        #self.linksOut = None
        if self.linksOut:
            updateSessionInfo('Search', self.session_info_out , \
                    self.__last_timestamp , None, 'ForumThreadsPage', \
                    self.task.instance_data.get('update'))
        return True
        
    @logit(log, '__getThreads')
    def __getThreads(self):
        """
        It will fetch each thread and its associate infomarmation
        and add the tasks
        """
        try:
            forum_links = [str(thread.find('div','fFname').a['href']) for thread in self.soup.find('div', 'fContent').findAll('div', recursive=False)[1:-1]]
            if not forum_links:
                log.info(self.log_msg('No forum_links found in url %s'%self.currenturi))
                return False
        except:
            log.info(self.log_msg('No threads found in url %s'%self.currenturi))
            return False
        for forum_link in forum_links:
            try:
                self.__current_thread_count += 1
                if  self.__current_thread_count > self.__max_threads_count:
                    log.info(self.log_msg('Reaching maximum post,Return false \
                                            from the url %s'%self.currenturi))
                    return False
                temp_task = self.task.clone()                    
                temp_task.instance_data['uri'] = forum_link
                self.linksOut.append(temp_task)
                log.info(self.log_msg('Task added'))
            except:
                log.exception(self.log_msg('Cannot find the thread url \
                                        in the uri %s'%self.currenturi))            
        return True
    
    @logit(log, '__setParentPage')
    def __setParentPage(self):
        """This will get the parent info
        """
        page = {}
        try:
            page['et_thread_hierarchy'] = self.__hierarchy = [x.strip() for x in stripHtml(self.soup.find('div', 'deck breadcrumbs').renderContents()).split('>') if x.strip()][1:]
            page['data'] = page['title'] = page['et_thread_hierarchy'][-1]
        except:
            log.exception(self.log_msg('Thread hierarchy and Title Not found for uri\
                                                            %s'%self.currenturi))
            return
        if checkSessionInfo(self.__genre, self.session_info_out, self.task.instance_data['uri'], \
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        try:
            result = updateSessionInfo('review', self.session_info_out, self.\
                task.instance_data['uri'], get_hash( page ), 'forum', self.task.instance_data.get('update'))
            if result['updated']:
                page['path'] = [self.task.instance_data['uri']] 
                page['parent_path'] = []
                page['uri'] = self.currenturi
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                page['data'] = ''
                page['entity'] = 'thread'
                page.update(self.__task_elements_dict)
                page['posted_date'] = page['pickup_date']
                self.pages.append(page)
                log.info(self.log_msg('Parent Page Added'))
            else:
                log.info(self.log_msg('Result[updated] returned True for \
                                                        uri'%self.currenturi))
        except:
            log.exception(self.log_msg("parent post couldn't be parsed"))
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = self.soup.findAll('div', 'fPostReply')
            if not posts:
                log.info(self.log_msg('No posts found in url %s'%self.currenturi))
                return False
            posts.reverse()
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
    def __addPost(self, post, is_question=False):
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            unique_key = post.find('a', attrs={'name':True})['name']
            permalink = self.currenturi + '#' + unique_key
            if checkSessionInfo(self.__genre, self.session_info_out, \
                        unique_key, self.task.instance_data.get('update'),\
                        parent_list=[self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'% \
                                                                permalink))
                return False
            page = self.__getData(post, is_question, unique_key)
            if not page:
                log.info(self.log_msg('page contains empty data, getdata \
                                    returns  False for uri %s'%self.currenturi))
                return True
            result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                get_hash( page ),'forum', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if result['updated']:
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [self.task.instance_data['uri'], unique_key ]
                page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
                page.update(self.__task_elements_dict)
                if is_question:
                    self.pages.insert(0, page)
                else:
                    self.pages.append(page)
            else:
                log.info(self.log_msg('Update session info returns False for \
                                                    url %s'%self.currenturi))
        except:
            log.exception(self.log_msg('Cannot add the post for the uri %s'%self.currenturi))
        return True
    
    @logit(log, '__getData')
    def __getData(self, post, is_question, unique_key):
        """ This will return the page dictionry
        """
        page = {'entity':'question' if is_question else 'answer'}
        try:
            page['uri'] = self.currenturi + '#' + str(post.find('span', 'wintiny').a['href'])
        except:
            log.info(self.log_msg('uri not found in %s'% self.currenturi))
            page['uri'] = self.currenturi
        try:
            page['data'] = stripHtml(post.find('div', 'fMsg').renderContents())
        except:
            log.info(self.log_msg('Empty data found in %s'%page['uri']))
            return 
        try:
            date_str = [x.strip() for x in stripHtml(post.find('div', 'msgHead').findAll('div','floatl')[-1].renderContents()).splitlines() if x.strip()][-2]
            date_object = datetime.strptime(date_str, 'Posted: %b-%d %I:%M %p')
            current_year = datetime.utcnow().year
            if date_object.month > datetime.utcnow().month:
                current_year -=  1
            my_object = datetime(year=current_year, month=date_object.month, day=date_object.day, hour=date_object.hour, minute=date_object.minute)
            page['posted_date'] = datetime.strftime(my_object, "%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.exception(self.log_msg('Posted date not found'))
        try:
            page['et_author_name'] = stripHtml(post.find('div', id=re.compile('from\-')).renderContents()).replace('From: ','').strip()
        except:
            log.info(self.log_msg('Author name not found in %s'% self.currenturi))
        if len(self.__hierarchy) >= 2:
            page['title'] = page['et_thread_topic'] = self.__hierarchy[-1]
            page['et_thread_forum'] = self.__hierarchy[-2]
            if not is_question:
                page['title'] = 'Re: ' + page['title']
        return page
    
    @logit(log, '__goToLastPage')
    def __goToLastPage(self):
        '''This will find the Last Page and set the Soup
        '''
        try:
            self.currenturi = self.__baseuri +  self.soup.find('div', 'pagination').findAll('a', text=re.compile('^\d+$'))[-1].parent['href']
            self.__setSoupForCurrentUri()
        except:
            log.info(self.log_msg('Last page tag not found'))            
                        
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
