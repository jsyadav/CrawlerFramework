'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved
This software is confidential and proprietary information of Serendio Software.
It is disclosed pursuant to a non-disclosure agreement between the recipient
and Serendio. This source code is provided for informational purposes only, and
Serendio makes no warranties, either express or implied, in this. Information 
in this program, including URL and other Internet website references, is 
subject to change without notice. The entire risk of the use or the results of 
the use of this program remains with the user. Complying with all applicable 
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

#Rakesh Soni
#Skumar

import re
import logging
from urllib2 import urlparse
from datetime import datetime

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('FicoForumsConnector')
class FicoForumsConnector(BaseConnector):
    '''Connector for FicoForums.com
    '''
        
    @logit(log , 'fetch')
    def fetch(self):
        """
        fetch method of FicoForumsConnector
        http://ficoforums.myfico.com/fico/search?f=subject&f=body&one_or_more=&
        page_size=50&author=&q=citibank&phrase=&submitted=true&advanced=true&
        type=message&without=&sort_by=-date
        http://ficoforums.myfico.com/fico/board/message?board.id=creditcard&
                                                            thread.id=203609
        http://ficoforums.myfico.com/fico/board?board.id=generalcredit
        """
        try:
            self.__baseuri = 'http://ficoforums.myfico.com'
            self.__total_threads_count = 0
            self.__last_timestamp = datetime(1980, 1, 1)
            self.__task_elements_dict = {
                    'priority':self.task.priority,
                    'level': self.task.level,
                    'last_updated_time':datetime.strftime(datetime.utcnow()
                                                , "%Y-%m-%dT%H:%M:%SZ"),
                    'pickup_date':datetime.strftime(datetime.utcnow(), \
                                                    "%Y-%m-%dT%H:%M:%SZ"),
                    'connector_instance_log_id': self.task.\
                                                connector_instance_log_id,
                    'connector_instance_id':self.task.connector_instance_id,
                    'workspace_id':self.task.workspace_id,
                    'client_id':self.task.client_id,
                    'client_name':self.task.client_name,
                    'versioned':False,
                    'category':self.task.instance_data.get('category',''),
                    'task_log_id':self.task.id }
            self.__max_threads_count = int(tg.config.get(path='Connector', \
                                key='ficoforums_max_threads_to_process'))
            self.__setSoupForCurrentUri()
            is_search_type = self.currenturi.startswith\
                                ('http://ficoforums.myfico.com/fico/search')
            is_forum_type = self.currenturi.startswith\
                            ('http://ficoforums.myfico.com/fico/board?board')
            if is_forum_type or is_search_type:
                while True:
                    try:
                        results =  self.__getThreads() if is_search_type else \
                                                        self.__addThreadUrls()
                        if not results:
                            log.info(self.log_msg('Reached Maxmum threads\
                                                /Reached Last Crawled Page'))
                            break
                        self.currenturi = self.__baseuri + \
                            re.search('go\(\'(.*?)\'\)', self.soup.find('a', \
                            text='Next Page').findParent('table')['onclick'])\
                                                                    .group(1)
                        self.__setSoupForCurrentUri()
                    except Exception, exce:
                        log.exception(self.log_msg('Cannot found the next page\
                                                in url %s'%self.currenturi))
                        break
                if self.linksOut:
                    updateSessionInfo('Search', self.session_info_out, \
                            self.__last_timestamp , None, 'ForumThreadsPage', \
                            self.task.instance_data.get('update'))
                return True
            elif self.currenturi.startswith\
                            ('http://ficoforums.myfico.com/fico/board/message'):
                self.__setParentPage()
                question_post = self.soup.find('tr', attrs={'id':\
                                                        re.compile('M\d+')})
                self.__addPost(question_post, True)
                self.currenturi = self.currenturi + '&view=by_date_descending'
                self.__setSoupForCurrentUri()
                while True:
                    try:
                        if not self.__iteratePosts():
                            log.info(self.log_msg('Crawled all posts in url %s'\
                                                            %self.currenturi))
                            break
                        self.currenturi = self.__baseuri + self.soup.find('a', 
                                text=re.compile('Next Page')).parent['href']
                        self.__setSoupForCurrentUri()
                    except Exception, exce:
                        log.info(self.log_msg('Crawled all pages, no more page\
                                                        %s'%self.currenturi))
                        break
            return True
        except Exception, exce:
            log.exception(self.log_msg('Exception in fetch'))
            return False
        
    @logit(log, '__addPost')
    def __addPost(self, post, is_question=False):
        '''This will take a Post Div and Parse Info and add it to self.pages
        '''
        if checkSessionInfo('review', self.session_info_out, post['id'],
                                self.task.instance_data.get('update'),
                                parent_list =[self.task.instance_data['uri']]):
            log.info(self.log_msg('session info return True in url %s'%\
                                                            self.currenturi))
            return False
        page = {}
        page['uri'] = self.currenturi + '#' + post['id']
        page['entity'] = page['et_data_post_type'] = 'question' if is_question\
                                                                else 'answer'
        try:
            page['et_author_name'] = stripHtml(post.find('a', 'auth_text')\
                                                            .renderContents())
        except Exception, exce:
            log.info(self.log_msg('Author name not available in url %s'\
                                                            %self.currenturi))
        try:
            author_info = [x.strip() for x in stripHtml(post.find('td', \
                'msg_user_cell').renderContents()).split('\n') if x.strip()]
            page['et_author_title'] = author_info[1]
        except Exception, exce:
            log.info(self.log_msg('Author title not available in url %s'\
                                                            %self.currenturi))
        try:
            page['ei_author_posts_count'] = int(author_info[2].split(':')[-1]\
                                                            .replace(',',''))
        except Exception, exce:
            log.info(self.log_msg('Author Posts count not available in url %s'\
                                                            %self.currenturi))
        try:
            page['ei_data_views_count'] = int(re.search('\d+', author_info[-1]\
                                    .split(':')[-1].replace(',', '')).group())
        except Exception, exce:
            log.info(self.log_msg('Data view count not available in url %s'\
                                                            %self.currenturi))
        try:
            date_str = author_info[3].split(':')[-1].strip()
            page['edate_author_registration_date'] = datetime.strftime\
                                (datetime.strptime (date_str, '%m-%d-%Y'), \
                                                        "%Y-%m-%dT%H:%M:%SZ")
        except Exception, exce:
            log.info(self.log_msg('Author registration date not available in \
                                                    url %s'%self.currenturi))
        try:
            date_str = stripHtml(post.find('td', 'msg_date_cell')\
                                                            .renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(\
                        date_str, '%m-%d-%Y %I:%M %p'), "%Y-%m-%dT%H:%M:%SZ")
        except Exception, exce:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), \
                                                        "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('posted date not found, taking current date \
                                                in  url %s'%self.currenturi))
        try:
            data_tag = post.find('table', 'msg_table')
            tags_to_remove = data_tag.findAll('blockquote') # Quoted Post
            [tag.extract() for tag in tags_to_remove]                    
            page['data'] = stripHtml(data_tag.renderContents())
        except Exception, exce:
            log.exception(self.log_msg('Data cannot be fetched in url %s'\
                                                            %self.currenturi))
            page['data'] = ''
        try:
            page['title'] = stripHtml(post.findAll('td', 'subjectbar')[1].\
                                    renderContents()).split('\n')[0].strip()
        except Exception, exce:
            log.info(self.log_msg('Title not found in url %s'%self.currenturi))
            page['title'] = ''
        if not page['title'] and page['data']:
            log.info(self.log_msg('No data found in url %s'%self.currenturi))
            return True # Ignore this posts, and fetch rest of the posts
        try:
            if len(self.__hierarchy)==4:
                page['et_data_forum'] = self.__hierarchy[1]
                page['et_data_subforum'] = self.__hierarchy[2]
                page['et_data_topic'] = self.__hierarchy[3]
        except Exception, exce:
            log.info(self.log_msg('data forum not found'))
        try:
            result = updateSessionInfo('review', self.session_info_out, \
                post['id'], get_hash(page), 'Review', self.task.instance_data\
                .get('update'), parent_list=[self.task.instance_data['uri']])
            if result['updated']:
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [self.task.instance_data['uri'], post['id']]
                page['category'] = self.task.instance_data.get('category','')
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                page.update(self.__task_elements_dict)
                self.pages.append(page)
            else:
                log.info(self.log_msg('Result not updated in url %s'\
                                                            %self.currenturi))
        except Exception, exce:
            log.exception(self.log_msg('Error while adding session info in url\
                                                        %s'%self.currenturi))
        return True        
            
        
    @logit(log , '__IteratePosts')
    def __iteratePosts(self):
        """Get All the posts in the current page
        """
        for post in self.soup.findAll('tr', attrs={'id':re.compile('M\d+')}):            
            if not self.__addPost(post):
                return False
        return True
    
    
    @logit(log, '__getParentPage')
    def __setParentPage(self):
        """fetch the Info about the Threads Page
        """
        page = {}
        try:
            self.__hierarchy = page['et_thread_hierarchy'] = [x.strip() for x \
                in stripHtml(self.soup.find('td', 'navbar').renderContents())\
                .split('>')]
            page['title'] = self.__hierarchy[-1]
        except Exception, exce:
            log.exception(self.log_msg('Thread hierarchy not found'))
            return
        page_elements = ['title', 'posted_date', 'et_author_name', \
                        'ei_thread_replies_count', 'ei_thread_views_count']
        [page.update({each:self.task.pagedata[each]}) for each in page_elements
                                            if self.task.pagedata.get(each)]
        try:
            if not page.has_key('posted_date'):
                date_str = stripHtml(self.soup.find('td', 'msg_date_cell')\
                                                            .renderContents())
                page['posted_date'] = datetime.strftime(datetime.strptime\
                        (date_str, '%m-%d-%Y %I:%M %p'), "%Y-%m-%dT%H:%M:%SZ")
        except Exception, exce:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), \
                                                        "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('posted date not found, taking current date \
                                                in  url %s'%self.currenturi))
        try:
            result = updateSessionInfo('review', self.session_info_out, \
                self.task.instance_data['uri'], get_hash(page), 'Post', \
                                        self.task.instance_data.get('update'))
            if result['updated']:
                page['path'] = [self.task.instance_data['uri']]
                page['parent_path'] = []
                page['uri'] = self.task.instance_data['uri']
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                page['data'] = ''
                page['entity'] = 'thread'
                page.update(self.__task_elements_dict)
                self.pages.append(page)
        except Exception, exce:
            log.exception(self.log_msg("parent post couldn't be parsed in url \
                                                        %s"%self.currenturi))
            
    @logit(log, '__addThreadUrls')
    def __addThreadUrls(self):
        '''This will add the thread urls and create tasks
        '''
        threads = [each.findParent('tr') for each in self.soup.findAll('a', \
                                                    'subj_unread',href=True)]
        #log.info(len(threads))
        if not threads:
            log.info(self.log_msg('No threads are found for url %s'%\
                                                        self.currenturi))
            return False
        for thread in threads:
            if thread.find('img', src=re.compile\
                                            ('icon_thread_readonly_new.gif')):
                log.info(self.log_msg('Its a Sticky Thread, Ignore it in\
                                            the url %s'%self.currenturi))
                continue
            if  self.__total_threads_count >= self.__max_threads_count:
                log.info(self.log_msg('Reaching maximum post,Return false \
                                        from the url %s'%self.currenturi))
                return False
            self.__total_threads_count += 1
            # Each Thread is a table record, Consisting of Status Icon,
            #Image,URI and Posted date,views and Replies in respective TD tag
            try:
                thread_info = thread.findAll('td', recursive=False)
                if not thread_info:
                    log.info(self.log_msg('No thread info found in url %s'\
                                                            %self.currenturi))
                    continue
                date_and_author = [x.strip() for x in stripHtml(thread_info[5]\
                                .renderContents()).split('\n') if x.strip()]
                thread_time  = datetime.strptime(re.sub('\s+', ' ', \
                                    date_and_author[0]), '%m-%d-%Y %I:%M %p')
            except Exception, exce:
                log.exception(self.log_msg('Cannot fetch the date for the url\
                                                        %s'%self.currenturi))
                continue
            if checkSessionInfo('Search', self.session_info_out, thread_time, \
                                    self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info Returns True for url\
                                                        %s'%self.currenturi))
                return False
            self.__last_timestamp = max(thread_time , self.__last_timestamp )
            temp_task = self.task.clone()                   
            temp_task.pagedata['edate_last_post_date'] =  datetime.\
                            strftime(thread_time, "%Y-%m-%dT%H:%M:%SZ")
            temp_task.pagedata['et_thread_last_post_author']  =  \
                                                            date_and_author[-1]
            try:
                temp_task.instance_data['uri'] = self.__baseuri + thread.find\
                                        ('a','subj_unread',href=True)['href']
            except Exception, exce:
                log.info(self.log_msg('Thread url not found in url %s'\
                                                            %self.currenturi))
                continue
            try:
                temp_task.pagedata['et_author_name'] = stripHtml(thread_info[4]
                                                            .renderContents())
            except Exception, exce:
                log.info(self.log_msg('Author name not found in the url\
                                                    %s'%self.currenturi))
            try:
                temp_task.pagedata['ei_thread_views_count'] = int(stripHtml(\
                        thread_info[2].renderContents()).replace(',', ''))
            except Exception, exce:
                log.info(self.log_msg('Views count not found in the url\
                                                %s'%self.currenturi))
            try:
                temp_task.pagedata['ei_thread_replies_count'] = int(stripHtml\
                        (thread_info[3].renderContents()).replace(',', ''))
            except Exception, exce:
                log.info(self.log_msg('Replies count not found in the url\
                                                %s'%self.currenturi))            
            self.linksOut.append(temp_task)
            log.info(self.log_msg('Task Added'))
        return True
        
    @logit(log , '__getThreads')
    def __getThreads( self ):
        """ Get thread information and create tasks.
        """
        threads = [each.findParent('tr') for each in self.soup.findAll('a', 
                                                            'SUBJECT_STYLE')]
        if not threads:
            log.info(self.log_msg('No Results in url %s'%self.currenturi))
            return False
        for thread in threads:
            if self.__total_threads_count > self.__max_threads_count:
                log.info(self.log_msg('Reaching maximum post,Return false'))
                return False
            self.__total_threads_count +=  1
            thread_info = thread.findAll('td', recursive=False)
            try:
                date_str = re.sub('\s+', ' ', stripHtml(thread_info[-1].\
                                                            renderContents()))
                thread_time = datetime.strptime(date_str, "%m-%d-%Y %I:%M %p")
            except Exception, exce:
                log.info(self.log_msg('Posted date not found in url %s'\
                                                            %self.currenturi))
                continue
            if checkSessionInfo('Search', self.session_info_out, thread_time, 
                                        self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info return True'))
                return False
            self.__last_timestamp = max(thread_time, self.__last_timestamp)
            temp_task =  self.task.clone()
            try:                
                temp_task.pagedata['title'] = stripHtml(thread.find('a', \
                        'SUBJECT_STYLE').renderContents().strip()).split(':')\
                                                                [-1].strip()
            except Exception, exce:
                log.info(self.log_msg('Thread title not available'))
            try:
                temp_task.instance_data['uri'] = self.__baseuri + thread.find\
                            ('a', 'SUBJECT_STYLE')['href'].split('#')[0]
            except Exception, exce:
                log.info(self.log_msg('Thread uri not available'))
                continue
            try:
                temp_task.pagedata['et_author_name'] = stripHtml(thread.find\
                                        ('a', 'auth_text').renderContents())
            except Exception, exce:
                log.exception(self.log_msg('Thread author name not available'))
            try:
                temp_task.pagedata['ei_thread_replies_count'] = int(\
                            stripHtml(thread_info[-3].renderContents())\
                                                        .replace(',', ''))
            except Exception, exce:
                log.exception(self.log_msg('replies count not available'))
            try:
                temp_task.pagedata['ei_thread_views_count'] = int(stripHtml\
                        (thread_info[-2].renderContents()).replace(',', ''))
            except Exception, exce:
                log.exception(self.log_msg('Thread views count not available'))
            self.linksOut.append(temp_task)
        return True
    
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

