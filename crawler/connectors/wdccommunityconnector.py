'''
Copyright (c)2008-2010 Serendio Software Private Limited
All Rights Reserved
This software is confidential and proprietary information of Serendio Software.
It is disclosed pursuant to a non-disclosure agreement between the recipient 
and Serendio. This source code is provided for informational purposes only, 
and Serendio makes no warranties, either express or implied, in this. 
Information in this program, including URL and other Internet website 
references, is subject to change without notice. The entire risk of the use 
or the results of the use of this program remains with the user. Complying 
with all applicable copyright laws is the responsibility of the user. Without 
limiting the rights under copyright, no part of this program may be reproduced,
stored in, or introduced into a retrieval system, or distributed or 
transmitted in any form or by any means (electronic, mechanical, photocopying,
recording, on a website, or otherwise) or for any purpose, without the express 
written permission of Serendio Software.
Serendio may have patents, patent applications, trademarks, copyrights, or 
other intellectual property rights covering subject matter in this program. 
Except as expressly provided in any written license agreement from Serendio, 
the furnishing of this program does not give you any license to these patents, 
trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('WdcCommunityConnector')
class WdcCommunityConnector(BaseConnector):
    '''
    This will fetch the info for forums.seagate.com
    Sample uris is
    http://community.wdc.com/t5/WD-TV-HD/bd-p/tv_hd
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of forums.seagate.com
        """
        try:
            self.__setSoupForCurrentUri()
            if '/bd-p/' in self.currenturi:
                return self.__createTasksForThreads()
            else:
                return self.__addThreadAndPosts()
                #this will fetch the thread links and Adds Tasks                
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'\
                                                            %self.currenturi))
            return False
        
    @logit(log, '__addThreadAndPosts')
    def __addThreadAndPosts(self):
        """
        This will add the Thread info from setParentPage method and 
        Add the posts of the thread in addPosts mehtod
        """
        self.__genre = "Review"
        self.__hierarchy = []
        self.__baseuri = 'http://community.wdc.com'
        self.__task_elements_dict = {
                        'priority':self.task.priority,
                        'level': self.task.level,
                        'last_updated_time':datetime.strftime(datetime.utcnow()
                                                    , "%Y-%m-%dT%H:%M:%SZ"), 
                        'pickup_date':datetime.strftime(datetime.utcnow(),  
                                                        "%Y-%m-%dT%H:%M:%SZ"), 
                        'connector_instance_log_id': \
                                        self.task.connector_instance_log_id, 
                        'connector_instance_id':
                                            self.task.connector_instance_id, 
                        'workspace_id':self.task.workspace_id, 
                        'client_id':self.task.client_id, 
                        'client_name':self.task.client_name, 
                        'versioned':False, 
                        'category':self.task.instance_data.get('category',''), 
                        'task_log_id':self.task.id }
        self.__setParentPage()
        question_post = self.soup.find('div', 'lia-message-view')
        self.__addPost(question_post, True)
        self.__goToLastPage()
        while self.__iteratePosts():
            try:
                self.currenturi = self.__baseuri + self.soup.find('ul', 'lia-paging-full').find('span', text='Next').findParent('a')['href'].split(';')[0]
                self.__setSoupForCurrentUri()
            except:
                log.info(self.log_msg('No Previous URL found for url \
                                                    %s'%self.currenturi))
                break
        return True
        
    @logit(log, '__createTasksForThreads')
    def __createTasksForThreads(self):
        """
        This will create Tasks for the threads found on the given url
        The # of Tasks are limited by Config Variable
        """
        self.__total_threads_count = 0
        self.__baseuri = 'http://community.wdc.com'
        self.__last_timestamp = datetime(1980, 1, 1)
        #The Maximum No of threads to process, Bcoz, not all the forums get
        #updated Everyday, At maximum It will 100
        self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                            'wdccommunity_maxthreads'))
        self.__setSoupForCurrentUri()
        while self.__getThreads():
            try:
                self.currenturi = self.__baseuri + self.soup.find('a', \
                        text='Next').findParent('a')['href'].split(';')[0]
                self.__setSoupForCurrentUri()
            except:
                log.info(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                break
        #self.linksOut = []
        if self.linksOut:
            updateSessionInfo('Search', self.session_info_out, \
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
            threads = self.soup.find('div', id='messageList').find('table', \
                        'lia-list-wide').tbody.findAll('tr', recursive=False)
            if not threads:
                log.info(self.log_msg('No threads are found for url %s'%\
                                                            self.currenturi))
                return False
        except:
            log.info(self.log_msg('exception while getting threads'))
            return False
        for thread in threads:
            if 'lia-list-row-float' in thread.get('class',''):
                log.info(self.log_msg('Its a Sticky Thread, Ignore it in\
                                            the url %s'%self.currenturi))
                continue
            self.__total_threads_count += 1
            if  self.__total_threads_count > self.__max_threads_count:
                log.info(self.log_msg('Reaching maximum post,Return false \
                                        from the url %s'%self.currenturi))
                return False
            try:
                date_tag = thread.find('span', 'local-friendly-date', title=True)
                if date_tag:
                    date_str = date_tag['title'].__str__()
                else:
                    date_str = re.sub('\s+', ' ',stripHtml(thread.find('span',\
                                                'DateTime').renderContents()))
                thread_time = datetime.strptime(date_str, '%m-%d-%Y %I:%M %p')
            except:
                log.exception(self.log_msg('data not found in %s'%\
                                                        self.currenturi))
                continue
            if checkSessionInfo('Search', self.session_info_out, thread_time, \
                                 self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info Returns True for %s'%\
                                                        self.currenturi))
                return False
            self.__last_timestamp = max(thread_time, self.__last_timestamp)
            temp_task = self.task.clone()                    
            try:
                temp_task.instance_data[ 'uri' ] = self.__baseuri + thread\
                    .find('h2', 'message-subject').a['href'].split(';')[0]
            except:
                log.exception(self.log_msg('Cannot find the thread url \
                                        in the uri %s'%self.currenturi))
                continue
            temp_task.pagedata['edate_last_post_date'] =  datetime.\
                            strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
            temp_task.pagedata['et_thread_last_post_author'] = stripHtml\
                    (thread.find('div', 'MessagePostDateAndAuthorCell')\
                            .renderContents()).splitlines()[-1].strip()
            try:
                temp_task.pagedata['et_author_name'] = stripHtml(thread.\
                            find('div', 'lia-user-name').renderContents())
            except:
                log.info(self.log_msg('Author name not found in the url\
                                                    %s'%self.currenturi))
            try:
                temp_task.pagedata['ei_thread_replies_count'] = int(stripHtml\
                    (thread.find('td', attrs={'class':re.compile\
                            ('repliesCountColumn')}).renderContents()))

            except:
                log.info(self.log_msg('Views count not found in the url\
                                                %s'%self.currenturi))                
            try:
                temp_task.pagedata['ei_thread_views_count'] = int(stripHtml\
                    (thread.find('td', attrs={'class':re.compile\
                            ('viewsCountColumn')}).renderContents()))
            except:
                log.info(self.log_msg('Views count not found in the url\
                                                %s'%self.currenturi))                
            self.linksOut.append(temp_task)
        return True
    
    @logit(log, '__setParentPage')
    def __setParentPage(self):
        """
        This will get the parent info
        """
        page = {}
        try:
            page['et_thread_hierarchy'] = self.__hierarchy = [x.strip() for x \
                in stripHtml(self.soup.find('ul','lia-list-standard-inline')\
                                .renderContents()).split(':') if x.strip()][1:]
            page['data'] = page['title'] = page['et_thread_hierarchy'][-1]
        except:
            log.info(self.log_msg('Thread hierarchy and Title Not found for uri\
                                                            %s'%self.currenturi))
            return
        if checkSessionInfo(self.__genre, self.session_info_out, self.task.\
                instance_data['uri'], self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        page_data_keys = ['et_author_name', 'ei_thread_replies_count', \
                            'ei_thread_replies_count','edate_last_post_date']
        [page.update({each:self.task.pagedata.get(each)}) for each in page_data_keys if self.task.pagedata.get(each)]
        try:
            date_str = re.sub('\s+', ' ', stripHtml(self.soup.find('div', \
                'lia-panel-message-content').find('span', \
                'DateTime lia-message-posted-on lia-component-common-widget-date')\
                                                            .renderContents()))            
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str, \
                                '%m-%d-%Y %I:%M %p'), '%Y-%m-%dT%H:%M:%SZ')
        except:
            log.info(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), \
                                                        "%Y-%m-%dT%H:%M:%SZ")
        try:
            result = updateSessionInfo('review', self.session_info_out, self.\
                task.instance_data['uri'], get_hash( page ), 'forum', \
                                    self.task.instance_data.get('update'))
            if result['updated']:
                page['path'] = [ self.task.instance_data['uri'] ] 
                page['parent_path'] = []
                page['uri'] = self.currenturi
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])                
                page['entity'] = 'thread'
                page.update(self.__task_elements_dict)
                self.pages.append(page)
            else:
                log.info(self.log_msg('Result[updated] returned True for \
                                                        uri'%self.currenturi))
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))
            
    @logit(log, '__goToLastPage')
    def __goToLastPage(self):
        """This will set the soup the last page of the post
        """
        try:
            self.currenturi = self.__baseuri + self.soup.find('li', \
                'lia-component-pagesnumbered').findAll('a', text=re.compile\
                                    ('^\d+$'))[-1].parent['href'].split(';')[0]
            self.__setSoupForCurrentUri()
        except:
            log.info(self.log_msg('Last page cannot find from the given page no \
                                    for url %s'%self.task.instance_data['uri']))
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = self.soup.findAll('div', 'lia-message-view')
            if not posts:
                log.info(self.log_msg('No posts found'))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(posts)))
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
            unique_key = post.find('a')['name']
            if checkSessionInfo(self.__genre, self.session_info_out, unique_key, \
                             self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'%unique_key))
                return False
            page = self.__getData(post, is_question)
            if not page:
                log.info(self.log_msg('page contains empty data, getdata \
                                    returns  False for uri %s'%self.currenturi))
                return True
            result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                get_hash( page ),'forum', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if result['updated']:
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [ self.task.instance_data['uri'], unique_key]
                page['uri'] = self.currenturi + '#' + unique_key
                page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
                page.update(self.__task_elements_dict)
                self.pages.append(page)
            else:
                log.info(self.log_msg('Update session info returns False for \
                                                    url %s'%self.currenturi))
        except:
            log.exception(self.log_msg('Cannot add the post for the uri %s'%self.currenturi))
        return True
    
    @logit(log, '__getData')
    def __getData(self, post, is_question):
        """ This will return the page dictionry
        """
        page = {'entity':'question' if is_question else 'answer'}
        try:
            page['title'] = stripHtml(post.find('div', 'lia-message-subject').h1.renderContents())
        except:
            log.info(self.log_msg('Title not found'))
            page['title'] = ''
        try:
            data_tag = post.find('div', 'lia-message-body-content')
            [x.extract() for x in data_tag.findAll('blockquote')]
            user_signature = post.find('div', 'UserSignature lia-message-signature')
            if user_signature:
                user_signature.extract()
            page['data'] = stripHtml(data_tag.renderContents())
        except:
            log.info(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''
        #Sometimes only Image is uploaded on the Post, in that case data will be empty
        if not page['data'] and page['title']: 
            log.info(self.log_msg("Data and Title are not found for %s,discarding this Post"%(self.currenturi)))
            return False 
        try:
            date_tag = post.find('span', 'local-friendly-date', title=True)
            if date_tag:
                date_str = date_tag['title'].__str__()
            else:
                date_str = re.sub('\s+', ' ', stripHtml(post.find('span', 'DateTime lia-message-posted-on lia-component-common-widget-date').renderContents()))            
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str, '%m-%d-%Y %I:%M %p'), '%Y-%m-%dT%H:%M:%SZ')
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        try:
            author_tag = post.find('div', 'lia-message-author-username')
            page['et_author_name'] = stripHtml(author_tag.renderContents())
        except:
            log.info(self.log_msg('author name not found'))
        try:
            page['et_author_category'] = stripHtml(post.find('div','lia-message-author-rank').renderContents())
        except:
            log.info(self.log_msg('author category not found'))
        try:
            page['et_author_profile'] =  self.__baseuri + author_tag.a['href'].split(';')[0]
        except:
            log.info(self.log_msg('author profile not found'))
        try:
            page['ei_author_posts_count'] = int(stripHtml(post.find('div', 'lia-message-author-post-count').renderContents()).split(':')[-1].strip())
        except:
            log.info(self.log_msg('author posts count not found'))
        try:
            author_date_tag = post.find('div', 'lia-message-author-registered-date')
            date_tag = author_date_tag.find('span', 'local-friendly-date', title=True)
            if date_tag:
                page['edate_author_member_since'] = datetime.strftime(datetime.strptime(date_tag['title'].__str__(), '%m-%d-%Y %I:%M %p')                , '%Y-%m-%dT%H:%M:%SZ')
            else:
                date_str = re.sub('\s+', ' ', stripHtml(author_date_tag.renderContents())).split(':')[-1].strip()
                page['edate_author_member_since'] = datetime.strftime(datetime.strptime(date_str, '%m-%d-%Y'), '%Y-%m-%dT%H:%M:%SZ')
        except:
            log.exception(self.log_msg('author registered date not found'))
        try:
            page['ef_data_rating'] = float(stripHtml(post.find('span' ,'MessageKudosCount').renderContents()))
        except:
            log.info(self.log_msg('Rating not found'))
        try:
            page['ei_data_views_count'] = int(re.sub('[^\d+]', '', stripHtml(post.find('div', 'lia-message-statistics').findAll('span')[-1].renderContents())))
        except:
            log.info(self.log_msg('datda views count not found'))            
        if len(self.__hierarchy) >= 3:
            page['et_thread_topic'] = self.__hierarchy[-1]
            page['et_thread_forum'] = self.__hierarchy[-3]
            page['et_thread_subforum'] = self.__hierarchy[-2]
        else:
            log.info(self.log_msg('Cannot find the Data thread details'))
        return page 
            
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