'''
Copyright (c)2008-2010 Serendio Software Private Limited
All Rights Reserved
This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Packiaraj

import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime,timedelta

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('FinancialAidForumConnector')
class FinancialAidForumConnector(BaseConnector):
    '''
    This will fetch the info for financialaidforum.com
    Sample uris is
    forum url: http://www.financialaidforum.com/discussions/student-loans/
    thread url: http://www.financialaidforum.com/discussions/federal-loans/left-school-for-a-year-now-want-to-return-stafford-loan-q-t1590.html
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """ Fetch of financialaidforum.com """
        try:
            self.__setSoupForCurrentUri()
            if re.search('t\d+.html$',self.currenturi):
                #Its a Thread page, Which will fetch the thread links and Adds Tasks
                return self.__addThreadAndPosts()
            else:
                return self.__createTasksForThreads()
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'%self.currenturi))
            return False
        
    @logit(log, '__addThreadAndPosts')
    def __addThreadAndPosts(self):
        """
        This will add the Thread info from setParentPage method and 
        Add the posts of the thread in addPosts mehtod """
        self.__genre = "Review"
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
        question_post = self.soup.find('div',id='page-body')
        if question_post:
            self.__addPost(question_post, True)
        self.__goToLastPage()
        while self.__iteratePosts():
            try:
                self.currenturi =  self.soup.find('div','pagination').find('span')\
                                        .find('a')['href'] # Previous page
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
        self.__last_timestamp = datetime( 1980,1,1 )
        # The Maximum No of threads to process, Bcoz, not all the forums get
        # updated Everyday, At maximum It will 100
        self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                            'financialaid_maxthreads'))
        #self.__max_threads_count = 50
        while self.__getThreads():
            try:
                self.currenturi =  self.soup.find('a',text='Next').parent['href']
                self.__setSoupForCurrentUri()
            except:
                log.info(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                break
        if self.linksOut:
            updateSessionInfo('Search', self.session_info_out,\
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
            threads = self.soup.find('ul','topiclist topics').findAll('li', recursive=False)
            if not threads:
                log.info(self.log_msg('No threads are found for url %s'%\
                                                            self.currenturi))
                return False
            for thread in threads:
                try:
                    self.__total_threads_count += 1
                    if  self.__total_threads_count > self.__max_threads_count:
                        log.info(self.log_msg('Reaching maximum post,Return false \
                                                from the url %s'%self.currenturi))
                        return False
                    try:
                        last_post_author , date_str  = [x.strip() for x in stripHtml\
                                (thread.find('dd','lastpost').renderContents()).\
                                                        split('\n') if x.strip()]
                        thread_time  = datetime.strptime (date_str, '%a %b %d, %Y %I:%M %p')
                    except:
                        log.exception(self.log_msg('Cannot fetch the date for the url\
                                                                %s'%self.currenturi))
                        continue
                    if checkSessionInfo('Search', self.session_info_out, thread_time,\
                                            self.task.instance_data.get('update')):
                        log.info(self.log_msg('Session info Returns True for url %s'%self.currenturi))
                        return False
                    self.__last_timestamp = max(thread_time , self.__last_timestamp )
                    temp_task = self.task.clone()
                    title_tag = thread.find('a', 'topictitle', href=True)
                    if not title_tag:
                        log.info(self.log_msg('Cannot find the thread url \
                                            in the uri %s'%self.currenturi))
                    temp_task.instance_data[ 'uri' ] = title_tag['href']
                    temp_task.pagedata['edate_last_post_date']=  datetime.\
                                    strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    temp_task.pagedata['et_thread_last_post_author'] = last_post_author.replace('Last post  by','').strip()
                    try:
                        temp_task.pagedata['et_author_name'] = stripHtml(str(title_tag.findParent('dt').findAll('a')[-1].next))
                    except:
                        log.info(self.log_msg('Author name not found in the url\
                                                            %s'%self.currenturi))
                    try:
                        date_str = stripHtml(str(title_tag.findParent('dt').findAll('a')[-1].next.next))
                        temp_task.pagedata['posted_date'] = datetime.strftime(datetime.strptime (date_str, '%a %b %d, %Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg('Posted date not found in the url\
                                                            %s'%self.currenturi))
                    try:
                        temp_task.pagedata['ei_thread_num_views'] = int(str(thread.find('dd', 'posts').next).strip())
                    except:
                        log.info(self.log_msg('Views count not found in the url\
                                                        %s'%self.currenturi))
                    try:
                        temp_task.pagedata['ei_thread_num_replies'] = int(str(thread.find('dd', 'views').next).strip())
                    except:
                        log.info(self.log_msg('Replies count not found in the url\
                                                        %s'%self.currenturi))
                    self.linksOut.append(temp_task)
                    
                except:
                    log.info(self.log_msg('No threads are not found in the url %s'%\
                                                self.currenturi))
            return True
        except:
            log.info(self.log_msg('Thread Page not found in the url %s'%\
                                                            self.currenturi))
                                                            
    @logit(log, '__setParentPage')
    def __setParentPage(self):
        """
        This will get the parent info
        """
        page = {}
        try:
            self.__hierarchy = page['et_thread_hierarchy'] =  [stripHtml(x.renderContents()) for x in self.soup.find('li', 'icon-home').findAll('a')]
            page['title'] = page['et_thread_hierarchy'][-1]
        except:
            log.info(self.log_msg('Thread hierarchy and Title Not found for uri\
                                                            %s'%self.currenturi))
            return
        if checkSessionInfo(self.__genre, self.session_info_out, self.task.instance_data['uri'],\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        page_data_keys = ['et_author_name','ei_thread_num_replies','ei_thread_num_views'\
                                                    ,'edate_last_post_date','posted_date']
        for key in page_data_keys:
            page_data_value = self.task.pagedata.get(key)
            if page_data_value:
                page[key] = page_data_value
        try:
            result = updateSessionInfo('review', self.session_info_out, self.\
                task.instance_data['uri'], get_hash(page), 'forum', self.task.instance_data.get('update'))
            if result['updated']:
                page['path']= [ self.task.instance_data['uri'] ]
                page['parent_path']= []
                page['uri'] = self.currenturi
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                page['data'] = ''
                page['entity'] = 'thread'
                page.update(self.__task_elements_dict)
                if not page.get('posted_date'):
                    page['posted_date'] = page['pickup_date']
                self.pages.append(page)
            else:
                log.info(self.log_msg('updated returned False for uri %s'%self.currenturi))
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))
            
    @logit(log, '__goToLastPage')
    def __goToLastPage(self): 
        """
        This will set the soup the last page of the post, from here 
        It needs to be navigated in the reverse order.
        If the posts are in within 3 pages, There won't be any Last Page Tag
        Then find max of page no ( 2 or 3) and Navigate to the page
        find out the List of posts and reverse it and then start adding it
        First add the Question, then above steps will be done, 
        session info will take of duplicates posts not adding to pages
        """
        try:
            pagination_tag = self.soup.find('div','pagination')
            if not pagination_tag:
                log.info(self.log_msg('pagination not found, posts exists in current\
                                                            url%s'%self.currenturi))
                return
            last_page_num = (int(stripHtml(str(pagination_tag.find('a').contents[-1]))) -1) * 15
            if not last_page_num>1:
                log.info(self.log_msg('pagination not found, posts exists in current\
                                                            url%s'%self.currenturi))
                return
            self.currenturi = self.currenturi.replace('.html','-' + str(last_page_num)+'.html')
            self.__setSoupForCurrentUri()
        except:
            log.info(self.log_msg('Last page cannot find from the given page no \
                                    for url %s'%self.task.instance_data['uri']))
            
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = self.soup.find('div',id='page-body').findAll('div','inner')
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
            unique_key_tag = post.find('div','postbody').find('h3').find('a')
            if not unique_key_tag and not unique_key_tag.get('href'):                
                log.info(self.log_msg('Permalink not found, ignoring the Post in the url %s'%self.currenturi))
                return True
            unique_key = unique_key_tag['href']
            if checkSessionInfo(self.__genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'%unique_key))
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
                page['parent_path'] = page['path'] = [self.task.instance_data['uri']]
                page['path'].append(unique_key)
                page['uri'] = unique_key
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
    def __getData(self, post, is_question, permalink):
        """ This will return the page dictionry
        """
        try:
            page = {'entity':'question' if is_question else 'answer'}
            try:
                data_tag = post.find('div','postbody').find('div','content')
                previous_message_tags = data_tag.findAll('b', text='Quote:')
                for previous_message_tag in previous_message_tags:
                    prev_tag = previous_message_tag.parent.findParent('div')
                    if prev_tag:
                        prev_tag.extract()
                page['data'] = stripHtml(data_tag.renderContents())
            except:
                log.info(self.log_msg('Data not found for the url %s'%permalink))
                page['data'] = ''
            post_info_tag = post.find('div','postbody')
            try:
                page['title'] = stripHtml(post_info_tag.find('h3').find('a').renderContents().replace('Re:',''))
            except:
                log.info(self.log_msg('Title not found for the url %s'%permalink))
                page['title'] = ''
            if not page['data'] and page['title']:
                log.info(self.log_msg("Data and Title are not found for %s,discarding this Post"%(permalink)))
                return False
            author_tag = post_info_tag.find('p','author').find('strong')
            if author_tag:
                page['et_author_name'] = stripHtml(author_tag.renderContents())
            else:
                log.info(self.log_msg('Author name tag not found for this Post'%permalink))
            date_str =  stripHtml(post.find('p','author').contents[-1])
            if date_str:
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str, '%a %b %d, %Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
            else:
                log.info(self.log_msg('Posted date not found for this Post'%permalink))
            aut_cat_tag = post.find('dl','postprofile').findAll('dd')
            try:
                page['et_author_category'] = stripHtml(aut_cat_tag[0].renderContents())
            except:
                log.info(self.log_msg('Author category not found for the url %s'%permalink))
            try:
                page['ei_author_posts_count'] = stripHtml(aut_cat_tag[2].renderContents().replace('Posts:',''))
            except:
                log.info(self.log_msg('Author post not found for the url %s'%permalink))
            try:
                page['et_author_location'] = stripHtml(aut_cat_tag[4].renderContents().replace('Location:',''))
            except:
                log.info(self.log_msg('Author location not found for the url %s'%permalink))
            if len(self.__hierarchy) >= 3:
                page['et_thread_topic'] = self.__hierarchy[-1]
                page['et_thread_forum'] = self.__hierarchy[-3]
                page['et_thread_subforum'] = self.__hierarchy[-2]
            else:
                log.info(self.log_msg('Cannot find the Data thread details for this Post'%permalink))
            author_post_date_since = stripHtml(aut_cat_tag[3].renderContents().replace('Joined:',''))
            try:
                page['et_author_post_since'] = datetime.strftime(datetime.strptime(author_post_date_since, '%a %b %d, %Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.info(self.log_msg('Author posted date since not found for this Post'%permalink))
            return page
        except:
            log.info(self.log_msg('Data not found for this Post'%permalink))
            
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