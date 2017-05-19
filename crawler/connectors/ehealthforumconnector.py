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

log = logging.getLogger('EHealthForumConnector')
class EHealthForumConnector(BaseConnector):
    '''
    This will fetch the info for ehealthforum.com
    Sample uris is
    http://ehealthforum.com/health/breast_cancer.html
    http://ehealthforum.com/health/topic45848.html
    http://ehealthforum.com/health/mens_sexual_health.html
    Note:Don't take Sticky Posts, it contains general Info about the forum
    '''
        
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of talk ehealthforums.com  
        """
        try:
            self.__genre = 'review'
            self.__baseuri = 'http://ehealthforum.com'
            self.__setSoupForCurrentUri()
            
            if not re.search('\d+\.html$', self.currenturi):
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
        question_post = self.soup.find('div', 'vt_first_postrow')
        if not question_post:
            log.info(self.log_msg('No posts found in url %s'%self.currenturi))
            return False
        self.__addPost(question_post, True)
        self.__goToLastPage()
        while self.__iteratePosts():
            try:
                self.currenturi = self.currenturi.rsplit('/',1)[0] + '/' + self.soup.find('div', 'pagination_container vt_pagination_container').find('a', text='&lt;&lt;').parent['href']
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
        self.__current_thread_count = 0
        self.__last_timestamp = datetime( 1980,1,1 )
        self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                            'ehealthforums_maxthreads'))
        while self.__getThreads():
            try:
                self.currenturi = self.currenturi.rsplit('/',1)[0] + '/' + self.soup.find('div', 'vt_pagination').find('a', text='&gt;&gt;').parent['href']
                self.__setSoupForCurrentUri()
            except:
                log.exception(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                break
        log.info('Total # of tasks found is %d'%len(self.linksOut))
        #self.linksOut = None
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
            threads = self.soup.find('div', 'fp_left').findAll('div', attrs={'class':re.compile('fp_topic_')}, recursive=False)
            if not threads:
                log.info(self.log_msg('No threads are found for url %s'%\
                                                            self.currenturi))
                return False
            for thread in threads[1:]:
                self.__current_thread_count += 1
                if  self.__current_thread_count > self.__max_threads_count:
                    log.info(self.log_msg('Reaching maximum post,Return false \
                                            from the url %s'%self.currenturi))
                    return False
                
                try:
                    date_str = stripHtml(thread.find('span', 'fp_last_post_time').renderContents())
                    thread_time  = datetime.strptime(date_str, 'Last post: %m-%d-%Y %H:%M%p')
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
                try:
                    temp_task.instance_data['uri'] = thread.find('a', 'topictitle')['href']
                except:
                    log.exception(self.log_msg('Cannot find the thread url \
                                            in the uri %s'%self.currenturi))
                    continue
                temp_task.pagedata['edate_last_post_date']=  datetime.\
                                strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                last_post_author = thread.find('div', 'fp_topic_last_post_author', title=True)
                if last_post_author:
                    temp_task.pagedata['et_thread_last_post_author']  =  last_post_author['title']
                try:
                    author_tag = thread.find('span', 'fp_topic_author')
                    author_name_tag = author_tag.span.extract()
                    temp_task.pagedata['et_author_name'] = stripHtml(author_name_tag.renderContents())
                except:
                    log.info(self.log_msg('Author name not found in url %s'%self.currenturi))
                try:
                    temp_task.pagedata['ei_thread_views_count'] = int(stripHtml(author_tag.renderContents()).replace(',', '').replace('views',''))
                except:
                    log.info(self.log_msg('Views count not found in the url\
                                                        %s'%self.currenturi))
                try:
                    replies_str = stripHtml(thread.find('div', 'fp_topic_content_replies').renderContents()).replace(',', '')
                    if re.match('\d+', replies_str):
                        temp_task.pagedata['ei_thread_replies_count'] = int(replies_str)
                except:
                    log.exception(self.log_msg('Replies count not found in the url\
                                                        %s'%self.currenturi))
                self.linksOut.append(temp_task)
            return True
    
    @logit(log, '__setParentPage')
    def __setParentPage(self):
        """This will get the parent info
        """
        page = {}
        try:
            page['et_thread_hierarchy'] = self.__hierarchy = [x.strip() for x in re.split('>|\n',stripHtml(self.soup.find('div', 'vt_h2').renderContents())) if x.strip()]
            page['title'] = page['et_thread_hierarchy'][-1]
        except:
            log.info(self.log_msg('hierachies not found in url %s'%self.currenturi))
            return 
        try:
            date_str = stripHtml(self.soup.find('span', 'vt_first_timestamp').renderContents())
            date_str = re.sub("(\d+)(st|nd|rd|th)",r"\1", date_str)
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str
                , 'on %B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date not  found in %s'% self.currenturi))
            
        if checkSessionInfo(self.__genre, self.session_info_out, self.task.instance_data['uri'],\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        page_data_keys = ['et_author_name','ei_thread_replies_count','ei_thread_views_count'\
                    ,'edate_last_post_date','et_thread_last_post_author']
        [page.update({each:self.task.pagedata.get(each)}) for each in page_data_keys if self.task.pagedata.get(each) ]
        try:
            result = updateSessionInfo('review', self.session_info_out, self.\
                task.instance_data['uri'], get_hash( page ), 'forum', self.task.instance_data.get('update'))
            if result['updated']:
                page['path']=[ self.task.instance_data['uri'] ] 
                page['parent_path']=[]
                page['uri'] = self.currenturi
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                page['data'] = ''
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
        """This go the last page page of the thread and set the soup
        """
        try:
            self.currenturi = self.currenturi = self.currenturi.rsplit('/',1)[0] + '/' +self.soup.find('div', 'pagination_container vt_pagination_container').findAll('a', text=re.compile ('^\d+$'))[-1].parent['href']
            self.__setSoupForCurrentUri()
        except:
            log.info(self.log_msg('Last page cannot find from the given page no \
                                    for url %s'%self.task.instance_data['uri']))
                                    
    @logit(log, '__addPost')
    def __addPost(self, post,is_question=False):
        '''This will add the post
        '''
        try:
            page = self.__getData(post,is_question)
            if not page:
                log.info(self.log_msg('No data found in url %s'%self.currenturi))        
                return True
            unique_key = get_hash({'data':page['data'], 'title':page['title']})
            if checkSessionInfo(self.__genre, self.session_info_out, \
                    unique_key, self.task.instance_data.get('update'),\
                    parent_list=[self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'% \
                                                            self.currenturi))
                return False            
            result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                get_hash( page ),'forum', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if result['updated']:
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [self.task.instance_data['uri'], unique_key ]
                page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
                page.update(self.__task_elements_dict)
                self.pages.append(page)
            else:
                log.info(self.log_msg('Update session info returns False for \
                                                    url %s'%self.currenturi))
        except:
            log.exception(self.log_msg('Cannot add the post in url %s'%self.currenturi))
        return True
        
    @logit(log , '__addPosts')
    def __iteratePosts(self):
        """This will add the replies to the post
        """
        try:
            replies = self.soup.find('div','vt_postrow_holder').findAll('div', 'vt_postrow_rest')
            if not replies:
                log.info(self.log_msg('No posts found in url %s'%self.currenturi))
                return False
            replies.reverse()
            for reply in replies:
                if not self.__addPost(reply):
                    log.info(self.log_msg('Reached last crawled page in url %s'%self.currenturi))
                    return False
            return True
        except:
            log.info(self.log_msg('cannot add the data in url %s'%self.currenturi))
    
    @logit(log, '__getData')
    def __getData(self, post,is_question=False):
        """ This will return the page dictionry
        """
        page = {'entity':'question' if is_question else 'answer', 'uri':self.currenturi}
        css_class_name = 'span' if is_question else 'div'
        if post.find('div', text='Post temporarily unavailable'):
            log.info(self.log_msg('Message Temporarily not available in url %s'%self.currenturi))
            return False
        if post.find('form', id='frm_quick_reply_submit'):
            log.info(self.log_msg('It is not a post'))
            return False
        try:
            page['et_author_name'] = stripHtml(post.find(css_class_name, 'vt_asked_by_user').renderContents())
        except:
            log.info(self.log_msg('Author name not found in %s'% self.currenturi))
        try:
            date_str = stripHtml(post.find(css_class_name,attrs={'class':re.compile('vt_.+?_timestamp')}).renderContents()).replace('replied ','').strip()
            date_str = re.sub("(\d+)(st|nd|rd|th)",r"\1", date_str)
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str
                , 'on %B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date not be found in %s'% self.currenturi))
        try:
            page['et_author_category'] = stripHtml(post.find('span', 'vt_user_rank').renderContents())
        except:
            log.info(self.log_msg('Author name not found in %s'% self.currenturi))
        try:
            data_tag = post.find('div', 'vt_post_body')
            ads_tag = post.findAll('div',attrs={'class':re.compile('vt_post_body_ad_[l/r]')})
            [each.extract() for each in ads_tag]
            page['data'] = stripHtml(data_tag.renderContents())
        except:
            log.info(post)
            log.info(self.log_msg('Cannot find the Data for this Post %s'%self.currenturi))
            page['data'] = ''
        try:
            if is_question:
                page['title'] = stripHtml(str(post.find('div', 'vt_post_subject').span.next.next))
            else:
                page['title'] = 'Re: ' + self.__hierarchy[-1]
        except:
            log.info(self.log_msg('Cannot find the Data thread details for this Post %s'%self.currenturi))
            page['title'] = ''
        if not (page['data'] and page['title']):
            log.info(self.log_msg('No data found in url %s'%self.currenturi))
            return 
        if len(self.__hierarchy) >= 3:
            page['et_thread_topic'] = self.__hierarchy[-1]
            page['et_thread_forum'] = self.__hierarchy[-3]
            page['et_thread_subforum'] = self.__hierarchy[-2]
        return page
            
    @logit(log,'__setSoupForCurrentUri')
    def __setSoupForCurrentUri(self, data=None, headers={}):
        """It will set soup object for the Current URI
        """
        res = self._getHTML(data=data, headers=headers)
        if res:
            self.rawpage = re.sub('<span class="post_quote">.+</span post_quote>','',res['result'])
        else:
            log.info(self.log_msg('Page Content cannot be fetched for the url: \
                                                            %s'%self.currenturi))
            raise Exception('Page content not fetched for th url %s'%self.currenturi)
        self._setCurrentPage()