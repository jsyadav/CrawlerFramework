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

log = logging.getLogger('LiveStrongConnector')
class LiveStrongConnector(BaseConnector):
    '''
    This will fetch the info for livestrong.com
    Sample uris is
    
    Note:Don't take Sticky Posts, it contains general Info about the forum
    '''
        
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of talk livestrong.com
        """
        try:
            self.__genre = 'review'
            self.__baseuri = 'http://www.livestrong.com'
            self.__setSoupForCurrentUri()
            if '/viewforum.php?f' in self.currenturi:
                return self.__createTasksForThreads()
            else:
                #==> Pickup the List of Question and Answer
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
        try:
            question_post = self.soup.find('div', 'postbody').findParent('table').findParent('table')
            self.__addPost(question_post, True)
        except:
            log.exception(self.log_msg('Question post not not found in url %s'%self.currenturi))
            return False
        try:
            self.currenturi += '&start=0'
            self.__setSoupForCurrentUri()
        except:
            log.info(self.log_msg('Last page cannot find from the given page no \
                                    for url %s'%self.task.instance_data['uri']))
        while self.__iteratePosts():
            try:
                self.currenturi = self.__baseuri + self.soup.find('a', text='Next').parent['href']
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
                                            'livestrong_maxthreads'))
        while self.__getThreads():
            try:
                self.currenturi = self.__baseuri + self.soup.find('a', text='Next').parent['href']
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
            try:
                threads = self.soup.find('b', attrs={'class':'gensmall'},text='Topics').findParent('tr').findNextSiblings()[:-1]
                if not threads:
                    log.info(self.log_msg('No threads found in url %s'%self.currenturi))
                    return False
            except:
                log.info(self.log_msg('No threads found in url %s'%self.currenturi))
                return False
            for thread in threads:
                self.__current_thread_count += 1
                if  self.__current_thread_count > self.__max_threads_count:
                    log.info(self.log_msg('Reaching maximum post,Return false \
                                            from the url %s'%self.currenturi))
                    return False
                
                try:
                    topic_info = thread.findAll('td',recursive=False)
                    date_str, last_post_author = [x.strip() for x in stripHtml(topic_info[-1].renderContents()).splitlines() if x.strip()]
                    thread_time  = datetime.strptime(date_str, '%a %b %d, %Y %I:%M %p')
                except:
                    log.exception(self.log_msg('Cannot fetch the date for the url\
                                                            %s'%self.currenturi))
                    continue
                if checkSessionInfo('Search', self.session_info_out, thread_time,\
                                        self.task.instance_data.get('update')):
                        log.info(self.log_msg('Session info Returns True for url %s'%self.currenturi))
                        return False
                self.__last_timestamp = max(thread_time , self.__last_timestamp )
                temp_task=self.task.clone()                    
                try:
                    temp_task.instance_data[ 'uri' ] = self.__baseuri + thread.find('a','topictitle')['href']
                except:
                    log.exception(self.log_msg('Cannot find the thread url \
                                            in the uri %s'%self.currenturi))
                    continue
                temp_task.pagedata['edate_last_post_date']=  datetime.\
                                strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                temp_task.pagedata['et_thread_last_post_author']  =  last_post_author
                try:
                    temp_task.pagedata['et_author_name'] = stripHtml(topic_info[2].renderContents())
                except:
                    log.info(self.log_msg('Author name not found in the url\
                                                        %s'%self.currenturi))
                try:
                    temp_task.pagedata['ei_thread_views_count'] = int(stripHtml(topic_info[-2].renderContents()).replace(',',''))
                except:
                    log.info(self.log_msg('Views count not found in the url\
                                                    %s'%self.currenturi))
                try:
                    temp_task.pagedata['ei_thread_replies_count'] = int(stripHtml(topic_info[-3].renderContents()).replace(',',''))
                except:
                    log.info(self.log_msg('Replies count not found in the url\
                                                    %s'%self.currenturi))
                self.linksOut.append(temp_task)
                log.info('Task added')
            return True
    
    @logit(log, '__setParentPage')
    def __setParentPage(self):
        """This will get the parent info
        """
        page = {}
        try:
            page['title'] = stripHtml(self.soup.find('div', id='pageheader').find('h2').renderContents())
        except:
            log.exception(self.log_msg('Thread hierarchy and Title Not found for uri\
                                                            %s'%self.currenturi))
            return
        try:
            self.__hierarchy = [x.strip() for x in stripHtml(self.soup.find('p', 'breadcrumbs').renderContents()).split(u'\xbb') if x.strip()]
            self.__hierarchy.append(page['title'])
            page['et_thread_hierarchy'] = self.__hierarchy
        except:
            log.info(self.log_msg('hierachies not found in url %s'%self.currenturi))
        try:
            date_str = stripHtml(self.soup.find('b', text='Posted:').findParent('div').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str, 'Posted:  %a %b %d, %Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date not be found in %s'% self.currenturi))
        if checkSessionInfo(self.__genre, self.session_info_out, self.task.instance_data['uri'],\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        page_data_keys = ['et_author_name','ei_thread_replies_count','ei_thread_views_count'\
                                                        ,'edate_last_post_date']
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
        except:
            log.exception(self.log_msg("parent post couldn't be parsed"))
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = [x.findParent('table').findParent('table') for x in self.soup.findAll('div', 'postbody')]
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
                self.pages.append(page)
            else:
                log.info(self.log_msg('Update session info returns False for \
                                                    url %s'%self.currenturi))
        except:
            log.exception(self.log_msg('Cannot add the post for the uri %s'%self.currenturi))
        return True
    
    @logit(log, '__getData')
    def __getData(self, post, is_question,unique_key):
        """ This will return the page dictionry
        """
        permalink = self.currenturi + '#' + unique_key
        page = {'entity':'question' if is_question else 'answer'}
        page['uri'] = permalink
        try:
            date_str = stripHtml(post.find('b',text='Posted:').findParent('div').renderContents())
            
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str, 'Posted:  %a %b %d, %Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date not be found in %s'% self.currenturi))
        try:
            page['et_author_name'] = stripHtml(post.find('b','postauthor').renderContents())
        except:
            log.info(self.log_msg('Author name not found in %s'% self.currenturi))
        try:
            page['title'] = re.sub('^Post subject: ','',stripHtml(post.find('b',text='Post subject:').findParent('div').renderContents())).strip()
        except:
            log.info(self.log_msg('Author name not found in %s'% self.currenturi))
            page['title'] = ''
        try:
            aut_info = [x.strip() for x in stripHtml(post.find('span', 'postdetails').renderContents()).splitlines() if x.strip()]
            page['edate_author_member_since'] = datetime.strftime(datetime.strptime(aut_info[0],'Joined:  %a %b %d, %Y %I:%M %p'), "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Author info not found in %s'%self.currenturi))
        try:
            page['ei_author_posts_count'] = int(re.search('\d+',aut_info[1].replace(',','')).group())
        except:
            log.info(self.log_msg('Author posts count not found in %s'%self.currenturi))
        try:
            data_tag = post.find('div', 'postbody')
            quote_tag = post.find('div', 'quotecontent')
            quote_title_tag = post.find('div', 'quotetitle')
            if quote_tag:
                quote_tag.extract()
            if quote_title_tag:
                quote_title_tag.extract()        
            page['data'] = stripHtml(data_tag.renderContents())
        except:
            log.info(self.log_msg('Empty data found in %s'%permalink))
            page['data'] = ''
        if not (page['data'] and page['title']):
            log.info(self.log_msg('Empty data found in url %s'%self.currenturi))
            return False
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