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
from datetime import datetime
from BeautifulSoup import BeautifulSoup

from tgimport import tg
from baseconnector import BaseConnector
from utils.decorators import logit
from utils.utils import stripHtml, get_hash
from utils.httpconnection import HTTPConnection
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('AboutForumsConnector')
class AboutForumsConnector(BaseConnector):
    '''
    This will fetch the info for forums.about.com
    Sample uris is
    http://forums.about.com/n/pfx/forum.aspx?folderId=5&listMode=13&nav=messages&webtag=ab-womenshealth
    '''
        
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of forums.about.com
        """
        try:
            self.__genre = 'review'
            self.__baseuri = 'http://forums.about.com'
            conn = HTTPConnection()
            conn.createrequest(self.currenturi)
            self.soup = BeautifulSoup(conn.fetch().read())
            if 'folderId=' in self.currenturi:
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
        while self.__iteratePosts():
            try:
                self.currenturi = self.__baseuri + self.soup.find('a', title='Next')['href']
                conn = HTTPConnection()
                conn.createrequest(self.currenturi)
                self.soup = BeautifulSoup(conn.fetch().read())
            except:
                log.exception(self.log_msg('No Previous URL found for url \
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
        self.__last_timestamp = datetime(1980, 1, 1)
        self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                            'aboutforums_maxthreads'))
        self.__getThreads()
        log.info('Total # of tasks found is %d'%len(self.linksOut))
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
            threads = self.soup.find('table', 'ptcContentTable').findAll('tr', recursive=False)[2:]
            if not threads:
                log.info(self.log_msg('No threads are found for url %s'%\
                                                            self.currenturi))
                return False
        except:
            log.exception(self.log_msg('No Threads found'))
            return False
        for thread in threads:
            self.__current_thread_count += 1
            if  self.__current_thread_count > self.__max_threads_count:
                log.info(self.log_msg('Reaching maximum post,Return false \
                                        from the url %s'%self.currenturi))
                return False
            
            try:
                topic_info = thread.findAll('td', recursive=False)
                #log.info(topic_info)
                curr_date = datetime.utcnow()
                my_date_str = stripHtml(topic_info[-1].renderContents())
                if my_date_str.endswith('AM') or my_date_str.endswith('PM'):
                    # if contains only, tat means Today's time
                    full_date_str = ' '.join([str(x) for x in [curr_date.year,curr_date.month, curr_date.day, my_date_str]])
                    thread_time = datetime.strptime(full_date_str, '%Y %m %d %I:%M %p')
                elif '-' in my_date_str:
                    #contains only month and date, tat means current year
                    date_str = str( curr_date.year ) + '-' + my_date_str
                    thread_time = datetime.strptime(date_str, '%Y-%b-%d')
                else:
                    # contains month, day, year 
                    thread_time = datetime.strptime(my_date_str, '%m/%d/%y')
            except:
                log.exception(self.log_msg('Cannot fetch the date for the url\
                                                        %s'%self.currenturi))
                continue
            if checkSessionInfo('Search', self.session_info_out, thread_time, \
                                    self.task.instance_data.get('update')):
                    log.info(self.log_msg('Session info Returns True for url %s'%self.currenturi))
                    return False
            self.__last_timestamp = max(thread_time , self.__last_timestamp )
            temp_task = self.task.clone()                    
            try:
                temp_task.instance_data['uri'] = self.__baseuri + re.sub('tsn=\d+', 'tsn=1', topic_info[1].find('a', 'navLink')['href'])
            except:
                log.exception(self.log_msg('Cannot find the thread url \
                                        in the uri %s'%self.currenturi))
                continue
            temp_task.pagedata['edate_last_post_date'] =  datetime.\
                            strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
            try:
                temp_task.pagedata['et_author_name'] = stripHtml(topic_info[2].renderContents())
            except:
                log.info(self.log_msg('Author name not found in the url\
                                                    %s'%self.currenturi))
            try:
                temp_task.pagedata['ei_thread_views_count'] = int(stripHtml(topic_info[-3].renderContents()).replace(',',''))
            except:
                log.info(self.log_msg('Views count/Thread age not found in the url\
                                                %s'%self.currenturi))
            try:
                temp_task.pagedata['ei_thread_replies_count'] = int(stripHtml(topic_info[-2].renderContents()).replace(',',''))
            except:
                log.info(self.log_msg('Replies count not found in the url\
                                                %s'%self.currenturi))
            self.linksOut.append(temp_task)
        return True
    
    @logit(log, '__setParentPage')
    def __setParentPage(self):
        """This will get the parent info
        """
        page = {}
        try:
            page['title'] = stripHtml(self.soup.find('div', 'pfDiscussionTitle').renderContents())
            page['et_thread_hierarchy'] = self.__hierarchy = [ stripHtml(self.soup.find('span', 'ptbFolderName').renderContents()), page['title']] 
        except:
            log.exception(self.log_msg('Hierachy/Title not found in url %s'%self.currenturi))
            return 
        try:
            date_str = self.soup.find('span', text=re.compile('Posted:')).next.__str__()
            #Sample Date Mar 01 10 12:10 PM
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str 
                , '%b %d %y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date not be found in %s'% self.currenturi))
        if checkSessionInfo(self.__genre, self.session_info_out, self.task.instance_data['uri'], \
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        page_data_keys = ['et_author_name', 'ei_thread_replies_count', 'ei_thread_views_count'\
                            , 'edate_last_post_date']
        [page.update({each:self.task.pagedata.get(each)}) for each in page_data_keys if self.task.pagedata.get(each) ]
        try:
            result = updateSessionInfo('review', self.session_info_out, self.\
                task.instance_data['uri'], get_hash( page ), 'forum', self.task.instance_data.get('update'))
            if result['updated']:
                page['path'] = [ self.task.instance_data['uri'] ] 
                page['parent_path'] = []
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
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = self.soup.findAll('div', 'pfMessage')
            log.info(self.log_msg('# of posts found in the page is %d'%len(posts)))
            if not posts:
                log.info(self.log_msg('No posts found in url %s'%self.currenturi))
                return False
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
            unique_key = post.find('span', 'ptcListLink').a['href']
            if checkSessionInfo(self.__genre, self.session_info_out, \
                        unique_key, self.task.instance_data.get('update'),\
                        parent_list=[self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'% \
                                                                self.currenturi))
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
    def __getData(self, post, is_question, unique_key):
        """ This will return the page dictionry
        """
        is_question = False
        try:
            if int(stripHtml(post.find('span', 'ptcListLink').findParent('li').findAll('span')[-1].renderContents()).split(' of ')[0].replace(',', '')) == 1:
                is_question = True
        except:
            log.info(self.log_msg('Could not find wheather its is question or not'))
        page = {'entity':'question' if is_question else 'answer', 'uri':self.__baseuri + unique_key}
        try:
            date_str = post.find('span', text=re.compile('Posted:')).next.__str__()
            #Sample Date Mar 01 10 12:10 PM
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str 
                , '%b %d %y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.exception(self.log_msg('Date not be found in %s'% self.currenturi))
        try:
            author_info = post.find('ul', 'ptcListStack')            
            page['et_author_name'], author_id = [x.strip() for x in stripHtml(author_info.find('a', 'ptcListLink').renderContents()).splitlines()]
            page['et_author_user_name'] = author_id[1:-1]
        except:
            log.info(self.log_msg('Author name not found in url %s'%self.currenturi))
        try:
            page['et_author_last_visit'] = stripHtml(author_info.find('span', text='Last Visit: ').next.renderContents())
        except:
            log.info(self.log_msg('Author Last visit not found in url %s'%self.currenturi))
        try:
            page['ei_author_posts_count'] = int(stripHtml(author_info.find('span', text=re.compile('Posts:')).next.renderContents()))
        except:
            log.info(self.log_msg('Author Posts count not found in url %s'%self.currenturi))
        try:
            page['et_data_replied_author'] = stripHtml(post.find('td', 'pfMsgEnvelope').find('span', 'ptcPrefix').next.next.__str__())
        except:
            log.info(self.log_msg('Data replied author not found in url %s'%self.currenturi))
        try:
            page['data'] = stripHtml(post.find('div', 'pfMsgText').renderContents())
        except:
            log.info(self.log_msg('Data not found%s'%self.currenturi))
            return False
        try:
            page['title'] = page['et_thread_topic'] = self.__hierarchy[-1]
            page['et_thread_forum'] = self.__hierarchy[-2]
            if not is_question:
                page['title'] = 'Re: ' + page['title'] 
        except:
            log.exception(self.log_msg('Title not found url %s'%self.currenturi))
            page['title'] = ''
        return page