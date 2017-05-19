'''
Copyright (c)2008-2010 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

import re
import logging
from urllib2 import urlparse
from datetime import datetime
from BeautifulSoup import BeautifulSoup

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging.getLogger("IvillageConnector")

class IvillageConnector(BaseConnector):
    '''Connector for http://messageboards.ivillage.com'''

    @logit(log,'fetch')
    def fetch(self):
        """
        sample url:
        http://outline.ivillage.com/n/mb/outline.asp?webtag=iv-bhfamilyfrie&popup=true&gfc=1&fid=1&tid=0&fpid=-1
        http://outline.ivillage.com/n/mb/outline.asp?webtag=iv-bhalcohol&popup=true&gfc=1&fid=1&tid=0&fpid=-1
        """
        try:
            self.__SITE_ROOT = 'http://outline.ivillage.com'
            self.__genre = 'review'
            self.__setSoupForCurrentUri()

            if self.currenturi.split('?')[0].endswith('outline.asp'):
                return self.__createTasksForThreads()
            else:
                return self.__addThreadAndPosts()
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s' % self.currenturi))
            return False


    @logit(log, '__createTasksForThreads')
    def __createTasksForThreads(self):
        """
        This will create Tasks for the threads found on the given url
        The # of Tasks are limited by Config Variable
        """
        self.__current_thread_count = 0
        self.__last_timestamp = datetime(1980, 1, 1)
        self.__max_threads_count = int(tg.config.get(path='Connector', 
                                                     key='ivillage_maxthreads'))
        while self.__getThreads():
            try:
                link_next = self.soup.find('a', href=True, text='Next').parent['href']
                self.currenturi = link_next

                self.__setSoupForCurrentUri()
            except:
                log.exception(self.log_msg('Next Page link not found for url %s' % self.currenturi))
                break

        log.info('Total # of tasks found is %d' % len(self.linksOut))
        if self.linksOut:
            updateSessionInfo('Search', self.session_info_out, 
                              self.__last_timestamp , None, 'ForumThreadsPage', 
                              self.task.instance_data.get('update'))
        return True


    def __getThreads(self):
        threads = []

        hrs = self.soup.findAll('hr')
        for hr in hrs:
            threads.append(hr.findNextSiblings(['b', 'i', 'ul'], limit=3))

        if not threads:
            log.info(self.log_msg('No threads are found for url %s' % self.currenturi))
            return False

        for thread in threads:
            if len(thread) < 3:
                continue
            
            self.__current_thread_count += 1
            if  self.__current_thread_count > self.__max_threads_count:
                log.info(self.log_msg('reached max (%d) threads for  %s' %
                                      (self.__max_threads_count, self.currenturi)))
                return False 

            thread_topic = stripHtml(str(thread[0]))

            parts = str(thread[1]).split()
            thread_author = parts[2]

            try:
                thread_no_of_replies = int(parts[7].strip('(')) - 1
            except:
                thread_no_of_replies = int(parts[6].strip('(')) - 1

            try:
                thread_url = thread[2].find('a', href=True)['href']
            except:
                log.info(self.log_msg('couldnt get the url for the thread %s' % thread_topic))
                
            thread_last_post_date, thread_last_post_by = self.__last_post_date_author(thread)

            if checkSessionInfo('Search', self.session_info_out, thread_last_post_date, 
                                self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info Returns True for url %s' % 
                                      self.currenturi))
                return False
           
            self.__last_timestamp = max(thread_last_post_date, self.__last_timestamp )

            temp_task = self.task.clone()                    
            temp_task.instance_data['uri'] = 'http://messageboards.ivillage.com/n/mb/' + thread_url
            
            try:
                temp_task.pagedata['edate_last_post_date']=  datetime.strftime(
                    thread_last_post_date, "%Y-%m-%dT%H:%M:%SZ")
            except:
                log.info(self.log_msg('thread last post date could not be fetched %s' 
                                      % self.currenturi))

            temp_task.pagedata['et_thread_last_post_author']  =  thread_last_post_by
            temp_task.pagedata['et_author_name'] = thread_author
            temp_task.pagedata['et_thread_topic'] = thread_topic

            try:
                temp_task.pagedata['ei_thread_replies_count'] = int(thread_no_of_replies)
            except:
                log.info(self.log_msg('Replies count not found in the url %s' % self.currenturi))

            self.linksOut.append(temp_task)

        return True


    @logit(log, '__setParentPage')
    def __setParentPage(self):
        page = {}
        try:
            page['title'] = stripHtml(str(self.soup.find('h2', 'fs10')))
        except:
            log.exception(self.log_msg('Thread title not found for uri %s' % self.currenturi))
            return

        try:
            self.__hierarchy = []
            self.__hierarchy.append(page['title'])
            page['et_thread_hierarchy'] = self.__hierarchy
        except:
            log.info(self.log_msg('hierachies not found in url %s' % self.currenturi))

        try:
            op = self.soup.find('div', 'dBoxMain')
            op_date_str = stripHtml(str(op.findAll('span', 'fs11')[-1]))
            op_date = datetime.strftime(self.__format_date(op_date_str), "%Y-%m-%dT%H:%M:%SZ")
            page['posted_date'] = op_date
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date could not be found in %s' % self.currenturi))

        if checkSessionInfo(self.__genre, self.session_info_out, self.task.instance_data['uri'],\
                                self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return

        page_data_keys = ['et_author_name', 'ei_thread_replies_count', 'edate_last_post_date', 
                          'et_thread_last_post_author', 'et_thread_topic']

        [page.update({each:self.task.pagedata.get(each)}) for each in page_data_keys if self.task.pagedata.get(each)]

        try:
            result = updateSessionInfo('review', self.session_info_out, 
                                       self.task.instance_data['uri'], get_hash(page), 
                                       'forum', self.task.instance_data.get('update'))
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
                log.info(self.log_msg('Result[updated] returned True for uri' % self.currenturi))
        except :
            log.exception(self.log_msg("parent post couldn't be parsed")) 


    @logit(log, '__goToLastPage')
    def __go_to_last_page(self):
        try:
            most_recent = self.soup.findAll('a', 'ls1')[-2]['href']

            most_recent.index('message.asp')
            self.currenturi = self.__SITE_ROOT + most_recent
            self.currenturi = self.currenturi.replace('http://outline', 'http://messageboards')

            self.__setSoupForCurrentUri()
        except:
            log.info(self.log_msg('Could not find last page for url %s' % 
                                  self.task.instance_data['uri']))


    @logit(log, '__addThreadAndPosts')
    def __addThreadAndPosts(self):
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

        op = self.soup.find('div', 'dBoxMain')
        if not op:
            log.info(self.log_msg('No posts found in url %s' % self.currenturi))
            return False

        self.__addPost(op, True)
        self.__go_to_last_page()

        while self.__iteratePosts():
            try:
                previous_page = self.soup.find('span', 'p_msgnavnumbers').b.previous.previous.parent['href']
                self.currenturi = self.__SITE_ROOT + previous_page
                #self.currenturi = self.__SITE_ROOT + self.soup.findAll('a', 'ls1')[-1]['href']
                self.currenturi = self.currenturi.replace('http://outline', 'http://messageboards')      
                
                self.__setSoupForCurrentUri()
            except:
                log.info(self.log_msg('No Previous URL found for url %s' % self.currenturi))
                break
        return True


    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        try:
            posts = self.soup.findAll('div', 'dBoxMain')
            if not posts:
                log.info(self.log_msg('No posts found in url %s' % self.currenturi))
                return False

            posts.reverse()
            for post in posts:
                if not self.__addPost(post):
                    log.info(self.log_msg('Post not added to self.pages for url %s' % self.currenturi))
                    return False
        except:
            log.exception(self.log_msg('Reviews are not found for url %s' % self.currenturi))
            return False

        try:
            if stripHtml(str(self.soup.find('span', 'p_msgnavnumbers').b)) == '1-5':
                return False
            else:
                return True
        except:
            log.info(self.log_msg('Done with page one... %s' % self.currenturi))


    @logit(log, '__addPost')
    def __addPost(self, post, is_original_post=False):
        try:
            unique_key = stripHtml(str(post.findAll('div', 'oneLine')[2])).split()[2]

            page = self.__get_data(post, is_original_post, unique_key)
            if not page: 
                log.info(self.log_msg('page is empty, __get_data returns  False for uri %s' % 
                                      self.currenturi))
                return True

            if checkSessionInfo(self.__genre, self.session_info_out, 
                                unique_key, self.task.instance_data.get('update'), 
                                parent_list=[self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s' % 
                                      self.task.instance_data['uri']))
                return False

            result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, 
                                       get_hash(page),'forum', self.task.instance_data.get('update'), 
                                       parent_list=[self.task.instance_data['uri']])
            if result['updated']:
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [self.task.instance_data['uri'], unique_key]
                page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
                page.update(self.__task_elements_dict)
                self.pages.append(page)
            else:
                log.info(self.log_msg('Update session info returns False for url %s' % self.currenturi))
        except:
            log.exception(self.log_msg('Cannot add the post for the uri %s' % self.currenturi))

        return True


    @logit(log, '__get_data')    
    def __get_data(self, post, is_original_post, unique_key):
        page = {'entity':'question' if is_original_post else 'answer'}
        page['uri'] = self.currenturi

        try:
            d = self.__format_date(stripHtml(str(post.findAll('div', 'oneLine')[-1].span)))
            page['posted_date'] = datetime.strftime(d, "%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date could not be found in %s for unique_key: %s' % 
                                  (self.currenturi, unique_key)))

        page['et_author_name'] = stripHtml(str(post.find('a', 'memname')))
        page['data'] = stripHtml(str(post.find('div', 'mainBodyText')))

        title = stripHtml(str(post.find('h2')))
        if not is_original_post:
            page['title'] = 'Re: ' + title
        else:
            page['title'] = title
            
        #print page
        return page


    def __setSoupForCurrentUri(self, url=None, data=None, headers={}):
        """
            It will take the URL and change it to self.currenturi and set soup,
            if url is mentioned. If url is not given it will take the 
            self.current uri
        """
        if url:
            self.currenturi = url

        res = self._getHTML(data=data, headers=headers)
        if res:
            self.rawpage = res['result']
        else:
            log.info(self.log_msg('HTML Content cannot be fetched for the url: %s' % self.currenturi))
            return False

        self._setCurrentPage()

        return True


    def __format_date(self, date_str):
        # '5/21/2009 12:51 pm'
        # 'Feb-20 8:37 pm'

        months = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7, 
                  'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}

        p = re.compile('\d{1,2}/\d{1,2}/\d{1,4}')
        if not re.search(p, date_str):
            parts = date_str.split()
            try:
                month = months[parts[0].split('-')[0]]
                day = parts[0].split('-')[1]
                if (datetime.now().month - month) < 0:
                    s = '%d/%s/%d %s %s' % (month, day, datetime.now().year - 1, parts[1], parts[2])
                else:
                    s = '%d/%s/%d %s %s' % (month, day, datetime.now().year, parts[1], parts[2])
            except KeyError:
                now = datetime.now()
                s = '%d/%d/%d %s %s' % (now.month, now.day, now.year, parts[0], parts[1])

            d = datetime.strptime(s, '%m/%d/%Y %I:%M %p')
        else:
            d = datetime.strptime(date_str, '%m/%d/%Y %I:%M %p')

        return d


    def __last_post_date_author(self, thread):
        try:
            info = thread[2].findAll('i')
            date_author = []

            for i in info:
                parts = stripHtml(str(i)).split()
                d = self.__format_date(' '.join(parts[5:8]))
                date_author.append((d, parts[1])) 

            date_author.sort()
            date_author.reverse()

            return date_author[0]
        except:
            log.info(self.log_msg('problem finding last post information for %s' % self.currenturi))
            raise
