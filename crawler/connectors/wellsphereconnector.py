'''
Copyright (c)2008-2010 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

import re
import math
import logging
from urllib2 import urlparse
from datetime import datetime
from BeautifulSoup import BeautifulSoup

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging.getLogger("WellsphereConnector")

class WellsphereConnector(BaseConnector):
    '''Connector for dailystrength.org'''
    
    @logit(log,'fetch')
    def fetch(self):
        """
        Fetches data from;
        sample url:
        http://www.dailystrength.org/c/Alcoholism/forum
        """
        try:
            self.__SITE_ROOT = 'http://www.wellsphere.com'
            self.__genre = 'review'
            self.__setSoupForCurrentUri()

            if self.currenturi.endswith('/2'):
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
                                                     key='wellsphere_maxthreads'))
        while self.__getThreads():
            try:
                link_next = self.soup.find('a', text='Next &raquo;', href=True).parent['href']
                self.currenturi = self.__SITE_ROOT + link_next
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
        threads = self.soup.findAll('div', 'p15top p20bottom clearff')
        if not threads:
            log.info(self.log_msg('No threads are found for url %s' % self.currenturi))
            return False

        for thread in threads:
            self.__current_thread_count += 1
            if  self.__current_thread_count > self.__max_threads_count:
                log.info(self.log_msg('Reaching max threads, return false from the url %s' % 
                                      self.currenturi))
                return False 

            try:
                thread_topic = stripHtml(thread.find('div', 'leftAlign').find('a', 'text12 boldfont').contents[0])
            except:
                log.info(self.log_msg('could not find thread topic in %s' % self.currenturi))

            # TODO
            # thread_no_of_replies = text[1]

            try:
                thread_author = stripHtml(thread.find('div', 'leftAlign').find('div', 'm10left p6top').a.contents[0])
            except:
                log.info(self.log_msg('could not find thread author in %s' % self.currenturi))

            try:
                thread_op_date = stripHtml(str(thread.find('div', 'leftAlign').find('div', 'm10left p6top grey')))
            except:
                log.info(self.log_msg('could not find thread op date in %s' % self.currenturi))

            try:
                thread_url = thread.find('div', 'leftAlign').find('a', 'text12 boldfont')['href'].strip()
            except:
                log.info(self.log_msg('could not find thread url in %s' % self.currenturi))

            if thread.find('div', 'rightAlign'):
                try:
                    thread_last_post_date = stripHtml(str(thread.find('div', 'rightAlign').find('div', 'm10sides p6top grey')))
                except:
                    log.info(self.log_msg('could not find last post date for thread in %s' % self.currenturi))

                try:
                    thread_last_post_by = stripHtml(thread.find('div', 'rightAlign').find('div', 'm10sides p6top').a.contents[0])
                except:
                    log.info(self.log_msg('could not find author of last post for thread in %s' % self.currenturi))
            else:
                thread_last_post_date = thread_op_date
                thread_last_post_by = thread_author
                log.info(self.log_msg('could not find author and date of last post for thread in %s' % self.currenturi))

            try:
                if thread_last_post_date:
                    thread_time = datetime.strptime(thread_last_post_date, '%b %d %Y %I:%M%p')
                else:
                    thread_time = datetime.strptime(thread_op_date, '%b %d %Y %I:%M%p')
            except:
                log.exception(self.log_msg('Cannot fetch date for the url %s' % self.currenturi))
                continue

            if checkSessionInfo('Search', self.session_info_out, 
                                thread_time, self.task.instance_data.get('update')):

                log.info(self.log_msg('Session info Returns True for url %s' % self.currenturi))
                return False

            self.__last_timestamp = max(thread_time, self.__last_timestamp )

            temp_task = self.task.clone()                    
            temp_task.instance_data['uri'] = self.__SITE_ROOT + thread_url
            
            try:
                temp_task.pagedata['edate_last_post_date']=  datetime.strftime(
                    datetime.strptime(thread_last_post_date, '%b %d %Y %I:%M%p'),
                    "%Y-%m-%dT%H:%M:%SZ")
            except:
                log.info(self.log_msg('thread last post date could not be fetched %s' % self.currenturi))


            temp_task.pagedata['et_thread_last_post_author']  =  thread_last_post_by
            temp_task.pagedata['et_author_name'] = thread_author
            temp_task.pagedata['et_thread_topic'] = thread_topic

            self.linksOut.append(temp_task)

        return True


    @logit(log, '__setParentPage')
    def __setParentPage(self):
        page = {}
        try:
            page['title'] = self.soup.find('h1', 'text16 global-mfc pm0all textLine12').contents[1].strip()
        except:
            log.exception(self.log_msg('Thread title not found for uri %s' % 
                                       self.currenturi))
            return

        try:
            self.__hierarchy = []
            self.__hierarchy.append(page['title'])
            page['et_thread_hierarchy'] = self.__hierarchy
        except:
            log.info(self.log_msg('hierachies not found in url %s' % self.currenturi))

        # posted date not available
        page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")

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
            s = self.soup.find('div', 'leftAlign p10right').span
            posts_count = stripHtml(str(s)).split()[-1]

            if s:
                last_page = int(math.ceil((int(posts_count) * 1.0)/20)) - 1
                if last_page > 0:
                    self.currenturi = self.currenturi.split('?')[0] + '?pageIndex=%d' % last_page
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

        op = self.soup.find('div', 'text12 textLine12 commentBodyBlock')
        if not op:
            log.info(self.log_msg('No posts found in url %s' % self.currenturi))
            return False

        self.__addPost(op, True)
        self.__go_to_last_page()
        while self.__iteratePosts():
            try:
                previous = self.soup.find('div', 'navigation p5top').find('a', text='&laquo; Previous').parent['href']
                self.currenturi = self.SITE_ROOT + previous
                self.__setSoupForCurrentUri()
            except:
                log.info(self.log_msg('No Previous URL found for url %s' % self.currenturi))
                break
        return True


    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        posts = self.soup.findAll('div', 'clearff p10top p20bottom')
        
        try:
            if not posts:
                log.info(self.log_msg('No posts found in url %s' % self.currenturi))
                return False

            posts.reverse()
            for post in posts:
                if not self.__addPost(post):
                    log.info(self.log_msg('Post not added to self.pages for url %s' % self.currenturi))
                    return False
            return True
        except:
            log.exception(self.log_msg('Reviews are not found for url %s' % self.currenturi))
            return False


    @logit(log, '__addPost')
    def __addPost(self, post, is_original_post=False):
        try:
            """if is_original_post:
                unique_key = hash(stripHtml(([e.strip() for e in post.findAll(text=True) if e.strip()][3])))
            else:
                #unique_key = stripHtml([e.strip() for e in post.findAll(text=True) if e.strip()][1])
                unique_key = hash(stripHtml(str(post.find('div', 'textLine12 text12 commentBodyBlock'))))"""

            page = self.__get_data(post, is_original_post)
            if not page: 
                log.info(self.log_msg('page is empty, __get_data returns  False for uri %s' % 
                                      self.currenturi))
                return True
            unique_key = get_hash(page)
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
                page['path'] = [self.task.instance_data['uri'], unique_key ]
                page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
                page.update(self.__task_elements_dict)
                self.pages.append(page)
            else:
                log.info(self.log_msg('Update session info returns False for url %s' % self.currenturi))
        except:
            log.exception(self.log_msg('Cannot add the post for the uri %s' % self.currenturi))
        
        return True

    @logit(log, '__get_data')    
    def __get_data(self, post, is_original_post):
        page = {'entity':'question' if is_original_post else 'answer'}
        page['uri'] = self.currenturi

        page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")

        auth_info = self.__get_author_info(post, is_original_post)
        if auth_info['name']:
            page['et_author_name'] = auth_info['name']
        if auth_info['location']:
            page['et_author_location'] = auth_info['location']

        if is_original_post:
            page['data'] = stripHtml([e.strip() for e in post.findAll(text=True) if e.strip()][3])
        else:
            #page['data'] = stripHtml([e.strip() for e in post.findAll(text=True) if e.strip()][1])
            page['data'] = stripHtml(str(post.find('div', 'textLine12 text12 commentBodyBlock')))

        if page['data'] == 'Add as friend':
            log.info(self.log_msg('post data could not be found in url %s' % self.currenturi))
            return False

        title = stripHtml(str(self.soup.find('h1', 'text16 global-mfc pm0all textLine12')))
        if not is_original_post:
            page['title'] = 'Re: ' + title
        else:
            page['title'] = title
        return page


    @logit(log, '__get_author_info')    
    def __get_author_info(self, post, is_original_post):
        author_info = {'name': '', 'location': ''}

        if is_original_post:
            url = self.__SITE_ROOT + post.find('a', 'boldfont')['href'].strip()
        else:
            url = self.__SITE_ROOT + post.find('a', 'text12 boldfont')['href'].strip()

        soup = BeautifulSoup(self._getHTML(uri=url)['result'])

        author_info['name'] = stripHtml(str(soup.find('div', 'clearff')))

        l = soup.find('div', 'p7top color777')
        if l:
            author_info['location'] = stripHtml(str(l))
            
        return author_info


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
