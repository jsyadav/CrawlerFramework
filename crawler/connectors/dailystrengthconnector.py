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
log = logging.getLogger("DailyStrengthConnector")

class DailyStrengthConnector(BaseConnector):
    '''Connector for dailystrength.org'''
    
    @logit(log,'fetch')
    def fetch(self):
        """
        Fetches data from;
        sample url:
        http://www.dailystrength.org/c/Alcoholism/forum
        """
        try:
            self.__SITE_ROOT = 'http://www.dailystrength.org'
            self.__genre = 'review'
            self.__setSoupForCurrentUri()

            if self.currenturi.endswith('/forum'):
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
                                                     key='dailystrength_maxthreads'))
        while self.__getThreads():
            #self.__current_thread_count += 1
            try:
                link_next = self.soup.find('a', {'class':'medium'}, 
                                           text='next &gt;', href=True).parent['href']

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
        threads = self.soup.findAll('tr', attrs={'class':re.compile('sectiontableentry\d')})
        if not threads:
            log.info(self.log_msg('No threads are found for url %s' % self.currenturi))
            return False

        for thread in threads:
            self.__current_thread_count += 1
            if  self.__current_thread_count > self.__max_threads_count:
                log.info(self.log_msg('Reaching maximum post,Return false from the url %s' % 
                                      self.currenturi))
                return False 

            # find Topics, Replies, Author and Last Post
            # review comment: use individual try block
            text = thread.findAll(text=True)
            text = [e.strip() for e in text if e.strip()]

            thread_topic = text[0]
            thread_no_of_replies = text[1]
            thread_author = text[2]
            thread_last_post_by = text[5]
            thread_last_post_date = text[6]
            thread_url = thread.find('a', 'strong', text=thread_topic, href=True).parent['href']

            try:
                thread_time = datetime.strptime(thread_last_post_date, '%m/%d/%y %I:%M %p')
            except:
                log.exception(self.log_msg('Cannot fetch date for the url %s' % self.currenturi))
                continue

            if checkSessionInfo('Search', self.session_info_out, thread_time, 
                                self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info Returns True for url %s' % 
                                      self.currenturi))
                return False
           
            self.__last_timestamp = max(thread_time, self.__last_timestamp )

            temp_task = self.task.clone()                    
            temp_task.instance_data['uri'] = self.__SITE_ROOT + thread_url
            
            try:
                temp_task.pagedata['edate_last_post_date']=  datetime.strftime(
                    datetime.strptime(thread_last_post_date, '%m/%d/%y %I:%M %p'),
                    "%Y-%m-%dT%H:%M:%SZ")
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
            # better stripHtml
            page['title'] = stripHtml(self.soup.find('div', 
                                                     'discussion_topic_header_subject').contents[0])
        except:
            log.exception(self.log_msg('Thread title not found for uri %s' % 
                                       self.currenturi))
            return

        try:
            self.__hierarchy = []
            #self.__hierarchy.append(self.stripHtml(str(soup.find('div', 'moduleheader_left').h1)))
            self.__hierarchy.append(page['title'])
            page['et_thread_hierarchy'] = self.__hierarchy
        except:
            log.info(self.log_msg('hierachies not found in url %s' % self.currenturi))

        try:
            op = self.soup.find('div', 'discussion_text longtextfix485')
            op_date = datetime.strftime(datetime.strptime(stripHtml(str(op.find('span'))), 
                                                          'Posted on %m/%d/%y, %I:%M %p'),
                                        "%Y-%m-%dT%H:%M:%SZ")
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
            most_recent = self.soup.find('tr', 'tool_bar').find('a', text='Most Recent',
                                                                href=True).parent['href']
            if most_recent.strip(): # Not needed really
                self.currenturi = self.__SITE_ROOT + most_recent
                self.__setSoupForCurrentUri()
            else:
                log.info(self.log_msg('Posts found in same page %s' % self.currenturi))
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

        op = self.soup.find('table', 'discussion_topic').find('tr', recursive=False)
        if not op:
            log.info(self.log_msg('No posts found in url %s' % self.currenturi))
            return False

        self.__addPost(op, True)
        self.__go_to_last_page()
        while self.__iteratePosts():
            try:
                self.currenturi = self.__SITE_ROOT + self.soup.find('a', text='Previous',
                                                                  href=True).parent['href']
                self.__setSoupForCurrentUri()
            except:
                log.info(self.log_msg('No Previous URL found for url %s' % self.currenturi))
                break
        return True


    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        try:
            posts = [post for post in self.soup.find('table', 'reply_table').findAll('tr', recursive=False) \
                     if post.find('td','avatar_display')]
            
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
        # get permalink/unique key
        try:
            if is_original_post:
                unique_key = self.currenturi
            else:
                unique_key = stripHtml(str(post.findAll('span', 
                                                        'graytext')[0].contents[0].strip().strip('- ')))

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
                page['path'] = [self.task.instance_data['uri'], unique_key ]
                page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
                log.info(page)
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
            if is_original_post:
                p = self.soup.find('div', 'discussion_text longtextfix485')
                page['posted_date'] = datetime.strftime(
                    datetime.strptime(stripHtml(str(p.find('span'))), 
                                      'Posted on %m/%d/%y, %I:%M %p'), "%Y-%m-%dT%H:%M:%SZ")
            else:
                date_parts = post.findAll('span', 'graytext')[1].contents[0].split('&nbsp;')
                date_str = date_parts[0].strip() + ' ' + date_parts[1].strip()

                page['posted_date'] = datetime.strftime(
                    datetime.strptime(date_str, '%m/%d/%y %I:%M%p'), "%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date could not be found in %s for unique_key: %s' % 
                                  (self.currenturi, unique_key)))

        try:
            author_info = self.__get_author_info(post)
            page['et_author_name'] = author_info['name']
            if author_info['gender']:
                page['et_author_gender'] = author_info['gender'][0]

            page['et_author_location'] = author_info['location']
            #page['et_author_member_since'] = author_info['member_since']
            
            if author_info['age']:
                page['ei_author_age'] = int(author_info['age'][0])

            if author_info['member_since']:
                page['edate_author_member_since'] = datetime.strftime(
                    datetime.strptime(author_info['member_since'], '%B %d, %Y'), 
                    '%Y-%m-%dT%H:%M:%SZ')
        except:
            log.info(self.log_msg('author info could not be found in %s for unique_key: %s' % 
                                  (self.currenturi, unique_key)))

        try:
            if is_original_post:
                page['data'] = stripHtml(str(post.find('div', 'discussion_text longtextfix485')))
            else:
                page['data'] = stripHtml(str(post.find('div', 'discussion_text longtextfix485')))
                page['data'] = page['data'].replace('<!--\n\n Add Your Reply \n\n\t\t\t\t-->', '').strip()
        except:
            log.info(self.log_msg('post data could not be found in %s' % self.currenturi))
        
        title = stripHtml(self.soup.find('div', 'discussion_topic_header_subject').contents[0])
        if not is_original_post:
            page['title'] = 'Re: ' + title
        else:
            page['title'] = title
            
        #print page
        return page


    @logit(log, '__get_author_info')    
    def __get_author_info(self, post):
        author_info = {'name': '', 'member_since': '', 'gender': '', 'age': '', 'location': ''}

        url = self.__SITE_ROOT + post.find('p', 'username').find('a')['href']
        soup = BeautifulSoup(self._getHTML(uri=url)['result'])

        author_info['name'] = stripHtml(str(soup.find('div', 'user_image').h1))

        info = soup.find('div', 'user_image').find('p', 'meta')
        if info:
            if soup.find('div', 'private_message'):
                author_info['member_since'] = stripHtml(info.contents[-1]).split('Member since ')[1]
            else:
                if info.contents[0] and info.contents[0].strip():
                    author_info['gender'] = re.findall('Male|Female', stripHtml(info.contents[0]))
                    author_info['age'] = re.findall('\d+', stripHtml(info.contents[0]))

                author_info['member_since'] = stripHtml(info.contents[-1]).split('Member since ')[1]

                if len(info.contents) > 2:
                    author_info['location'] = stripHtml(info.contents[2]).strip('><br')
            
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

