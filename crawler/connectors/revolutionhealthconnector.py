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
from datetime import datetime, timedelta

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('RevolutionHealthConnector')
class RevolutionHealthConnector(BaseConnector):
    '''
    This will fetch the info for revolutionhealth.com forums
    Sample uris is
    http://www.revolutionhealth.com/forums/cancer
    '''        
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of http://www.revolutionhealth.com/forums
        """
        try:
            self.__genre = 'review'
            self.__baseuri = 'http://www.revolutionhealth.com'
            self.__setSoupForCurrentUri()
            #if  re.search('/forums/[^/]+$', self.currenturi):
            if not self.task.instance_data.get('already_parsed'):
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
        if not self.__addQuestionInfo():
            log.info(self.log_msg('No Questions found'))
            return False
        self.__goToLastPage()
        while self.__iteratePosts():
            try:
                self.currenturi =  self.__baseuri + self.soup.find('div', 'pagination_content').find('a', text='Previous').parent['href']
                self.__setSoupForCurrentUri()
            except:
                log.info(self.log_msg('No Previous URL found for url \
                                                    %s'%self.currenturi))
                break
        
    @logit(log, '__createTasksForThreads')
    def __createTasksForThreads(self):
        """
        This will create Tasks for the threads found on the given url
        The # of Tasks are limited by Config Variable
        """
        self.__current_thread_count = 0
        self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                            'revolutionhealth_forums_maxthreads'))
        while self.__getThreads():
            try:
                self.currenturi =  self.__baseuri + self.soup.find('span', \
                                    'next_link').find('a', href=True)['href']
                self.__setSoupForCurrentUri()
            except:
                log.info(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                break
        log.info('Total # of tasks found is %d'%len(self.linksOut))
        return True
    
    @logit(log, '__getThreads')
    def __getThreads(self):
        """
        It will fetch each thread and its associate infomarmation
        and add the tasks
        """
        thread_links = [('http://www.revolutionhealth.com' + x.find('a', href=re.compile('/forums/topics'))['href']).strip() for x in self.soup.findAll('div', 'community_post')]
        if not thread_links:
            log.info(self.log_msg('No thread links found'))
            return False
        for thread_link in thread_links:
            self.__current_thread_count += 1
            if  self.__current_thread_count > self.__max_threads_count:
                log.info(self.log_msg('Reaching maximum post,Return false \
                                        from the url %s'%self.currenturi))
                return False
            temp_task = self.task.clone()
            temp_task.instance_data['uri'] = thread_link
            temp_task.instance_data['already_parsed'] = True
            log.info(thread_link)
            self.linksOut.append(temp_task)
            log.info(self.log_msg('Task Added'))
        return True
        
    @logit(log, '__addQuestionInfo')
    def __addQuestionInfo(self):
        """
        This will get Question Info
        """
        question = self.soup.find('div', 'question')
        if not question:
            log.info(self.log_msg('No Question Info Found'))
            return False
            #raise Exception('Question Not Found, Cannot Continue')
        page = {'uri':self.currenturi}
        try:            
            self.__thread_topic = page['data'] = page['title'] = stripHtml(question.find('dd', id='topic_text').renderContents())
        except:
            log.info(self.log_msg('No Question Data Found'))
            return False
        try:
            created_text = stripHtml(question.find('cite', id='created').renderContents())
            match_object = re.search('Posted (?P<date_str>.+?) in (?P<et_thread_category>.+?) by (?P<et_author_name>.+?$)', created_text)
            page.update(match_object.groupdict())
        except:
            log.info(self.log_msg('Not enough information'))
        try:
            date_str = page.pop('date_str')
            page['posted_date'] = self.__getDate(re.sub('\(.+?\)', '', date_str))
            #page['posted_date'] = datetime.strftime(datetime.strptime(re.sub('\(.+?\)', '', date_str), '%I:%M%p  on %Y-%m-%d'), "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('posted_date not found in url %s'%self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")        
        try:
            rating_str = stripHtml(self.soup.find('span', id='overall_rating_score').renderContents())
            if not rating_str == '--':
                page['ef_thread_rating'] = float(rating_str)
        except:
            log.info(self.log_msg('Rating not found'))
        try:
            page['ei_replies_count'] = int(stripHtml(self.soup.find('span', id='answer_count').renderContents()).split(' of ')[1].replace(',', ''))
        except:
            log.info(self.log_msg('Replies count not found'))
        try:
            if checkSessionInfo(self.__genre, self.session_info_out, self.task.instance_data['uri'],    \
                                         self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info return True, Already exists'))
                return True
            result = updateSessionInfo('review', self.session_info_out, self.\
                task.instance_data['uri'], get_hash( page ), 'forum', self.task.instance_data.get('update'))
            if result['updated']:
                page['path'] = [ self.task.instance_data['uri'] ] 
                page['parent_path'] = []
                page['uri'] = self.currenturi
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                page['entity'] = 'question'
                page.update(self.__task_elements_dict)
                self.pages.append(page)
            else:
                log.info(self.log_msg('Result[updated] returned True for \
                                                        uri'%self.currenturi))
        except:
            log.exception(self.log_msg("parent post couldn't be parsed"))
        return True
    
    @logit(log, '__goToLastPage')
    def __goToLastPage(self):
        """
        This will set the soup the last page of the post
        Sample url :
        http://www.revolutionhealth.com/forums/mens-health/111324
        which has 14 pages on March 19, 2010
        """
        try:
            pagination_tag = self.soup.find('div', 'pagination_content')
            if not pagination_tag:
                log.info(self.log_msg('pagination not found, posts exists in current\
                                                            url%s'%self.currenturi))
                return 
            list_of_page_links = pagination_tag.findAll('a', text=re.compile('\d+'), href=True)
            if not list_of_page_links:
                log.info(self.log_msg('pagination not found, posts exists in current\
                                                            url%s'%self.currenturi))
                return
            self.currenturi = self.__baseuri + list_of_page_links[-1].parent['href']
            self.__setSoupForCurrentUri()
        except:
            log.info(self.log_msg('Last page cannot find from the given page no \
                                    for url %s'%self.task.instance_data['uri']))
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = self.soup.findAll('div', 'answer')
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
    def __addPost(self, post):
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            unique_key = post.find('dd', 'user_generated', id=True)['id']
            if checkSessionInfo(self.__genre, self.session_info_out, unique_key, \
                             self.task.instance_data.get('update'),parent_list\
                                                = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'%unique_key))
                return False
            page = self.__getData(post)
            if not page:
                log.info(self.log_msg('page contains empty data, getdata \
                                    returns  False for uri %s'%self.currenturi))
                return True
            result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                get_hash( page ),'forum', self.task.instance_data.get('update'), \
                                parent_list=[self.task.instance_data['uri']])
            if result['updated']:
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [self.task.instance_data['uri'], unique_key]
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
    def __getData(self, post):
        """ This will return the page dictionry
        """
        page = {'entity':'answer', 'uri':self.currenturi, 'title': 'Re: ' + self.__thread_topic}
        try:
            page['data'] = stripHtml(post.find('dd', 'user_generated').renderContents())
        except:
            log.info(self.log_msg('Data not found for the url %s'%self.currenturi))
            return True
        try:
            created_text = stripHtml(post.find('cite').renderContents())
            match_object = match_object = re.search('Posted (.+?) by (.+?)$', created_text)
            page['et_author_name'] = match_object.group(2)
        except:
            log.exception(self.log_msg('Not enough information'))
        try:
            page['posted_date'] = self.__getDate(re.sub('\(.+?\)', '', match_object.group(1)))
            #page['posted_date'] = datetime.strftime(datetime.strptime(re.sub('\(.+?\)', '', date_str), '%I:%M%p  on %Y-%m-%d'), "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('posted_date not found in url %s'%self.currenturi))
            
        try:
            rating_tag = post.find('div', 'rate_rating').p
            rating_value_tag = rating_tag.span.extract()
            rating_value = stripHtml(rating_value_tag.renderContents())
            if not rating_value == '--':
                page['ef_rating_overall'] = float(rating_value)
            page['ei_ratings_count'] = int(re.search('\d+', stripHtml(rating_tag.renderContents())).group())
        except:
            log.info(self.log_msg('Ratings count not found'))
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
    
    @logit(log, '__getDate')
    def __getDate(self, date_str):
        '''This will get the date str and converts to Proper date time format
        '''
        try:
            date_obj = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            match_object = re.search('(\d+) (weeks?|months?|years?|days?|hours?) ago',date_str)
            num_of_hours = no_of_days = 0
            if match_object:
                days_str = match_object.group(2)
                if days_str.startswith('day'):
                    no_of_days = int (match_object.group(1))
                elif days_str.startswith('week'):
                    no_of_days = int (match_object.group(1)) * 7
                elif days_str.startswith('year'):
                    no_of_days = int (match_object.group(1)) * 365
                elif days_str.startswith('month'):
                    no_of_days = int (match_object.group(1)) * 30
                elif days_str.startswith('hour'):
                    num_of_hours = int(match_object.group(1))
                date_obj = datetime.strftime(datetime.now() - timedelta(hours = (no_of_days*24) + num_of_hours) ,"%Y-%m-%dT%H:%M:%SZ")
            else:
                date_obj = datetime.strftime(datetime.strptime(date_str, '%I:%M%p  on %Y-%m-%d'), "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('posted date not found'))
        return date_obj
                