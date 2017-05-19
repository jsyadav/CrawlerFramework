'''
Copyright (c)2008-2010 Serendio Software Private Limited
All Rights Reserved
This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
import copy
import logging
import urllib2
from cgi import parse_qsl
from urllib2 import urlparse
from datetime import datetime,timedelta

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('CrackberryForumsConnector')
class CrackberryForumsConnector(BaseConnector):
    '''
    This will fetch the info for forum.androidcentral.com
    Sample uris is
    http://forums.crackberry.com/f2/
    Note: No need to take Sticky Posts, Since it contains general Info abt the forum
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of forums.crackberry.com
        """
        try:
            self.__links_to_process = []
            if re.search('/f\d+/$', self.currenturi) or 'search.php' in self.currenturi:
                self.__createTasksForThreads()
            if self.__links_to_process[:5]:
                for each_link in self.__links_to_process:
                    self.currenturi = each_link
                    self.__addThreadAndPosts()
            return True
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'%self.currenturi))
            return False
        
    @logit(log, '__addThreadAndPosts')
    def __addThreadAndPosts(self):
        """
        This will add the Thread info from setParentPage method and 
        Add the posts of the thread in addPosts mehtod
        """
        try:
            self.__setSoupForCurrentUri()
            self.__genre = "Review"
            self.__task_elements_dict = {
                            'priority':self.task.priority,
                            'level': self.task.level,
                            'last_updated_time':datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ"),
                            'pickup_date':datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ"),
                            'connector_instance_log_id': self.task.connector_instance_log_id,
                            'connector_instance_id':self.task.connector_instance_id,
                            'workspace_id':self.task.workspace_id,
                            'client_id':self.task.client_id,
                            'client_name':self.task.client_name,
                            'versioned':False,
                            'category':self.task.instance_data.get('category',''),
                            'task_log_id':self.task.id }
            question_post = self.soup.find('div', id=re.compile ('^edit\d+'))
            main_page_soup = copy.copy(self.soup)
            if question_post:
                self.__addPost(question_post, True)
            self.__goToLastPage(main_page_soup)
            main_page_soup = copy.copy(self.soup)
            while self.__iteratePosts():
                try:
                    self.currenturi =  main_page_soup.find('div', 'pagenav').\
                                            find('a',text='&lt;').parent['href']
                    self.__setSoupForCurrentUri()
                    main_page_soup = copy.copy(self.soup)
                except:
                    log.info(self.log_msg('No Previous URL found for url \
                                                        %s'%self.currenturi))
                    break
        except:
            log.exception(self.log_msg('Exception while fetching posts'))
        return True
    
    @logit(log, '__createTasksForThreads')
    def __createTasksForThreads(self):
        """
        This will create Tasks for the threads found on the given url
        The # of Tasks are limited by Config Variable
        """
        self.__total_threads_count = 0
        self.__last_timestamp = datetime( 1980,1,1 )
        if 'search.php' in self.currenturi:
            query_term = self.task.instance_data['query_terms']
            url_template = 'http://forums.crackberry.com/search.php?do=process&childforums=1&do=process&exactname=1&query=%s&quicksearch=1&s=&securitytoken=guest&showposts=0'
            self.currenturi = url_template % urllib2.quote(urllib2.unquote(query_term.strip().encode('utf-8')))
            data = dict(parse_qsl(self.currenturi.split('?')[1]))
            self.__setSoupForCurrentUri(data=data)
        else:
            self.__setSoupForCurrentUri()
        #The Maximum No of threads to process, Bcoz, not all the forums get
        #updated Everyday, At maximum It will 100
        self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                            'crackberryforums_maxthreads'))
        
        while self.__getThreads():
            try:
                self.currenturi =  self.soup.find('a', rel='next')['href']
                if not self.currenturi.startswith('http://forums.crackberry.com/'):
                    self.currenturi = 'http://forums.crackberry.com/' + self.currenturi
                self.__setSoupForCurrentUri()
            except:
                log.info(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                break
        if self.__links_to_process:
            updateSessionInfo('Search', self.session_info_out,\
                    self.__last_timestamp , None, 'ForumThreadsPage', \
                    self.task.instance_data.get('update'))
        log.info(self.log_msg('# of tasks added is %d'%len(self.linksOut)))
        return True
    
    @logit(log, '__getThreads')
    def __getThreads(self):
            """
            It will fetch each thread and its associate infomarmation
            and add the tasks
            """
            threads = [each.findParent('tr') for each in self.soup.find('table',\
                id='threadslist').findAll('td', id=re.compile('td_threadtitle_'))]
            if not threads:
                log.info(self.log_msg('No threads are found for url %s'%\
                                                            self.currenturi))
                return False
            for thread in threads:
                if thread.find('b', text='Sticky'):
                    log.info(self.log_msg('Its a Sticky Thread, Ignore it in\
                                                the url %s'%self.currenturi))
                    continue
                self.__total_threads_count += 1
                if  self.__total_threads_count > self.__max_threads_count:
                    log.info(self.log_msg('Reaching maximum post,Return false \
                                            from the url %s'%self.currenturi))
                    return False
                # Each Thread is a table record, Consisting of Status Icon,
                #Image,URI and Posted date,views and Replies in respective TD tag
                try:
                    thread_info = thread.findAll('td', recursive=False)
                    date_str , last_post_author = [x.strip() for x in stripHtml\
                        (thread_info[3].find('div','smallfont').renderContents())\
                                            .split('\n') if not x.strip()=='']
                    thread_time  = self.__getDateObj(date_str,'%m-%d-%Y %I:%M %p')
                except:
                    log.exception(self.log_msg('Cannot fetch the date for the url\
                                                            %s'%self.currenturi))
                    continue
                if checkSessionInfo('Search', self.session_info_out, thread_time,\
                                        self.task.instance_data.get('update')):
                        log.info(self.log_msg('Session info Returns True for url %s'%self.currenturi))
                        return False
                self.__last_timestamp = max(thread_time , self.__last_timestamp )
                try:
                    temp_url = thread_info[2].find('a', id=re.compile('thread_title_'))['href']
                    if not temp_url.startswith('http://forums.crackberry.com/'):
                        temp_url = 'http://forums.crackberry.com/' + temp_url.split('?')[0]
                    log.info(temp_url)
                    self.__links_to_process.append(temp_url)
                except:
                    log.exception(self.log_msg('Cannot find the thread url \
                                            in the uri %s'%self.currenturi))
                    continue
            return True
    
    @logit(log, '__goToLastPage')
    def __goToLastPage(self, main_page_soup):
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
            pagination_tag = main_page_soup.find('div', 'pagenav')
            if not pagination_tag:
                log.info(self.log_msg('pagination not found, posts exists in current\
                                                            url%s'%self.currenturi))
                self.soup = main_page_soup
                return
            last_page_tag = pagination_tag.find(title=re.compile('Last Page'))
            if not last_page_tag:
                log.info(self.log_msg('Last page tag not found for url %s'%\
                                                self.task.instance_data['uri']))
                number_tags = pagination_tag.findAll('a',text=re.compile ('^\d+$'))
                if len(number_tags)>0:
                    last_page_tag = number_tags[-1].parent
            if last_page_tag:
                self.currenturi = last_page_tag['href']
                self.__setSoupForCurrentUri()
            else:
                self.soup = main_page_soup
        except:
            log.info(self.log_msg('Last page cannot find from the given page no \
                                    for url %s'%self.currenturi))
            self.soup = main_page_soup
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = self.soup.findAll('div', id=re.compile ('^edit\d+'))
            
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
            unique_key_tag = post.find('a' ,id=re.compile ('postcount\d+'))
            if not unique_key_tag and not unique_key_tag.get('href'):                
                log.info(self.log_msg('Permalink not found, ignoring the Post in the url %s'%self.currenturi))
                return True
            unique_key = unique_key_tag['href']
            if checkSessionInfo(self.__genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info returns True for uri %s'%unique_key))
                return False
            page = self.__getData(post, is_question, unique_key)
            if not page:
                log.info(self.log_msg('page contains empty data, getdata \
                                    returns  False for uri %s'%self.currenturi))
                return True
            result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                get_hash( page ),'forum', self.task.instance_data.get('update'))
            if result['updated']:
                page['parent_path'] = []
                page['path'] = [unique_key]
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
        page = {'entity':'question' if is_question else 'answer'}
        try:
            data_tag = post.find('div', id=re.compile('post_message_'))
            previous_message_tags = data_tag.findAll('b', text='Quote:')
            for previous_message_tag in previous_message_tags:
                prev_tag = previous_message_tag.parent.findParent('div')
                if prev_tag:
                    prev_tag.extract()
            page['data'] = stripHtml(data_tag.renderContents())
        except:
            log.info(self.log_msg('Data not found for the url %s'%permalink))
            page['data'] = ''
        #Sometimes only Image is uploaded on the Post, in that case data will be empty
        if not page['data']: 
            log.info(self.log_msg("Data is not found for %s,discarding this Post"%(permalink)))
            return False 
        author_tag = post.find('a','bigusername')
        if author_tag:
            page['et_author_name'] = stripHtml(author_tag.renderContents())
        else:
            log.info(self.log_msg('Author name tag not found for this Post'%permalink))
        aut_cat_tag = post.find('div','smallfont')
        if aut_cat_tag:
            page['et_author_category'] = stripHtml(aut_cat_tag.renderContents())
        try:
            page['et_author_profile'] = author_tag['href']
        except:
            log.info(self.log_msg('author name not found for the url %s'%self.currenturi))
        try:
            temp_hierarchy = [each for each in [stripHtml(x.renderContents()) for \
                x in self.soup.find('table','tborder').table.findAll('td')] if each]
            hierarchy = [x.strip() for x in temp_hierarchy[0].split('>')]
            hierarchy.append(temp_hierarchy[1])
            page['et_thread_forum'] = hierarchy[-3]
            page['et_thread_subforum'] = hierarchy[-2]
            page['et_thread_hierarchy'] = hierarchy 
            page['title'] = page['et_thread_topic'] = hierarchy[-1]
            if not is_question:
                 page['title'] = 'Re: ' + page['title']
        except:
            page['title'] = ''
            log.info(self.log_msg('Cannot find the Data thread details for this Post %s'%permalink))
        post_date_div_tag = post.find('td','thead')
        date_tag = None
        if post_date_div_tag:
            date_tags = post.find('td', 'thead').findAll('div')
            if date_tags and len(date_tags)>0:
                date_tag = date_tags[-1]
        page['posted_date'] = datetime.strftime( self.__getDateObj(date_tag, \
                                '%m-%d-%Y, %I:%M %p') ,'%Y-%m-%dT%H:%M:%SZ')
        return page
    
    @logit(log,'__getDateObj')
    def __getDateObj(self, date_str_tag, format):
        '''
        It will get the HTML Tag as Input Or date_str as input 
        and returns posted_date
        '''
        date_obj = datetime.utcnow()
        if not date_str_tag:
            return date_obj
        try:
            date_str =  stripHtml(str(date_str_tag))
            if date_str.startswith('Today') or date_str.startswith('Yesterday'):
                day_str = re.split('[\s,]',date_str)[0]
                day_dict = {'Today':0, 'Yesterday':1}
                date_str = (datetime.strftime(datetime.utcnow()-timedelta(days=day_dict\
                    [day_str]),"%m-%d-%Y") + date_str.replace(day_str, '')).strip()                                
            date_obj = datetime.strptime(date_str,format)
        except:
            log.exception(self.log_msg('Posted date not found for the url %s'%self.currenturi))
        return date_obj
            
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