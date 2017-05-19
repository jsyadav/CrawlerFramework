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
import copy
import logging
from datetime import datetime
from urllib2 import urlparse

from tgimport import tg
from utils.utils import stripHtml, get_hash
from baseconnector import BaseConnector
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging.getLogger('MdJunctionConnector')
class MdJunctionConnector(BaseConnector):
    '''A Connector to fetch the information from mdjunction.com
    '''

    @logit(log, 'fetch')
    def fetch(self):
        '''fetch method of MdJunction Connector
            Sample uri are
            http://www.mdjunction.com/forums/fibromyalgia-discussions/fibro-and-family
            http://www.mdjunction.com/forums/fibromyalgia-discussions/fibro-and-family/1260624-my-husband-advice		
        '''
        try:
            self.__baseuri = 'http://www.mdjunction.com'
            self.__setSoupForCurrentUri()
            if re.search('/\d+(\-\w+)+$', self.currenturi):
                self.__addThreadAndPosts()
            else:
                self.__createTasksForThreads()
            return True
        except:
            log.info(self.log_msg('Exception in fetch'))
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
        main_page_soup = copy.copy(self.soup)
        previous_uri = self.currenturi
        try:
            question_post = self.soup.find('table', 'postTable')
            self.__addPost(question_post, self.currenturi, True)
        except:
            log.exception(self.log_msg('Question post not not found in url %s'%self.currenturi))
            return False
        self.soup = copy.copy(main_page_soup)
        self.currenturi = previous_uri
        self.__goToLastPage()        
        while True:
            main_page_soup = copy.copy(self.soup)
            self.__iteratePosts()
            try:
                previous_page_tag =  main_page_soup.find('a', text='&lt; Prev')
                if not previous_page_tag:
                    log.info(self.log_msg('Crawled all the pages'))
                    break
                self.currenturi = previous_page_tag.parent['href']
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
        log.info('hello')
        self.__current_thread_count = 0
        self.__last_timestamp = datetime(1980, 1, 1)
        self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                            'mdjunction_maxthreads'))
        while self.__getThreads():
            try:
                current_page_tag = self.soup.find('strong', text=re.compile('^\[\d+\]$'))
                self.currenturi = current_page_tag.findParent('td').find('a', text=str(int(current_page_tag[1:-1])+1)).parent['href']
                self.__setSoupForCurrentUri()
            except:
                log.exception(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                break
        log.info('Total # of tasks found is %d'%len(self.linksOut))
        #self.linksOut = None
        if self.linksOut:
            updateSessionInfo('Search', self.session_info_out, \
                    self.__last_timestamp, None, 'ForumThreadsPage', \
                    self.task.instance_data.get('update'))
        return True
        
    @logit(log, '__getThreads')
    def __getThreads(self):
        """
        It will fetch each thread and its associate infomarmation
        and add the tasks
        """
        
        threads = self.soup.findAll('tr', attrs={'class':re.compile('sectiontableentry\d')})
        if not threads:
            log.info(self.log_msg('No threads found in url %s'%self.currenturi))
            return False
        for thread in threads:
            self.__current_thread_count += 1
            if  self.__current_thread_count > self.__max_threads_count:
                log.info(self.log_msg('Reaching maximum post,Return false \
                                        from the url %s'%self.currenturi))
                return False
            
            try:
                topic_info = thread.findAll('td', recursive=False)
                date_str, last_post_author = [x.strip() for x in stripHtml(topic_info[-1].renderContents()).splitlines() if x.strip()]
                thread_time  = datetime.strptime(date_str, '%m/%d/%Y %I:%M %p')
            except:
                log.exception(self.log_msg('Cannot fetch the date for the url\
                                                        %s'%self.currenturi))
                continue
            if checkSessionInfo('Search', self.session_info_out, thread_time,\
                                    self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info Returns True for url %s'%self.currenturi))
                return False
            self.__last_timestamp = max(thread_time, self.__last_timestamp)
            temp_task = self.task.clone()                    
            try:
                temp_task.instance_data['uri'] = topic_info[2].find('a')['href']
            except:
                log.exception(self.log_msg('Cannot find the thread url \
                                        in the uri %s'%self.currenturi))
                continue
            temp_task.pagedata['edate_last_post_date'] =  datetime.\
                            strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
            temp_task.pagedata['et_thread_last_post_author']  =  re.sub('^by ', '', last_post_author).strip()
            try:
                temp_task.pagedata['et_author_name'] = stripHtml(topic_info[5].renderContents())
            except:
                log.info(self.log_msg('Author name not found in the url\
                                                    %s'%self.currenturi))
            try:
                temp_task.pagedata['ei_thread_views_count'] = int(stripHtml(topic_info[4].renderContents()).replace(',',''))
            except:
                log.info(self.log_msg('Views count not found in the url\
                                                %s'%self.currenturi))
            try:
                temp_task.pagedata['ei_thread_replies_count'] = int(stripHtml(topic_info[3].renderContents()).replace(',',''))
            except:
                log.exception(self.log_msg('Replies count not found in the url\
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
            page['et_thread_hierarchy'] = self.__hierarchy = [x.strip() for x in stripHtml(self.soup.find('div', 'pathway').renderContents()).splitlines() if x.strip()]
            page['title'] = page['et_thread_hierarchy'][-1]
        except:
            log.exception(self.log_msg('Thread hierarchy and Title Not found for uri\
                                                            %s'%self.currenturi))
            return
        try:
            date_str = stripHtml(self.soup.find('td', 'postTopBar').div.renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str, '%m/%d/%Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date not be found in %s'% self.currenturi))
        if checkSessionInfo('review', self.session_info_out, self.task.instance_data['uri'], \
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        page_data_keys = ['et_author_name', 'ei_thread_replies_count', 'ei_thread_views_count'\
                                                        , 'edate_last_post_date']
        [page.update({each:self.task.pagedata.get(each)}) for each in page_data_keys if self.task.pagedata.get(each)]
        try:
            result = updateSessionInfo('review', self.session_info_out, self.\
                task.instance_data['uri'], get_hash( page ), 'forum', self.task.instance_data.get('update'))
            if result['updated']:
                page['path'] = [self.task.instance_data['uri']] 
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
        except:
            log.exception(self.log_msg("parent post couldn't be parsed"))
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            review_url = self.currenturi
            posts = self.soup.findAll('table', 'postTable')
            if not posts:
                log.info(self.log_msg('No posts found in url %s'%self.currenturi))
                return False
            posts.reverse()
            for post in posts:
                if not self.__addPost(post, review_url, False):
                    log.info(self.log_msg('Post not added to self.pages for url\
                                                            %s'%self.currenturi))
                    return False
            return True
        except:
            log.exception(self.log_msg('Reviews are not found for url %s'%self.currenturi))
            return False
    
    @logit(log, '__addPost')
    def __addPost(self, post, review_url, is_question=False):
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            log.info(review_url)
            unique_key = post.find('a', attrs={'name':True})['name']
            permalink = review_url + '#' + unique_key
            if checkSessionInfo('review', self.session_info_out, \
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
            result = updateSessionInfo('review', self.session_info_out, unique_key, \
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
        permalink = self.currenturi + '#' + unique_key
        page = {'entity':'question' if is_question else 'answer'}
        page['uri'] = permalink        
        try:
            data_tag = post.find('td', 'sb_messagebody')
            unwanted_tags = data_tag.findAll('div')
            [each.extract() for each in unwanted_tags]
            page['data'] = stripHtml(data_tag.renderContents())
        except:
            log.info(self.log_msg('Empty data found in %s'%permalink))
            page['data'] = ''
        if not page['data']:
            log.info(self.log_msg('Empty data found in url %s'%self.currenturi))
            return
        try:
            date_str = stripHtml(post.find('td', 'postTopBar').div.renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str, '%m/%d/%Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date not be found in %s'% self.currenturi))
        try:
            author_tag = post.find('td','leftBorder')
            page['et_author_name'] = stripHtml(author_tag.renderContents())
        except:
            log.info(self.log_msg('Author name not found in %s'% self.currenturi))
        try:
            self.currenturi = page['et_author_profile'] = author_tag.b.a['href']
            self.__setSoupForCurrentUri()
            page.update([('et_author_'+y[0].lower()[:-1].replace(' ', '_'),y[1]) for y in [[stripHtml(x.renderContents()) for x in each.findAll('td')] for each in self.soup.find('div', 'cbAllLeft').findAll('tr', attrs= {'class':re.compile('sectiontableentry\d')}) if len(each.findAll('td'))>=2]])
            if page.has_key('et_author_a_little_about_me'):
                page['et_author_description'] = page.pop('et_author_a_little_about_me')
        except:
            log.info(self.log_msg('Author Profile Not found'))
        try:
            author_info = [x.strip() for x in stripHtml(author_tag.renderContents()).splitlines() if x.strip()]
            page['et_author_category'] = author_info[2]
        except:
            log.info(self.log_msg('Author posts count not found in %s'%self.currenturi))
        try:
            page['ei_author_posts_count'] = int(author_info[1].split(':')[-1].replace(',', '').strip())
        except:
            log.info(self.log_msg('Author posts count not found in %s'%self.currenturi))
        if len(self.__hierarchy) >= 3:
            page['et_thread_forum'] = self.__hierarchy[-3]
            page['et_thread_subforum'] = self.__hierarchy[-2]
            page['title'] = page['et_thread_topic'] = self.__hierarchy[-1]
            if not is_question:
                 page['title'] = 'Re: ' + page['title']
        else:
            page['title'] = ''
            log.info(self.log_msg('Cannot find the Data thread details for this Post'%permalink))
        return page
    
    @logit(log, '__goToLastPage')
    def __goToLastPage(self):
        '''This will find the Last Page and set the Soup
        '''
        try:
            last_page_tag = self.soup.find('a', text='End &gt;&gt;')
            if not last_page_tag:
                log.info(self.log_msg('Posts found in only one page'))
                return
            self.currenturi = last_page_tag.parent['href']
            self.__setSoupForCurrentUri()
        except:
            log.info(self.log_msg('Last page tag not found'))
                        
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
