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
from datetime import datetime
from cgi import parse_qsl
from urllib import urlencode

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('AolMessageBoardsConnector')
class AolMessageBoardsConnector(BaseConnector):
    '''
    This will fetch the info for messageboards.aol.com
    Sample uris is
    http://messageboards.aol.com/aol/en_us/articles.php?boardId=544758&func=3&channel=Rants+%26+Raves
    '''        
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of messageboards.aol.com
        """
        try:
            self.__genre = 'review'
            self.__baseuri = 'http://messageboards.aol.com/'
            self.__setSoupForCurrentUri()
            if  not '&articleId=' in self.currenturi:
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
        question_post = self.soup.find('table', 'thread')
        if not question_post:
            log.info(self.log_msg('No posts found in url %s'%self.currenturi))
            return False
        self.__addPost(question_post, True)
        self.__goToLastPage()
        while self.__iteratePosts():
            try:
                previous_page_no = re.search('\((.+?)\)', self.soup.find('a', text='&lt;').parent['href']).group(1)
                pagination_url = self.__getPaginationUrl(previous_page_no)
                url_parts = self.task.instance_data['uri'].split('?')
                params = dict(parse_qsl(pagination_url.split('?')[-1]))
                self.currenturi = url_parts[0] + '?' + urlencode(params)
                self.__setSoupForCurrentUri(data=params)
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
                                            'aol_messageboards_maxthreads'))
        while self.__getThreads():
            try:
                self.currenturi =  self.task.instance_data['uri'].split('?')[0] + '?' + re.search('\(\'(.+?)\'\)', self.soup.find('a', text='&gt;').parent['href']).group(1).split('?')[-1]
                self.__setSoupForCurrentUri()
            except:
                log.info(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                break
            
        log.info('Total # of tasks found is %d'%len(self.linksOut))
        #self.linksOut = None #
        return True
    
    @logit(log, '__getThreads')
    def __getThreads(self):
        """
        It will fetch each thread and its associate infomarmation
        and add the tasks
        """
        try:
            threads = self.soup.find('table', id='myTable').find('tr', 'table_hdr')\
                    .findNextSiblings()[1:-1] # Last one is the footer
            if not threads:
                log.info(self.log_msg('No threads are found for url %s'%\
                                                            self.currenturi))
                return False
        except:
            log.exception(self.log_msg('Threads not found'))
            return False
        for thread in threads:
            self.__current_thread_count += 1
            if  self.__current_thread_count > self.__max_threads_count:
                log.info(self.log_msg('Reaching maximum post,Return false \
                                        from the url %s'%self.currenturi))
                return False
            try:
                topic_info = thread.findAll('td')
                date_str, last_post_author = [x.strip() for x in stripHtml\
                        (topic_info[-2].renderContents()).splitlines() \
                                                    if not x.strip()=='']
                thread_time = datetime.strptime(date_str, '%m/%d/%y at %I:%M %p')
            except:
                log.exception(self.log_msg('Cannot fetch the date for the url\
                                                        %s'%self.currenturi))
                continue
            if checkSessionInfo('Search', self.session_info_out, thread_time, \
                                    self.task.instance_data.get('update')):
                    log.info(self.log_msg('Session info Returns True for url \
                                                        %s'%self.currenturi))
                    return False
            self.__last_timestamp = max(thread_time, self.__last_timestamp )
            temp_task = self.task.clone()                    
            try:
                temp_task.instance_data['uri'] = self.task.instance_data['uri'].split('?')[0] + '?' + topic_info[1].find('a')['href'].split('?')[-1]
            except:
                log.exception(self.log_msg('Cannot find the thread url \
                                            in the uri %s'%self.currenturi))
                continue
            temp_task.pagedata['edate_last_post_date'] =  datetime.\
                                    strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
            temp_task.pagedata['et_thread_last_post_author'] = last_post_author
            try:
                rating = re.search('\d+', topic_info[2].img['src'].rsplit('/', 1)[-1])
                if rating:
                    temp_task.pagedata['ef_thread_rating'] = float(rating.group())
            except:
                log.info(self.log_msg('thread rating not found in the url\
                                                        %s'%self.currenturi))
            self.linksOut.append(temp_task)
            log.info(self.log_msg('Task Added'))
        return True
    
    def __setParentPage(self):
        """This will get the parent info
        """
        page = {}
        try:
            page['et_thread_hierarchy'] = self.__hierarchy = [x.strip() for x \
                        in stripHtml(self.soup.find('div', 'crumbsleft').\
                                    renderContents()).split('>') if x.strip()]
            page['data'] = page['title'] = page['et_thread_hierarchy'][-1]
        except:
            log.info(self.log_msg('hierachies not found in url %s'%self.currenturi))
            return
        try:
            date_str = stripHtml(self.soup.find('table', 'thread').tr.findAll\
                                ('div')[-2].renderContents()).split(' - ')[-1]
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str, 
                    'Posted on %m/%d/%y at %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date not be found in %s'% self.currenturi))
        if checkSessionInfo(self.__genre, self.session_info_out, self.task.instance_data['uri'], \
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        page_data_keys = ['edate_last_post_date', 'ef_thread_rating', \
                                                'et_thread_last_post_author']
        [page.update({each:self.task.pagedata.get(each)}) for each in \
                            page_data_keys if self.task.pagedata.get(each) ]
        try:
            result = updateSessionInfo('review', self.session_info_out, self.\
                task.instance_data['uri'], get_hash( page ), 'forum', self.task.instance_data.get('update'))
            if result['updated']:
                page['path'] = [ self.task.instance_data['uri'] ] 
                page['parent_path'] = []
                page['uri'] = self.currenturi
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
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
            last_page_no = re.search('\((.+?)\)', self.soup.find('a', text='&gt;|').parent['href']).group(1)
            pagination_url = self.__getPaginationUrl(last_page_no)
            if not pagination_url:
                return False
            url_parts = self.task.instance_data['uri'].split('?')
            params = dict(parse_qsl(pagination_url.split('?')[-1]))
            self.currenturi = url_parts[0] + '?' + urlencode(params)
            self.__setSoupForCurrentUri(data=params)
        except:
            log.info(self.log_msg('Last page cannot find from the given page no \
                                    for url %s'%self.task.instance_data['uri']))
    
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = self.soup.findAll('table', 'thread')
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
            unique_key = stripHtml(post.find('div', id=re.compile('msgId\d+'))\
                            .renderContents())[1:-1].replace('Msg Id: ', '')
            if checkSessionInfo(self.__genre, self.session_info_out, unique_key, \
                             self.task.instance_data.get('update'),parent_list\
                                        = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'%unique_key))
                return False
            page = self.__getData(post, is_question)
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
                log.info(self.log_msg('Page added'))
            else:
                log.info(self.log_msg('Update session info returns False for \
                                                    url %s'%self.currenturi))
                return False
        except:
            log.exception(self.log_msg('Cannot add the post for the uri %s'%self.currenturi))
        return True
    
    @logit(log, '__getData')
    def __getData(self, post, is_question):
        """ This will return the page dictionry
        """
        page = {'entity':'question' if is_question else 'answer', 'uri':self.currenturi}
        try:
            page['data'] = stripHtml(post.find('div', id=re.compile('content\d+')).renderContents())
        except:
            log.info(self.log_msg('Data not found for the url %s'%self.currenturi))
            return True
        try:
            if not is_question:
                page['uri'] = self.task.instance_data['uri'].split('?')[0] + '?'  + re.search('\(\'(.+?)\'\)', post.tr.findAll('div')[-1].a['onclick']).group(1).split('?')[-1]
            else:
                page['uri'] = self.task.instance_data['uri']
        except:
            #log.info(self.log_msg('uri not found'))
            #log.info(stripHtml(post.renderContents()))
            page['uri'] = self.currenturi
        try:
            rating_tag = post.find('div', 'rating')
            page['ei_data_voted_authors_count'] = int(stripHtml(rating_tag.find('span', id=re.compile('sample\d+')).renderContents()))
        except:
            log.info(self.log_msg('Rating tag not found'))
        try:
            rating_info = re.search('\d+', rating_tag.find('a', 'tips').img['src'].rsplit('/', 1)[-1])
            if rating_info:
                page['ef_data_rating'] = float(rating_info.group())            
        except:
            log.info(self.log_msg('Data rating not found'))
        try:
            date_str = stripHtml(post.tr.findAll\
                                ('div')[-2].renderContents()).split(' - ')[-1]
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str, 
                    'Posted on %m/%d/%y at %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date not be found in %s'% self.currenturi))
        try:
            page['et_author_name'] = stripHtml(post.find('div', id=re.compile('profile')).find('div','middle').div.renderContents())
        except:
            log.info(self.log_msg('Ratings count not found'))
        try:
            page['title'] = page['et_thread_topic'] = self.__hierarchy[-1]
            page['et_thread_forum'] = self.__hierarchy[-3]
            page['et_thread_subforum'] = self.__hierarchy[-2]
            if not is_question:
                page['title'] = 'Re: ' + page['title']
        except:
            log.info(self.log_msg('Cannot find the Data thread details for \
                                        this Post %s'%self.currenturi))
            
                
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
        
    @logit(log, '__getPaginationUrl')
    def __getPaginationUrl(self, page_id):
        '''This will return the pagination url from Java Scripts
        '''
        pagination_url = ''
        try:
            pagination_script = self.soup.find('div', 'btm_pageination').renderContents()
            pagination_url = re.search ('var bottom_page_url="(.+?)";', pagination_script).group(1)
            pagination_url = pagination_url.replace('%f%', 'forward=false')
            pagination_url = pagination_url.replace('%tt', 'listPos=' + re.search("bottom_page_tokens\[%s\] = '(.+?)';"%page_id, pagination_script ).group(1))
        except:
            log.exception(self.log_msg('Cannnot find the Pagination'))
        return pagination_url
