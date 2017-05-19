'''
Copyright (c)2008-2010 Serendio Software Private Limited
All Rights Reserved
This software is confidential and proprietary information of Serendio Software.
It is disclosed pursuant to a non-disclosure agreement between the recipient 
and Serendio. This source code is provided for informational purposes only, 
and Serendio makes no warranties, either express or implied, in this. 
Information in this program, including URL and other Internet website 
references, is subject to change without notice. The entire risk of the use 
or the results of the use of this program remains with the user. Complying 
with all applicable copyright laws is the responsibility of the user. Without 
limiting the rights under copyright, no part of this program may be reproduced,
stored in, or introduced into a retrieval system, or distributed or 
transmitted in any form or by any means (electronic, mechanical, photocopying,
recording, on a website, or otherwise) or for any purpose, without the express 
written permission of Serendio Software.
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
from urllib2 import urlparse
from urllib import urlencode
from datetime import datetime,timedelta
from cgi import parse_qsl

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('AnandTechForumsConnector')
class AnandTechForumsConnector(BaseConnector):
    '''
    This will fetch the info for forums.anandtech.com
    Sample uris is
    http://forums.anandtech.com/forumdisplay.php?f=17
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of forums.anandtech.com
        """
        try:
            self.__baseuri = 'http://forums.anandtech.com/'
            self.__setSoupForCurrentUri()
            if 'forumdisplay' in self.currenturi:
                return self.__createTasksForThreads()
            else:
                return self.__addThreadAndPosts()
                #this will fetch the thread links and Adds Tasks                
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'\
                                                            %self.currenturi))
            return False
        
    @logit(log, '__addThreadAndPosts')
    def __addThreadAndPosts(self):
        """
        This will add the Thread info from setParentPage method and 
        Add the posts of the thread in addPosts mehtod
        """
        self.__genre = "Review"
        self.__hierarchy = []
        self.__task_elements_dict = {
                        'priority':self.task.priority,
                        'level': self.task.level,
                        'last_updated_time':datetime.strftime(datetime.utcnow()
                                                    , "%Y-%m-%dT%H:%M:%SZ"), 
                        'pickup_date':datetime.strftime(datetime.utcnow(),  
                                                        "%Y-%m-%dT%H:%M:%SZ"), 
                        'connector_instance_log_id': \
                                        self.task.connector_instance_log_id, 
                        'connector_instance_id':
                                            self.task.connector_instance_id, 
                        'workspace_id':self.task.workspace_id, 
                        'client_id':self.task.client_id, 
                        'client_name':self.task.client_name, 
                        'versioned':False, 
                        'category':self.task.instance_data.get('category',''), 
                        'task_log_id':self.task.id }
        self.__setParentPage()
        question_post = self.soup.find('div', id=re.compile('^edit.*?'))
        self.__addPost(question_post, True)
        self.__goToLastPage()
        while self.__iteratePosts():
            try:
                next_page_uri = self.soup.find('a', text='&lt;',rel='prev').parent['href']
                data_dict = dict(parse_qsl(next_page_uri.split('?')[-1]))
                if 's' in data_dict.keys():
                    data_dict.pop('s')
                self.currenturi = self.__baseuri + 'showthread.php?'+ urlencode(data_dict)                    
                self.__setSoupForCurrentUri()
            except:
                log.exception(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                break
        return True
        
    @logit(log, '__createTasksForThreads')
    def __createTasksForThreads(self):
        """
        This will create Tasks for the threads found on the given url
        The # of Tasks are limited by Config Variable
        """
        self.__total_threads_count = 0
        self.__last_timestamp = datetime(1980, 1, 1)
        #The Maximum No of threads to process, Bcoz, not all the forums get
        #updated Everyday, At maximum It will 100
        self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                            'anandtechforums_maxthreads'))
        self.__setSoupForCurrentUri()
        while self.__getThreads():
            try:
                next_page_uri = self.soup.find('a', text='&gt;',rel='Next').parent['href']
                data_dict = dict(parse_qsl(next_page_uri.split('?')[-1]))
                if 's' in data_dict.keys():
                    data_dict.pop('s')
                self.currenturi = self.__baseuri + 'forumdisplay.php?'+ urlencode(data_dict)                    
                self.__setSoupForCurrentUri()
            except:
                log.exception(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                break
        log.info(self.log_msg('# of Tasks Added is %d'%len(self.linksOut)))
        #self.linksOut = []
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
            threads = [each.findParent('tr') for each in self.soup.find('table',id='threadslist').findAll('td',id=re.compile('^td_threadtitle_.*$'))]
            if not threads:
                log.info(self.log_msg('No threads are found for url %s'%\
                                                            self.currenturi))
                return False
        except:
            log.info(self.log_msg('exception while getting threads'))
            return False
        for thread in threads:
            if stripHtml(thread.find('td',id=re.compile('^td_threadtitle_\d+')).renderContents()).startswith('Sticky:'):
                log.info(self.log_msg('Its a Sticky Thread, Ignore it in\
                                            the url %s'%self.currenturi))
                continue
            self.__total_threads_count += 1
            if  self.__total_threads_count > self.__max_threads_count:
                log.info(self.log_msg('Reaching maximum post,Return false \
                                        from the url %s'%self.currenturi))
                return False
            try:
                last_post_and_author = [each.strip() for each in stripHtml(thread.find('td',title=re.compile('^Replies.*')).find('div').renderContents()).split('\n') if not each =='']
                post_date = last_post_and_author[0]
                if post_date.startswith('Today'):
                    post_date = post_date.replace('Today',datetime.strftime(datetime.utcnow(),'%m-%d-%Y'))
                if post_date.startswith('Yesterday'):
                    post_date = post_date.replace('Yesterday',datetime.strftime(datetime.utcnow() - timedelta(days=1),'%m-%d-%Y'))
                thread_time = datetime.strptime (post_date,'%m-%d-%Y %I:%M %p')
            except:
                log.exception(self.log_msg('date not found in %s'%\
                                                        self.currenturi))
                continue
            if checkSessionInfo('Search', self.session_info_out, thread_time, \
                                 self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info Returns True for %s'%\
                                                        self.currenturi))
                return False
            self.__last_timestamp = max(thread_time, self.__last_timestamp)
            temp_task = self.task.clone()                    
            try:
                temp_task.instance_data[ 'uri' ] = self.__baseuri  + thread.find('a',id=re.compile('^thread_title_.*$'))['href']
            except:
                log.exception(self.log_msg('Cannot find the thread url \
                                        in the uri %s'%self.currenturi))
                continue
            temp_task.pagedata['edate_last_post_date'] =  datetime.\
                            strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
            temp_task.pagedata['et_thread_last_post_author'] = re.sub('by','',last_post_and_author[1]).strip()
            try:
                temp_task.pagedata['et_author_name'] = stripHtml(thread.find('a',id=re.compile('^thread_title_.*$')).findNext('div','smallfont').renderContents())
            except:
                log.info(self.log_msg('Author name not found in the url\
                                                    %s'%self.currenturi))
            try:
                thread_reply_and_views = thread.find('td',title=re.compile('^Replies.*'))['title'].split('Views:')
                temp_task.pagedata['ei_thread_replies_count'] = int(re.sub('[^\d]','', thread_reply_and_views[0]).strip())
            except:
                log.info(self.log_msg('Replies count not found in the url\
                                                %s'%self.currenturi))                
            try:
                temp_task.pagedata['ei_thread_views_count'] = int(re.sub('[^\d]','', thread_reply_and_views[1]).strip())
            except:
                log.info(self.log_msg('Views count not found in the url\
                                                %s'%self.currenturi))                
            self.linksOut.append(temp_task)
        return True
    
    @logit(log, '__setParentPage')
    def __setParentPage(self):
        """
        This will get the parent info
        """
        page = {}
        try:
            page['et_thread_hierarchy'] = [each.replace('>','').strip() for each in stripHtml(self.soup.find('span','navbar').findParent('table').renderContents()).split('\n') if not each.strip()=='']
            page['data'] = page['title']= page['et_thread_hierarchy'][-1]
        except:
            log.info(self.log_msg('Thread hierarchy and Title Not found for uri\
                                                            %s'%self.currenturi))
            return
        if checkSessionInfo(self.__genre, self.session_info_out, self.task.\
                instance_data['uri'], self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        page_data_keys = ['et_last_post_author_name','ei_thread_replies_count','ei_thread_views_count','edate_last_post_date','ei_thread_votes_count','ef_thread_rating']
        [page.update({each:self.task.pagedata.get(each)}) for each in page_data_keys if self.task.pagedata.get(each)]
        try:
            page['et_thread_id'] = self.currenturi.split('&')[-1].split('=')[-1]
        except:
            log.info(self.log_msg('Thread id not found'))            
        try:
            date_str  = stripHtml(self.soup.find('div',id=re.compile('^edit.*?')).find('td','thead').renderContents())\
                                                        .split('\n')[-1].strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,\
                                    '%m-%d-%Y,  %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), \
                                                        "%Y-%m-%dT%H:%M:%SZ")
        try:
            result = updateSessionInfo('review', self.session_info_out, self.\
                task.instance_data['uri'], get_hash( page ), 'forum', \
                                    self.task.instance_data.get('update'))
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
        """This will set the soup the last page of the post
        """
        try:
            pagination_tag = self.soup.find('div', 'pagenav')
            if not pagination_tag:
                return
            uri = None
            last_page_tag = pagination_tag.find('a', title=re.compile('Last Page'))
            if last_page_tag:
                uri = last_page_tag['href']
            else:
                last_page_tag = pagination_tag.findAll('a', href=True, text=re.compile('^\d+$'))
                if last_page_tag:
                    uri = last_page_tag[-1].parent['href']
            if not uri:
                log.info(self.log_msg('Post found in only one page'))
                return
            data_dict = dict(parse_qsl(uri.split('?')[-1]))
            if 's' in data_dict.keys():
                data_dict.pop('s')
            self.currenturi = self.__baseuri + 'showthread.php?'+ urlencode(data_dict)
            self.__setSoupForCurrentUri()
        except:
            log.info(self.log_msg('Last page cannot find from the given page no \
                                    for url %s'%self.task.instance_data['uri']))
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = self.soup.findAll('div',id=re.compile('^edit.*?'))
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
            unique_key = post.find('a')['name']
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
                get_hash( page ),'forum', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if result['updated']:
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [ self.task.instance_data['uri'], unique_key]
                page['uri'] = self.currenturi + '#' + unique_key
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
    def __getData(self, post, is_question):
        """ This will return the page dictionry
        """
        page = {'entity':'question' if is_question else 'answer'}
        try:
            url = 'http://forums.zynga.com' + post.find('a',id=re.compile ('^postcount'))['href']
            data_dict = dict(parse_qsl(url.split('?')[-1]))
            if 's' in data_dict.keys():
                data_dict.pop('s')
            page['uri'] = self.__baseuri + 'showpost.php?'+ urlencode( data_dict )      
            if is_question:
                page['title'] = stripHtml((post.find('td',id = re.compile\
                    ('^td_post_.*')).find('div','smallfont')).renderContents())
            data_tag = post.find('div',id=re.compile('^post_message_.*'))
            try:
                quotes_tag = data_tag.findAll('table')
                for each in quotes_tag:
                    each.findParent('div').extract()
            except:
                log.info(self.log_msg('data does not contain the previous posts'))
            page['data']  =  stripHtml(data_tag.renderContents())
        except:
            log.exception(self.log_msg('Data not found for this post'))
            return False
        try:
            post_date  = stripHtml(post.find('td','thead').renderContents())\
                                                    .split('\n')[-1].strip()
            if post_date.startswith('Today'):
                post_date = post_date.replace('Today',datetime.strftime(datetime.utcnow(),'%m-%d-%Y'))
            if post_date.startswith('Yesterday'):
                post_date = post_date.replace('Yesterday',datetime.strftime(datetime.utcnow() - timedelta(days=1),'%m-%d-%Y'))
            #log.info(post_date)
            page['posted_date'] = datetime.strftime(datetime.strptime(post_date,\
                                    '%m-%d-%Y,  %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), \
                                                        "%Y-%m-%dT%H:%M:%SZ")
                                    
        try:
            page['et_author_name'] = stripHtml(post.find('a','bigusername').renderContents())
        except:
            log.info(self.log_msg('author name not found'))
        try:
            date_str = '01 '+ post.find('td','thead').findParent('table').find('div',text=re.compile('Join Date:.*')).split('Join Date:')[-1].strip()
            page['edate_author_member_since'] = datetime.strftime(datetime.strptime(date_str,'%d %b %Y'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Author member Since not found'))
        try:
            page['et_author_membership'] = stripHtml(post.find('a','bigusername').findNext('div','smallfont').renderContents())
        except:
            log.info(self.log_msg('Author membership not found'))
        try:
            page['ei_author_posts_count'] = int(re.sub('[^\d]', '', post.find('td','thead').findParent('table').find('div',text=re.compile('Posts:.*')).split('Posts:')[-1].strip()))
        except:
            log.info(self.log_msg('Author Post count not found'))
        try:
            page['et_author_location'] = post.find('td', 'thead').findParent('table').find('div',text=re.compile('Location:.*')).split('Location:')[-1].strip()
        except:
            log.info(self.log_msg('Author Location not found'))
        try:
            hierarchy = [each.replace('>','').strip() for each in stripHtml(self.soup.find('span','navbar').findParent('table').renderContents()).split('\n') if not each.strip()=='']
            page['et_data_forum'] = hierarchy[1]
            page['et_data_subforum'] = hierarchy[2]
            page['title'] = page['et_data_topic'] = hierarchy[3]
            if not is_question:
                page['title'] = "Re: " + page['title']  
        except:
            log.info(self.log_msg('data forum not found'))
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
