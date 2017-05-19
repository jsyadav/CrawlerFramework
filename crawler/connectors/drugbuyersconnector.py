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
from datetime import datetime, timedelta


from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('DrugBuyersConnector')

class DrugBuyersConnector(BaseConnector):
    '''
    This will fetch the info for fatwallet.com
    Sample uris is
    "http://drugbuyers.com/freeboard/ubbthreads.php/forums/56/1/
                                                        Anxiety_Panic_Stress"
    Note:Don't take Sticky Posts, it contains general Info about the forum
    '''
        
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of talk drugbuyers.com
        """
        try:
            self.__genre = 'review'
            self.__baseuri = 'http://drugbuyers.com'
            self.__setSoupForCurrentUri()
            #If format of the url is /forums/f[DIGIT]==> Pickup the List of Question and Answer
            #or 
            #If format of the url is /forums/[DIGIT]/
            #Its a Thread page, Which will fetch the thread links and Adds Tasks
            if '/forums/' in self.currenturi:
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
            question_post = self.soup.find('td','subjecttable').findParent('table')
        except:
            log.info(self.log_msg('No Posts found in url %s'%self.currenturi))
            return False
        self.__addPost(question_post, True)
        self.__goToLastPage()
        while self.__iteratePosts():
            try:
                self.currenturi = self.__baseuri +  self.soup.find('table', 't_standard pagination').find('a', text='&lt;').parent['href']
                self.__setSoupForCurrentUri()
            except:
                log.info(self.log_msg('Next Page link not found for url \
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
                                            'drugbuyers_maxthreads'))
        while self.__getThreads():
            try:
                self.currenturi = self.__baseuri + self.soup.find('table', 't_standard pagination').find('a', text='&gt;').parent['href']
                self.__setSoupForCurrentUri()
            except:
                log.exception(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                break
        #log.info('Total # of tasks found is %d'%len(self.linksOut))
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
            threads = self.soup.findAll('tr', id=re.compile('^postrow\-inline\-\d+$'))
            if not threads:
                log.info(self.log_msg('No threads are found for url %s'%\
                                                            self.currenturi))
                return False
            for thread in threads:
                if thread.find('img', title='Announcement'):
                    log.info(self.log_msg('Announcement thread, not to pickup'))
                    continue
                self.__current_thread_count += 1
                if self.__current_thread_count > self.__max_threads_count:
                    log.info(self.log_msg('Reaching maximum post,Return false \
                                            from the url %s'%self.currenturi))
                    return False
                try:
                    topic_time = [x.strip() for x in stripHtml(thread.find('td', attrs = {'class':re.compile('topictime$')}).renderContents()).split('\n') if x.strip() ]
                    thread_time  = self.__getDateObj(topic_time[0])
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
                    title_tag = thread.find('td', attrs={'class': re.compile('topicsubject$')})
                    temp_task.instance_data['uri'] = self.__baseuri + \
                                        title_tag.div.a['href'].split('#')[0]
                except:
                    log.exception(self.log_msg('Cannot find the thread url \
                                            in the uri %s'%self.currenturi))
                    continue
                temp_task.pagedata['edate_last_post_date'] =  datetime.\
                                strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                try:
                    temp_task.pagedata['et_thread_last_post_author']  =  \
                                                                topic_time[-1]
                except:
                    log.info(self.log_msg('Last Post author name not found \
                                            in the url %s'%self.currenturi))
                try:
                    temp_task.pagedata['et_author_name'] = re.sub('^by ', '', \
                                stripHtml(title_tag.find('span', 'small')\
                                                    .renderContents())).strip()
                except:
                    log.info(self.log_msg('Author name not found in the url\
                                                        %s'%self.currenturi))
                try:
                    temp_task.pagedata['ei_thread_views_count'] = int(stripHtml(thread.find('td', attrs = {'class':re.compile('topicviews')}).renderContents()))
                except:
                    log.info(self.log_msg('Views count/Thread age not found in the url\
                                                    %s'%self.currenturi))
                try:
                    temp_task.pagedata['ei_thread_replies_count'] = stripHtml(thread.find('td', attrs = {'class':re.compile('topicreplies')}).renderContents())
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
            page['et_thread_hierarchy'] = self.__hierarchy = [stripHtml(x) for x in self.soup.find('td', 'breadcrumbs').findAll('span')[-1].renderContents().split('&raquo;')][2:]
            page['title'] = page['et_thread_hierarchy'][-1]
        except:
            log.exception(self.log_msg('Thread hierarchy and Title Not found for uri\
                                                            %s'%self.currenturi))
            return
        
        try:
            date_str = stripHtml(self.soup.find('td', 'subjecttable').span.renderContents()).split('-')[-1].strip()
            page['posted_date'] = datetime.strftime(self.__getDateObj(date_str), "%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date not be found in %s'% self.currenturi))
        if checkSessionInfo(self.__genre, self.session_info_out, self.task.instance_data['uri'],\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        page_data_keys = ['et_author_name','ei_thread_replies_count','ei_thread_views_count'\
                            ,'edate_last_post_date','ei_thread_votes_count']
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
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))
            
    @logit(log, '__goToLastPage')
    def __goToLastPage(self):
        """This go the last page page of the thread and set the soup
        """
        try:
            self.currenturi = self.__baseuri + self.soup.find('table', \
                't_standard pagination').findAll('a', text=re.compile('^\d+$')\
                                                        )[-1].parent['href']
            self.__setSoupForCurrentUri()
        except:
            log.info(self.log_msg('Last page cannot find from the given page no \
                                    for url %s'%self.task.instance_data['uri']))
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = [x.findParent('table') for x in self.soup.findAll('td', \
                                                            'subjecttable')]
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
            unique_key = stripHtml(post.find('span',id=re.compile('^number\d+')).renderContents())
            permalink = self.currenturi + '#' + unique_key
            if checkSessionInfo(self.__genre, self.session_info_out, \
                        unique_key, self.task.instance_data.get('update'),\
                        parent_list=[self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'% \
                                                                permalink))
                return False
            page = self.__getData(post, is_question, unique_key)
            if not ( page['data'] and page['title']):
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
            author_tag = post.find('td', 'author-content')
            page['et_author_name'] = stripHtml(author_tag.find('span', id=re.compile('menu_control_\d+')).renderContents())
        except:
            log.info(self.log_msg('Author name for found for url %s'%self.currenturi))
        try:
            author_info =  [x.strip() for x in stripHtml(author_tag.find('span', 'small').renderContents()).splitlines() if x.strip()]
            page['et_author_category'] = author_info[0]
        except:
            log.info(self.log_msg('Author Category not found in url %s'%self.currenturi))
        for aut_info in author_info[1:]:
            try:
                values = aut_info.split(':')
                if values[0].strip() == 'Registered':
                    page['edate_author_member_since'] = datetime.strftime(datetime.strptime(values[-1].strip(), '%m/%d/%y'), "%Y-%m-%dT%H:%M:%SZ")
                elif values[0].strip() == 'Posts':
                    page['ei_author_posts_count'] = int(values[-1].strip())
                elif values[0].strip() == 'Loc':
                    page['et_author_location'] = values[1].strip()
            except:
                log.info(self.log_msg('Cannot find author info in url %s'%self.currenturi))
        try:
            date_str = stripHtml(post.find('td', 'subjecttable').span.renderContents()).split('-')[-1].strip()
            page['posted_date'] = datetime.strftime(self.__getDateObj(date_str), "%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date not be found in %s'% self.currenturi))
        try:
            replied_authors = [stripHtml(x.renderContents()).replace('Originally Posted By: ','') for x in post.findAll('div','ubbcode-header')]
            if replied_authors:
                page['et_data_replied_authors'] = replied_authors
        except:
            log.info(self.log_msg('No Replied Authors found in url %s'%self.currenturi))
        try:
            data_tag = post.find('div', 'post_inner')
            signatre_tag = data_tag.find('div', 'signature')
            if signatre_tag:
                signatre_tag.extract()
            previous_posts = data_tag.findAll('div', 'ubbcode-block')
            if previous_posts:
                [each.extract() for each in previous_posts]
            page['data'] = stripHtml(data_tag.renderContents())
        except:
            log.info(self.log_msg('Data not found in url %s'%self.currenturi))
            page['data'] = ''
        try:
            page['title'] = stripHtml(post.find('td', 'subjecttable').find('b').renderContents())
        except:
            log.info(self.log_msg('Title not found in url %s'%self.currenturi))
            page['title'] = ''
        
        if len(self.__hierarchy) >= 3:
            page['et_thread_topic'] = self.__hierarchy[-1]
            page['et_thread_forum'] = self.__hierarchy[-3]
            page['et_thread_subforum'] = self.__hierarchy[-2]
        else:
            log.info(self.log_msg('Cannot find the Data thread details for this Post %s'%permalink))        
        return page
    
    #@logit(log, '__getDate')
    def __getDateObj(self, date_str):
        '''This will get the Data string which have the format or starts with yesterday
        and return the Date in the standard format
        ''' 
        current_time = datetime.utcnow()       
        try:
            if date_str.endswith(' ago'):
                mins = secs = hours = 0
                match_object = re.search('(\d+) minutes?',date_str)
                if match_object:
                    mins = int(match_object.group(1))
                match_object = re.search('(\d+) seconds?',date_str)
                if match_object:
                    secs = int(match_object.group(1))
                match_object = re.search('(\d+) hours?',date_str)
                if match_object:
                    hours = int(match_object.group(1))
                total_seconds = ( hours * 3600 ) + ( mins *60 ) + secs
                current_time = current_time - timedelta(seconds=total_seconds)
            elif date_str.startswith('Yesterday at') or date_str.startswith('Today at'):
                if date_str.startswith('Yesterday at'):
                    current_time = current_time - timedelta(days=1)
                previous_day_str = '/'.join([str(each) for each in  [current_time.month,current_time.day,current_time.year]] )
                new_date_str = previous_day_str + date_str.split(' at')[-1] 
                current_time = datetime.strptime(new_date_str, '%m/%d/%Y %I:%M %p')
            else:
                current_time = datetime.strptime(date_str, '%m/%d/%y  %I:%M %p')
        except:
            log.info(self.log_msg('Posted date cannot be found in url %s'%self.currenturi))
        return current_time
            
    
            
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