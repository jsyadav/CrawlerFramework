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
from urllib2 import urlparse
from datetime import datetime,timedelta

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('TalkAndroidConnector')
class TalkAndroidConnector(BaseConnector):
    '''
    This will fetch the info for talkandroid.com
    Sample uris is
    http://www.talkandroid.com/android-forums/motorola-droid/
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of talk android 
        """
        try:
            self.__setSoupForCurrentUri()
            #If the url ends with .html==> Pickup the List of Question and Answer
            if self.currenturi.endswith('.html'):
                return self.__addThreadAndPosts() 
            else:
                #Its a Thread page, Which will fetch the thread links and Adds Tasks
                return self.__createTasksForThreads()
        except:
            log.exception(self.log_msg('soup not set for the uri %s'%self.currenturi))
            return False
        
    @logit(log, '__addThreadAndPosts')
    def __addThreadAndPosts(self):
        """
        This will add the Thread info from setParentPage method and 
        Add the posts of the thread in addPosts mehtod
        """
        self.genre = "Review"
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
        question_post = self.soup.find('div', id=re.compile ('^edit\d+'))
        main_page_soup = copy.copy(self.soup)
        if question_post:
            self.__addPost(question_post, True)
        self.__goToLastPage(main_page_soup)
        while True:
            main_page_soup = copy.copy(self.soup)
            if not self.__iteratePosts():
                break
            try:
                self.currenturi =  main_page_soup.find('div', 'pagenav').\
                                        find('a',text='&lt;').parent['href']
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
        try:
            self.__total_threads_count = 0
            self.__last_timestamp = datetime( 1980,1,1 )
            #The Maximum No of threads to process, Bcoz, not all the forums get
            #updated Everyday, At maximum It will 100
            self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                                'talkandroid_maxthreads'))
            self.__setSoupForCurrentUri()
            while True:
                try:
                    if not self.__getThreads():
                        break
                    self.currenturi =  self.soup.find('a', text='&gt;').parent['href']
                    self.__setSoupForCurrentUri()
                except:
                    log.info(self.log_msg('Next Page link not found for url \
                                                        %s'%self.currenturi))
                    break
            if self.linksOut:
                updateSessionInfo('Search', self.session_info_out,\
                        self.__last_timestamp , None, 'ForumThreadsPage', \
                        self.task.instance_data.get('update'))
            return True
        except:
            log.exception(self.log_msg('Exception while creating tasks for the url %s'\
                                                                %self.currenturi)) 
            return False
    
    def __getThreads(self):
            """
            It will fetch each thread and its associate infomarmation
            and add the tasks
            """
            threads = [each.findParent('tr') for each in self.soup.find('table',\
                id='threadslist').findAll('a', id=re.compile('thread_title_'))]
            if not threads:
                log.info(self.log_msg('No threads are found for url %s'%\
                                                            self.currenturi))
                return False
            for thread in threads:
                self.__total_threads_count += 1
                if  self.__total_threads_count > self.__max_threads_count:
                    log.info(self.log_msg('Reaching maximum post,Return false \
                                                in the url %s'%self.currenturi))
                    return False
                thread_info = thread.findAll('td', recursive=False)
                try:
                    date_str , last_post_author = [x.strip() for x in stripHtml\
                        (thread_info[2].find('div','smallfont').renderContents())\
                                            .split('\n') if not x.strip()=='']
                    thread_time  = self.__getDateObj(date_str,'%m-%d-%Y %I:%M %p')
                except:
                    log.exception(self.log_msg('Cannot fetch the date from the \
                                                        url %s'%self.currenturi))
                    continue
                
                if checkSessionInfo('Search',self.session_info_out, thread_time,\
                                    self.task.instance_data.get('update')):
                    log.info(self.log_msg('Session info reached last crawled\
                            page for uri %s'%self.task.instance_data['uri']))
                    return False
                self.__last_timestamp = max(thread_time , self.__last_timestamp )
                temp_task=self.task.clone()
                try:
                    temp_task.instance_data[ 'uri' ] = thread_info[1].find\
                            ('a', id=re.compile('thread_title_'))['href'] 
                except:
                    log.exception(self.log_msg('uri not found , cannot add the task for the\
                                                uri %s'%self.currenturi))
                    continue
                temp_task.pagedata['et_thread_last_post_author']  =  re.sub('^by\
                                                ', '', last_post_author).strip()
                temp_task.pagedata['edate_last_post_date']=  datetime.\
                                    strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                try:
                    author_and_rating_tag = thread_info[1].find('div', 'smallfont')
                    temp_task.pagedata['et_author_name'] = stripHtml(\
                                author_and_rating_tag.renderContents())                                                            
                except:
                    log.info(self.log_msg('Author name not found uri %s'%self.currenturi))
                try:
                    temp_task.pagedata['ei_thread_num_views'] = int\
                            (stripHtml(thread_info[-1].renderContents()).replace(',',''))
                except:
                    log.info(self.log_msg('num of views not found for uri '%self.currenturi))
                try:
                    temp_task.pagedata['ei_thread_num_replies'] = int\
                            (stripHtml(thread_info[-2].renderContents()).replace(',',''))
                except:
                    log.info(self.log_msg('num of repies not found for uri '%self.currenturi))
                try:
                    rating_str = author_and_rating_tag.find('img', \
                                            'inlineimg')['alt'].split(',')
                    temp_task.pagedata['ef_thread_rating'] =  float(\
                                    rating_str[1].replace('average.',''))
                except:
                    log.info(self.log_msg('Thread rating not found for url\
                                                    %s'%self.currenturi))
                try:
                    temp_task.pagedata['ei_thread_votes_count'] = int(re.search\
                                            ('\d+',rating_str[0]).group())
                except:
                    log.info(self.log_msg('Thread votes not found for url\
                                                    %s'%self.currenturi))
                self.linksOut.append( temp_task )
            return True
    
    @logit(log, '__getParentPage')
    def __setParentPage(self):
        """
        This will get the parent info
        """
        page = {}
        try:
            hierarchies = [each for each in [stripHtml(x.renderContents()) for \
                x in self.soup.find('table','tborder').table.findAll('td')] if each]
            self.__hierarchy = [x.strip() for x in hierarchies[0].split('>')] 
            page['title'] = hierarchies[1] 
        except:
            log.info(self.log_msg('Thread hierarchy is not found for uri %s'%self.currenturi)) 
            return
        try:
            self.__hierarchy.append(page['title'])
            page['et_thread_hierarchy'] = self.__hierarchy
        except:
            log.info(self.log_msg('Thread hierarchy is not found for uri %s'%self.currenturi))
            
        if checkSessionInfo(self.genre, self.session_info_out, self.task.instance_data['uri'],\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists for \
                                                        uri %s'%self.currenturi))
            return
        for each in ['et_author_name','ei_thread_num_replies','ei_thread_num_views'\
                ,'edate_last_post_date','ef_thread_rating','ei_thread_votes_count']:
            page_data_value = self.task.pagedata.get(each) 
            if page_data_value:
                page[each] = page_data_value
        date_str_tag = self.soup.find('div' ,id='posts')
        if date_str_tag:
            posted_date_obj = self.__getDateObj(date_str_tag.find('td', 'thead'),\
                                                        '%m-%d-%Y, %I:%M %p')
        else:
            posted_date_obj = self.__getDateObj(None,'%m-%d-%Y, %I:%M %p')
        page['posted_date'] = datetime.strftime( posted_date_obj ,'%Y-%m-%dT%H:%M:%SZ')
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
        except:
            log.exception(self.log_msg("parent post couldn't be parsed"))
    
    @logit(log, '__setLastPage')
    def __goToLastPage(self, main_page_soup):
        """
        This will set the soup the last page of the post, from here 
        It needs to be navigated in the reverse order.
        If the posts are in within 3 pages, There won't be any Last Page Tag
        Then find max of page no ( 2 or 3) and Navigate to the page
        find out the List of posts and reverse it and then start adding it
        First add the Question, then above steps will be done, 
        session info will take of duplicates posts not adding to pages #Mohit - Not True
        """
        last_page_tag = None
        try:
            last_page_tag = main_page_soup.find('div', 'pagenav').find(title=re.compile('Last Page'))
        except:
            log.info(self.log_msg('Last page tag not found for url %s'%self.task.instance_data['uri']))
        try:
            if not last_page_tag:
                last_page_tag = main_page_soup.find('div', 'pagenav').findAll('a',\
                                        text=re.compile ('^\d+$'))[-1].parent
        except:
            log.info(self.log_msg('Last page cannot find from the given page no \
                                    for url %s'%self.task.instance_data['uri']))
        try:
            if not last_page_tag:
                log.info(self.log_msg('Posts exists only in the current page for \
                                            url %s'%self.task.instance_data['uri']))
                self.soup = main_page_soup
                return
            self.currenturi = last_page_tag['href']
            self.__setSoupForCurrentUri()
        except:
            log.info(self.log_msg('Exception occurred while Setting the last page \
                                    for url %s'%self.task.instance_data['uri']))
            self.soup = main_page_soup
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will add the posts for a particular thread
        """
        try:
            posts = self.soup.findAll('div', id=re.compile ('^edit\d+'))
            posts.reverse()
            if not posts:
                log.info(self.log_msg('No posts found for url %s'%self.currenturi)) 
                return False
            log.debug(self.log_msg('Total No of Posts found is %d in the url %s'%(len(posts),self.currenturi)))
            for post in posts:
                if not self.__addPost(post):
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
            unique_key_tag = post.find('a', title='permalink')
            if not unique_key_tag and not unique_key_tag.get('href'):                
                log.info(self.log_msg('Permalink not found, ignoring the Post in the url %s'%self.currenturi))
                return True
            unique_key = unique_key_tag['href']
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'%unique_key))
                return False
            page = self.__getData(post, is_question, unique_key)
            if not page:
                log.info(self.log_msg('page contains empty data, getdata \
                                    returns  False for uri %s'%self.currenturi))
                return True
            result = updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                get_hash( page ),'forum', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if result['updated']:
                page['parent_path'] = page['path'] = [self.task.instance_data['uri']]
                page['path'].append(unique_key)
                page['uri'] = unique_key
                page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
                page.update(self.__task_elements_dict)
                self.pages.append(page)
            else:
                log.info(self.log_msg('Update session info returns False for \
                                                    url %s'%self.currenturi))
        except:
            log.info(self.log_msg('Cannot add the post for the uri %s'%self.currenturi))
        return True
    
    @logit(log, '__getData')
    def __getData(self, post, is_question, permalink):
        """ This will return the page dictionry
        """
        page = {'entity':'question' if is_question else 'answer'}
        try:
            data_tag = post.find('div', id=re.compile('post_message_'))
            previous_message_tags = data_tag.findAll(True, text='Quote:')
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
            self.currenturi = page['et_author_profile'] = author_tag['href']
            self.__setSoupForCurrentUri()
            info_tags = self.soup.find('div', id='profile_tabs').findAll(True,'shade')
            for each in info_tags:
                try:
                    if each.name == 'span':
                        span_value = stripHtml(each.renderContents())
                        if span_value.endswith(':'):
                            key = span_value[:-1].lower().replace(':','').strip().replace(' ','_')
                            value = str(each.next.next).strip()
                            page[ 'et_author_' + key ] = value
                    elif each.name == 'dt':
                        key = 'et_author_' + stripHtml(each.renderContents()).lower().replace(' ','_')
                        page[key] = stripHtml(each.findNext('dd').renderContents())
                    else:
                        log.info(self.log_msg('Unknown Tag in uri %s'%self.currenturi))
                except:
                    log.info(self.log_msg('Author Info not found from the url %s'%self.currenturi))
        except:
            log.info(self.log_msg('author name not found for the url %s'%self.currenturi))
        if len(self.__hierarchy) >= 3:
            page['et_thread_forum'] = self.__hierarchy[-3]
            page['et_thread_subforum'] = self.__hierarchy[-2]
            page['title'] = page['et_thread_topic'] = self.__hierarchy[-1]
            if not is_question:
                 page['title'] = 'Re: ' + page['title']
        else:
            page['title'] = ''
            log.info(self.log_msg('Cannot find the Data thread details for this Post'%permalink))
        page['posted_date'] = datetime.strftime( self.__getDateObj(post.find('td', \
                            'thead'), '%m-%d-%Y, %I:%M %p') ,'%Y-%m-%dT%H:%M:%SZ')
        
        return page
    
    @logit(log,'__getDateObj')
    def __getDateObj(self, date_str_tag, format):
        '''
        It will get the HTML Tag as Input Or date_str as input 
        and returns posted_date
        '''
        try:
            if not date_str_tag:
                log.info(self.log_msg('No Date Tag is found for the url %s'%self.currenturi))
                return datetime.utcnow()
            date_str =  stripHtml(str(date_str_tag))
            if date_str.startswith('Today') or date_str.startswith('Yesterday'):
                day_str = re.split('[\s,]',date_str)[0]
                day_dict = {'Today':0, 'Yesterday':1}
                date_str = (datetime.strftime(datetime.utcnow()-timedelta(days=day_dict\
                    [day_str]),"%m-%d-%Y") + date_str.replace(day_str, '')).strip()                                
            date_obj = datetime.strptime(date_str,format)
        except:
            log.exception(self.log_msg('Posted date not found for the url %s'%self.currenturi))
            date_obj = datetime.utcnow()
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
            raise Exception('Page content not fetched for the url %s'%self.currenturi)
        self._setCurrentPage()