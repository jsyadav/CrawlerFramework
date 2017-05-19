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
from datetime import datetime


from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('FatWalletConnector')
class FatWalletConnector(BaseConnector):
    '''
    This will fetch the info for fatwallet.com
    Sample uris is
    "http://www.fatwallet.com/forums/finance/?attr[]=aor&attr[]=credit&attr[]=
    deal&attr[]=deposits&attr[]=discussion&attr[]=investing&attr[]=new-user-
    question&attr[]=personal-finance&attr[]=question&attr[]=real-estate"
    Note:Don't take Sticky Posts, it contains general Info about the forum
    '''
        
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of talk fatwallet.com
        """
        try:
            self.__genre = 'review'
            self.__baseuri = 'http://www.fatwallet.com'
            self.__setSoupForCurrentUri()
            #If format of the url is /forums/f[DIGIT]==> Pickup the List of Question and Answer
            #or 
            #If format of the url is /forums/[DIGIT]/
            #Its a Thread page, Which will fetch the thread links and Adds Tasks
            if not re.search('/\d+/$', self.currenturi.split('?')[0]):
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
        question_post = self.soup.find('div', id=re.compile('^t\d+$'))
        if not question_post:
            log.info(self.log_msg('No posts found in url %s'%self.currenturi))
            return False
        self.__addPost(question_post, True)
        self.__goToLastPage()
        while self.__iteratePosts():
            try:
                pagination_tag = self.soup.find('ul', 'pagination topPagination')
                current_page = int(stripHtml(pagination_tag.find('strong').renderContents()))
                next_page_no = str(current_page -  1)
                self.currenturi = self.__baseuri + pagination_tag.find('a', text=next_page_no).parent['href']                
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
        self.__current_thread_count = 0
        self.__last_timestamp = datetime( 1980,1,1 )
        self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                            'fatwallet_maxthreads'))
        while self.__getThreads():
            try:
                headers = []
                next_tag = self.soup.find('input', value='Next 20')
                form_tag = next_tag.findParent('form')
                input_values = form_tag.findAll('input', type='hidden')
                for input_value in input_values:
                    headers.append((input_value['name'],input_value['value'] ))
                self.currenturi = 'http://www.fatwallet.com' + form_tag['action'] + '?' + urlencode(headers )
                self.__setSoupForCurrentUri()
            except:
                log.exception(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                break
        log.info('Total # of tasks found is %d'%len(self.linksOut))
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
            threads = []
            forum_topics = self.soup.find('table', id='forumTopics')
            if not forum_topics:
                log.info(self.log_msg('No forum found in url %s'%self.currenturi))
                return False
            threads = forum_topics.findAll('tr', id=re.compile('^t+\d+$'))
            if not threads:
                log.info(self.log_msg('No threads are found for url %s'%\
                                                            self.currenturi))
                return False
            for thread in threads:
                if 'sticky' in thread['class']:
                    log.info(self.log_msg('Its a Sticky Thread, Ignore it in\
                                                the url %s'%self.currenturi))
                    continue
                self.__current_thread_count += 1
                if  self.__current_thread_count > self.__max_threads_count:
                    log.info(self.log_msg('Reaching maximum post,Return false \
                                            from the url %s'%self.currenturi))
                    return False
                
                try:
                    topic_info = thread.findAll('td','topicInfo')
                    last_post_author, date_str = [x.strip() for x in stripHtml(topic_info[-1].renderContents()).split('>') if not x.strip()=='']
                    thread_time  = datetime.strptime(date_str+'m', '%m%d/%y %I:%M%p')
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
                    temp_task.instance_data[ 'uri' ] = 'http://www.fatwallet.com' + thread.find('a','topicTitle')['href']
                except:
                    log.exception(self.log_msg('Cannot find the thread url \
                                            in the uri %s'%self.currenturi))
                    continue
                temp_task.pagedata['edate_last_post_date']=  datetime.\
                                strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                temp_task.pagedata['et_thread_last_post_author']  =  last_post_author
                try:
                    temp_task.pagedata['et_author_name'] = re.sub('^by ','',stripHtml(thread.find('td', 'headline').findAll('span')[-1].renderContents()))
                except:
                    log.info(self.log_msg('Author name not found in the url\
                                                        %s'%self.currenturi))
                try:
                    temp_task.pagedata['et_thread_age'], views_count = [x.strip() for x in stripHtml(topic_info[0].renderContents()).split('\n') if x.strip()]
                    temp_task.pagedata['ei_thread_views_count'] = int(re.sub(' Views$','',views_count))
                except:
                    log.info(self.log_msg('Views count/Thread age not found in the url\
                                                    %s'%self.currenturi))
                try:
                    temp_task.pagedata['ei_thread_replies_count'] = int(stripHtml(topic_info[1].renderContents()))
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
            post_title = self.soup.find('div', 'post_title')
            page['title'] = stripHtml(post_title.find('a').renderContents())
        except:
            log.exception(self.log_msg('Thread hierarchy and Title Not found for uri\
                                                            %s'%self.currenturi))
            return
        try:
            self.__hierarchy = [x.strip() for x in stripHtml(self.soup.find('div', attrs={'class':re.compile('^post_forum')}).renderContents()).split(u'\u203a') if x.strip()]            
            self.__hierarchy.append(stripHtml(post_title.span.renderContents()).split(u'\u203a')[-1])
            self.__hierarchy.append(page['title'] )
            page['et_thread_hierarchy'] = self.__hierarchy
        except:
            log.info(self.log_msg('hierachies not found in url %s'%self.currenturi))
        try:
            date_str = stripHtml(self.soup.find('div', 'post_date')\
                                                            .renderContents())
            
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str 
                + 'm', 'posted: %b. %d, %Y @ %I:%M%p'),"%Y-%m-%dT%H:%M:%SZ")
        except Exception, e:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date not be found in %s'% self.currenturi))
        if checkSessionInfo(self.__genre, self.session_info_out, self.task.instance_data['uri'],\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        page_data_keys = ['et_author_name','ei_thread_replies_count','ei_thread_views_count'\
                ,'edate_last_post_date','et_thread_age','ei_thread_votes_count']
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
            pages = self.soup.find('ul', 'pagination topPagination').findAll('a')
            if pages:
                self.currenturi = self.__baseuri + pages[-1]['href']
                self.__setSoupForCurrentUri()
            else:
                log.info(self.log_msg('Posts found in same page %s'%self.currenturi))
        except:
            log.info(self.log_msg('Last page cannot find from the given page no \
                                    for url %s'%self.task.instance_data['uri']))
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = self.soup.findAll('div', id=re.compile('^t\d+$'))
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
            unique_key = post.previous.previous['name']
            if post.find('div', id='wikiBody'):
                log.info(self.log_msg('Info abt a thread in %s'%self.currenturi))
                return True
            permalink = self.currenturi + '#' + unique_key
            if checkSessionInfo(self.__genre, self.session_info_out, \
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
            date_str = stripHtml(post.find('div', 'post_date')\
                                                            .renderContents())
            
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str 
                + 'm', 'posted: %b. %d, %Y @ %I:%M%p'),"%Y-%m-%dT%H:%M:%SZ")
        except Exception, e:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date not be found in %s'% self.currenturi))
            log.info(self.log_msg(e.message))
        rating_tag = post.find('a', id=re.compile('^rateLink'), title=True)
        if rating_tag:
            page['et_data_rating'] = rating_tag['title']
        author_infos = {'et_author_title':'user_title', \
                        'et_author_name':'user_name'}
        for author_info in author_infos.keys():
            author_info_tag = post.find('li', author_infos[author_info])
            if author_info_tag:
                page[author_info] = stripHtml(author_info_tag.renderContents())
        page['data'] = ''
        data_tag = post.find('div', 'userMsg')
        if data_tag:
            #[each.extract() for each in data_tag.findAll('span', 'post_quote', recursive=False)]
            page['data'] = stripHtml(data_tag.renderContents())
        else:
            log.info(self.log_msg('Empty data found in %s'%permalink))
            return False
        if len(self.__hierarchy) >= 3:
            page['title'] = page['et_thread_topic'] = self.__hierarchy[-1]
            page['et_thread_forum'] = self.__hierarchy[-3]
            page['et_thread_subforum'] = self.__hierarchy[-2]
        else:
            page['title'] = ''
            log.info(self.log_msg('Cannot find the Data thread details for this Post %s'%permalink))
        if not is_question:
            page['title'] = 'Re: ' + page['title']
        return page
            
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