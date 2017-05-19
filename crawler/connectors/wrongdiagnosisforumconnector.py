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
from cgi import parse_qsl
from urllib2 import urlparse
from urllib import urlencode
from datetime import datetime, timedelta


from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('WrongDiagnosisForumConnector')
class WrongDiagnosisForumConnector(BaseConnector):
    '''
    This will fetch the info for forums.wrongdiagnosis.com
    Sample uris is
    http://forums.wrongdiagnosis.com/forumdisplay.php?f=2159
    '''
        
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of http://forums.wrongdiagnosis.com/forumdisplay.php?f=2159
        """
        try:
            self.__genre = 'review'
            self.__baseuri = 'http://forums.wrongdiagnosis.com/'
            self.__setSoupForCurrentUri()
            if '/forumdisplay.php?f' in self.currenturi:
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
        question_post = self.soup.find('div', id=re.compile ('^edit\d+'))
        self.__addPost(question_post, True)
        self.__goToLastPage()
        while self.__iteratePosts():
            try:
                self.currenturi =  self.__removeSessionId( self.__baseuri + self.soup.find('div', 'pagenav').\
                                        find('a',text='&lt;').parent['href'] )
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
        self.__last_timestamp = datetime( 1980,1,1 )
        self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                            'wrongdiagnosis_forums_maxthreads'))
        while self.__getThreads():
            try:
                self.currenturi =  self.__removeSessionId( self.__baseuri + self.soup.find('div', 'pagenav').find('a', text='&gt;').parent['href'])
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
            threads = [each.findParent('tr') for each in self.soup.find('table',\
                id='threadslist').findAll('td', id=re.compile('td_threadtitle_'))]
            if not threads:
                log.info(self.log_msg('No threads are found for url %s'%\
                                                            self.currenturi))
                return False
            for thread in threads[1:]:
                self.__current_thread_count += 1
                if  self.__current_thread_count > self.__max_threads_count:
                    log.info(self.log_msg('Reaching maximum post,Return false \
                                            from the url %s'%self.currenturi))
                    return False
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
                temp_task=self.task.clone()                    
                try:
                    temp_task.instance_data['uri'] = self.__removeSessionId(self.__baseuri +  thread_info[2].find\
                                        ('a', id=re.compile('thread_title_'))['href'])
                    log.info(temp_task.instance_data['uri'])
                except:
                    log.exception(self.log_msg('Cannot find the thread url \
                                            in the uri %s'%self.currenturi))
                    continue
                temp_task.pagedata['edate_last_post_date']=  datetime.\
                                strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                temp_task.pagedata['et_thread_last_post_author']  =  re\
                                    .sub('^by ', '', last_post_author).strip()
                try:
                    author_and_rating_tag = thread_info[2].find('div', 'smallfont')
                    temp_task.pagedata['et_author_name'] = stripHtml(\
                                author_and_rating_tag.renderContents())                                                            
                except:
                    log.info(self.log_msg('Author name not found in the url\
                                                        %s'%self.currenturi))
                try:
                    temp_task.pagedata['ei_thread_num_views'] = int(stripHtml(thread_info\
                        [-1].renderContents()).replace(',',''))
                except:
                    log.info(self.log_msg('Views count not found in the url\
                                                    %s'%self.currenturi))
                try:
                    temp_task.pagedata['ei_thread_num_replies'] = int(stripHtml(thread_info\
                        [-2].renderContents()).replace(',',''))
                except:
                    log.info(self.log_msg('Replies count not found in the url\
                                                    %s'%self.currenturi))
                    
                try:
                    rating_str = author_and_rating_tag.find('img', \
                                            'inlineimg')['alt'].split(',')
                    temp_task.pagedata['ef_thread_rating'] =  float(\
                                    rating_str[1].replace('average.',''))
                except:
                    log.info(self.log_msg('Thread rating not found for url %s'%self.currenturi))
                try:
                    temp_task.pagedata['ei_thread_votes_count'] = int(re.search\
                                            ('\d+',rating_str[0]).group())
                except:
                    log.info(self.log_msg('Thread votes not found for url %s'%self.currenturi))
                self.linksOut.append(temp_task)
            return True
    @logit(log, '__setParentPage')
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
            self.__hierarchy.append(page['title'])
            page['et_thread_hierarchy'] = self.__hierarchy
        except:
            log.info(self.log_msg('Thread hierarchy and Title Not found for uri\
                                                            %s'%self.currenturi))
            return
        if checkSessionInfo(self.__genre, self.session_info_out, self.task.instance_data['uri'],\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        page_data_keys = ['et_author_name','ei_thread_num_replies','ei_thread_num_views'\
                ,'edate_last_post_date','ef_thread_rating','ei_thread_votes_count']
        [page.update({each:self.task.pagedata.get(each)}) for each in page_data_keys if self.task.pagedata.get(each)]
        date_tag = None
        posts_tag = self.soup.find('div', id='posts')
        if posts_tag:
            post_date_div_tag = posts_tag.find('td','thead')
            if post_date_div_tag:
                date_tags = post_date_div_tag.findAll('div')
                if date_tags: 
                    date_tag = date_tags[-1]
        posted_date_obj = self.__getDateObj(date_tag,'%m-%d-%Y, %I:%M %p')
        page['posted_date'] = datetime.strftime( posted_date_obj ,'%Y-%m-%dT%H:%M:%SZ')
        try:
            log.info(page)
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
            pagination_tag = self.soup.find('div', 'pagenav')
            if not pagination_tag:
                log.info(self.log_msg('pagination not found, posts exists in current\
                                                            url%s'%self.currenturi))
                return
            last_page_tag = pagination_tag.find(title=re.compile('Last Page'))
            if not last_page_tag:
                log.info(self.log_msg('Last page tag not found for url %s'%\
                                                self.task.instance_data['uri']))
                number_tags = pagination_tag.findAll('a',text=re.compile ('^\d+$'))
                if not number_tags:
                    log.debug(self.log_msg('Post available on current page itself'))
                    return
                else:
                    last_page_tag = number_tags[-1].parent
                
            self.currenturi = self.__removeSessionId(self.__baseuri + last_page_tag['href'])
            self.__setSoupForCurrentUri()
        except:
            log.info(self.log_msg('Last page cannot find from the given page no \
                                    for url %s'%self.task.instance_data['uri']))
        
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
            unique_key = self.__removeSessionId( self.__baseuri +  post.find('a' ,id=re.compile ('postcount\d+'), href=True)['href'])
            if checkSessionInfo(self.__genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'%unique_key))
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
                page['path'] = [self.task.instance_data['uri'],unique_key]
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
        page = {'entity':'question' if is_question else 'answer', 'uri':permalink}
        try:
            page['title'] =  stripHtml(post.find('td', id=re.compile ('td_post_')).find('div', 'smallfont').strong.renderContents())
        except:
            log.info(self.log_msg('Title not found for the url %s'%permalink))
            page['title'] = ''
        try:
            data_tag = post.find('div', id=re.compile('post_message_'))
            previous_message_tags = data_tag.findAll('div', text='Quote:')
            for previous_message_tag in previous_message_tags:
                prev_tag = previous_message_tag.parent.findParent('div')
                if prev_tag:
                    prev_tag.extract()
            page['data' ] = stripHtml(data_tag.renderContents())
        except:
            log.info(self.log_msg('Data not found for the url %s'%permalink))
            page['data'] = ''
        #Sometimes only Image is uploaded on the Post, in that case data will be empty
        if not (page['data'] and page['title']): 
            log.info(self.log_msg("Data is not found for %s,discarding this Post"%(permalink)))
            return False 
        author_tag = post.find('div', id=re.compile ('postmenu_'))
        try:
            aut_tag = author_tag.find('a')
            if aut_tag:
                page['et_author_name'] = stripHtml(aut_tag.renderContents())
            else:
                page['et_author_name'] = stripHtml(author_tag.renderContents())
        except:
            log.info(self.log_msg('Author name not found for this Post %s'%permalink))
        aut_cat_tag = post.find('div','smallfont')
        if aut_cat_tag:
            page['et_author_category'] = stripHtml(aut_cat_tag.renderContents())
        try:
            author_profile_tag = author_tag.find('a')
            if author_profile_tag:
                page['et_author_profile'] = self.__removeSessionId(self.__baseuri + author_profile_tag['href'])
        except:
            log.info(self.log_msg('author name not found for the url %s'%self.currenturi))
        try:
            author_info = [x.strip() for x in stripHtml(post.findAll('tr')[1].findAll('td')[-1].renderContents()).split('\n') if x.strip()]
            page['ei_author_posts_count'] = int(author_info[1].split(':')[-1].strip().replace(',', ''))
        except:
            log.info(self.log_msg('Author Posts count not available'))
        try:
            date_str = '01 ' +author_info[0].split(':')[-1].strip().replace(',', '')
            page['edate_author_member_since'] = datetime.strftime( datetime.strptime(date_str ,'%d %b %Y') ,'%Y-%m-%dT%H:%M:%SZ')
        except:
            log.info(self.log_msg('Author member since not avaialble'))
        if len(self.__hierarchy) >= 3:
            page['et_thread_forum'] = self.__hierarchy[-3]
            page['et_thread_subforum'] = self.__hierarchy[-2]
            page['et_thread_topic'] = self.__hierarchy[-1]
        post_date_div_tag = post.find('td','thead')
        try:
            date_tag = post.find('td', 'thead').findAll('div')[-1]
        except:
            log.info(self.log_msg('No posted_Date found in url %s'%self.currenturi))
            date_tag = None
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

    @logit(log, '__removeSessionId')
    def __removeSessionId(self, uri):
        '''This will remove the session id from current url
        '''
        try:
            url_sep = uri.split('?')
            info_dict = dict(parse_qsl(url_sep[-1]))
            if 's' in info_dict.keys():
                info_dict.pop('s')
            uri = url_sep[0] + '?' + urlencode(info_dict)
        except:
            log.info(self.log_msg('Cannot remove the Session id in url %s'%self.currenturi))
            log.info(uri)
        return uri
            