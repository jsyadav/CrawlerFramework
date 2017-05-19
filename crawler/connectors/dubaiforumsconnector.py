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

# Ravi Chandra Padmala

from BeautifulSoup import BeautifulSoup
import logging
import re
from datetime import datetime
from urllib2 import urlparse

from baseconnector import BaseConnector

from tgimport import tg
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('DubaiForumsConnector')

class DubaiForumsConnector(BaseConnector):
    """Connector for http://www.dubaiforums.com/"""
    
    @logit(log, 'fetch')
    def fetch(self):
        """Fetches and processes all threads."""
        try:
            self.__setSoup()
            if self.currenturi.endswith('html'):
                return self.__processThread()
            else:
                return self.__processForum()
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'\
                             % self.currenturi))

    @logit(log, '__processForum')
    def __processForum(self):
        """
            Stores the subforum urls in self.subforums,
            and processes them. Processes all the threads.
        """
        self.__last_timestamp = datetime(1980, 1, 1)
        self.__thread_count = 0
        self.__max_threads = int(tg.config.get(path='Connector', key='dubaiforums_maxthreads'))
        while self.__addThreads():
            try:
                self.currenturi = self.soup.find('a', text='Next').parent['href']
                self.__setSoup()
            except TypeError: #TODO: specific exception
                log.info( self.log_msg('Next Page link not found for url ' + \
                    self.currenturi) )
                break
        if self.linksOut:
            updateSessionInfo('Search', self.session_info_out, \
                    self.__last_timestamp , None, 'ForumThreadsPage', \
                    self.task.instance_data.get('update'))
        return True

    @logit(log, '__addThreads')
    def __addThreads(self):
        """Add the threads on the current page"""
        try:
            self.__setSoup()
            threads = self.soup.find(attrs={'class':'forumbg'}).find(attrs={'class': 'topiclist topics'}).findAll('li', recursive=False)
            if not threads:
                log.info(self.log_msg('No threads found for url %s'%\
                                                        self.currenturi))
                return False
        except:
            log.info(self.log_msg('exception while getting threads'))
            return False
        for thread in threads:
            if re.match('.*sticky.*', thread['class']):
                continue
            try:
                last_post_time = thread.find(attrs={'class':'lastpost'}).find('span').contents[-1].string
                last_post_time = self.__processTime(last_post_time)
                self.__thread_count += 1
            except:
                log.exception(self.log_msg('Date not found in %s'%\
                                                    self.currenturi))
                continue
            if self.__thread_count > self.__max_threads:
                log.info(self.log_msg('Reached maximum post,Returning False at \
                    the url %s'%self.currenturi))
                return False
            log.info(self.session_info_out)
            if checkSessionInfo('Search', self.session_info_out, last_post_time, \
                    self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info Returns True for %s'\
                    % self.currenturi))
                return False
            self.__last_timestamp = max(last_post_time, self.__last_timestamp)
            temp_task = self.task.clone()
            try:
                temp_task.instance_data[ 'uri' ] = thread.find('a', \
                    attrs={'class':'topictitle'})['href']
            except:
                log.exception(self.log_msg('Cannot find the thread url \
                                        in the uri %s'%self.currenturi))
                continue
            try:
                thread_time = self.__processTime(thread.find(title='No new posts').contents[-1].strip()[8:])
                temp_task.pagedata['edate_thread_post_date'] = datetime.\
                    strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                temp_task.pagedata['et_thread_author'] = thread.find(title='No new posts').find('a', href=re.compile('member')).renderContents()
                temp_task.pagedata['et_thread_author_profile'] = thread.find(title='No new posts').find('a', href=re.compile('member'))['href']
            except:
                log.info(self.log_msg('Exception raised when getting topic data from %s' % self.currenturi))
            try:
                temp_task.pagedata['edate_last_post_date'] = datetime.\
                    strftime(last_post_time,"%Y-%m-%dT%H:%M:%SZ")
                temp_task.pagedata['et_thread_last_post_author'] = thread.find\
                    (attrs={'class':'lastpost'}).find('a', href=re.compile \
                    ('member')).renderContents()
            except:
                log.info(self.log_msg('Exception raised when getting last\
                     post data from %s' % self.currenturi))
            try:
                temp_task.pagedata['ei_thread_replies_count'] = int(re.search('(\d+)', thread.find\
                    ('dd', attrs={'class': 'posts'}).renderContents()).groups()[0])
            except:
                log.info(self.log_msg('Replies count not found in the url %s' \
                    % self.currenturi))
            try:
                temp_task.pagedata['ei_thread_views_count'] = int(re.search('(\d+)', thread.find\
                    ('dd', attrs={'class': 'views'}).renderContents()).groups()[0])
            except:
                log.info(self.log_msg('Views count not found in the url %s' % \
                    self.currenturi))
            self.linksOut.append(temp_task)
        return True

    @logit(log, '__processThread')
    def __processThread(self):
        """Process a thread page, and store the posts"""
        self.__genre = "Review"
        self.__hierarchy = []
        self.__baseuri = 'http://www.dubaiforums.com/forum'
        self.__task_elements_dict = {
            'priority':self.task.priority,
            'level': self.task.level,
            'last_updated_time':datetime.strftime(datetime.utcnow(), \
                "%Y-%m-%dT%H:%M:%SZ"),
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
        firstpost = self.soup.find(id=re.compile(r'^p\d+$'))
        self.__addPost(firstpost, True)
        try:
            self.currenturi = self.soup.find(attrs={'class':'pagination'}).findAll('a')[-1]['href']
            self.__setSoup()
        except:
            log.info(self.log_msg('Current URI is the only page %s' % self.currenturi))
        while self.__iteratePosts():
            try:
                self.currenturi = self.soup.find(attrs={'class':'pagination'}).findAll('strong')[2].findPreviousSibling('a')['href']
                self.__setSoup()
            except:
                log.info(self.log_msg('No Previous URL found for url %s' % self.currenturi))
                break
        return True

    @logit(log, '__setParentPage')
    def __setParentPage(self):
        """Set the parent page info"""
        page = {}
        try:
            self.__hierarchy = [stripHtml(x.renderContents()) for x \
                in self.soup.find(attrs={'class':'icon-home'}).findAll('a')]
            self.__hierarchy.append(stripHtml(self.soup.find('h2').renderContents().strip()))
            page['et_thread_hierarchy'] = self.__hierarchy[1:]
            page['data'] = page['title'] = page['et_thread_hierarchy'][-1]
        except:
            log.info(self.log_msg('Thread hierarchy and Title Not found for uri\
                                                            %s'%self.currenturi))
            return
        if checkSessionInfo(self.__genre, self.session_info_out, self.task.\
                instance_data['uri'], self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        page_data_keys = ['et_author_name', 'ei_thread_replies_count', 'edate_thread_post_date', 'et_thread_author', 'et_thread_author_profile', \
                            'ei_thread_views_count','edate_last_post_date', 'et_thread_last_post_author']
        [page.update({each:self.task.pagedata.get(each)}) for each in page_data_keys if self.task.pagedata.get(each)]
        try:
            page['posted_date'] = datetime.strftime(self.__processTime(self.soup.find('p', attrs={'class': 'author'}).contents[-1][9:].strip()), '%Y-%m-%dT%H:%M:%SZ')
        except:
            log.info(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        try:
            result = updateSessionInfo('Review', self.session_info_out, self.\
                task.instance_data['uri'], get_hash( page ), 'forum', \
                                    self.task.instance_data.get('update'))
            if result['updated']:
                page['parent_path'] = [ self.task.instance_data['uri'] ]
                page['path'] = []
                page['uri'] = self.currenturi
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                page['entity'] = 'thread'
                page.update(self.__task_elements_dict)
                self.pages.append(page)
            else:
                log.info(self.log_msg('Result[updated] = False for %s' \
                    % self.currenturi))
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))

    @logit(log, '__iteratePosts')
    def __iteratePosts(self):
        """Iterates over posts in reverse order and adds them."""
        try:
            posts = []
            posts.extend(self.soup.findAll(id=re.compile(r'p\d+')))
            if len(posts) == 0:
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
        try:
            unique_key = re.search(r'(\d+)', post['id']).groups()[0]
            if checkSessionInfo(self.__genre, self.session_info_out, unique_key, \
                             self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for %s'%unique_key))
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
                page['uri'] = self.__baseuri + 'showpost.php?p=' + unique_key
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
        """ This will return the page dictionary."""
        page = {'entity':'question' if is_question else 'answer'}
        try:
            self.__topic = stripHtml(self.soup.title.renderContents().split('-')[0].strip())
        except:
            log.info(self.log_msg('Title Not found for uri %s'%self.currenturi))
        if is_question:
            page['title'] = self.__topic
        else:
            page['title'] = 'RE: ' + self.__topic
        try:
            data = post.find('div', attrs={'class':'content'})
            qs = data.findAll('blockquote')
            for q in qs: q.extract() # Removing quotes
            page['data'] = stripHtml(data.renderContents()).strip()
        except:
            log.info(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''
        #Sometimes only Image is uploaded on the Post, in that case data will be empty
        if not page['data'] and page['title']: 
            log.info(self.log_msg("Data and Title are not found for %s,discarding this Post"%(self.currenturi)))
            return False 
        try:
            page['posted_date'] = datetime.strftime(self.__processTime(post.find('p', attrs={'class': 'author'}).contents[-1][9:].strip()), '%Y-%m-%dT%H:%M:%SZ')
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        try:
            author_tag = post.find('p', attrs={'class':'author'}).find('a', href=re.compile('member'))
            page['et_author_name'] =  stripHtml(author_tag.renderContents())
        except:
            log.info(self.log_msg('author name not found'))
        try:
            page['et_author_category'] = stripHtml(post.find('dl', id=re.compile(r'profile\d+')).find('dd').renderContents().strip())
        except:
            log.info(self.log_msg('author category not found'))
        try:
            page['et_author_profile'] = author_tag['href']
        except:
            log.info(self.log_msg('author profile not found'))
        try:
            page['ei_author_posts_count'] = int(post.find('strong', text='Posts:').parent.nextSibling)
        except:
            log.info(self.log_msg('author posts count not found'))
        try:
            author_date_tag = post.find('strong', text='Joined:').parent.nextSibling
            page['edate_author_member_since'] = datetime.strftime(datetime\
                .strptime(author_date_tag.string.strip(), '%a %b %d, %Y %I:%M %p'), '%Y-%m-%dT%H:%M:%SZ')
        except:
            page['edate_author_member_since'] = page['posted_date']
            log.exception(self.log_msg('author registered date not found'))
        if len(self.__hierarchy) >= 3:
            page['et_thread_topic'] = self.__hierarchy[-1]
            page['et_thread_forum'] = self.__hierarchy[-3]
            page['et_thread_subforum'] = self.__hierarchy[-2]
        else:
            log.info(self.log_msg('Cannot find the Data thread details'))
            log.info(self.log_msg(str(self.__hierarchy)))
        return page 

    @logit(log, '__processTime')
    def __processTime(self, dt):
        """Process the time in the forum and return a datetime object."""
        try:
            return datetime.strptime(dt, '%a %b %d, %Y %I:%M %p')
        except:
            log.info(self.log_msg('Exception parsing time %s. Being nice and returning now' % dt))
            return datetime.now()

    @logit(log, '__setSoup')
    def __setSoup(self, data=None, headers={}):
        res = self._getHTML(data=data, headers=headers)
        if res:
            self.rawpage = res['result']
            self.soup = BeautifulSoup(self.rawpage)
        else:
            log.info(self.log_msg("Couldn't fetch page content for %s"\
                    % self.currenturi))
            raise Exception("Couldn't fetch page content for %s"\
                        % self.currenturi)
        self._setCurrentPage()
