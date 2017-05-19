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
from calendar import mdays

from baseconnector import BaseConnector

from tgimport import tg
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('DSSTesterConnector')

class DSSTesterConnector(BaseConnector):
    """Connector for http://forums.dsstester.com/"""
    
    @logit(log, 'fetch')
    def fetch(self):
        """Fetches and processes upto [dsstester_maxthreads] threads."""
        try:
            self.__setSoupForCurrentURI()
            if '/showthread.php' in self.currenturi:
                return self.__processThread()
            elif '/forumdisplay.php' in self.currenturi:
                if re.search( r'daysprune', self.currenturi):
                    return self.__processForum()
                else:
                    self.currenturi += '&daysprune=-1'
                    self.__setSoupForCurrentURI()
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
        self.__baseuri = 'http://forums.dsstester.com/vbb/'
        self.__thread_count = 0
        self.__max_threads = int(tg.config.get(path='Connector', key='dsstester_maxthreads'))
        while self.__getThreads():
            try:
                self.currenturi = self.__baseuri + self.soup.find('a', attrs = \
                    {'title': re.compile('Next Page')})['href']
                self.__setSoupForCurrentURI()
            except:
                log.info(self.log_msg('Next Page link not found for url %s' % \
                    self.currenturi))
                break
        if self.linksOut:
            updateSessionInfo('Search', self.session_info_out, \
                    self.__last_timestamp , None, 'ForumThreadsPage', \
                    self.task.instance_data.get('update'))
        return True

    @logit(log, '__getThreads') 
    def __getThreads(self):
        """Get the threads on the current page"""
        try:
            threads = self.soup.find(id = re.compile \
                (r'threadbits_forum_\d{1,4}')).findAll('tr', recursive=False)
            if not threads:
                log.info(self.log_msg('No threads found for url %s'%\
                                                            self.currenturi))
                return False
        except:
            log.info(self.log_msg('exception while getting threads'))
            return False
        for thread in threads:
            if thread.find('img',  attrs={'alt': 'Sticky Thread'}):
                continue
            if self.__thread_count > self.__max_threads:
                log.info(self.log_msg('Reaching maximum post,Return false at \
                    the url %s' % self.currenturi))
                return False
            try:
                thread_time = self.__processTime(thread.contents[7].\
                    find('div').contents[0].strip())
            except:
                log.exception(self.log_msg('date not found in %s'%\
                                                    self.currenturi))
            self.__thread_count += 1
            if checkSessionInfo('Search', self.session_info_out, thread_time,\
                    self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info Returns True for %s'\
                    % self.currenturi))
                return False
            self.__last_timestamp = max(thread_time, self.__last_timestamp)
            temp_task = self.task.clone()                    
            try:
                temp_task.instance_data[ 'uri' ] = self.__baseuri + thread\
                    .find('a', id=re.compile(r'^thread_title_\d*$'))['href']
            except:
                log.exception(self.log_msg('Cannot find the thread url \
                                        in the uri %s'%self.currenturi))
                continue
            try:
                temp_task.pagedata['et_thread_author'] = thread.find('span', attrs={'onclick': re.compile(r'.*member\.php.*')}).renderContents()
            except:
                log.info(self.log_msg('Exception raised when getting thread data from %s' % self.currenturi))
            try:
                temp_task.pagedata['edate_last_post_date'] = datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                temp_task.pagedata['et_thread_last_post_author'] = thread.find('a', attrs={'href': re.compile('lastposter.*')}).renderContents()
            except:
                log.info(self.log_msg('Exception raised when getting last\
                     post data from %s' % self.currenturi))
            try:
                temp_task.pagedata['ei_thread_replies_count'] = int(thread.find\
                    ('a', attrs={'href': re.compile('whoposted.*')}).renderContents())
            except:
                log.info(self.log_msg('Replies count not found in the url %s' \
                    % self.currenturi))
            try:
                temp_task.pagedata['ei_thread_views_count'] = int(thread.\
                    contents[-2].renderContents())
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
        self.__baseuri = 'http://forums.dsstester.com/vbb/'
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
        firstpost = self.soup.find(id='posts').find('div', align = 'center')
        self.__addPost(firstpost, True)
        self.__goToLastPage()
        self.__setSoupForCurrentURI()
        while self.__iteratePosts():
            try:
                self.currenturi = self.__prevPage()
                self.__setSoupForCurrentURI()
            except:
                log.info(self.log_msg('No Previous URL found for url %s' % self.currenturi))
                break
        return True

    @logit(log, '__setParentPage')
    def __setParentPage(self):
        """Set the parent page info"""
        page = {}
        try:
            self.__hierarchy = [ item.find('a').renderContents() for item in self.soup.findAll('span', attrs={'class': 'navbar'}) ] 
            self.__hierarchy.append(self.soup.find('td', attrs={'class': 'navbar'}).find('strong').string.strip())
            page['et_thread_hierarchy'] = self.__hierarchy
            page['data'] = page['title'] = page['et_thread_hierarchy'][-1]
        except:
            log.info(self.log_msg('Thread hierarchy and Title Not found for uri\
                                                            %s'%self.currenturi))
            return
        if checkSessionInfo(self.__genre, self.session_info_out, self.task.\
                instance_data['uri'], self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        page_data_keys = ['et_author_name', 'ei_thread_replies_count', \
                            'ei_thread_views_count','edate_last_post_date', 'et_thread_last_post_author', 'et_thread_author']
        [page.update({each:self.task.pagedata.get(each)}) for each in page_data_keys if self.task.pagedata.get(each)]
        try:
            dt = self.soup.find('div', id='posts').find('a', attrs={'name': re.compile(r'^post\d+$')}).nextSibling.strip()
            page['posted_date'] = datetime.strftime(self.__processTime(dt), '%Y-%m-%dT%H:%M:%SZ')
        except:
            log.info(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        try:
            result = updateSessionInfo('Review', self.session_info_out, self.\
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
                log.info(self.log_msg('Result[updated] = False for %s' \
                    % self.currenturi))
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))

    @logit(log, '__iteratePosts')
    def __iteratePosts(self):
        """Iterates over posts in reverse order and adds them."""
        try:
            posts = []
            posts.extend(self.soup.find(id='posts').findAll('div', id=re.compile(r'^edit\d+$')))
            if len(posts) == 0:
                log.info(self.log_msg('No posts found'))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d' % len(posts)))
            posts.reverse()
            for post in posts:
                if not self.__addPost(post):
                    log.info(self.log_msg('Post not added to self.pages for url %s' % self.currenturi))
                    return False
            return True
        except:
            log.exception(self.log_msg('Reviews are not found for url %s' % self.currenturi))
            return False

    @logit(log, '__addPost')
    def __addPost(self, post, is_question=False):
        try:
            unique_key = re.search('(\d+)', post.find('table', id = re.compile(r'^post\d+$'))['id']).groups()[0]
            if checkSessionInfo(self.__genre, self.session_info_out, unique_key, \
                             self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for %s' % unique_key))
                return False
            page = self.__getData(post, is_question)
            log.error(self.log_msg('page'))
            if not page:
                log.info(self.log_msg('page contains empty data or the date \
                            doesn\'t lie in the constraints, getdata \
                            returns  False for uri %s'%self.currenturi) )
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
        
    @logit(log, '__goToLastPage')
    def __goToLastPage(self):
        if not self.soup.find(attrs={'class':'pagenav'}):
            return
        else:
            lastpageno=self.soup.find(attrs={'class':'pagenav'}).find('td', attrs={\
                'class':'vbmenu_control'}, text=re.compile('^Page \d+ of \d+$'\
                )).split()[-1]
            self.currenturi = self.currenturi+'&page='+lastpageno

    @logit(log, '__prevPage')
    def __prevPage(self):
        ppage = self.soup.find(attrs={'class':'pagenav'}).find(title=re.compile('Prev Page.*'))
        return self.__baseuri + ppage['href']

    @logit(log, '__processTime')
    def __processTime(self, dt):
        """
            Accepts the date and time given in the forum
            Date tends to be fuzzy sometimes, with values like Today, Yesterday
            or mm-dd-yyyy
            Returns normalized datetime object.
        """
        try:
            return datetime.strptime(dt, '%m-%d-%Y')
        except ValueError:
            now = datetime.now()
            n, qty = dt.split()[:2]
            qty = qty.lower()
            n = int(n)
            if re.match('seconds?', qty): #NOTE: is this needed?
                if n > now.second:
                    return now.replace(minute=now.minute-1, second=60+(now.second-n))
                return now.replace(second=now.second-n)
            elif re.match('minutes?', qty):
                if n > now.minute:
                    return now.replace(hour=now.hour-1, minute=60+(now.minute-n))
                return now.replace(minute=now.minute-n)
            elif re.match('hours?', qty):
                if n > now.hour:
                    return now.replace(day=now.day-1, hour=24+(now.hour-n))
                return now.replace(hour=now.hour-n)
            elif re.match('days?', qty):
                if n > now.day:
                    return now.replace(month=now.month-1, day = mdays[now.month] + (now.day-(n))) # doesn't consider leap years
                return now.replace(day=now.day-n)
            elif re.match('weeks?', qty):
                if n*7 > now.day:
                    return now.replace(month=now.month-1, day = mdays[now.month] + (now.day-(n*7)))
                return now.replace(day=now.day-(n*7))
            else:
                log.exception(self.log_msg("Couldn't understand date on %s. dt = %s"
                        % self.currenturi, dt))
                raise Exception("Couldn't understand date on %s, using now."\
                        % self.currenturi)

    @logit(log, '__getData')
    def __getData(self, post, is_question):
        """ This will return the page dictionary."""
        page = {'entity':'question' if is_question else 'answer'}
        try:
            self.__topic = self.soup.title.renderContents().split('-')[0].strip()
        except:
            log.info(self.log_msg('Title Not found for uri %s'%self.currenturi))
        if is_question:
            page['title'] = self.__topic
        else:
            page['title'] = 'RE: ' + self.__topic
        try:
            data = post.find('div', id=re.compile(r'^post_message_\d+$'))
            qs = data.findAll('div')
            for q in qs: q.extract() # Removing quotes
            page['data'] = stripHtml(data.renderContents().strip())
        except:
            log.info(self.log_msg('Data not found for the url %s'%self.currenturi))
            log.info(self.log_msg(str(is_question)))
            page['data'] = ''
        #Sometimes only Image is uploaded on the Post, in that case data will be empty
        if not page['data'] and page['title']:
            log.info(self.log_msg("Data not found for %s,discarding this Post"%(self.currenturi)))
            return False
        try:
            dt = self.__processTime(stripHtml(post.find('td').renderContents()).strip())
            page['posted_date'] = datetime.strftime(dt, '%Y-%m-%dT%H:%M:%SZ')
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        try:
            author_tag = post.find('a', attrs={'class':'bigusername'})
            page['et_author_name'] = stripHtml(author_tag.renderContents()).strip()
        except:
            log.info(self.log_msg('author name not found'))
        try:
            page['et_author_category'] = author_tag.parent.parent.find('div', attrs={'class': 'smallfont'}).renderContents().strip()
        except:
            log.info(self.log_msg('author category not found'))
        try:
            page['et_author_profile'] =  self.__baseuri + author_tag['href']
        except:
            log.info(self.log_msg('author profile not found'))
        try:
            author_posts = post.find('div', text=re.compile('Posts:.*')).__str__()
            page['et_author_posts_count'] = int( re.search( r'(\d+)', author_posts.replace(',', '')).groups()[0] )
        except:
            log.exception(self.log_msg('author posts count not found'))
        try:
            author_date_tag = author_tag.parent.findNextSiblings('div')[-1].findAll('div')[0].string.split(': ')[1]
            page['edate_author_member_since'] = datetime.strftime(datetime.strptime(author_date_tag, '%b %Y'), '%Y-%m-%dT%H:%M:%SZ')
        except:
            page['edate_author_member_since'] = page['posted_date']
            log.exception(self.log_msg('author registered date not found, using posted date.'))
        if len(self.__hierarchy) >= 3:
            page['et_thread_topic'] = self.__hierarchy[-1]
            page['et_thread_forum'] = self.__hierarchy[-3]
            page['et_thread_subforum'] = self.__hierarchy[-2]
        else:
            log.info(self.log_msg('Cannot find the Data thread details'))
            log.info(self.log_msg(str(self.__hierarchy)))
        return page 
 

    @logit(log, '__setSoupForCurrentURI')
    def __setSoupForCurrentURI(self, data=None, headers={}):
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
