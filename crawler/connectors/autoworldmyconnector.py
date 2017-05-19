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

log = logging.getLogger('AutoWorldMyConnector')

class AutoWorldMyConnector(BaseConnector):
    """Connector for http://forum.autoworld.com.my/"""
    
    @logit(log, 'fetch')
    def fetch(self):
        """Fetches and processes upto [autoworldmy_maxthreads] threads."""
        try:
            self.__setSoupForCurrentURI()
            if 'showtopic' in self.currenturi:
                return self.__processThread()
            elif 'showforum' in self.currenturi:
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
        self.__max_threads = int(tg.config.get(path='Connector', key='autoworldmy_maxthreads'))
        while self.__getThreads():
            try:
                self.currenturi = self.soup.find('a', \
                    {'title': re.compile('Next page')})['href']
                self.__setSoupForCurrentURI()
            except:
                log.info(self.log_msg('Next page link not found for url %s' % \
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
            threads = [ thread.parent for thread in self.soup.findAll('td',
                    id = re.compile(r'tid-folder-\d+')) if not \
                    thread.parent.find('img', alt='Pinned') ] #Funky, but right.
            if not threads:
                log.info(self.log_msg('No threads found for url %s' % \
                                                            self.currenturi))
                return False
        except:
            log.info(self.log_msg('exception while getting threads'))
            return False
        for thread in threads:
            try:
                thread_time = self.__processTime(thread.find('span',
                    attrs={'class': 'lastaction'}).contents[0].__str__())
            except:
                log.exception(self.log_msg('date not found in %s'%\
                                                    self.currenturi))
            if checkSessionInfo('Search', self.session_info_out, thread_time,\
                    self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info Returns True for Ttime: '\
                    + thread_time.__str__() + ' ' + self.currenturi))
                return False
            self.__last_timestamp = max(thread_time, self.__last_timestamp)
            temp_task = self.task.clone()
            try:
                temp_task.instance_data[ 'uri' ] = thread.find('a', \
                        id=re.compile(r'^tid-link-\d+$'))['href'].__str__()
            except:
                log.exception(self.log_msg('Cannot find the thread url \
                                        in the uri %s'%self.currenturi))
                continue
            self.__thread_count += 1
            if self.__thread_count > self.__max_threads:
                log.info(self.log_msg('Reaching maximum post, Return false at \
                    the url %s' % self.currenturi))
                return False
            try:
                temp_task.pagedata['et_thread_author'] = thread.find('a', href=re.compile(r'showuser')).renderContents()
            except:
                log.info(self.log_msg('Exception raised when getting thread data from %s' % self.currenturi))
            try:
                temp_task.pagedata['edate_last_post_date'] = datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                temp_task.pagedata['et_thread_last_post_author'] = thread.find('span', attrs={'class': re.compile('lastaction')}).find('a', href=re.compile('showuser')).renderContents()
                temp_task.pagedata['et_thread_last_post_author_profile'] = thread.find('span', attrs={'class': re.compile('lastaction')}).find('a', href=re.compile('showuser'))['href']
            except:
                log.info(self.log_msg('Exception raised when getting last\
                     post data from %s' % self.currenturi))
            try:
                temp_task.pagedata['ei_thread_replies_count'] = int(thread.find\
                    ('a', attrs={'href': re.compile('who_posted.*')}).renderContents())
            except:
                log.exception(self.log_msg('Replies count not found in the url %s' \
                    % self.currenturi))
            try:
                temp_task.pagedata['ei_thread_views_count'] = int(thread.\
                    findAll('td', recursive=False)[-2].renderContents().replace(',',''))
            except:
                log.exception(self.log_msg('Views count not found in the url %s' % \
                    self.currenturi))
            self.linksOut.append(temp_task)
        return True

    @logit(log, '__processThread')
    def __processThread(self):
        """Process a thread page, and store the posts"""
        self.__genre = "Review"
        self.__hierarchy = []
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
        firstpost = self.soup.find('td', id=re.compile(r'post-main-\d+')).parent
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
            self.__hierarchy = [ stripHtml(item.renderContents()) for item in self.soup.find(id='navstrip').findAll('a') ] 
            self.__hierarchy.append(stripHtml(' -'.join(self.soup.title.renderContents().split(' -')[:-1])))
            page['et_thread_hierarchy'] = self.__hierarchy
            page['data'] = page['title'] = page['et_thread_hierarchy'][-1]
        except:
            log.exception(self.log_msg('Thread hierarchy and Title Not found for uri\
                                                            %s'%self.currenturi))
            return
        if checkSessionInfo(self.__genre, self.session_info_out, self.task.\
                instance_data['uri'], self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        page_data_keys = ['et_author_name', 'ei_thread_replies_count', 
                            'ei_thread_views_count','edate_last_post_date',
                            'et_thread_last_post_author', 'et_thread_author',
                            'et_thread_last_post_author_profile']
        [page.update({each:self.task.pagedata.get(each)}) for each in page_data_keys if self.task.pagedata.get(each)]
        log.info(self.log_msg('PARENT: '+ str(self.task.pagedata)))
        try:
            dt = self.soup.find('span', attrs={'class': 'postdetails'}).contents[-1].__str__().strip()
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
            posts.extend( [ e.parent for e in self.soup.findAll('td', id=re.compile(r'post-main-\d+')) ] )
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
            unique_key = re.search(r'(\d+)', post.find('div', id = re.compile(r'^post-\d+$'))['id']).groups()[0]
            if checkSessionInfo(self.__genre, self.session_info_out, unique_key, \
                             self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for %s' % unique_key))
                return False
            page = self.__getData(post, is_question)
            if not page:
                log.info(self.log_msg('page contains empty data __getData \
                            returns  False for uri %s'%self.currenturi) )
                return True
            result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                get_hash( page ),'forum', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if result['updated']:
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [ self.task.instance_data['uri'], unique_key]
                page['uri'] = post.findPrevious('a', attrs = {'onclick': re.compile('link_to_post')})['href'].__str__()
                page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
                page.update(self.__task_elements_dict)
                self.pages.append(page)
            else:
                log.info(self.log_msg('Update session info returns False for \
                                                    url %s' % self.currenturi))
        except:
            log.exception(self.log_msg('Cannot add the post for the uri %s' % self.currenturi))
        return True
        
    @logit(log, '__goToLastPage')
    def __goToLastPage(self):
        if not self.soup.find('span', attrs={'class':'pagelink'}):
            return
        elif self.soup.find('span', attrs={'class':'pagelinklast'}):
            self.currenturi = self.soup.find(attrs={'class':'pagelinklast'}).\
                       find('a')['href'].__str__()
        else:
            pagelinks = [e.find('a') for e in self.soup.findAll('span', attrs={'class':'pagelink'}) if e.find('a', text=re.compile('^\d+$'))]
            self.currenturi = pagelinks[-1]['href'].__str__()
    @logit(log, '__prevPage')
    def __prevPage(self):
        ppage = self.soup.find('a', attrs={'title': 'Previous Page'})['href']
        return ppage

    @logit(log, '__processTime')
    def __processTime(self, dt):
        """
            Accepts the date and time given in the forum
            Date tends to be fuzzy sometimes, with values like Today, Yesterday
            or mm-dd-yyyy
            Returns normalized datetime object.
        """
        dt=dt.strip()
        log.debug('DATeTIMe: ' + dt)
        if ',' in dt:
            try:
                return datetime.strptime(dt, '%b %d %Y, %I:%M %p')
            except:
                date, time = dt.split(', ')
                now = datetime.now()
                today = datetime.strptime(time, '%I:%M %p')
                today = today.replace(year=now.year, month=now.month, day=now.day)
                if date.lower() == 'today':
                    return today
                elif date.lower() == 'yesterday':
                    return today.replace(day=today.day-1)
                else:
                    log.exception(self.log_msg("Couldn't understand date on "
                            + self.currenturi + ' DT: ' + str(dt)))
                    raise Exception("Couldn't understand date on %s, using now."\
                            % self.currenturi)
        elif len(re.findall('\d+', dt)) is 2:
            return datetime.strptime(dt, '%d-%B %y')
        else:
            dtx = re.sub("(\d+)(st|nd|rd|th)",r"\1", dt).strip()
            return datetime.strptime(dtx, '%d %B %Y - %I:%M %p')

    @logit(log, '__getData')
    def __getData(self, post, is_question):
        """ This will return the page dictionary."""
        page = {'entity':'question' if is_question else 'answer'}
        try:
            self.__topic = stripHtml(' -'.join(self.soup.title.renderContents().split(' -')[:-1]).strip())
        except:
            log.info(self.log_msg('Title Not found for uri %s'%self.currenturi))
        if is_question:
            page['title'] = self.__topic
        else:
            page['title'] = 'RE: ' + self.__topic
        try:
            data = post.find('div', id=re.compile(r'^post-\d+$'))
            qs = data.findAll('div', attrs={'class': re.compile('quote')})
            for q in qs: q.extract() # Removing quotes
            page['data'] = stripHtml(data.renderContents()).strip()
        except:
            log.info(self.log_msg('Data not found for the url %s' % self.currenturi))
            log.info(self.log_msg(str(is_question)))
            page['data'] = ''
        #Sometimes only Image is uploaded on the Post, in that case data will be empty
        if not page['data'] and page['title']:
            log.info(self.log_msg("Data not found for %s,discarding this Post"\
                        % (self.currenturi)))
            return False
        try:
            dt = self.__processTime(stripHtml(post.findPrevious('tr').find('span', attrs={'class': 'postdetails'}).renderContents()).strip())
            page['posted_date'] = datetime.strftime(dt, '%Y-%m-%dT%H:%M:%SZ')
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        try:
            author_tag = post.parent.find('span', attrs={'class': 'normalname'}).find('a')
            page['et_author_name'] = stripHtml(author_tag.renderContents().strip())
        except:
            log.info(self.log_msg('author name not found'))
        try:
            author_info = re.findall( '\t\t([A-Z]\\w+): (.*)<',
                        post.find('span', attrs={'class': 'postdetails'})
                        .__str__())
            page['et_author_category'] = stripHtml(author_info[0][1])
        except:
            log.info(self.log_msg('author category not found'))
        try:
            page['et_author_profile'] =  author_tag['href']
        except:
            log.info(self.log_msg('author profile not found'))
        try:
            page['et_author_posts_count'] = int( re.subn( ',', '', author_info[1][1])[0] )
        except:
            log.exception(self.log_msg('author posts count not found'))
        try:
            page['edate_author_member_since'] = datetime.strftime(datetime.strptime(author_info[2][1], '%d-%B %y'), '%Y-%m-%dT%H:%M:%SZ')
        except:
            try:
                page['edate_author_member_since'] = datetime.strftime(self.__processTime(author_info[2][1]), '%Y-%m-%dT%H:%M:%SZ')
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
