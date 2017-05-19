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

log = logging.getLogger('HighStreet5Connector')

class HighStreet5Connector(BaseConnector):
    """Connector for http://forum.highstreet5.com.my/"""
    
    @logit(log, 'fetch')
    def fetch(self):
        """Fetches and processes upto [highstreet5_maxthreads] threads."""
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
        self.__max_threads = int(tg.config.get(path='Connector', key='highstreet5_maxthreads'))
        self.__baseuri = 'http://forum.highstreet5.com.my/'
        while self.__getThreads():
            try:
                self.currenturi = self.__baseuri + self.soup.find('div', 
                    attrs={'class': 'pagenumber'}).find(text=re.compile\
                        ('&nbsp;\d+&nbsp;')).findNextSibling\
                        ('a')['href']
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
            threads = self.soup.findAll('tr', id = re.compile(r'trTopic\d+'))
            if not threads:
                log.info(self.log_msg('No threads found for url %s' % \
                                                            self.currenturi))
                return False
        except:
            log.exception(self.log_msg('exception while getting threads'))
            return False
        for thread in threads:
            try:
                first_post_time = self.__processTime(thread.findAll('td')[3].contents[-1].strip())
                try:
                    last_post_time = self.__processTime( thread.findAll('td')[-1].find('div').findAll('div', recursive=False)[1].find('div').renderContents().strip() )
                except AttributeError:
                    last_post_time = first_post_time
            except:
                log.exception(self.log_msg('date not found in %s'%\
                                                    self.currenturi))
            if checkSessionInfo('Search', self.session_info_out, last_post_time,\
                    self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info Returns True for %s'\
                    % self.currenturi))
                return False
            self.__last_timestamp = max(last_post_time, self.__last_timestamp)
            temp_task = self.task.clone()
            try:
                temp_task.instance_data[ 'uri' ] = self.__baseuri + thread.find('a')['href']
            except:
                log.exception(self.log_msg('Cannot find the thread url \
                                        in the uri %s' % self.currenturi))
                continue
            self.__thread_count += 1
            if self.__thread_count > self.__max_threads:
                log.info(self.log_msg('Reaching maximum post, Return false at \
                    the url %s' % self.currenturi))
                return False
            try:
                temp_task.pagedata['et_thread_author'] = stripHtml(thread.findAll('td')[3].find('a').renderContents())
                temp_task.pagedata['et_thread_author_profile'] = self.__baseuri + thread.findAll('td')[3].find('a')['href']
            except:
                log.info(self.log_msg('Exception raised when getting thread data from %s' % self.currenturi))
            try:
                temp_task.pagedata['edate_last_post_date'] = datetime.strftime(last_post_time, "%Y-%m-%dT%H:%M:%SZ")
                try:
                    temp_task.pagedata['et_thread_last_post_author'] = stripHtml(thread.findAll('td')[-1].findAll('a')[-1].renderContents())
                    temp_task.pagedata['et_thread_last_post_author_profile'] = self.__baseuri + thread.findAll('a')[-1]['href']
                except:
                    temp_task.pagedata['et_thread_last_post_author'] = temp_task.pagedata['et_thread_author']
                    temp_task.pagedata['et_thread_last_post_author_profile'] = temp_task.pagedata['et_thread_author_profile']
            except:
                log.info(self.log_msg('Exception raised when getting last\
                     post data from %s' % self.currenturi))
            try:
                temp_task.pagedata['ei_thread_replies_count'] = int(thread.findAll('td')[4].renderContents())
            except:
                log.exception(self.log_msg('Replies count not found in the url %s' \
                    % self.currenturi))
            try:
                temp_task.pagedata['ei_thread_views_count'] = int(thread.findAll('td')[5].renderContents())
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
        firstpost = self.soup.find('div', id='postsContainer').find('table')
        self.__addPost(firstpost, True)
        self.__goToLastPage()
        self.__setSoupForCurrentURI()
        while self.__iteratePosts():
            try:
                self.currenturi = self.__prevPage()
                self.__setSoupForCurrentURI()
            except:
                log.exception(self.log_msg('No Previous URL found for url %s' % self.currenturi))
                break
        return True

    @logit(log, '__setParentPage')
    def __setParentPage(self):
        """Set the parent page info"""
        page = {}
        try:
            self.__hierarchy = [ stripHtml(e.renderContents()) for e in self.soup.find('div', attrs = {'class': 'nav'}).findAll('a') ]
            self.__hierarchy.append( stripHtml(self.soup.find('div', attrs = {'class': 'nav'}).findAll('a')[-1].findNextSibling('span').renderContents()) )
            page['et_thread_hierarchy'] = self.__hierarchy
            page['data'] = page['title'] = page['et_thread_hierarchy'][-1]
        except:
            log.exception(self.log_msg('Thread hierarchy and Title Not found for uri\
                                                            %s' % self.currenturi))
            return
        if checkSessionInfo(self.__genre, self.session_info_out, self.task.\
                instance_data['uri'], self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        page_data_keys = [ 'ei_thread_replies_count', 
                            'ei_thread_views_count','edate_last_post_date',
                            'et_thread_last_post_author', 'et_thread_last_post_author_profile',
                            'et_thread_author', 'et_thread_author_profile']
        [page.update({each:self.task.pagedata.get(each)}) for each in page_data_keys if self.task.pagedata.get(each)]
        log.info( self.log_msg('PARENT: '+ str(self.task.pagedata)) )
        try:
            dt = self.soup.find('table', attrs={'class': 't_msg'}).find('td').contents[-2].contents[-1].strip()
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
            posts.extend( [ e for e in self.soup.find('div', id='postsContainer').findAll('table', recursive=False)\
                    if e.find(attrs={'class': 't_msg'}) ] )
            if len(posts) == 0:
                log.info(self.log_msg('No posts found'))
                return False
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
            unique_key = post.find('a')['name']
            if checkSessionInfo(self.__genre, self.session_info_out, unique_key, \
                             self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for %s' % unique_key))
                return False
            page = self.__getData(post, is_question)
            log.info(self.log_msg('PAIGE'))
            if not page:
                log.info(self.log_msg('page contains empty data or the date \
                            doesn\'t lie in the constraints, getdata \
                            returns False for uri %s' % self.currenturi))
                return True
            result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                get_hash( page ),'forum', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if result['updated']:
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [ self.task.instance_data['uri'], unique_key ]
                url_pieces = self.currenturi + '#' + unique_key
                page['uri'] = ''.join(url_pieces)
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
        self.__baseuri = 'http://forum.highstreet5.com.my/'
        try:
            log.debug('LASTPG: ' + self.__baseuri + self.soup.find('span', attrs={'class':'pagenumber'}).findAll('a')[-1]['href'])
            self.currenturi = self.__baseuri + self.soup.find('span', attrs={'class':'pagenumber'}).findAll('a')[-1]['href']
        except:
            log.debug('ONEPG ' + self.currenturi)
            self.currenturi = self.currenturi

    @logit(log, '__prevPage')
    def __prevPage(self):
        ppage = self.__baseuri + self.soup.find('span', 
                    attrs={'class': 'pagenumber'}).find(text=re.compile\
                        (r'&nbsp;\d+&nbsp;')).findPreviousSibling\
                        ('a')['href']
        log.debug('PPG: ' + ppage)
        return ppage

    @logit(log, '__processTime')
    def __processTime(self, dt):
        """
            Accepts the date and time given in the forum
            Date tends to be fuzzy sometimes, with values like Today, Yesterday
            or mm-dd-yyyy
            Returns normalized datetime object.
        """
        dt = dt.strip()
        try:
            return datetime.strptime(dt, '%Y-%m-%d %H:%M')
        except ValueError:
            try:
                return datetime.strptime(dt, '%m/%d/%Y %I:%M:%S %p')
            except:
                log.exception(self.log_msg("Couldn't understand date on "
                        + self.currenturi + ' DT: ' + str(dt)))
                raise Exception("Couldn't understand date on %s, using now."\
                        % self.currenturi)

    @logit(log, '__getData')
    def __getData(self, post, is_question):
        """ This will return the page dictionary."""
        page = {'entity':'question' if is_question else 'answer'}
        try:
            self.__topic = stripHtml('-'.join(self.soup.title.renderContents().split('-')[:-2]).strip())[18:]
        except:
            log.info(self.log_msg('Title Not found for uri %s' % self.currenturi))
        if is_question:
            page['title'] = self.__topic
        else:
            page['title'] = 'RE: ' + self.__topic
        try:
            data = post.find('div', id=re.compile(r'^message\d+$'), attrs={'class': 't_msgfont'})
            qs = data.findAll('div', attrs={'class': re.compile('msgheader|msgborder')})
            imgs = data.findAll('img')
            for q in qs: q.extract() # Removing quotes
            for img in imgs: img.extract()
            page['data'] = stripHtml(data.renderContents())            
        except:
            log.exception(self.log_msg('Data not found for the url %s' % self.currenturi))
            log.info(self.log_msg(str(is_question)))
            page['data'] = ''
        ##Sometimes only Image is uploaded on the Post, in that case data will be empty
        if not page['data']:
            log.info(self.log_msg("Data not found for %s, discarding this Post"\
                        % (self.currenturi)))
            return False
        try:
            dt_tag = post.find('table', attrs={'class': 't_msg'}).find('td').findAll('div')[-1]
            if dt_tag.find('a'): dt_tag.find('a').extract()
            dt = self.__processTime(stripHtml(dt_tag.renderContents()).strip())
            page['posted_date'] = datetime.strftime(dt, '%Y-%m-%dT%H:%M:%SZ')
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        try:
            uinfo_tag = post.find('table')
            author_tag = uinfo_tag.find('a', href=re.compile('userinfo'))
            page['et_author_name'] = stripHtml(author_tag.renderContents()).strip()
        except:
            log.info(self.log_msg('author name not found'))
        try:
            page['et_author_category'] = stripHtml(uinfo_tag.findAll('tr', recursive=False)[3].contents[1]).strip()
        except:
            log.info(self.log_msg('author category not found'))
        try:
            page['et_author_profile'] = self.__baseuri + author_tag['href']
        except:
            log.info(self.log_msg('author profile not found'))
        try:
            uinfo = dict([ (stripHtml(u).strip().split(':', 1)[0].lower(), stripHtml(u).strip().split(':', 1)[1]) for u in uinfo_tag.findAll('tr')[4].find('td').renderContents().replace('\n','').split('<br />') if ':' in u ])
            #log.info(uinfo)
            page['ei_author_posts_count'] = int(uinfo['post'].strip())
        except:
            log.exception(self.log_msg('author posts count not found'))
        try:
            page['edate_author_member_since'] = datetime.strftime(self.__processTime(uinfo['register'].strip()), '%Y-%m-%dT%H:%M:%SZ')
        except:
            log.exception(self.log_msg('author registered date not found'))
        try:
            page['et_author_location'] = uinfo['from'].strip()
        except:
            log.exception(self.log_msg('author location not found'))
        try:
            page['ei_author_best_posts'] = int(uinfo['best'].strip())
        except:
            log.exception(self.log_msg('author best posts not found'))
        try:
            page['ei_author_bucks'] = int(uinfo['buck'].strip())
        except:
            log.exception(self.log_msg('author bucks not found'))
        try:
            page['ef_author_reputation'] = float(uinfo['reputation'].strip())
        except:
            log.exception(self.log_msg('author reputation not found'))
        try:
            page['ef_author_treasure'] = float(uinfo['treasure'].split()[0])
        except:
            log.exception(self.log_msg('author treasure not found'))
        if len(self.__hierarchy) >= 3:
            page['et_thread_topic'] = self.__hierarchy[-1]
            page['et_thread_forum'] = self.__hierarchy[-3]
            page['et_thread_subforum'] = self.__hierarchy[-2]
        else:
            log.info(self.log_msg('Cannot find the thread details'))
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
