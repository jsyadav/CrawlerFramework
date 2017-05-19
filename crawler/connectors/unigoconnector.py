'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Rakesh Soni, Sep 30, 2009
#unigo.com has both, reviews as well as forums
#Skumar

import re
import time 
import random 
from BeautifulSoup import BeautifulSoup
from datetime import datetime,timedelta
from utils.httpconnection import HTTPConnection
import logging
from urllib2 import urlparse
import urllib2
from tgimport import tg
import copy
from cgi import parse_qsl
import md5

from urllib2 import urlopen
import urllib
import cookielib


from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('UnigoConnector')

class UnigoConnector(BaseConnector):
    '''Connector for Unigo
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch information from unigo connector
        """
        try:
            self.baseuri = 'http://www.unigo.com'
            self.genre="Review"
            self.parent_uri = self.currenturi
            cookie_jar = cookielib.LWPCookieJar()
            handlers = []
            cookie_handler = urllib2.HTTPCookieProcessor(cookie_jar)
            handlers.append(cookie_handler)
            self.opener = urllib2.build_opener(*handlers)
            if self.currenturi.startswith('http://www.unigo.com/forum/replies'):
                if not self.__setNextPage():
                    return False
                self.__getForumParentPage()
                self.post_type= True
                next_page_no = 2
                total_page_no = 1
                try:
                    total_page_no = int(re.search('\d+',stripHtml(self.soup.find('a',title='Next Page').parent.renderContents())).group())
                except:
                    log.info(self.log_msg('No next page found'))
                while True:
                    self.__addForumPosts()
                    try:
                        if next_page_no >total_page_no:
                            break
                        headers = {}
                        headers['Referer'] = self.parent_uri
                        data=dict(parse_qsl(self.parent_uri.split('?')[-1]))
                        data['__EVENTARGUMENT'] = '||' + str(next_page_no)
                        data['__EVENTTARGET'] = 'ctl00:ContentPlaceHolder2:CenterReplyList1:ctl_posts_prt'
                        data['__VIEWSTATE'] = self.soup.find('input',id='__VIEWSTATE')['value']
                        if not self.__setNextPage(data=data,headers=headers):
                            break
                        next_page_no = next_page_no + 1
                    except:
                        log.info(self.log_msg('Next page not found'))
                        break
                return True
            elif self.currenturi.startswith('http://www.unigo.com/forum'):
                if not self.__setSoup():
                    return False
                self.currenturi = 'http://www.unigo.com/forum/' + self.soup.find('form',id='aspnetForm')['action']
                self.parent_uri = self.currenturi
                if not self.__setNextPage():
                    return False
                self.total_forum_threads_count = 0
                self.last_timestamp = datetime( 1980,1,1 )
                self.max_forum_threads_count = tg.config.get(path='Connector',key='unigo_max_forum_threads_to_process')
                if not self.max_forum_threads_count:
                    self.max_forum_threads_count = '50'
                self.max_forum_threads_count = int(self.max_forum_threads_count)
                next_page_no = 2
                total_page_no = 1
                try:
                    total_page_no = int(re.search('\d+',stripHtml(self.soup.find('a',title='Next Page').parent.renderContents())).group())
                except:
                    log.info(self.log_msg('No next page found'))
                while True:
                    if not self.__getForumThreads():
                        log.info(self.log_msg('Forums: Error in getThreads(), returned False'))
                        break
                    try:
                        if next_page_no >total_page_no:
                            break
                        log.info(total_page_no)
                        log.info(next_page_no)
                        #next_page = self.soup.find('a',title='Next Page')
                        #if not next_page:
                        #    log.info(self.log_msg('No more pages'))
                        #    break
                        headers = {}
                        headers['Referer'] = self.parent_uri
                        data=dict(parse_qsl(self.parent_uri.split('?')[-1]))
                        data['__EVENTARGUMENT'] = '||' + str(next_page_no)
                        data['__EVENTTARGET'] = 'ctl00:ContentPlaceHolder2:ug_prt_posts'
                        data['__VIEWSTATE'] = self.soup.find('input',id='__VIEWSTATE')['value']
                        if not self.__setNextPage(data=data,headers=headers):
                            break
                        next_page_no = next_page_no + 1
                    except:
                        log.info(self.log_msg('Next page not found'))
                        break
                log.info('len is ')
                log.info(len(self.linksOut))
                if self.linksOut:
                    updateSessionInfo('Search', self.session_info_out,self.last_timestamp , None, \
                            'ForumThreadsPage', self.task.instance_data.get('update'))
                    log.info(self.log_msg('SessionInfo updated for getForumThreads'))
                return True
            else:
                if not self.__setSoup():
                    return False
                self.currenturi = 'http://www.unigo.com/explorer/reviews/' + self.soup.find('form',id='aspnetForm')['action']
                self.parent_uri = self.currenturi
                if not self.__setNextPage():
                    return False
                self.__getReviewsParentPage()
                #main_uri = 'http://www.unigo.com/explorer/reviews/' + self.soup.find('form',id='aspnetForm')['action']
                main_uri = self.currenturi
                main_soup = copy.copy(self.soup)
                total_page_no = 1
                try:
                    total_page_no = int(re.search('\d+',stripHtml(self.soup.find('a',title='Next Page').parent.renderContents())).group())
                except:
                    log.info(self.log_msg('No next page found'))
                next_page_no = 2
                while True:
                    self.__getReviews()
                    try:
                        if next_page_no >total_page_no:
                            break
                        log.info(total_page_no)
                        log.info(next_page_no)
                        headers = {}
                        headers['Referer'] = self.parent_uri
                        data=dict(parse_qsl(main_uri.split('?')[-1]))
                        data['__EVENTARGUMENT'] = 'Date|10|' + str(next_page_no)
                        data['__EVENTTARGET'] = 'ctl00:ContentPlaceHolder2:ctl_AddReview_uc:ctl_Review_prt'
                        data['__VIEWSTATE'] = main_soup.find('input',id='__VIEWSTATE')['value']
                        self.currenturi = main_uri
                        #log.info(data)
                        #log.info(headers)
                        if not self.__setNextPage(data=data,headers=headers):
                            break
                        main_soup = copy.copy(self.soup)
                        next_page_no = next_page_no + 1
                    except:
                        log.exception(self.log_msg('Next page not found'))
                        break
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False
                
    @logit(log , '__getReviews')
    def __getReviews(self ):
        """Get Question and Replies for the thread
        """
        try:
            reviews = self.soup.find('div',id='ctl00_ContentPlaceHolder2_ctl_pv_MostView') \
                    .findAll('div','BlogDirectory-main')
        except:
            log.exception(self.log_msg('Reviews not found'))
            return False

        for review in reviews:
            page = {}
            try:
                page['et_college_name'] = stripHtml(review.find('div','small').find('a','slinkbold0066ff')
                                        .renderContents())
            except:
                log.info(self.log_msg('College name not available'))
            try:
                page['et_author_name'] = stripHtml(review.find('a',id=re.compile('ctl00_ContentPlaceHolder2_ctl_AddReview_uc_ctl_Review_prt_.*_ctl_user')) \
                                                .renderContents())
            except:
                log.info(self.log_msg('Review author name not available'))
                page['et_author_name'] = 'Anonymous'

            try:
                page['title'] = stripHtml(review.find('a',id=re.compile('ctl00_ContentPlaceHolder2_ctl_AddReview_uc_ctl_Review_prt.*linkProfile')) \
                                .renderContents())
            except:
                log.info(self.log_msg('Title notavailable, take data as title'))
                page['title'] = ''
            try:
                self.currenturi = self.baseuri + review.find('a',id=re.compile('ctl00_ContentPlaceHolder2_ctl_AddReview_uc_ctl_Review_prt.*linkProfile'))['href']
            except:
                log.info(self.log_msg('Error while getting url for review'))
                continue
            try:
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set in getReviews'))
                    continue
                data = ""
                posts = self.soup.find('div',id='ctl00_ContentPlaceHolder2_CenterReviewDetail1_StereotypeDiv') \
                        .findAll('div','h-2-box h2-mbg')
                for post in posts:
                    try:
                        data = data + stripHtml(post.find('h2').renderContents().strip()) + '@$@'
                        #'@$@' is just a delimeter to separate Question from Reply
                        data = data + stripHtml(post.findNext('div','margin-18tb').renderContents().strip())
                        data = data + '$$$'
                        # '$$$' is just a delimeter to separate
                    except:
                        continue
            except:
                log.info(self.log_msg('stereotypes data not found'))
                data = ""

            try:
                posts = self.soup.find('div',id='ctl00_ContentPlaceHolder2_CenterReviewDetail1_PictureDiv') \
                        .findAll('div','h-2-box h2-mbg')
                for post in posts:
                    try:
                        data = data + stripHtml(post.find('h2').renderContents().strip()) + '@$@'
                        #'@$@' is just a delimeter to separate Question from Reply
                        data = data + stripHtml(post.findNext('div','margin-18tb').renderContents().strip())
                        data = data + '$$$'
                        # '$$$' is just a delimeter to separate
                    except:
                        continue
            except:
                log.info(self.log_msg('Big picture data not found'))
            try:
                posts = self.soup.find('div',id='ctl00_ContentPlaceHolder2_CenterReviewDetail1_FinalDiv') \
                        .find('div',id='ctl00_ContentPlaceHolder2_CenterReviewDetail1_ctl_Final_pnl') \
                        .findAll('span','fontbold003366 fontS14')

                for post in posts:
                    try:
                        data = data + stripHtml(post.renderContents().strip()) + '@$@'
                        #'@$@' is just a delimeter to separate Question from Reply
                        data = data + stripHtml(post.findNext('div').next.strip())
                        data = data + '$$$'
                        # '$$$' is just a delimeter to separate
                    except:
                        continue
            except:
                log.info(self.log_msg('In closing data not available'))


            if data=="":
                if page['title']!='':
                    page['data'] = page['title']
                else:
                    continue
            else:
                page['data'] = data

            if page['title']=='':
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
            try:
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
            except:
                log.exception(self.log_msg('unique_key not found'))
                continue
            try:
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                    self.task.instance_data.get('update'),parent_list\
                        =[self.parent_uri]):
                    log.info(self.log_msg('session info return True'))
                    continue
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")

                entities = self.soup.find('div',id='ctl00_ContentPlaceHolder2_CenterReviewDetail1_ctl_div_null').find('div','bubble-wrap').find('div',id='ctl00_ContentPlaceHolder2_CenterReviewDetail1_divInterval') \
                            .findNext('div','bubble-wg').findAll('div','margin-lr')

                flag = 0
                for entity in entities:
                    try:
                        if flag==0:
                            ext_entities = entity.findAll('span')[1:]
                            #avoiding title
                            flag = 1
                        else:
                            ext_entities = entity.findAll('span')
                    except:
                        continue

                    for each in ext_entities:
                        try:
                            var = 'et_author_'+stripHtml(each.renderContents().strip())
                            page[var] = stripHtml(each.nextSibling.strip())
                        except:
                            continue
            except:
                log.info(self.log_msg('Extracted entities not found'))
            try:
                review_hash = get_hash( page )
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            review_hash,'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    continue
                parent_list = [self.parent_uri]
                page['parent_path']=copy.copy(parent_list)
                parent_list.append(unique_key)
                page['path']=parent_list
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                page['client_name'] = self.task.client_name
                page['last_updated_time'] = page['pickup_date']
                page['versioned'] = False
                page['entity'] = 'Review'
                page['category'] = self.task.instance_data.get('category','')
                page['task_log_id']=self.task.id
                page['uri'] = self.currenturi
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append( page )
                log.info(page)
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error in getReview'))

    @logit(log , '__addForumPosts')
    def __addForumPosts(self):
        """
            Get post informations
        """

        try:
            reviews = self.soup.find('div',id='ctl00_ContentPlaceHolder2_CenterReplyList1_ctl_rap_MultiPage') \
                    .findAll('div','margin-lr')
        except:
            log.exception(self.log_msg('Reviews not found'))
            return False

        for i, review in enumerate(reviews):
            page = {}
            post_type = "Question"
            if i==0 and self.post_type:
                post_type = "Question"
                self.post_type = False
            else:
                post_type = "Suggestion/Reply"
            try:
                page['data'] = stripHtml(review.findNext('div','divclear margin-b').renderContents().strip())
            except:
                log.info(self.log_msg('data not available in addPosts()'))
                continue
            try:
                page['title'] = stripHtml(self.soup.find('div',id='divGeneral').find('span','small') \
                                    .renderContents())
            except:
                try:
                    log.info(self.log_msg('Title cannot be found'))
                    if len(page['data']) > 50:
                        page['title'] = page['data'][:50] + '...'
                    else:
                        page['title'] = page['data']
                except:
                    log.exception(self.log_msg('title not found'))
                    continue
            try:
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
            except:
                log.exception(self.log_msg('unique_key not found'))
                continue
            try:
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                        self.task.instance_data.get('update'),parent_list\
                            =[self.parent_uri]):
                        log.info(self.log_msg('session info return True in addforumPosts'))
                        continue
            except:
                log.info(self.log_msg('error while checking session info'))
                continue
            try:
                date_str = stripHtml(review.findAll('br')[-1].next)
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%m/%d/%y %I:%M %p'), \
                                        "%Y-%m-%dT%H:%M:%SZ")
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                log.info(self.log_msg('posted date not found, taking current date'))
            try:
                page['et_author_name'] = stripHtml(review.find('span','small ').renderContents().strip()) \
                                        .split('Posts:')[0].strip().split(',')[0].strip()
            except:
                log.info(self.log_msg('Author name not found'))
            try:
                page['et_author_location'] = stripHtml(review.find('span','small ').renderContents().strip()) \
                                        .split('Posts:')[0].strip().split(',')[1].strip()
            except:
                log.info(self.log_msg('Author location not found'))
            try:
                page['ei_author_post_counts'] = int(stripHtml(review.find('a','linkbold0066ff fontS10') \
                                            .renderContents().strip()))
            except:
                log.info(self.log_msg('Author post count not found'))
            try:
                page['et_data_post_type'] = post_type
                page['et_data_subforum'] = self.hierarchy[1]
                page['et_data_topic'] = self.hierarchy[-1]
            except:
                log.info(self.log_msg('Post type not available'))
            try:
                review_hash = get_hash( page )
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            review_hash,'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    continue
                parent_list = [self.parent_uri]
                page['parent_path']=copy.copy(parent_list)
                parent_list.append(unique_key)
                page['path']=parent_list
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                page['client_name'] = self.task.client_name
                page['last_updated_time'] = page['pickup_date']
                page['versioned'] = False
                page['entity'] = 'Review'
                page['category'] = self.task.instance_data.get('category','')
                page['task_log_id']=self.task.id
                page['uri'] = self.currenturi
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append( page )
                log.info(self.log_msg('Forum Post Added'))
            except:
                log.exception(self.log_msg('Error in addForumPosts'))

    @logit(log, '__getParentPage')
    def __getForumParentPage(self):
        """Get the parent information
        """

        page = {}
        try:
            if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info return True, Already exists'))
                return False
            div_tag = self.soup.find('div',id='divGeneral')
            hierarchy = [   div_tag.find('a',id='ctl00_ContentPlaceHolder2_CenterReplyList1_ctl_lk_forum'),
                div_tag.find('a',id='ctl00_ContentPlaceHolder2_CenterReplyList1_ctl_lk_topic'),
                div_tag.find('span','small')]
            self.hierarchy = page['et_thread_hierarchy'] = [stripHtml(x.renderContents()) for x in hierarchy]
            page['title']= self.hierarchy[-1]
        except:
            log.exception(self.log_msg('Thread hierarchy not found'))
        for each in ['title','edate_thread_last_posted_date','et_thread_author_name','ei_thread_replies_counts']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('page data cannot be extracted for %s'%each))
        try:
            post_hash = get_hash( page )
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result = updateSessionInfo( self.genre, self.session_info_out, self.\
                   parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                return False
            page['path']=[self.parent_uri]
            page['parent_path']=[]
            page['uri'] = normalize( self.currenturi )
            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
            page['priority']=self.task.priority
            page['level']=self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            if not page.has_key('posted_date'):
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            page['client_name'] = self.task.client_name
            page['last_updated_time'] = page['pickup_date']
            page['versioned'] = False
            page['data'] = ''
            page['task_log_id']=self.task.id
            page['entity'] = 'Post'
            page['category']=self.task.instance_data.get('category','')

            self.pages.append(page)

            return True
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False



    @logit(log , '__getForumThreads')
    def __getForumThreads( self ):
        """Get thread information and create task clones with thread uris
        """
        try:
            threads = self.soup.find('table','w-100 GeneralTopic').findAll('tr')[2:]
            if len(threads)==0:
                log.info(self.log_msg('No Thread is found'))
                return False
        except:
            log.exception(self.log_msg('Forum: No thread found, cannot proceed'))
            return False
        for thread in threads:
            try:
                if self.max_forum_threads_count and self.total_forum_threads_count > self.max_forum_threads_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_forum_threads_count = self.total_forum_threads_count + 1
                thread_uri = self.baseuri + thread.find('td','w-49').find('a')['href']
                date_str = stripHtml(thread.find('td','w-26 fontS10').renderContents().strip())
                thread_time = datetime.strptime(date_str,'%m/%d/%y %I:%M %p')
                if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')):
                    log.info(self.log_msg('Session info return True in getForumThreads'))
                    continue
                self.last_timestamp = max(thread_time , self.last_timestamp )
                temp_task =  self.task.clone()
                temp_task.pagedata['edate_thread_last_posted_date'] = datetime.strftime(thread_time,"%Y-%m-%dT%I:%M:%SZ")
                temp_task.instance_data[ 'uri' ] = thread_uri
                temp_task.pagedata[ 'title' ] = stripHtml(thread.find('td','w-49').find('a').renderContents().strip())
                temp_task.pagedata['et_thread_author_name'] = stripHtml(thread.findAll('td')[2].renderContents().strip())
                temp_task.pagedata['ei_thread_replies_counts'] = int(stripHtml(thread.findAll('td')[-2]. \
                                                                    renderContents().strip()))
                self.linksOut.append( temp_task )
                log.info(self.log_msg('taskAdded'))
            except:
                log.exception(self.log_msg('Thread uri not available'))
                continue
        return True

    @logit(log, '__getParentPage')
    def __getReviewsParentPage(self):
        """Get the parent information about the Reivews
        """
        page = {}
        try:
            if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info return True, Already exists'))
                return False
            page['title'] = stripHtml(self.soup.find('h2').renderContents())
        except:
            log.exception(self.log_msg('Title not found'))
        try:
            post_hash = get_hash( page )
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result = updateSessionInfo( self.genre, self.session_info_out, self.\
                   parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                return False
            page['path']=[self.parent_uri]
            page['parent_path']=[]
            page['uri'] = normalize( self.currenturi )
            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
            page['priority']=self.task.priority
            page['level']=self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            page['client_name'] = self.task.client_name
            page['last_updated_time'] = page['pickup_date']
            page['versioned'] = False
            page['data'] = ''
            page['task_log_id']=self.task.id
            page['entity'] = 'Post'
            page['category']=self.task.instance_data.get('category','')
            self.pages.append(page)
            return True
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False
        
    @logit(log, "_setSoup")
    def __setSoup( self, url = None, data = None, headers = {} ):
        """
            It will set the uri to current page, written in seperate
            method so as to avoid code redundancy
        """
        if url:
            self.currenturi = url
        try:
            log.info(self.log_msg( 'for uri %s'%(self.currenturi) ))
            res = self._getHTML( data = data, headers=headers  )
            if res:
                self.rawpage = res[ 'result' ]
            else:
                log.info(self.log_msg('self.rawpage not set.... so Sorry..'))
                return False
            self._setCurrentPage()
            return True
        except Exception, e:
            log.exception(self.log_msg('Page not for  :%s'%url))
            raise e

    @logit(log, "__setSoup")
    def __setNextPage( self, url = None, data={}, headers={}):
        """
            It will set the current page soup
            and maintains the cookies
        """
        try:
            if url:
                self.currenturi = url
            log.info(self.log_msg( 'for uri %s'%(self.currenturi) ))
            headers['User-Agent'] = 'Mozilla/5.0 (Windows; U; Win98; en-US; rv:1.8.1) Gecko/20061010'
            request = urllib2.Request(self.currenturi,urllib.urlencode(data),headers)
            response = self.opener.open(request)
            self.soup =  BeautifulSoup (response.read())
            #self._setCurrentPage()
            return True
        except:
            log.exception(self.log_msg('Soup not set '))
            return False
        