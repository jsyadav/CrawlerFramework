
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
from datetime import datetime
import logging
from urllib2 import urlparse
from tgimport import tg
import copy
from BeautifulSoup import BeautifulSoup

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('TeamSystemRocksConnector')
class TeamSystemRocksConnector(BaseConnector):
    '''
    This will fetch the info for team system rocks forums
    Sample uris is
    http://teamsystemrocks.com/forums/59.aspx
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of sql team system rocks forum
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            self.total_posts_count = 0
            self.last_timestamp = datetime( 1980,1,1 )
            self.base_url = 'http://teamsystemrocks.com'
            self.max_posts_count = int(tg.config.get(path='Connector',key='teamsystemrocks_max_threads_to_process'))
            self.hrefs_info = self.currenturi.split('/')
            if self.currenturi.startswith('http://teamsystemrocks.com/forums/t/'):
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                self.post_type = True
                while True:
                    self.__addPosts()
                    try:
                        self.currenturi = self.base_url +  self.soup.find('a',text='Next >').parent['href']
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                return True
            else:
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                while True:
                    if not self.__getThreadPage():
                        break
                    try:
                        self.currenturi = self.base_url +  self.soup.find('a',text='Next >').parent['href']
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                if self.linksOut:
                    updateSessionInfo('Search', self.session_info_out,self.last_timestamp , None,'ForumThreadsPage', self.task.instance_data.get('update'))
                return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log , '__addPosts')
    def __addPosts(self):
        """ It will add Post for a particular thread
        """
        try:
            reviews = self.soup.findAll('div','ForumPostArea')
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        for i, review in enumerate(reviews):
            if i==0 and self.post_type:
                post_type = "Question"
                self.post_type = False
            else:
                post_type = "Suggestion"
            try:
                unique_key = review.previous.previous['name']
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    log.info(self.log_msg('Session info returns True'))
                    continue
                page = self.__getData( review, post_type )
            except:
                log.exception(self.log_msg('unique key not found'))
                continue
            try:
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            get_hash( page ),'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    continue
                parent_list = [ self.parent_uri ]
                page['parent_path'] = copy.copy(parent_list)
                parent_list.append( unique_key )
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
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))

    @logit(log , '__getThreadPage')
    def __getThreadPage( self ):
            """
            It will fetch each thread and its associate infomarmation
            and add the tasks
            """
            threads = [x.findParent('tr') for x in self.soup.findAll('td','CommonListCell ForumMyNameColumn')]
            for thread in threads:
                try:
                    last_post_info = [x.strip() for x in stripHtml(thread.find('td','ForumSubListCellLeftMost ForumLastPost').renderContents()).split('\n') if not x.strip()=='']
                    thread_time = datetime.strptime( last_post_info[-1] ,', %m-%d-%Y %I:%M %p')
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                except:
                    log.exception(self.log_msg('Cannot find the last post date, continue'))
                    continue
                if  self.total_posts_count > self.max_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    if checkSessionInfo('Search',self.session_info_out, thread_time,\
                                        self.task.instance_data.get('update')) and \
                                        self.max_posts_count >= self.total_posts_count:
                        continue
                    temp_task=self.task.clone()
                    try:
                        title_tag = thread.find('a', 'ForumNameUnRead')
                        temp_task.pagedata['title']= stripHtml(title_tag.renderContents())
                        temp_task.instance_data[ 'uri' ] = self.base_url +  title_tag['href']
                    except:
                        log.info(self.log_msg('Cannot find the uri'))
                        continue
                    try:
                        temp_task.pagedata['et_thread_last_post_author']  =  re.sub('^by ','',last_post_info[1]).strip()
                    except:
                        log.info(self.log_msg('Cannot find the replies count'))
                    try:
                        temp_task.pagedata['ei_thread_replies_count'] = int(stripHtml(thread.find('td','CommonListCell ForumMyRepliesColumn').renderContents()))
                    except:
                        log.info(self.log_msg('Cannot find the views count'))
                    try:
                        temp_task.pagedata['edate_last_post_date']=  datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg('Cannot find the last posted'))
                    log.info(temp_task.pagedata)
                    self.linksOut.append( temp_task )
                except:
                    log.info(self.log_msg('Cannot add the Task'))
            return True

    @logit(log, '__getData')
    def __getData(self, review, post_type ):
        """ This will return the page dictionry
        """
        page = {'title':''}
        try:
            aut_tag = review.find('li','ForumPostUserName')
            page['et_author_name'] = stripHtml(aut_tag.renderContents())
            page['et_author_profile'] = self.base_url +  aut_tag.find('a')['href']
            previous_uri = self.currenturi
            previous_soup = copy.copy(self.soup)
            page = self.__getAuthorInfo(page)
            self.soup = copy.copy(previous_soup)
            self.currenturi = previous_uri
        except:
            log.info(self.log_msg('author name not found'))
        try:
            page['et_author_title'] = stripHtml(review.find('li','ForumPostUserIcons').img['alt'])
        except:
            log.info(self.log_msg('author title not found'))
        try:
             page['title'] = stripHtml(review.find('h4','ForumPostTitle').renderContents())
        except:
            log.exception(self.log_msg('Title not found'))
            page['title'] = ''
        try:
            page['data'] = stripHtml(review.find('div','ForumPostContentText').renderContents())
        except:
            log.info(self.log_msg('data not found'))
            page['data'] =''
        try:
            if page['title'] =='':
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
        try:
            page['et_data_reply_to'] = self.thread_id
        except:
            log.info(self.log_msg('data reply to is not found'))
        try:
            date_str = stripHtml(review.find('h4','ForumPostHeader').find('td',align='left').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%m-%d-%Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Posted date not found for this post'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['et_data_post_type'] = post_type
        except:
            log.info(self.log_msg('Page info is missing'))
        try:
            page['et_data_forum'] = self.hierarchy[0]
            page['et_data_subforum'] = self.hierarchy[1]
            page['et_data_topic'] = self.hierarchy[2]
        except:
            log.exception(self.log_msg('data forum not found'))
        return page

    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will get the parent info
        """
        page = {}
        try:
            self.hierarchy =  page['et_thread_hierarchy'] = [stripHtml(x.renderContents()) for x in self.soup.find('div','CommonBreadCrumbArea').findAll('a')][1:]
            page['title']= page['et_thread_hierarchy'][-1]
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            page['title']=''
        try:
            self.thread_id =  page['et_thread_id'] = unicode(self.currenturi.split('/')[-1].replace('.aspx',''))
        except:
            log.info(self.log_msg('Thread id not found'))
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False

        for each in ['et_thread_last_post_author','ei_thread_replies_count','edate_last_post_date']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('page data cannot be extracted for %s'%each))
        try:
            post_hash = get_hash( page )
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo( self.genre, self.session_info_out, self.\
                   parent_uri, post_hash,'Forum',self.task.instance_data.get('update'), Id=id)
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
            log.info(page)
            log.info(self.log_msg('Parent Page added'))
            return True
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False
        
    @logit(log, "_getAuthorInfo")
    def __getAuthorInfo( self, page ):
        '''This will fetch the authro info
        '''
        try:
            self.currenturi = page['et_author_profile']
            if not self.__setSoup():
                return page
        except:
            log.info(self.log_msg('author info not odun'))
            return page
        date_info = {'Member since:':'edate_author_member_since','Last visited:':'edate_author_last_login',\
                    'Timezone:':'et_author_timezone','Post Rank:':'ei_author_posts_rank','Total Posts:':'ei_author_posts_count'}
        for each in date_info.keys():
            try:
                value = stripHtml(self.soup.find('td',text=re.compile(each)).findParent('tr').findAll('td')[-1].renderContents())
                if date_info[each].startswith('edate_'):
                    page[date_info[each]] = datetime.strftime(datetime.strptime(value,'%m-%d-%Y'),"%Y-%m-%dT%H:%M:%SZ")
                elif date_info[each].startswith('ei_'):
                    page[date_info[each]] = int(value)
                else:
                    page[date_info[each]] = re.sub('\s+',' ',value,re.S)
            except:
                log.info(self.log_msg('%s is not found'%each))
        return page
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
        