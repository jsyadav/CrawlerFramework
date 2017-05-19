
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

log = logging.getLogger('SqlServerCentralConnector')
class SqlServerCentralConnector(BaseConnector):
    '''
    This will fetch the info for smallbizserver.net forums
    Sample uris is
    http://www.sqlservercentral.com/Forums/Forum147-1.aspx
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of sql server central
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            self.total_posts_count = 0
            self.last_timestamp = datetime( 1980,1,1 )
            self.max_posts_count = int(tg.config.get(path='Connector',key='sqlservercentral_numresults'))
            self.hrefs_info = self.currenturi.split('/')
            if self.currenturi.startswith('http://www.sqlservercentral.com/Forums/Topic'):
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                self.post_type = True
                next_page_no = 2
                while True:
                    self.__addPosts()
                    try:
                        self.currenturi = 'http://www.sqlservercentral.com/Forums/' +  self.soup.find('table',id= re.compile('FooterTable')).find('a',text=str(next_page_no)).parent['href']
                        if not self.__setSoup():
                            break
                        next_page_no = next_page_no + 1
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                return True
            else:
                self.currenturi = self.currenturi.replace('Default.aspx','afcol/0/afsort/DESC/Default.aspx')
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                next_page_no = 2
                while True:
                    if not self.__getThreadPage():
                        break
                    try:
                        self.currenturi = 'http://www.sqlservercentral.com/Forums/' +  self.soup.find('a',title='Next Page')['href']
                        if not self.__setSoup():
                            break
                        next_page_no = next_page_no + 1
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
            #review_info_header = soup.findAll('tr',id=re.compile('trRow1$'))
            #review_info_body = soup.findAll('tr',id=re.compile('trRow2$'))
            #review_info_footer = soup.findAll('tr',id=re.compile('trRow3$'))
            #reviews = [ BeautifulSoup('<test>' + ''.join([each.__str__() for each in review]) + '</test>') \
            #                for review in zip(review_info_header,review_info_body,\
            #                                                review_info_footer)]
            reviews = self.soup.findAll('span',id=re.compile('FullMessage'))
            log.info(len(reviews))
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
                unique_key = stripHtml(review.findNext('a',id=re.compile('PostLink')).renderContents()).split('#')[-1]
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    log.info(self.log_msg('Session info returns True'))
                    continue
                page = self.__getData( review, post_type )
            except:
                log.info(self.log_msg('unique key not found'))
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
            threads = [x.findParent('table').findParent('tr') for x in self.soup\
                                .findAll('span',id=re.compile('.EditableSubject'))]
            for thread in threads:
                try:
                    date_str = stripHtml(thread.find('span',id=re.compile('LastPostDate')).renderContents())
                    log.info(date_str)
                    thread_time = datetime.strptime(date_str,'%A, %B %d, %Y %I:%M %p')
                    #page['edate_thread_last_post_date'] = datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                except:
                    log.exception(self.log_msg('Todays Post, so ignoring'))
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
                        title_tag = thread.find('span',id=re.compile('.EditableSubject'))
                        temp_task.pagedata['title']= stripHtml(title_tag.renderContents())
                        temp_task.instance_data[ 'uri' ] =  normalize(title_tag.find('a')['href'])
                    except:
                        log.info(self.log_msg('Cannot find the uri'))
                        continue
                    try:
                        temp_task.pagedata['title']= stripHtml(thread.find('span',\
                                id=re.compile('EditableSubject')).renderContents())
                    except:
                        log.info(self.log_msg('Cannot find the title'))
                    try:
                        temp_task.pagedata['et_author_name'] = stripHtml(thread.\
                                        find('a',id=re.compile('AuthorName')).\
                                                                renderContents())
                    except:
                        log.info(self.log_msg('Cannot find the title'))
                    try:
                        temp_task.pagedata['et_thread_last_post_author']  = stripHtml(thread.find('span',id=re.compile('LastPostBy')).renderContents())
                        
                    except:
                        log.info(self.log_msg('Cannot find the replies count'))
                    try:
                        view_reply = {'ei_thread_num_replies':'lblReplies','ei_thread_num_views':'lblViews'}
                        for each in view_reply.keys():
                            temp_task.pagedata[each] = int(stripHtml(thread.find('span',id=re.compile(view_reply[each])).renderContents()))
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
            page['et_author_name'] = unicode(re.search('Username=([^ ]+)',review.findPrevious('div',id=re.compile('smAuthorName')).__str__()).group(1).replace('+',' '))
        except:
            log.info(self.log_msg('author name not found'))
        try:
            aut_info = [x.strip() for x in stripHtml(review.findPrevious('td','SmallTxt').renderContents()).split('\n') if not x.strip()=='']
            for each in aut_info:
                if len(each.split(':'))==1:
                    page['et_author_title'] = each
                if each.startswith('Group:'):
                    page['et_author_group'] = each.replace('Group:','').strip()
                if each.startswith('Last Login:'):
                    try:
                        page['edate_author_last_login'] = datetime.strftime(datetime.strptime(each.replace('Last Login:','').strip(),'Posted %A, %B %d, %Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg('login not found'))

                if each.startswith('Points:') or each.startswith('Visits:'):
                    try:
                        each_str = each.split(':')
                        page['ei_author_' + each_str[0].strip().lower() + '_count'] = int(re.sub('[^\d]','',each_str[-1]))
                    except:
                        log.info(self.log_msg('points count or visits count not found'))
        except:
            log.exception(self.log_msg('Author info not found'))
        try:
            page['data'] = stripHtml(review.renderContents())
        except:
            log.info(self.log_msg('data not found'))
            page['data'] =''
        try:
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
            date_str = stripHtml(review.findPrevious('span',id=re.compile('PostedDate')).renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'Posted %A, %B %d, %Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Posted date not found for this post'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            if len(page['data']) > 50:
                page['title'] = page['data'][:50] + '...'
            else:
                page['title'] = page['data']
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
        try:
            page['et_data_post_type'] = post_type
        except:
            log.info(self.log_msg('Page info is missing'))
        try:
            page['et_data_forum'] = self.hierarchy[0]
            page['et_data_subforum'] = self.hierarchy[1]
            page['et_data_topic'] = self.hierarchy[2    ]
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
            self.hierarchy =  page['et_thread_hierarchy'] = [stripHtml(x.renderContents()) for x in self.soup.find('table','BreadCrumb_InnerTableCSS').findAll('a')][1:]
            page['title']= page['et_thread_hierarchy'][-1]
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            page['title']=''
        try:
            self.thread_id =  page['et_thread_id'] = self.currenturi.split('/')[-1].split('-')[0].replace('Topic','')
        except:
            log.info(self.log_msg('Thread id not found'))
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        
        for each in ['et_author_name','ei_thread_num_replies','ei_thread_num_views','edate_last_post_date']:
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
            #page['first_version_id']=result['first_version_id']
            #page['id'] = result['id']
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