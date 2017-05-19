'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
from datetime import datetime,timedelta
from BeautifulSoup import BeautifulSoup
from cgi import parse_qsl
import logging
from urllib2 import urlparse
from tgimport import tg
import copy

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('CodeProjectConnector')
class CodeProjectConnector(BaseConnector):
    '''This will fetch the info from codeproject.com
    the sample uri is http://www.codeproject.com/Forums/12076/ASP-NET.aspx
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """fetch method of code Project Connector,fetches all the info in forum
        """
        self.genre="Review"
        try:
            if not self.__setSoup():
                log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                return False
            self.total_posts_count = 0
            self.last_timestamp = datetime( 1980,1,1 )
            self.max_posts_count = int(tg.config.get(path='Connector',key='codeproject_forum_numresults'))
            log.info(self.task.instance_data)
            if not self.task.instance_data.get('alread_parsed'):
                while True:
                    if not self.__getThreads():
                        break
                    try:
                        self.currenturi  = 'http://www.codeproject.com' + self.soup.find('a',text=re.compile('Next.*')).parent['href']
                        self.currenturi = self.currenturi.split('#')[0]
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('page not found '))
                        break
                if self.linksOut:
                    log.info(self.last_timestamp)
                    updateSessionInfo('Search', self.session_info_out,self.last_timestamp , None,'ForumThreadsPage', self.task.instance_data.get('update'))
                return True
            else:
                self.__getParentPage()
                self.post_type = True
                while True:
                    self.__addPosts()
                    try:
                        self.currenturi  = 'http://www.codeproject.com' + self.soup.find('a',text=re.compile('Next.*')).parent['href']
                        self.currenturi = self.currenturi.split('#')[0]
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('page not found '))
                        break
            return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log , '__addPosts')
    def __addPosts(self):
        """ It will add Post for a particular thread
        """
        try:
            reviews = [ BeautifulSoup(x) for x in  self.soup.find('table','Frm_MsgTable').__str__().split('<!-- Start Message head -->')[1:]]
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        post_type = "Question"
        log.info([review.find('a')['name'] for review in reviews])
        for i, review in enumerate(reviews):
            if i==0 and self.post_type:
                post_type = "Question"
                self.post_type = False
            else:
                post_type = "Suggestion"
            page = self.__getData( review , post_type )
            if not page:
                log.info(self.log_msg('Todays Post , so, continue with other post'))
                continue
            try:
                review_hash = get_hash( page )
                #unique_key = review.find('a')['name']
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    continue
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
                page['uri'] = page.get('uri',self.parent_uri)
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append( page )
                #log.info(page)
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))

    @logit(log, '__getData')
    def __getData(self, review, post_type ):
        """ This will return the page dictionry
        """
        page = {'title':''}
        try:
            author_tag = review.find('td','Frm_MsgAuthor')
            page['et_author_name'] = stripHtml(author_tag.renderContents())
            page['et_author_profile'] = 'http://www.codeproject.com' + author_tag.find('a')['href']
        except:
            log.info(self.log_msg('author name not found'))
        try:
            date_str = stripHtml(review.find('td','Frm_MsgDate').renderContents())
            if date_str.endswith('ago'):
                return False
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,"%H:%M %d %b '%y"),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('posted date may be todays date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            data_tag = review.find('td','Frm_MsgFt').findPrevious('td')
            div_tag = data_tag.find('div','ForumSig')
            if div_tag:
                div_tag.extract()
            page['data'] = stripHtml(data_tag.renderContents())
        except:
            log.info(self.log_msg('data not found'))
            page['data']=''
        try:
            page['title']= stripHtml(review.find('td','Frm_MsgSubject').renderContents())
        except:
            log.info(self.log_msg('Cannot find the title'))
            page['title'] = ''
        try:
            page['et_data_reply_to'] = self.thread_id
        except:
            log.info(self.log_msg('data reply to is not found'))
        try:
            if page['title']=='':
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
            page['et_data_topic'] =  self.hierarchy[2]
        except:
            log.info(self.log_msg('data forum not found'))
        try:
            page['uri'] = 'http://www.codeproject.com' + review.find('a',text='PermaLink').parent['href']
        except:
            log.info(self.log_msg('uri not found'))
        return page
    
    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """This will get the thread info
        """
        page = {}
        try:
            review = self.soup.find('table','Frm_MsgTable')
        except:
            log.info(self.log_msg('No Forum Found'))
            return False
        try:
            forum_title = review.find('td','Frm_MsgSubject')
            self.parent_uri =  'http://www.codeproject.com' + forum_title.find('a')['href']
        except:
            log.info(self.log_msg('Cannot find the thread uri'))
        try:
            page['title']= stripHtml(forum_title.renderContents())
            self.hierarchy =page['et_thread_hierarchy'] = [ 'Code Project Forums', stripHtml(self.soup.find('a',id='ctl00_MC_ForumName').renderContents()),page['title'] ]
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            page['title']=''
        try:
            self.thread_id = page['et_thread_id'] =  review.find('a',text='PermaLink').parent['href'].split('/')[2]
        except:
            log.info(self.log_msg('Thread id not found'))
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        try:
            date_str = stripHtml(review.find('td','Frm_MsgDate').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,"%H:%M %d %b '%y"),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('data not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            post_hash = get_hash( page )
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo( self.genre, self.session_info_out, self.\
                   parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                return False
            page['path'] = [self.parent_uri]
            page['parent_path']=[]
            page['uri'] = normalize( self.parent_uri )
            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
            page['priority']=self.task.priority
            page['level']=self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
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
    @logit(log , '__getThreads')
    def __getThreads( self ):
        """
        It will fetch each thread and its associate infomarmation
        and add the tasks
        """
        threads = [x.findParent('table') for x in self.soup.findAll('img',src='/script/Forums/Images/msg_question.gif') if x.findParent('table')]
        for thread in threads:
            if  self.total_posts_count > self.max_posts_count:
                log.info(self.log_msg('Reaching maximum post,Return false'))
                return False
            self.total_posts_count = self.total_posts_count + 1
            try:
                date_str = stripHtml(thread.find('td','Frm_MsgDate').renderContents())
                if date_str.endswith(' ago'):
                    log.info(self.log_msg('Todays posts continue'))
                    self.total_posts_count = self.total_posts_count - 1
                    continue
                thread_time = datetime.strptime(date_str,"%H:%M %d %b '%y")
            except:
                log.exception(self.log_msg('Date cannot found, continue with other posts'))
            try:
                thread_time
                if checkSessionInfo('Search',self.session_info_out, thread_time,\
                                    self.task.instance_data.get('update')) and \
                                    self.max_posts_count >= self.total_posts_count:
                    continue
                self.last_timestamp = max(thread_time , self.last_timestamp )
                temp_task=self.task.clone()
                try:
                    temp_task.pagedata['title']= stripHtml(thread.find('td','Frm_MsgSubject').renderContents())
                    temp_task.instance_data[ 'uri' ] = 'http://www.codeproject.com' + thread.findNext('a',text='View&nbsp;Thread').parent['href']
                    temp_task.instance_data[ 'alread_parsed' ] = True
                except:
                    log.exception(self.log_msg('Cannot find the uri'))
                    continue
                try:
                    temp_task.pagedata['et_author_name'] = stripHtml(thread.find('td','Frm_MsgAuthor').renderContents())
                except:
                    log.info(self.log_msg('Cannot find the author name'))
                self.linksOut.append( temp_task )
                log.info(self.log_msg('Task  added'))
            except:
                log.exception(self.log_msg('Cannot add the Task'))
        return True