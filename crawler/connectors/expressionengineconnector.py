'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
from datetime import datetime,timedelta
import logging
from urllib2 import urlparse
from tgimport import tg
import copy

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('ExpressionEngineConnector')
class ExpressionEngineConnector(BaseConnector):
    '''
    This will fetch the info for msexchange forums
    Sample uris is 
    http://expressionengine.com/forums/viewforum/75/
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of ExpressionEngine
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            if self.currenturi.startswith('http://expressionengine.com/forums/viewforum'):
                self.total_posts_count = 0
                self.last_timestamp = datetime( 1980,1,1 )
                self.max_posts_count = int(tg.config.get(path='Connector',key='expressionengine_forum_numresults'))
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                while True:
                    if not self.__getThreads():
                        break
                    try:
                        self.currenturi = self.soup.find('a',text='Next').parent['href']
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                if self.linksOut:
                    updateSessionInfo('Search', self.session_info_out,self.last_timestamp , None,'ForumThreadsPage', self.task.instance_data.get('update'))
                log.info(self.last_timestamp)
                return True
            else:
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                self.post_type= True
                while True:
                    self.__addPosts()
                    try:
                        self.currenturi = self.soup.find('a',text='Next').parent['href']
                    except:
                        log.info(self.log_msg('Next page not set'))
                        break
                    if not self.__setSoup():
                        log.info(self.log_msg('cannot continue'))
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
            reviews=[each.findParent('table') for each in self.soup.findAll('td','tableCellOne postCellPost')]
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        for i, review in enumerate(reviews):
            post_type = "Question"
            if i==0 and self.post_type:
                post_type = "Question"
                self.post_type = False
            else:
                post_type = "Suggestion"
            page = self.__getData( review , post_type )
            try:
                review_hash = get_hash( page )
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
                page['uri'] = self.currenturi
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append( page )
                log.info(page)
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))

    @logit(log , '__getThreads')
    def __getThreads( self ):
            """ It will fetch the thread info and create tasks
            
            """
            try:
                forum_topics = self.soup.find('td',text='Topic Title').findParent('table')
                threads = [each.findParent('tr') for each in forum_topics.findAll('div','topicTitle')]
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False
            for thread in threads:
                if  self.total_posts_count > self.max_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    last_post_date_and_author =  [each.strip() for each in stripHtml(thread.find('div','tablePostInfo').renderContents()).split('\n') if not each.strip()=='']
                    date_str = re.sub('Posted:','',last_post_date_and_author[0]).strip()
                    try:
                        thread_time = datetime.strptime(date_str,'%m-%d-%Y %I:%M %p')
                    except:
                        date_str = re.sub('ago$','',date_str).strip()
                        total_seconds = 0
                        hours = re.search('(\d+) hours',date_str)
                        minutes = re.search('(\d+) minutes',date_str)
                        if hours:
                            total_seconds = int(hours.group(1)) * 60 * 60
                        if minutes:
                            total_seconds = int(minutes.group(1)) * 60
                        if total_seconds==0:
                            log.info(self.log_msg('Pls check the date string %s'%date_str))
                            continue
                        thread_time = datetime.utcnow()-timedelta(seconds=360)
                    if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        log.info(self.log_msg('Session info return True or Reaches max count'))
                        continue
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    topic_title = thread.find('div','topicTitle')
                    temp_task.instance_data[ 'uri' ] = topic_title.find('a')['href']
                    temp_task.pagedata['et_author_name'] =  stripHtml(topic_title.findParent('td').renderContents()).split('Author:')[-1].strip()
                    temp_task.pagedata['title']= stripHtml(topic_title.find('a',title=True).renderContents())
                    temp_task.pagedata['ei_thread_replies_count'] = int(stripHtml(topic_title.findParent('td').findNextSibling('td').renderContents()))
                    temp_task.pagedata['ei_thread_views_count'] = int(stripHtml(topic_title.findParent('td').findNextSibling('td').findNextSibling('td').renderContents()))
                    temp_task.pagedata['et_last_post_author_name'] = last_post_date_and_author[1].split('Author:')[-1].strip()
                    temp_task.pagedata['edate_last_post_date']=  datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    log.info(temp_task.pagedata)
                    log.info(temp_task.instance_data[ 'uri' ])
                    log.info('taskAdded')
                    self.linksOut.append( temp_task )
                except:
                    log.exception( self.log_msg('Task Cannot be added') )
                    continue
            return True
    @logit(log, '__getData')
    def __getData(self, review, post_type ):
        """ This will return the page dictionry
        """ 
        page = {'title':''}
        try:
            page['et_author_name'] = stripHtml(review.find('div','largeLinks').renderContents())
        except:
            log.info(self.log_msg('author name not found'))
        try:
            post_info = [stripHtml(each.renderContents()) for each in review.findAll('div','userBlock')]
            for each in post_info:
                if each.startswith('Total Posts:'):
                    page['ei_author_posts_count'] = int(each.split('Total Posts:')[-1].strip())
                if each.startswith('Joined'):
                    try:
                        date_str = each.split('Joined')[-1].strip()
                        page['edate_author_member_since'] = datetime.strftime(datetime.strptime(date_str,'%m-%d-%Y'),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg('Cannot find Author member sinc'))
        except:
            log.info(self.log_msg('Post info cannot be found'))
        try:
            rating = len(review.find('div','rankStars').findAll('img'))
            if rating>0:
                page['ef_author_rating'] = float(rating)
        except:
            log.info(self.log_msg('author rating cannot found'))
        try:
            page['et_author_title'] =  stripHtml(review.find('div',{'class':re.compile('^rank.*')}).renderContents())
        except:
            log.info(self.log_msg('author title cannot found'))
        try:
            date_str  = re.sub('^Posted:','',review.find('td',text=re.compile('^Posted:.*'))).strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%d %B %Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Posted date not found for this post'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['et_data_reply_to'] = self.parent_uri.split('/')[-2]
        except:
            log.info(self.log_msg('data reply to is not found'))
        try:
            page['data'] =stripHtml(review.find('div','post').renderContents())
        except:
            page['data'] = ''
            log.exception(self.log_msg('Data not found for this post'))
        try:
            page['et_data_post_type'] = post_type
        except:
            log.info(self.log_msg('Page info is missing'))
        try:
            page['et_data_forum'] = self.hierarchy[-3]
            page['et_data_subforum'] = self.hierarchy[-2]
            page['et_data_topic'] = self.hierarchy[-1]
        except:
            log.info(self.log_msg('data forum not found'))
        return page
    
    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will get the parent info
        thread url.split('&')[-1].split('=')[-1]

        """
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        page = {}
        try:
            self.hierarchy = [each.strip() for each in stripHtml(self.soup.find('div','breadcrumb').renderContents()).split('>')]
            self.hierarchy [-1] = stripHtml(self.soup.find('div','topicHeading').renderContents())
            page['et_thread_hierarchy'] = self.hierarchy
            page['title']= page['et_thread_hierarchy'][-1]
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            page['title']=''
        for each in ['et_last_post_author_name','ei_thread_replies_count','ei_thread_views_count','edate_last_post_date']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('page data cannot be extracted for %s'%each))
        try:
            page['et_thread_id'] = self.currenturi.split('/')[-2]
        except:
            log.info(self.log_msg('Thread id not found'))
        try:
            post_hash = get_hash( page )
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo( self.genre, self.session_info_out, self.\
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
