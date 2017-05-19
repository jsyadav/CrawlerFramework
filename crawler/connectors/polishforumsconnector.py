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

log = logging.getLogger('PolishForumsConnector')
class PolishForumsConnector(BaseConnector):
    '''
    This will fetch the info for www.polishfroums.com
    Sample uris is
    http://www.polishforums.com/business_poland-f14_0.html
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of polish forums
        sample uri : http://www.polishforums.com/business_poland-f14_0.html
        
        """
        self.genre="Review"
        try:
            #self.currenturi = 'http://www.polishforums.com/business-poland-14/poland-most-attractive-european-23994/'
            self.parent_uri = self.currenturi
            if urlparse.urlparse(self.currenturi)[2].split('-')[-1].startswith('f'):
                self.total_posts_count = 0
                self.last_timestamp = datetime( 1980,1,1 )
                self.max_posts_count = int(tg.config.get(path='Connector',key='polishforums_numresults'))
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                while True:
                    if not self.__getThreads():
                        break
                    try:
                        self.currenturi = self.soup.find('a',text='&raquo;&raquo;').parent['href']
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                if self.linksOut:
                    updateSessionInfo('Search', self.session_info_out,self.last_timestamp , None,'ForumThreadsPage', self.task.instance_data.get('update'))
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
                        self.currenturi = self.soup.find('a',text='&raquo;&raquo;').parent['href']
                        if not self.__setSoup():
                            break
                    except:
                        log.exception(self.log_msg('Next page not set'))
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
            reviews = [x.findParent('table') for x in self.soup.findAll('div','postedText')]
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        for i, review in enumerate(reviews[:]):
            post_type = "Question"
            if i==0 and self.post_type:
                post_type = "Question"
                self.post_type = False
            else:
                post_type = "Suggestion"
            try:
                unique_key = review.findPrevious('a')['name']
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    log.info(self.log_msg('session info return True'))
                    continue
                page = self.__getData( review , post_type )
                log.info(page)
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
                #log.info(page)
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))

    @logit(log , '__getThreads')
    def __getThreads( self ):
            """ It will fetch the thread info and create tasks

            """
            try:
                threads = self.soup.findAll('tr',attrs={'class':re.compile('tbCel[12]')})
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False
            for thread in threads:
                if  self.total_posts_count > self.max_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    thread_info = thread.findAll('td','caption1')
                    if not len(thread_info)==5:
                        log.info(self.log_msg('not enough info from thread'))
                        continue
                    date_str = stripHtml(thread_info[4].find('font').renderContents())
                    try:
                        thread_time =  datetime.strptime(date_str,'%b %d, %y, %H:%M')
                    except:
                        log.info(self.log_msg('Last post date not found'))
                        continue
                    if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        log.info(self.log_msg('Session info return True or Reaches max count'))
                        continue
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] = thread_info[1].find('a')['href']
                    temp_task.pagedata['et_author_name'] =  stripHtml(thread_info[4].renderContents()).split('\n')[0]
                    temp_task.pagedata['title']= stripHtml(thread_info[1].renderContents())
                    temp_task.pagedata['ei_thread_replies_count'] = int(stripHtml(thread_info[2].renderContents()))
                    date_str = stripHtml(thread_info[3].find('font').extract().renderContents())
                    temp_task.pagedata['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%b %d, %y, %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
                    temp_task.pagedata['et_last_post_author_name'] = stripHtml(thread_info[3].renderContents())
                    temp_task.pagedata['edate_last_post_date']=  datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    log.info(temp_task.pagedata)
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
        page = {}
        try:
            aut_tag = review.find('td','author')
            aut_info = [x.strip() for x in stripHtml(aut_tag.renderContents()).split('\n') if not x.strip()=='']
            page['et_author_name'] = aut_info[0]
            page['et_author_type'] = aut_info[1]
            page['ei_author_threads_count'] = int(aut_info[2].split('Threads:')[-1].strip())
            date_str = aut_info[4].split('Joined:')[-1].strip()
            page['edate_author_member_since'] = datetime.strftime(datetime.strptime(date_str, '%b %d, %y'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('author info not found, may be a Guest'))
        parent_soup = copy.copy(self.soup)
        parent_uri = self.currenturi
        try:
            page['et_author_profile'] = aut_tag.find('a')['href']
            page = self.__getAuthorInfo(page)
        except:
            log.info(self.log_msg('author profile info not found, may be a Guest'))
        self.soup = copy.copy(parent_soup)
        self.currenturi = parent_uri
        try:
            page['et_author_description'] =stripHtml(review.find('tr').find('div','rounded').find('span').renderContents())
        except:
            log.info(self.log_msg('author desc not found'))
        try:
            date_str =  ' '.join(review.find('tr').find('span','txtSm').contents[-1].__str__().split('&nbsp;')[1:]).strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%b %d, %y, %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('posted date not found'))
        try:
            page['data'] = stripHtml(review.find('div','postedText').renderContents())
        except:
            page['data'] = ''
            log.info(self.log_msg('Data not found for this post'))
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
            page['et_data_topic'] = self.forum_title
        except:
            log.info(self.log_msg('data forum not found'))
        return page

    @logit(log,"__getAuthorInfo")
    def __getAuthorInfo(self, page):
        """ This will get info abt the Author
        """
        try:
            self.currenturi = page['et_author_profile']
            if not self.__setSoup():
                return page
            author_info = [ x.findParent('tr') for x in self.soup.findAll('td','caption4')]
            for each in author_info:
                info = stripHtml(each.find('td','caption4').renderContents())[:-1].strip()
                key = re.sub('[^\w]','',info.lower())
                value = stripHtml(each.find('td','caption5').renderContents())
                if info in ['Posts','Threads']:
                    page['ei_author_'+key] = int(value)
                    continue
                page['et_author_' + key] = value
        except:
            log.info(self.log_msg('author info not found for %s'%each))
            
        return page
    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will get the parent info
        thread url.split('&')[-1].split('=')[-1]

        """
        page = {}
        try:
            self.hierarchy =page['et_thread_hierarchy'] = [x.strip() for x in stripHtml(self.soup.find('table','forums').renderContents()).split('/') if not x.strip()=='']
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
        try:
            self.forum_title = page['title']=stripHtml(self.soup.find('h1','headingTitle').renderContents())
        except:
            log.info(self.log_msg('title not found'))
            page['title'] = ''
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        for each in ['et_last_post_author_name','ei_thread_replies_count','edate_last_post_date','et_author_name','posted_date']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('page data cannot be extracted for %s'%each))
        try:
            page['et_thread_id'] = urlparse.urlparse(self.currenturi)[2].split('-')[-1][:-1]
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