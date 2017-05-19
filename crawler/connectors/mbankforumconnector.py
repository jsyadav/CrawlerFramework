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
from urllib import urlencode
from cgi import parse_qsl
import copy

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('MbankForumConnector')
class MbankForumConnector(BaseConnector):
    '''

    Sample uris is
    http://www.mbank.pl/forum/list.html?cat=29
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of forum page
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            self.base_url = 'http://www.mbank.pl'
            if not self.parent_uri.startswith('http://www.mbank.pl/forum/read'):
                self.total_posts_count = 0
                self.last_timestamp = datetime( 1980,1,1 )
                self.max_posts_count = int(tg.config.get(path='Connector',key='mbank_forum_numresults'))
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                while True:
                    if not self.__getThreads():
                        break
                    try:
                        self.currenturi = self.soup.find('a',text=re.compile('nast.pna')).parent['href']
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
                self.__addPosts()
                    
                return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log , '__addPosts')
    def __addPosts(self):
        """ It will add Post for a particular thread
        """
        try:
            reviews = self.soup.findAll('div','commItem')
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        for i,review in enumerate(reviews):
            post_type = ""
            if i==0:
                post_type = "Question"
            else:
                post_type = "Suggestion"
            try:
                unique_key = review.find('a')['name']
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    log.info(self.log_msg('session info return True'))
                    continue
                page = self.__getData( review , post_type )
                review_hash = get_hash( page )
                log.info(page)
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            review_hash,'Post', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    log.info(self.log_msg('reuslt not updated '))
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
                threads=self.soup.find('table',id='tForum').findAll('tr','gray')
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False
            for thread in threads:
                if  self.total_posts_count >= self.max_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    try:
                        date_str = stripHtml(thread.find('td','td3 br').renderContents()).split('\n')
                        thread_time = datetime.strptime(date_str[1],'%d-%m-%y %H:%M')
                    except:
                        log.info(self.log_msg('Cannot continue, thread time not found '))
                        continue
                    if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                    #if checkSessionInfo('search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        log.info(self.log_msg('Session info return True or Reaches max count'))
                        return False
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] = thread.find('td','cloudText').find('a')['href']
                    temp_task.pagedata['edate_thread_last_post_date']=  datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    temp_task.pagedata['posted_date']=  datetime.strftime(datetime.strptime(date_str[0],'%d-%m-%y %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
                    temp_task.pagedata['et_author_name'] =  stripHtml(thread.find('td','td2').find('a').renderContents())
                    temp_task.pagedata['ei_thread_replies_count'] = int(stripHtml(thread.find('td','cloudCount').renderContents()))
                    try:
                        temp_task.pagedata['ei_thread_category'] =  stripHtml(thread.find('td','td2').find('span').find('a').renderContents())
                    except:
                        log.info(self.log_msg('Catefory not found'))
                    log.info(temp_task.pagedata)
                    log.info('taskAdded')
                    self.linksOut.append( temp_task )
                except:
                    log.exception( self.log_msg('Task Cannot be added') )
            return True
    @logit(log, '__getData')
    def __getData(self, review, post_type ):
        """ This will return the page dictionry
        """
        page = {}
        try:
            page['et_data_post_type'] = post_type
            page['title'] = stripHtml(review.find('p','subject').renderContents())
        except:
            log.info(self.log_msg('title not found'))
        try:
            page['et_data_forum'] = self.forum_title
        except:
            log.info(self.log_msg('forum title not found'))
        try:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            post_info =  stripHtml(review.find('p','date').renderContents()).split(',')
            page['posted_date'] = datetime.strftime(datetime.strptime(post_info[0],'%d-%m-%y %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
            page['et_data_reference'] = post_info[1].replace('---.','').strip()
        except:
            log.info(self.log_msg('Ip address not found'))
        try:
            page['data'] =  '\n'.join([x.strip() for x in  stripHtml(review.find('p','text').renderContents()).split('\n') if not x.strip()=='' and not x.strip().startswith('>') and not re.search('napisa.\(a\):$',x.strip())])
            #page['data'] = stripHtml(review.find('p','text').renderContents())
        except:
            log.exception(self.log_msg('Posted date not found for this post'))
            page['data'] = ''
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
            aut_tag = review.find('p','userNickname').find('a')
            page['et_author_profile'] = aut_tag['href']
            page['et_author_nickname'] = stripHtml(aut_tag.renderContents())
            page['et_author_range'] = stripHtml(review.find('p','userRange').renderContents())
        except:
            log.info(self.log_msg('author info not found'))
        previous_soup = copy.copy(self.soup)
        previous_url = self.currenturi
        try:
            page = self.__getAuthorInfo(page)
        except:
            log.info(self.log_msg('not found'))
        self.currenturi = previous_url
        self.soup = copy.copy(previous_soup)
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
            self.forum_title = page['title']= stripHtml(self.soup.find('p','bigHeader').renderContents())
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            page['title']=''
        for each in ['et_author_name','edate_thread_last_post_date','ei_thread_replies_count','posted_date']: # ,'ei_thread_views_count','edate_last_post_date']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('page data cannot be extracted for %s'%each))
            try:
                 title_info = [x.strip() for x in stripHtml(self.soup.find('p','bigHeader').findNext('div').renderContents()).split('::')]
                 page['et_author_name'] = title_info[0]
                 page['et_thread_category'] = title_info[2]
                 page['posted_date'] = datetime.strftime(datetime.strptime(title_info[1],'%d-%m-%y %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.info(self.log_msg('title info not found'))
        try:
            post_hash = get_hash( page )
            log.info(page)
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
            log.info(self.log_msg('Parent Page added'))
            return True
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False
    @logit(log,"__getAuthorInfo")
    def __getAuthorInfo( self, page):
        '''it will create url
        '''
        try:
            self.currenturi = page['et_author_profile']
            if not self.__setSoup():
                return page
            try:
                aut_info = [stripHtml(x.renderContents()).split(':')[1].strip() for x in  self.soup.findAll('div','forumInfoPanel')]
                if len(aut_info)==2:
                    page['ei_author_comments_count'] = int(aut_info[1])
                    page['edate_author_member_since'] = datetime.strftime(datetime.strptime(aut_info[0],'%d.%m.%Y'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.info(self.log_msg('author info not found'))
        except:
            log.info(self.log_msg('author info not found'))
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