
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
import logging
from cgi import parse_qsl
from urllib2 import urlparse
from datetime import datetime
from BeautifulSoup import BeautifulSoup

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('VbCityConnector')
class VbCityConnector(BaseConnector):
    '''
    This will fetch the info for  vb city forums
    Sample uris is
    http://www.vbcity.com/forums/forum.asp?fid=44
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of Vb city
        """
        self.genre="Review"
        try:
            self.baseurl = 'http://www.vbcity.com/forums/'
            self.post_type = True
            self.parent_uri = self.currenturi
            self.total_posts_count = 0
            self.last_timestamp = datetime( 1980,1,1 )
            self.max_posts_count = int(tg.config.get(path='Connector',key='vbcity_numresults'))
            self.hrefs_info = self.currenturi.split('/')
            if self.currenturi.startswith('http://www.vbcity.com/forums/topic'):
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                while True:
                    self.__addPosts()
                    try:
                        self.currenturi = 'http://www.vbcity.com' + self.soup.find('a',text='&raquo;').parent['href']
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('no more posts'))
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
                        self.currenturi = 'http://www.vbcity.com' + self.soup.find('a',text='&raquo;').parent['href']
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
            tr_tags = self.soup.find('td','body').findParent('tr').findParent('table').findAll('tr',recursive=False)[1:-2]
            reviews = []
            review_str = tr_tags[0].__str__()
            for each in tr_tags[1:]:
                if not each.find('font','subject'):
                    review_str = review_str + each.__str__()
                    continue
                else:
                    reviews.append(BeautifulSoup(review_str))
                    review_str = each.__str__()
            reviews.append(BeautifulSoup(review_str))
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        post_type = "Question"
        for i, review in enumerate(reviews):
            if i==0 and self.post_type:
                post_type = "Question"
                self.post_type = False
            else:
                post_type = "Suggestion"
            try:
                unique_key = self.baseurl + review.find('font','subject').a['href']
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    log.info(self.log_msg('Session info returns True'))
                    continue
            except:
                log.exception(self.log_msg ('unique key not found'))
                continue
            try:
                page = self.__getData( review, post_type )
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            get_hash( page ),'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    continue
                parent_list = [self.parent_uri]
                page['parent_path'] = parent_list[:]
                parent_list.append(unique_key)
                page['path'] = parent_list
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
            threads = self.soup.findAll('tr',height='25')
            for thread in threads:
                if  self.total_posts_count > self.max_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    thread_info = thread.findAll('td',recursive=False)
                    if not len(thread_info)==7:
                        log.info(self.log_msg('Not enough fields to proceed, continue with next thread'))
                        continue
                    date_str = re.sub('\s+',' ',stripHtml(thread_info[-1].renderContents()))
                    thread_time =  datetime.strptime(date_str ,'%m/%d/%Y %I:%M:%S %p')
                except:
                    log.exception(self.log_msg('Date cannot found, continue with other posts'))
                    continue
                try:
                    if checkSessionInfo('Search',self.session_info_out, thread_time,\
                                        self.task.instance_data.get('update')):
                        continue
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    try:
                        title_tag = thread_info[3].find('a')
                        temp_task.pagedata['title']= stripHtml(title_tag.renderContents())
                        temp_task.instance_data[ 'uri' ] = self.baseurl + title_tag['href']
                    except:
                        log.exception(self.log_msg('Cannot find the uri'))
                        continue
                    try:
                        temp_task.pagedata['et_author_name'] = stripHtml(thread_info[-3]\
                                                                        .renderContents())
                    except:
                        log.info(self.log_msg('Cannot find the author name'))
                    try:
                        info_str = stripHtml(thread_info[3].renderContents())
                        temp_task.pagedata['et_thread_last_post_author']  =   re.\
                            search('last posted by (.*)$',info_str).group(1).strip()
                        temp_task.pagedata['ei_thread_num_views'] = int(re.search('viewed (\d+) times',info_str).group(1))
                    except:
                        log.info(self.log_msg('Cannot find the views count'))
                    try:
                        temp_task.pagedata['ei_thread_num_replies'] = int(stripHtml(thread_info[-2].renderContents()))
                    except:
                        log.info(self.log_msg('Cannot find the replies count'))
                    try:
                        temp_task.pagedata['edate_last_post_date']=  datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg('Cannot find the last posted'))
##                    try:
##                        temp_task.pagedata['ef_thread_rating']=  float(re.search\
##                                 ('(\d+) out of 5',thread.find('div','rating').\
##                                                        span['title']).group(1))
##                    except:
##                        log.info(self.log_msg('Thread rating not found'))
                    self.linksOut.append( temp_task )
                    log.info(self.log_msg('Task  added'))
                except:
                    log.info(self.log_msg('Cannot add the Task'))
            return True

    @logit(log, '__getData')
    def __getData(self, review, post_type ):
        """ This will return the page dictionry
        """
        page = {'title':''}
        try:
            author_tag = review.findAll('tr',recursive=False)[1]
            try:
                page['et_author_name'] = stripHtml(author_tag.find('font','row').b.renderContents())
            except:
                log.info(self.log_msg('Author name not found'))
            try:
                page['et_author_title'] = stripHtml(author_tag.find('font','sub-row').small.renderContents())
            except:
                log.info(self.log_msg('author title not found'))
            try:
                aut_stat = ['posts','since','from']
                for each in aut_stat:
                    value =  author_tag.find('b',text=each).next.__str__()[1:].strip()
                    if each=='posts':
                        page['ei_author_posts_count']= int(value)
                    if each=='from':
                        page['et_author_location']= value
                    if each=='since':
                        page['edate_author_member_since'] = datetime.strftime(datetime.strptime(value,'%b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception(self.log_msg('author data not found'))
        except:
            log.info(self.log_msg('author name not found'))
        try:
            page['ef_rating_overall'] = float(stripHtml(review.find('font','sub-rating').b.renderContents()))
        except:
            log.info(self.log_msg('Author info not found'))
        try:
            title_tag = review.find('font','subject')
            page['title'] = stripHtml(title_tag.b.renderContents())
            page['uri'] = self.baseurl +  title_tag.a['href']
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
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
            page['et_data_reply_to'] = self.thread_id
        except:
            log.info(self.log_msg('data reply to is not found'))
        try:
            date_str = stripHtml(review.find('font','sub-body').renderContents())
            page['posted_date'] =  datetime.strftime(datetime.strptime(date_str,'%m/%d/%Y %I:%M:%S %p'),"%Y-%m-%dT%H:%M:%SZ")
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
        try:
            data_tag = review.find('font','body')
            dir_tag = data_tag.find('dir')
            if dir_tag:
                dir_tag.extract()
            page['data'] = stripHtml(data_tag.renderContents())
        except:
            log.info(self.log_msg('data not found'))
            page['data'] =''
            
        return page

    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will get the parent info
        """
        page = {}
        try:
            self.hierarchy =  page['et_thread_hierarchy'] =  [stripHtml(x.renderContents()) for x in self.soup.find('font','nav-title').findAll('a')][1:]
            page['title']= page['et_thread_hierarchy'][-1]
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            page['title']=''
        try:
            self.thread_id =  page['et_thread_id'] = dict(parse_qsl(self.currenturi.split('?')[-1]))['tid']
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
            page['uri'] = self.currenturi
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