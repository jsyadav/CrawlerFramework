
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
import copy
import logging
from cgi import parse_qsl
from urllib2 import urlparse
from datetime import datetime

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('SilverLightConnector')
class SilverLightConnector(BaseConnector):
    '''
    This will fetch the info for smallbizserver.net forums
    Sample uris is
    http://silverlight.net/forums/17.aspx
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
            self.max_posts_count = int(tg.config.get(path='Connector',key='silverlight_numresults'))
            self.hrefs_info = self.currenturi.split('/')
            if '/forums/t/' in self.currenturi:
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                self.__addPosts()
                return True
            else:
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                while True:
                    if not self.__getThreadPage():
                        break
                    try:
                        self.currenturi =  self.soup.find('a',text='Next >').parent['href']
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
            """for block_quote in re.findall('<BLOCKQUOTE>.*?</BLOCKQUOTE>',self.rawpage,re.S):
                self.rawpage = self.rawpage.replace(block_quote,'')
            self._setCurrentPage()
            #reviews = self.soup.findAll('div','thread')"""
            reviews = self.soup.findAll('div','wrapper_comment')
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        for i, review in enumerate(reviews):
            post_type = "Question"
            if i==0:
                post_type = "Question"
            else:
                post_type = "Suggestion"
            try:
                unique_key = dict(parse_qsl(review.find('div','commentbox_nav').find('a',text='Reply').parent['href'].split('?')[-1]))['ReplyToPostID']
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    log.info(self.log_msg('Session info returns True'))
                    continue
                page = self.__getData( review, post_type )
                log.info(page)
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
            threads = [x.findParent('tr') for x in self.soup.findAll('td','tbl_forum_thread')]
            if len(threads )==0:
                log.info(self.log_msg('No threads are found'))
                return False
            for thread in threads:
                self.total_posts_count = self.total_posts_count + 1
                try:
                    thread_info = thread.find('td','tbl_forum_thread').findAll('a')
                    date_str = stripHtml(thread_info[-1].next.next.__str__())
                    thread_time = datetime.strptime(date_str,'on %B %d, %Y')
                except:
                    log.exception(self.log_msg('Date cannot found, continue with other posts'))
                    continue
                if  self.total_posts_count > self.max_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                try:
                    if checkSessionInfo('Search',self.session_info_out, thread_time,\
                                        self.task.instance_data.get('update')):
                        continue
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    try:
                        title_tag = thread.find('h2')
                        temp_task.pagedata['title']= stripHtml(title_tag.renderContents())
                        temp_task.instance_data[ 'uri' ] = 'http://silverlight.net' + title_tag.find('a')['href']
                        temp_task.pagedata['et_author_name'] = stripHtml(thread.find('p').find('a').renderContents())
                        temp_task.pagedata['et_thread_last_post_author']  =  stripHtml(thread_info[-1].renderContents())
                        view_reply = {'ei_thread_num_replies':'tbl_forum_views','ei_thread_num_views':'tbl_forum_replies'}
                        for each in view_reply.keys():
                            temp_task.pagedata[each] = int(re.sub('[^\d]','',stripHtml(thread.find('td',view_reply[each]).renderContents())))
                        temp_task.pagedata['edate_last_post_date']=  datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.exception(self.log_msg('Cannot find the uri'))
                        continue
                    try:
                        temp_task.pagedata['ef_thread_rating']=  float(re.search('\[([^ ]+)',thread.find('span','ForumThreadRateControl star_ratings')['title']).group(1))
                    except:
                        log.info(self.log_msg('Thread rating not found'))
                    self.linksOut.append( temp_task )
                    log.info(self.log_msg('Task  added'))
                except:
                    log.info(self.log_msg('Cannot add the Task'))
            return True
        
    @logit(log, '__getData')
    def __getData(self,review,post_type):
        """ This will return the page dictionry
        """
        page = {'title':''}
        try:
            page['et_author_name'] = stripHtml(review.find('p','post_title').find('a').renderContents()).replace('> ','').replace('...','')
        except:
            log.info(self.log_msg('author name not found'))
        try:
            aut_info = {'ei_author_points_count':'crp_points','ei_author_posts_count':'forum_posts_count'}
            for each in aut_info.keys():
                page[each] = int(re.search('\d+',stripHtml(review.find('p',aut_info[each]).renderContents())).group())
        except:
            log.info(self.log_msg('Author posts count not found'))
        try:
            page['et_author_membership'] = stripHtml(review.find('p','crp_level').renderContents())
        except:
            log.info(self.log_msg('Author member ship not found'))
        try:
            post_tag = review.find('div','commentbox_mid')
            page['title'] = stripHtml(post_tag.find('h2','post_title').renderContents())
            date_str = stripHtml(post_tag.find('p','post_date').renderContents()).split('|')[0].strip()
            page['posted_date'] =  datetime.strftime(datetime.strptime(date_str,'%m-%d-%Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
            remove_tags = {'div':['commentbox_nav','commentbox_sig'],'h2':['post_title'],'p':['post_date']}
            for each_key in remove_tags.keys():
                for each in remove_tags[each_key]:
                    tag = post_tag.find(each_key,each)
                    if tag:
                        tag.extract()
            tags = review.findAll('blockquote')
            for each in tags:
                each.extract()
            page['data'] = stripHtml(post_tag.renderContents())
        except:
            log.exception(self.log_msg('title not found'))
            return False
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
            page['et_data_post_type'] = post_type
        except:
            log.info(self.log_msg('Page info is missing'))
        try:
            page['et_data_forum'] = self.hierarchy[0]
            page['et_data_subforum'] = self.hierarchy[1]
            page['et_data_topic'] = self.hierarchy[2]
        except:
            log.exception(self.log_msg('data forum not found'))
##        try:
##            data_str = review.find('div','threadText')
##
##            data_tag = review.find('div','threadDetails')
##            [x.findParent('div') for x in data_tag.findAll('blockquote')]
##            for each in ['threadSubject','threadLinks']:
##                tag = data_tag.find('div',each)
##                if tag:
##                    tag.extract()
##            page['data'] = stripHtml(data_tag.renderContents()).replace('______________________________________________________\nPlease mark replies as answers if they answered your question...','').strip()
##        except:
##            log.info(self.log_msg('data not found'))
##            page['data'] =''
        return page

    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will get the parent info
        """
        page = {}
        try:
            self.hierarchy =  page['et_thread_hierarchy'] =  [stripHtml(x.renderContents()) for  x in self.soup.find('p','breadcrumb').findAll('a')][2:]
            page['title']= page['et_thread_hierarchy'][-1]
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            page['title']=''
        try:
            self.thread_id =  page['et_thread_id'] = self.currenturi.split('/')[-1].replace('.aspx','')
        except:
            log.info(self.log_msg('Thread id not found'))
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False

        for each in ['et_author_name','ei_thread_num_replies','ei_thread_num_views','edate_last_post_date','ef_thread_rating']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('page data cannot be extracted for %s'%each))
        try:
            date_str = stripHtml(self.soup.find('div','wrapper_comment').find('p','post_date').renderContents()).split('|')[0].strip()
            page['posted_date'] =  datetime.strftime(datetime.strptime(date_str,'%m-%d-%Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Posted date not found'))
        try:
            post_hash = get_hash( page )
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result = updateSessionInfo( self.genre, self.session_info_out, self.\
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