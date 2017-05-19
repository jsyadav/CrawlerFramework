
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

log = logging.getLogger('GetSatisfactionConnector')
class GetSatisfactionConnector(BaseConnector):
    '''
    This will fetch the info for Get Satisfication
    Sample uris is
    http://getsatisfaction.com/microsoft/products/microsoft_internet_explorer_8
    http://getsatisfaction.com/dell/topics/_ac_power_adapter_not_determined_and_power_adapter_wont_work
    http://getsatisfaction.com/dell/topics/dell_xps_m1330_problems_with_graphics_video_driver
    http://getsatisfaction.com/microsoft/topics/database_problem_with_office_2008_for_mac
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of Get satisfication
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            self.total_posts_count = 0
            self.last_timestamp = datetime( 1980,1,1 )
            self.max_posts_count = int(tg.config.get(path='Connector',key='get_statisfication_max_threads'))
            self.hrefs_info = self.currenturi.split('/')
            if 'topics' in self.hrefs_info:
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                self.post_type = True
                self.__addPosts()
                return True
            else:
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                next_page_no = 2
                while True:
                    if not self.__getThreadPage():
                        break
                    try:
                        self.currenturi = 'http://getsatisfaction.com' +  self.soup.find('a',text='next &raquo;').parent['href']
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
##        try:
##            unique_key = review_div['id']
##            if not checkSessionInfo(self.genre, self.session_info_out, unique_key,\
##                             self.task.instance_data.get('update'),parent_list\
##                                                            =[self.parent_uri]):
##                log.info(self.log_msg('Session info returns True'))
##                page =self.__getData(review_div,self.task.pagedata.get('et_thread_type','Question'))
##                self.__addPage(page)
##        except:
##            log.info(self.log_msg('Posts cannot be added'))
        final_reviews = []
        final_reviews.append([self.soup.find('div','topic')])
        try:
            reviews = []
            for each in self.soup.findAll('ul','replies'):
                reviews.extend(each.findAll('li',recursive=False))
            each_review = [ reviews[0] ]
            for review in reviews:
                if 'reply' in review.get('class'):
                    final_reviews.append(each_review)
                    each_review = [review]
                else:
                    each_review.append(review)
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        for i, review in enumerate(final_reviews):
            try:
                unique_key = review[0].get('id')
                if not checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    if i==0 and self.post_type:
                        post_type =  self.task.pagedata.get('et_thread_type','Question')
                        self.post_type = False
                    else:
                        post_type = "Suggestion"
                    page = self.__getData( review[0], post_type )
                    result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            get_hash( page ),'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                    if result['updated']:
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
                if len(review)>1 :
                    log.info(self.log_msg('Comment found and needs to be picked'))
                    if self.task.instance_data.get('pick_comments'):
                        log.info(self.log_msg('comments needst to added'))
                        for each in review[1:]:
                            comment_unique_key = each['id']
                            if not checkSessionInfo(self.genre, self.session_info_out, comment_unique_key,\
                                                    self.task.instance_data.get('update'),parent_list\
                                                                        =[self.parent_uri,unique_key]):
                                page = self.__getData( each,'Comment')
                                result=updateSessionInfo(self.genre, self.session_info_out, comment_unique_key, \
                                        get_hash( page ),'Comment', self.task.instance_data.get('update'),\
                                                                    parent_list=[self.parent_uri,unique_key])
                                if result['updated']:
                                    parent_list = [ self.parent_uri,unique_key ]
                                    page['parent_path'] = copy.copy(parent_list)
                                    parent_list.append( comment_unique_key )
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
                                    page['entity'] = 'Comment'
                                    page['category'] = self.task.instance_data.get('category','')
                                    page['task_log_id']=self.task.id
                                    page['uri'] = self.currenturi
                                    page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                                    self.pages.append( page )
                                    log.info(self.log_msg('Comment Added'))

            except:
                log.exception(self.log_msg('unique key not found'))
                continue
    @logit(log , '__getThreadPage')
    def __getThreadPage( self ):
            """
            It will fetch each thread and its associate infomarmation
            and add the tasks
            """
            try:
                threads = self.soup.find('ul','topic_list').findAll('li',recursive=False)
            except:
                log.exception(self.log_msg('No Threads are found'))
            for thread in threads:
                try:
                    date_str = stripHtml(thread.find('span',id=re.compile('stamp'))['title'])
                    thread_time =  datetime.strptime(date_str,'%B %d, %Y %H:%M')
                    self.last_timestamp = max(thread_time , self.last_timestamp)
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
                        title_tag = thread.find('a','subject')
                        temp_task.pagedata['title']= stripHtml(title_tag.renderContents())
                        temp_task.instance_data[ 'uri' ] =  'http://getsatisfaction.com' + title_tag['href']
                    except:
                        log.info(self.log_msg('Cannot find the uri'))
                        continue
                    try:
                        temp_task.pagedata['et_author_name'] = stripHtml(thread.find('a','creator_name').renderContents())
                    except:
                        log.info(self.log_msg('Cannot find the title'))
                    try: 
                        temp_task.pagedata['et_thread_last_post_author']  = stripHtml(thread.find('div','meta').findAll('a')[-1].renderContents())
                    except:
                        log.info(self.log_msg('Cannot find Last post author name'))
                    try:
                        temp_task.pagedata['et_thread_type']  = {'answer':'Question','solution':'Problem'}[stripHtml(thread.find('a','topic_status_action').renderContents()).replace('Needs ','')]
                        
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
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            if not post_type=='Suggestion':
                author_info = self.soup.find('div',id='topic_creator_details')
                date_str =  stripHtml(author_info.find('p','topic_created').span['title'])
                page['et_author_profile'] = 'http://getsatisfaction.com' + author_info.find('a','mini_profile_icon')['href']
                page['et_author_name'] = stripHtml(author_info.find('p','topic_created').a.renderContents())
            else:
                author_info = review.find('div','metadata')
                date_str =  author_info.find('span',id=re.compile('^stamp\-'))['title']
                page['et_author_profile'] = author_info.find('a',attrs={'class':re.compile('(creator_name|user_name)')})['href']
                page['et_author_name'] = stripHtml(author_info.find('a','creator_name ').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%B %d, %Y %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(review)
            log.exception(self.log_msg('Author info not found'))
        try:
            if not post_type=='Suggestion':
                page['title'] = stripHtml(self.soup.find('h1').renderContents())
            else:
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
        except:
            log.exception(self.log_msg('No Title found'))
            page['title'] = ''
        try:
            page['data'] = stripHtml(review.find('div','content').renderContents())
        except:
            log.info(review)
            log.info(self.log_msg('data not found'))
            page['data'] =''
        try:
            page['et_author_emotion'] = stripHtml(review.find('div','emotion').renderContents())
        except:
            log.info(self.log_msg('emotion is not found'))
        try:
            if not post_type=='Suggestion':
                page['ei_authors_similar_count'] = int(re.search('\d+',stripHtml(review.find('div',id='me_too_count').renderContents())).group())
        except:
            log.info(self.log_msg('authors_having_similar_problem is not found'))
        try:
            pass
        except:
            log.info(self.log_msg('no author having this porblem'))
        try:
            page['et_data_post_type'] = post_type
        except:
            log.info(self.log_msg('Page info is missing'))
        return page

    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will get the parent info
        """
        page = {}
        try:
            #self.hierarchy =  page['et_thread_hierarchy'] = [stripHtml(x.renderContents()) for x in self.soup.find('table','BreadCrumb_InnerTableCSS').findAll('a')][1:]
            page['title']= stripHtml(self.soup.find('h1').renderContents())
        except:
            log.info(self.log_msg('Thread Title is not found'))
            page['title']=''
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False

        for each in ['et_author_name','et_thread_last_post_author','et_thread_type','edate_last_post_date']:
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