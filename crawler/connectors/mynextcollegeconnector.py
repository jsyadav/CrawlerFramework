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

log = logging.getLogger('MyNextCollegeConnector')
class MyNextCollegeConnector(BaseConnector):
    '''

    Sample uris is
    http://www.mynextcollege.com/college-reviews/discussion-room-f6.html
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of polish forums
        sample uri :  http://www.mynextcollege.com/college-reviews/discussion-room-f6.html
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            self.currenturi = self.currenturi.split('-sid=')[0]
            if self.currenturi=='http://www.mynextcollege.com/college-reviews/':
                try:
                    if not self.__setSoup():
                        return False
                    self.__addFortumLinks()
                except:
                    log.info(self.log_msg('cannot add tasks'))
                    return False
            if re.match('.*?\-f\d+\.html$', self.currenturi):
                self.total_posts_count = 0
                self.last_timestamp = datetime( 1980,1,1 )
                self.max_posts_count = int(tg.config.get(path='Connector',key='mynextcollege_numresults'))
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                while True:
                    if not self.__getThreads():
                        break
                    try:
                        self.currenturi = 'http://www.mynextcollege.com/college-reviews' + self.soup.find('a',text='Next').parent['href'][1:].split('-sid=')[0]
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
                        self.currenturi = 'http://www.mynextcollege.com/college-reviews' + self.soup.find('a',text='Next').parent['href'][1:].split('-sid=')[0]
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next page not set'))
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
            reviews = self.soup.findAll('div',attrs={'class':re.compile('post bg[12]')})
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
                unique_key = review['id']
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
                threads = [x.findParent('li') for x in self.soup.findAll('a','topictitle')]
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False
            for thread in threads:
                if  self.total_posts_count > self.max_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    last_post_details = thread.find('dd','lastpost')
                    date_str = stripHtml(last_post_details.renderContents()).split('\n')[-1].strip()
                    try:
                        thread_time =   datetime.strptime(date_str,'%a %b %d, %Y %I:%M %p')
                    except:
                        log.info(self.log_msg('Last post date not found'))
                        continue
                    if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        log.info(self.log_msg('Session info return True or Reaches max count'))
                        continue
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    title_tag = thread.find('a','topictitle')
                    temp_task.instance_data[ 'uri' ] = 'http://www.mynextcollege.com/college-reviews' + title_tag['href'][1:].split('-sid=')[0].strip()
                    log.info( temp_task.instance_data[ 'uri' ])
                    temp_task.pagedata['et_author_name'] =  stripHtml(title_tag.parent.findAll('a')[-1].renderContents())
                    temp_task.pagedata['title']= stripHtml(title_tag.renderContents())
                    temp_task.pagedata['ei_thread_replies_count'] = int(re.search('^\d+',stripHtml(thread.find('dd','posts').renderContents())).group())
                    temp_task.pagedata['ei_thread_views_count'] = int(re.search('^\d+',stripHtml(thread.find('dd','views').renderContents())).group())
                    date_str =stripHtml(title_tag.parent.renderContents().split('&raquo;')[-1])
                    temp_task.pagedata['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%a %b %d, %Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
                    temp_task.pagedata['et_last_post_author_name'] = stripHtml(last_post_details.find('a').renderContents())
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
            
            aut_tag = review.find('p','author').find('strong')
            page['et_author_name'] = stripHtml(aut_tag.renderContents())
            date_str = stripHtml(aut_tag.nextSibling.__str__())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%a %b %d, %Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('author info not found, may be a Guest'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['title'] = stripHtml(review.find('h3').renderContents())
        except:
            log.info(self.log_msg('title not found'))
            page['title'] = ''
        try:
            aut_info = [x.strip() for x in [stripHtml(x.renderContents()) for x in review.find('dl','postprofile').findAll('dd')] if not x.strip()=='']
            page['et_author_membership'] = aut_info[0]
            for each in aut_info[1:]:
                if each.startswith('Posts:'):
                    page['ei_author_posts_count']= int(each.split('Posts:')[-1].strip())
                if each.startswith('Joined:'):
                    date_str = each.split('Joined:')[-1].strip()
                    page['edate_author_member_since']=  datetime.strftime(datetime.strptime(date_str,'%a %b %d, %Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
                if each.startswith('Location:'):
                    page['et_author_location']= each.split('Location:')[-1].strip()
        except:
            log.info(self.log_msg('Author info not found'))
        try:
            page['data'] = stripHtml(review.find('div','content').renderContents())
        except:
            log.info(self.log_msg('Data not found'))
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
            page['et_data_post_type'] = post_type
        except:
            log.info(self.log_msg('Page info is missing'))
        try:
            page['et_data_forum'] = self.hierarchy[1]
            page['et_data_subforum'] = self.hierarchy[2]
            page['et_data_topic'] = self.forum_title
        except:
            log.info(self.log_msg('data forum not found'))
        return page
    

        return page
    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will get the parent info
        thread url.split('&')[-1].split('=')[-1]

        """
        page = {}
        try:
            self.hierarchy =page['et_thread_hierarchy'] = [stripHtml(x.renderContents()) for x in self.soup.find('li','icon-home').findAll('a')]
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
        try:
            self.forum_title = page['title']= stripHtml(self.soup.find('div',id='page-body').find('h2').renderContents())
        except:
            log.info(self.log_msg('title not found'))
            page['title'] = ''
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        log.info(self.task.pagedata)
        log.info( self.task.instance_data[ 'uri' ])
        for each in ['et_last_post_author_name','ei_thread_replies_count','edate_last_post_date','et_author_name','posted_date','ei_thread_views_count']:
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

    @logit(log,'__addForumLinks')
    def __addFortumLinks(self):
        '''This will add the list of forums to Tasks
        '''
        try:
            list_of_universities = ['http://www.mynextcollege.com/college-reviews'+x['href'][1:].split('-sid=')[0] for x in self.soup.find('a',text='The Schools').findParent('div').find('ul','topiclist forums').findAll('a','forumtitle')]
            for university in list_of_universities:
                try:
                    self.currenturi = university
                    if not self.__setSoup():
                        continue
                    forum_urls = ['http://www.mynextcollege.com/college-reviews'+x['href'][1:].split('-sid=')[0] for x in self.soup.findAll('a','forumtitle')]
                    for forum_url in forum_urls:
                        temp_task=self.task.clone()
                        temp_task.instance_data[ 'uri' ] = normalize( forum_url )
                        self.linksOut.append( temp_task )
                except:
                    log.info(self.log_msg('Cannot add forum urls'))
        except:
            log.info(self.log_msg('Cannot find the list of universities'))

                    
                    