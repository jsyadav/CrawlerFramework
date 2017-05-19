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

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('SqlCommunityConnector')
class SqlCommunityConnector(BaseConnector):
    '''
    This will fetch the info for SqlCommunity  forums
    Sample uris is
    http://sqlcommunity.com/SQLServerForums/tabid/54/forumid/4/scope/threads/Default.aspx
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of Smallbizserver.net
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            self.total_posts_count = 0
            self.post_type=True
            self.last_timestamp = datetime( 1980,1,1 )
            self.max_posts_count = int(tg.config.get(path='Connector',key='sqlcommunity_numresults'))
            if not self.currenturi.endswith('/threads/Default.aspx'):
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                while True:
                    try:
                        self.__addPosts()
                        self.currenturi = self.soup.find('td','Forum_Footer').find('a',text='Next').parent['href']
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
                        self.currenturi = self.soup.find('td','Forum_Footer').find('a',text='Next').parent['href']
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
            reviews = self.soup.findAll('td',attrs={'class':re.compile('Forum_PostBody_Container')})
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        post_type = "Suggestion"
        for i, review in enumerate(reviews):
            if i==0 and self.post_type:
                post_type = "Question"
                self.post_type = False
            else:
                post_type = "Suggestion"
            page = self.__getData( review, post_type )
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
                #page['id'] = result['id']
                #page['first_version_id']=result['first_version_id']
                #page['parent_id']= '-'.join(result['id'].split('-')[:-1])
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
            It finds the thread and add tasks which ever comes after the previous
            crawl
            """
            threads = [x.findParent('tr') for x in self.soup.findAll('td','Forum_Row')]
            threads.extend([x.findParent('tr') for x in self.soup.findAll('td','Forum_Row_Alt')])
            for thread in threads:
                try:
                    self.total_posts_count = self.total_posts_count + 1
                    if  self.total_posts_count >= self.max_posts_count:
                        log.info(self.log_msg('Reaching maximum post,Return false'))
                        return False
                    thread_info = thread.findAll('td',attrs={'class':re.compile('Forum_Row')})
                    if not len(thread_info) == 4:
                        continue
                    try:
                        last_post_info = stripHtml(thread_info[3].renderContents()).split('\n')
                        date_str = re.sub('<?.*>','',stripHtml(thread.find('span','Forum_LastPostText').previous.previous))
                        thread_time = datetime.strptime(date_str,'%m/%d/%Y %I:%M %p')
                        #page['edate_last_post_date'] = datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                        if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                            continue
                        self.last_timestamp = max(thread_time , self.last_timestamp )
                        temp_task = self.task.clone()
                    except:
                        log.exception( self.log_msg('Posted date not found') )
                        continue
                    try:
                        title_tag = thread_info[0].find('a',attrs={'class': re.compile('Forum') })
                        temp_task.instance_data[ 'uri' ] = normalize( title_tag['href'] )
                        temp_task.pagedata['title']= stripHtml(title_tag.renderContents())
                        temp_task.pagedata['et_author_name'] = re.sub('^by','',stripHtml(thread_info[0].renderContents()).split('\n')[-1].strip()).strip()
                    except:
                        log.info(self.log_msg('Title not found'))
                    try:
                        view_reply ={'replies':1,'views':2}
                        for each in view_reply.keys():
                            temp_task.pagedata['ei_thread_' + each + '_count'] = int(stripHtml(thread_info[view_reply[each]].renderContents()))
                    except:
                        log.info(self.log_msg('No of Replies not found'))
                    try:
                        temp_task.pagedata['edate_last_post_date'] = datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                        temp_task.pagedata['et_thread_last_post_author_name'] = re.sub('^by','',last_post_info[-1].strip()).strip()
                    except:
                        log.info(self.log_msg('Last post author name not found'))
                    self.linksOut.append( temp_task )
                    log.info(self.log_msg('Task added'))
                except:
                    log.info(self.log_msg('Cannot add the task'))
            return True

    @logit(log, '__getData')
    def __getData(self, review, post_type ):
        """ This will return the page dictionry
        for a post
        reviews = a=[each.parent for each in soup.findAll('td',{'class':re.compile('^afpostinfo[12]')})]
        """
        page = {'title':''}
        try:
            aut_tag = review.findPrevious('a','Forum_Profile')
            page['et_author_name'] = stripHtml(aut_tag.renderContents())
            page['et_author_profile'] = aut_tag['href']
        except:
            log.exception(self.log_msg('Author info Not found'))
        try:
            page['ei_author_posts_count'] = int(re.search('\d+',stripHtml(review.findPrevious('span',id='spAuthorPostCount').renderContents())).group())
        except:
            log.exception(self.log_msg('Authro info not found'))
        try:
            date_str = stripHtml(review.findPrevious('span','Forum_HeaderText').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%m/%d/%Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('Posted date not found for this post'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['data'] =  stripHtml(review.find('span',id='spBody').renderContents())
        except:
            page['data'] = ''
            log.exception(self.log_msg('Data not found for this post'))
        try:
            page['title'] =  stripHtml(review.find('span',id='spSubject').renderContents())
        except:
            log.info(self.log_msg('Title not found'))
            page['title'] =''
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
            page['et_data_reply_to'] = self.thread_id
        except:
            log.info(self.log_msg('data reply to is not found'))
        return page

    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will get the parent info
        """
        page={}
        try:
            page['et_thread_hierarchy'] = [stripHtml(x.renderContents()) for  x in self.soup.find('tr',id='NavigationToolbar').findNextSiblings('tr')[1].findAll('a')]
            page['et_thread_hierarchy'].remove('Discussions')
            self.hierarchy = page['et_thread_hierarchy']
            page['title'] = page['et_thread_hierarchy'][-1]
        except:
            log.info(self.log_msg('Thread hirarchy not found'))
        for each in ['posted_date','title','et_author_name','ei_thread_num_replies','ei_thread_num_views','edate_last_post_date']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('page data cannot be extracted for %s'%each))
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
##        try:
##            page['et_thread_hierarchy'] = [each.strip() for each in stripHtml(self.soup.find('div','afcrumb').renderContents()).split('>')]
##            page['title']= page['et_thread_hierarchy'][-1]
##        except:
##            log.info(self.log_msg('Thread hierarchy is not found'))
##            page['title']=''
        try:
            self.thread_id = page['et_thread_id'] = re.search('/threadid/(.*?)/', self.currenturi ).group(1)
        except:
            log.info(self.log_msg('Thread id not found'))
        try:
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo( self.genre, self.session_info_out, self.\
                   parent_uri, get_hash( page ),'Forum',self.task.instance_data.get('update'), Id=id)
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