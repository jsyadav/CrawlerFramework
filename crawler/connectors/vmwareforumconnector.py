
'''
Copyright (c)2008-2010 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Packiaraj

import re
from datetime import datetime
import logging
from urllib2 import urlparse
from tgimport import tg
import copy

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('VmwareForumConnector')

class VmwareForumConnector(BaseConnector):
    '''
    This will fetch the info for smallbizserver.net forums
    Sample uris is
    http://www.vmwareforum.org/cgi-bin/yabb2/YaBB.pl?board=virtualcentre
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of sql server central
        """
        self.genre = "Review"
        try:
            self.parent_uri = self.currenturi
            self.total_posts_count = 0
            self.last_timestamp = datetime( 1980,1,1 )
            self.max_posts_count = int(tg.config.get(path='Connector',key='vmwareforum_maxthreads'))
            #self.max_posts_count = 50      # TODO: make the posts count read from the config file
            if '?board=' in self.currenturi:
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                next_page_no = 2
                while True:
                    if not self.__getThreadPage():
                        break
                    try:
                        self.currenturi = self.soup.find('td','catbg').find('a',text=str(next_page_no)).parent['href']
                        if not self.__setSoup():
                            break
                        next_page_no = next_page_no + 1
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                if self.linksOut:
                    updateSessionInfo('Search', self.session_info_out,
                                      self.last_timestamp , None,'ForumThreadsPage',
                                      self.task.instance_data.get('update'))
                return True
            else:
                self.question_post = True
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                next_page_no = 2
                while True:
                    if not self.__addPosts(self.currenturi):
                        break
                    try:
                        self.currenturi =  self.soup.find('td','catbg').find('a',text=str(next_page_no)).parent['href']
                        if not self.__setSoup():
                            break
                        next_page_no = next_page_no + 1
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log , '__addPosts')
    def __addPosts(self,parent_uri):
        """ It will add Post for a particular thread
        """
        try:
            posts = self.soup.find('form',attrs={'name':'multidel'}).findAll('div','displaycontainer')
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        for i, post in enumerate(posts):
            try:
                page = {}
                post_type = "Question"
                if i==0 and self.question_post:
                    post_type = "Question"
                    page['uri'] = parent_uri
                    self.question_type = False
                else:
                    post_type = "Suggestion"
                    page['uri'] = parent_uri + stripHtml(post.find('div','dividerbot').findAll('span','small')[-2].b.renderContents()).replace('Reply ','').replace('-','')
                if checkSessionInfo(self.genre, self.session_info_out,page['uri'],\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    log.info(self.log_msg('Session info returns True'))
                    continue
                page.update(self.__getData( post, post_type ))
            except:
                log.exception(self.log_msg('unique key not found'))
                continue
            try:
                result=updateSessionInfo(self.genre, self.session_info_out, page['uri'], \
                            get_hash( page ),'thread', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    continue
                page['path'] = page['parent_path'] = [ self.parent_uri ]
                page['path'].append( page['uri'] )
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
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append( page )
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))

        return True

    @logit(log , '__getThreadPage')
    def __getThreadPage( self ):
        """
        It will fetch each thread and its associate infomarmation
        and add the tasks
        """
        threads = self.soup.find('table',attrs={'class':'bordercolor','width':'100%','cellpadding':'3'}).findAll('tr',recursive=False)[1:]
        for thread in threads:
            self.total_posts_count = self.total_posts_count + 1
            try:
                thread_info = thread.findAll('td',recursive=False)
                date_str = re.sub("(\d+) (st|nd|rd|th)",r"\1",stripHtml(stripHtml(thread_info[6].find('a').renderContents())))
                thread_time = datetime.strptime(date_str,'%b %d , %Y at %I:%M%p')
            except:
                log.exception(self.log_msg('Date cannot found, continue with other posts'))
                continue
            if  self.total_posts_count > self.max_posts_count:
                log.info(self.log_msg('Reaching maximum post,Return false'))
                return False
            try:
                if checkSessionInfo('Search',self.session_info_out, thread_time,\
                                    self.task.instance_data.get('update')):
                    return False
                self.last_timestamp = max(thread_time , self.last_timestamp )
                temp_task = self.task.clone()
                try:
                    title_tag = thread_info[2].find('a')
                    temp_task.pagedata['title']= stripHtml(title_tag.renderContents())
                    temp_task.instance_data[ 'uri' ] = stripHtml(thread_info[2].find('a')['href'])
                except:
                    log.exception(self.log_msg('Cannot find the uri'))
                    continue
                try:
                    temp_task.pagedata['et_author_name'] = stripHtml(thread_info[3].renderContents())
                except:
                    log.info(self.log_msg('Cannot find the author name'))
                try:
                    temp_task.pagedata['et_thread_last_post_author'] = stripHtml(thread_info[6].find('a',rel='nofollow').renderContents())
                except:
                    log.info(self.log_msg('Cannot find the thread last post author'))
                try:
                    temp_task.pagedata['ei_thread_num_replies'] = stripHtml(thread_info[4].renderContents())
                except:
                    log.info(self.log_msg('Cannot find the replies count'))
                try:
                    temp_task.pagedata['ei_thread_num_views'] =stripHtml(thread_info[5].renderContents())
                except:
                    log.info(self.log_msg('Cannot find the views count'))
                try:
                    temp_task.pagedata['edate_last_post_date']=  datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.info(self.log_msg('Cannot find the last posted'))
             
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
            page['et_author_name'] = stripHtml(review.find('td',attrs = {'class' :re.compile('windowbg')}).find('b').renderContents())
        except:
            log.exception(self.log_msg('author name not found'))
        try:
            aut_info = [x.strip() for x in stripHtml(review.find('td',attrs = {'class' :re.compile('windowbg')}).find('div','small').renderContents()).split('\n') if not x.strip()=='']
            page['et_author_level']=aut_info[0]
            page['ei_author_rating']=len(review.find('td',attrs = {'class' :re.compile('windowbg')}).find('div','small').findAll('img',alt='*'))
            page['ei_author_status']=aut_info[1]
            page['et_author_description']=aut_info[2]
            page['ei_author_posts_count'] = int(re.sub('[^\d]+','',aut_info[3]))
        except:
            log.exception(self.log_msg('Author info not found'))
        try:
            page['title'] = stripHtml(review.find('span',style='float: left; width: 46%; vertical-align: middle;').find('b').renderContents())
        except:
            log.exception(self.log_msg('Title not found'))
        try:
            date_tag = review.find('span',style='float: left; width: 46%; vertical-align: middle;').find('span','small')
            date_str = re.sub("(\d+) (st|nd|rd|th)",r"\1",stripHtml(date_tag.renderContents().replace(date_tag.find('b').__str__(),'')))
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%b %d , %Y at %I:%M%p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Posted date not found for this post'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['et_data_post_type'] = post_type
        except:
            log.info(self.log_msg('Page info is missing'))
        try:
            page['et_data_forum'] = self.hierarchy[1]
            page['et_data_subforum'] = self.hierarchy[2]
            page['et_data_topic'] = self.hierarchy[3]
        except:
            log.exception(self.log_msg('data forum not found'))
        try: 
            page['data'] = stripHtml(review.find('div','message').renderContents())
        except:
            log.info(self.log_msg('data not found'))
            return False
        return page

    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will get the parent info
        """
        page = {}
        try:
            self.hierarchy = page['et_thread_hierarchy'] =  [x.strip() for x in stripHtml(self.soup.find('a','nav').findParent('tr').renderContents()).split(u'\u203a')]
            page['title'] = self.task.pagedata['title']
            date_tag = self.soup.find('form',attrs={'name':'multidel'}).findAll('div','displaycontainer').find('span',style='float: left; width: 46%; vertical-align: middle;').find('span','small')
            date_str = re.sub("(\d+) (st|nd|rd|th)",r"\1",stripHtml(date_tag.renderContents().replace(date_tag.find('b').__str__(),'')))
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%b %d , %Y at %I:%M%p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            return False
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
            result=updateSessionInfo( self.genre, self.session_info_out, self.\
                   parent_uri, post_hash,'thread',self.task.instance_data.get('update'))
            if not result['updated']:
                return False
            page['path']=[self.parent_uri]
            page['parent_path']=[]
            page['uri'] = self.currenturi
            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
            page['priority']=self.task.priority
            page['level']=self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            #page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
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
            #log.info(page)
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