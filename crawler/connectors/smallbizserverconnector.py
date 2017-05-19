
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

log = logging.getLogger('SmallBizServerConnector')
class SmallBizServerConnector(BaseConnector):
    '''
    This will fetch the info for smallbizserver.net forums
    Sample uris is 
    http://www.smallbizserver.net/Forums/tabid/53/view/topics/forumid/103/Default.aspx
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
            self.last_timestamp = datetime( 1980,1,1 )
            self.max_posts_count = int(tg.config.get(path='Connector',key='smallbizserver_numresults'))
            self.hrefs_info = self.currenturi.split('/')
            if 'postid' in self.hrefs_info:
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                self.__addPosts()
                return True
            else:
                self.currenturi = self.currenturi.replace('Default.aspx','afcol/0/afsort/DESC/Default.aspx')
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                while self.max_posts_count > self.total_posts_count:
                    self.__getThreadPage()
                    try:
                        self.currenturi = self.soup.find('a',text=' >').parent['href']
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
            reviews = [each.parent for each in self.soup.findAll('td',{'class':re.compile('^afpostinfo[12]')})]               
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        for i, review in enumerate(reviews):
            post_type = "Question"
            if i==0:
                post_type = "Question"
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
            It will fetch each thread and its associate infomarmation
            threads=[each.parent for each in soup.findAll('td','AFTopicRow1')]
            for thread in threads:
                thread_title = stripHtml(thread.find('td','afsubject').renderContents())
                thread_title = re.sub('^New article:','',thread_title).strip()
                thread_info = thread.findAll('td','AFTopicRow1')
                if len(thread_info) == 5:
                    thread_owner =  stripHtml(thread_info[1].renderContents())
                    thread_no_replies =  int(stripHtml(thread_info[2].renderContents()))
                    thread_no_of_views = int(stripHtml(thread_info[3].renderContents()))
                    date_str = stripHtml(thre   ad_info[4].renderContents())
                    posted_date = datetime.strftime(datetime.strptime(date_str,'%m/%d/%Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
            """
            threads = [each.parent for each in self.soup.findAll('td','afcol1')]
            for thread in threads:
                page = {}
                thread_info = thread.findAll('td','AFTopicRow1')
                if not len(thread_info) == 5:
                    continue
                try:
                    page['title'] =  stripHtml(thread_info[0].renderContents())
                except:
                    log.info(self.log_msg('Title not found'))
                try:
                    page['et_author_name'] =  stripHtml(thread_info[1].renderContents())
                except:
                    log.info(self.log_msg('Thread author name not found'))
                try:    
                    page['ei_thread_num_replies'] = int(stripHtml(thread_info[2].renderContents()))
                except:
                    log.info(self.log_msg('No of Replies not found'))
                try:                    
                    page['ei_thread_num_views'] = int(stripHtml(thread_info[3].renderContents()))
                except:
                    log.info(self.log_msg('no of views not found'))
                try:
                    date_str = stripHtml(thread_info[4].renderContents()).split('\n')[-1].strip()
                    thread_time = datetime.strptime(date_str,'%m/%d/%Y %I:%M %p')
                    page['edate_last_post_date'] = datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                except:
                    log.exception( self.log_msg('Posted date not found') )
                    continue
                self.total_posts_count = self.total_posts_count + 1
                try:
                    if not checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        
                        temp_task=self.task.clone()
                        temp_task.instance_data[ 'uri' ] = normalize( thread_info[0].find('a')['href'] )
                        temp_task.pagedata['title']= page['title']
                        temp_task.pagedata['et_author_name'] = page['et_author_name']
                        temp_task.pagedata['ei_thread_num_replies'] = page['ei_thread_num_replies']  
                        temp_task.pagedata['ei_thread_num_views'] = page['ei_thread_num_views']  
                        temp_task.pagedata['edate_last_post_date']=  page['edate_last_post_date']
                        self.linksOut.append( temp_task )
                except:
                    log.exception(self.log_msg('Task cannot be created'))
            
    @logit(log, '__getData')
    def __getData(self, review_tag, post_type ):
        """ This will return the page dictionry
        for a post
        reviews = a=[each.parent for each in soup.findAll('td',{'class':re.compile('^afpostinfo[12]')})]
        """ 
        page = {'title':''}
        try:
            page['et_author_name'] = stripHtml(review_tag.find('td',{'class':re.compile('afpostinfo[12]')}).next.__str__())
            info_str = stripHtml(review_tag.find('td',{'class':re.compile('afpostinfo[12]')}).renderContents()).replace(page['et_author_name'],'').strip()
            if info_str.find('Member since')>0:
                page['et_author_location'] = info_str.split('Member since')[0].strip()
                info_str = info_str.replace(page['et_author_location'],'').strip()
                info_str = info_str.replace('Member since','').strip()
                date_str = re.search('\d+/\d+/\d+',info_str).group()
                page['edate_author_member_since'] = datetime.strftime(datetime.strptime(date_str,'%m/%d/%Y'),"%Y-%m-%dT%H:%M:%SZ") 
                info_str = info_str.replace(date_str,'').strip()
                posts_count_and_membership = info_str.split('Posts:')
                if len(posts_count_and_membership) == 2:
                    page['et_author_membership'] = posts_count_and_membership[0].strip()
                    page['ei_author_posts_count'] = int( posts_count_and_membership[1].strip())
        except:
            log.info(self.log_msg('Author info Not found'))
        try:
            page['et_data_reply_to'] = self.hrefs_info[self.hrefs_info.index('postid') + 1 ]
        except:
            log.info(self.log_msg('data reply to is not found'))
        try:
            date_str = stripHtml(review_tag.find('td',{'class':re.compile('afpostreply[12]')}).find('td','afsubrow').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%m/%d/%Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Posted date not found for this post'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['data'] =  stripHtml(review_tag.find('td','afpostbody').renderContents())
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
            hierarchy = [each.strip() for each in stripHtml(self.soup.find('div','afcrumb').renderContents()).split('>')]
            page['et_data_forum'] = hierarchy[1]
            page['et_data_subforum'] = hierarchy[2]
        except:
            log.info(self.log_msg('data forum not found'))
        try:
            page['et_data_topic'] =  re.sub('^Subject:','',stripHtml(self.soup.find('div','afsubjectheader').renderContents())).strip()
        except:
            log.info(self.log_msg('Data topic cannot be found'))
        return page

    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will get the parent info
        """
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        page = {}
        try:
            page['et_thread_hierarchy'] = [each.strip() for each in stripHtml(self.soup.find('div','afcrumb').renderContents()).split('>')]
            page['title']= page['et_thread_hierarchy'][-1]            
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            page['title']=''
        for each in ['posted_date','title','et_author_name','ei_thread_num_replies','ei_thread_num_views','edate_last_post_date']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('page data cannot be extracted for %s'%each))
        try:
            page['et_thread_id'] = self.hrefs_info[self.hrefs_info.index('postid') + 1 ]
        except:
            log.info(self.log_msg('Thread id not found'))
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