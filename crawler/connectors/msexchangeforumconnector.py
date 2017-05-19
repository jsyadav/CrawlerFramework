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

log = logging.getLogger('MsExchangeForumConnector')
class MsExchangeForumConnector(BaseConnector):
    '''
    This will fetch the info for msexchange forums
    Sample uris is 
    http://forums.msexchange.org/Message_Routing/forumid_18/tt.htm
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of http://forums.msexchange.org/Message_Routing/forumid_18/tt.htm
        """
        self.genre="Review"
        try:
            #self.currenturi ='http://forums.msexchange.org/Outlook_anywhere/m_1800490386/tm.htm'
            self.parent_uri = self.currenturi
            forum_id = self.currenturi.split('/')[-2]
            if forum_id.startswith('forumid'):
                self.total_posts_count = 0
                self.last_timestamp = datetime( 1980,1,1 )
                self.max_posts_count = int(tg.config.get(path='Connector',key='msexchange_forum_numresults'))
                self.currenturi = 'http://forums.msexchange.org/%s/p_1/tmode_1/smode_1/tt.htm'%forum_id
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                while True:
                    if not self.__getThreads():
                        break
                    try:
                        self.currenturi = 'http://forums.msexchange.org' + self.soup.find('a',text='next &gt;').findParent('a')['href']
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                if self.linksOut:
                    updateSessionInfo('Search', self.session_info_out,self.last_timestamp , None,'ForumThreadsPage', self.task.instance_data.get('update'))
                return True
            else:
                #self.currenturi = 'http://forums.msexchange.org/%s/p_1/tmode_2/smode_1/tt.htm'%forum_id
                #headers = {'Referer':self.task.pagedata['Referer']}
                #log.info(headers)
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                self.post_type= True
                while True:
                    self.__addPosts()
                    try:
                        self.currenturi = 'http://forums.msexchange.org' + self.soup.find('a',text='next &gt;').findParent('a')['href']
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
            reviews = [each.findParent('table') for each in self.soup.findAll('td','cat')]
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        for i, review in enumerate(reviews[1:]):
            post_type = "Question"
            if i==0 and self.post_type:
                post_type = "Question"
                self.post_type = False
            else:
                post_type = "Suggestion"
            page = self.__getData( review , post_type )
            try:
                review_hash = get_hash( page )
                unique_key = review.find('a')['name']
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
                #log.info(page)
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))

    @logit(log , '__getThreads')
    def __getThreads( self ):
            """ It will fetch the thread info and create tasks
            
            """
            try:
                threads=[each.findParent('tr') for each in self.soup.findAll('script',text=re.compile('^showArrow\(.*?\)'))]
                log.info([each.find('a',onclick=None)['href'] for each in threads])
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False
            for thread in threads:
                if  self.total_posts_count > self.max_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    thread_info = [stripHtml(each) for each in thread.findAll('td',text=True)if not stripHtml(each)=='']
                    if len(thread_info)==9 and  thread_info[1]=='Top:':
                        thread_info.remove('Top:')
                    if not len(thread_info)==8:
                        log.info(self.log_msg('not enough info from thread'))
                        continue
                    date_str = thread_info[6]
                    try:
                        thread_time = datetime.strptime(date_str,'%d.%b.%Y %I:%M:%S %p')
                    except:
                        try:
                            thread_time = datetime.strptime(date_str,'%d.%b%Y %I:%M:%S %p')
                        except:
                            log.info(self.log_msg('Cannot find date string'))
                            continue
                    if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        log.info(self.log_msg('Session info return True or Reaches max count'))
                        continue
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] = 'http://forums.msexchange.org' + thread.find('a',onclick=None)['href']
                    temp_task.pagedata['et_author_name'] =  thread_info[4]
                    temp_task.pagedata['title']= thread_info[2]
                    temp_task.pagedata['ei_thread_replies_count'] = int(thread_info[3])
                    temp_task.pagedata['ei_thread_views_count'] = int(thread_info[5])
                    temp_task.pagedata['et_last_post_author_name'] = thread_info[7]
                    #temp_task.pagedata['et_thread_parent_uri'] = self.currenturi
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
            page['et_author_name'] = stripHtml(review.find('a','subhead').string)
        except:
            log.info(self.log_msg('author name not found'))
        try:
            post_info = [each.strip() for each in  stripHtml(review.find('td','ultrasmall').renderContents()).split('\n')if not each.strip()=='']
            for each in post_info:
                if each.startswith('Posts:'):
                    page['ei_author_posts_count'] = int(each.split('Posts:')[-1].strip())
                if each.startswith('Joined:'):
                    date_str = each.split('Joined:')[-1].strip()
                    try:
                        page['edate_author_member_since'] = datetime.strftime(datetime.strptime(date_str,'%d.%b.%Y'),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        try:
                            page['edate_author_member_since'] = datetime.strftime(datetime.strptime(date_str,'%d.%b%Y'),"%Y-%m-%dT%H:%M:%SZ")
                        except:
                            log.info(self.log_msg('Cannot find date string'))
                if each.startswith('From:'):
                    page['et_author_location'] = each.split('From:')[-1].strip()
                if each.startswith('titleAndStar'):
                    try:
                        options = re.search('\((.*?)\)',each).group(1).split(',')
                        for i,value in enumerate(options):
                            if value=='null':
                                options[i]=None
                            if value == 'true':
                                options[i]=True
                            if value == 'false':
                                options[i]=False
                        title_and_star = self.__getTitleAndStar(int(options[0]),int(options[1]),options[2],options[3],options[4].replace('"',''),options[5].replace('"',''))
                        page['et_author_title'] = title_and_star['title']
                        page['ef_author_rating'] = float(title_and_star['star'])
                    except:
                        log.info(self.log_msg('title and star cannot found'))
        except:
            log.info(self.log_msg('Post info cannot be found'))
        try:
            date_str  =  review.find('td','cat').find('strong').span.string
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%d.%b.%Y %I:%M:%S %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Posted date not found for this post'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['et_data_reply_to'] = self.parent_uri.split('/')[-2]
        except:
            log.info(self.log_msg('data reply to is not found'))
        
        try:
            page['data'] = stripHtml(review.find('td','msg').renderContents()).replace('>> -->','').replace('<!-- <<','').strip()
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
            page['et_data_forum'] = self.hierarchy[1]
            page['et_data_subforum'] = self.hierarchy[2]
            page['et_data_topic'] = self.hierarchy[3]
        except:
            log.info(self.log_msg('data forum not found'))
        return page
    
    @logit(log,"__getTitleAndStart")
    def __getTitleAndStar(self,totalPosts, score, isMod, isAdmin, customTitle, customPic):
        """
        This will get the Title and star of a Author
        script sourec is http://forums.msexchange.org/js/TitleAndRating.js
        
        """
        try:
            userLevelNameMod = "Moderator"
            userLevelNameAdmin = "Administrator"
            title=None
            strTitle = [[15,5,"New Member",1],[30,15,"Starting Member",2],[45,25,"Junior Member",3],[60,35,"Senior Member",4],[90,45,"Super Member",5]]
            if score==None or score=="":
                score=0
            if isMod:
                title = userLevelNameMod
                star = 5
            if isAdmin:
        		title = userLevelNameAdmin;
        		star = 5
            if not title:
                for each in strTitle:
                    if totalPosts<=each[0] and score<=each[1]:
                        title = each[2]
                        star = each[3]
                        break
            if not title:
                title = "Super Member"
                star = 5
            if not customTitle=="":
                title = customTitle
                star = customPic[0]
            return {'title':title,"star":star}
        except:
            log.info(self.log_msg('title and star cannot be found'))
            return None
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
            self.hierarchy =page['et_thread_hierarchy'] = [each.strip() for each in stripHtml(self.soup.find('a',text='All Forums').findParent('td').renderContents()).split('>>')]
            page['title']= page['et_thread_hierarchy'][-1]            
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            page['title']=''
        for each in ['title','et_last_post_author_name','ei_thread_replies_count','ei_thread_views_count','edate_last_post_date']:
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