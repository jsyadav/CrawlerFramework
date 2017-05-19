
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
import copy
from urllib2 import urlparse
from tgimport import tg

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('EpistemeArstechnicaConnector')
class EpistemeArstechnicaConnector(BaseConnector):
    '''
    This will fetch the info for http://episteme.arstechnica.com/
    Sample uris is 
    http://episteme.arstechnica.com/eve/forums/a/frm/f/99609816
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        http://episteme.arstechnica.com/eve/forums/a/frm/f/99609816
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            next_page_no = 1
            if self.currenturi.startswith('http://episteme.arstechnica.com/eve/forums/a/tpc'):
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                self.post_type= True
                while True:
                    self.__addPosts()
                    try:
                        next_page_no = next_page_no + 1
                        self.currenturi = self.soup.find('a',text=str(next_page_no)).parent['href']
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                return True
            elif self.currenturi.startswith('http://episteme.arstechnica.com/eve/forums/a/frm'):
                # needs to be sorted, but no way found to sort
                self.total_posts_count = 0
                self.last_timestamp = datetime( 1980,1,1 )
                self.max_posts_count = int(tg.config.get(path='Connector',key='epistemearstechnica_numresults'))
                if not self.__setSoup():
                    log.info( self.log_msg ( 'Soup not set , Returning False from Fetch') )
                    return False
                while True:
                    if not self.__getThreads():
                        break
                    try:
                        next_page_no = next_page_no + 1
                        self.currenturi = self.soup.find('a',text=str(next_page_no)).parent['href']
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
            reviews =  self.soup.findAll('table',id=re.compile('^post_.*'))               
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        for i, review in enumerate(reviews):
            post_type = "Question"
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
            """
            It will fetch each thread and its associate infomarmation
            threads = [each.parent for each in soup.findAll('td','ev_ubbx_frm_title')]
            for thread in threads:
                find title, uri, posted date no of replies and views count
            """
            try:
                threads = [each.parent for each in self.soup.findAll('td','ev_ubbx_frm_title')]
            except:
                log.info(self.log_msg('Error with catching threads '))
                return False
            for thread in threads:
                try:
                    if  self.max_posts_count <= self.total_posts_count :
                        log.info(self.log_msg('Reaching maximum post count,Return false'))
                        return False
                    self.total_posts_count = self.total_posts_count + 1
                    thread_time = datetime.strptime(stripHtml(thread.find('td','ev_ubbx_frm_lastreply').find('noscript').renderContents()),'%B %d, %Y  %H:%M')
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')):
                        log.info(self.log_msg('Session info return True'))
                        continue
                    temp_task=self.task.clone()
                    title_tag = thread.find('a','ev_ubbx_frm_title_link')
                    temp_task.instance_data[ 'uri' ] = normalize( title_tag['href'] )
                    temp_task.pagedata['title']= stripHtml(title_tag.renderContents())
                    try:
                        temp_task.pagedata['et_author_name'] = stripHtml(thread.find('td','ev_ubbx_frm_author').renderContents())
                    except:
                        log.info(self.log_msg('Author name not found'))
                    reply_view = {'replies':'ev_ubbx_frm_replies','views':'ev_ubbx_frm_views'}
                    for each in reply_view.keys():
                        try:
                            temp_task.pagedata['ei_thread_' + each + '_count' ] = int( stripHtml(thread.find('td',reply_view[each] ).renderContents()))
                        except:
                            log.info(self.log_msg('replies count or views count not found'))
                    try:
                        rating_str = thread.find('td','ev_ubbx_frm_rating').img['title']
                        match_object = re.search('(^\d+).*\((\d+) Votes.*',rating_str)
                        temp_task.pagedata['ef_thread_rating'] = float(match_object.group(1))  
                        temp_task.pagedata['ei_thread_votes_count'] = int(match_object.group(2))
                    except:
                        log.info(self.log_msg('Rating and votes not found'))
                    try:
                        temp_task.pagedata['edate_thread_last_post_date'] =  datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg('Last post date not found'))
                    self.linksOut.append( temp_task )
                    log.info(temp_task.pagedata)
                    log.info(self.log_msg('Task Added'))
                except:
                    log.info(self.log_msg('Task cannot be added'))
            return True
            
    @logit(log, '__getData')
    def __getData(self, review, post_type ):
        """ This will return the page dictionry
        for a post
        reviews = a=[each.parent for each in soup.findAll('td',{'class':re.compile('^afpostinfo[12]')})]
        """ 
        page = {'title':''}
        try:
            page['et_author_name'] =  stripHtml(review.find('div','ev_ubbx_tpc_author').renderContents())
        except:
            log.info(self.log_msg('Author name not found'))
        try:
            page['et_author_membership'] = stripHtml( review.find('span','ev_text_small').next)
        except:
            log.info(self.log_msg('member ship not found'))
        try:
            page['et_author_location']  = re.sub('^Tribus:','',stripHtml(review.find('div',id='ev_tpc_location').renderContents())).strip()
        except:
            log.info(self.log_msg('Author location not found'))
        try:
            date_str = re.sub('^Registered:','',stripHtml(review.find('div',id='ev_tpc_reg').renderContents())).strip()
            page['edate_author_member_since'] =  datetime.strftime(datetime.strptime(date_str,'%B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Author member since not found'))
        try:
            page['ei_author_posts_count'] = int(re.sub('^Posts:','',stripHtml(review.find('div',id='ev_tpc_post').renderContents())).strip())
        except:
            log.info(self.log_msg('Author posts count not found'))
        try:
            date_str = stripHtml(review.find('noscript').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%B %d, %Y %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Posted date not found for this post'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['et_data_reply_to'] = self.parent_uri.split('/')[-1]
        except:
            log.info(self.log_msg('data reply to is not found'))
        try:
            page['data'] =  stripHtml(review.find('div','ev_ubbx_tpc_alt').renderContents())
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
            hierarchy = [stripHtml(each) for each in  re.split('<img.*?>',self.soup.find('div',id='eve_bc_top').renderContents())]
            page['et_data_forum'] = hierarchy[1]
            page['et_data_subforum'] = hierarchy[2]
            page['et_data_topic'] = hierarchy[3]
        except:
            log.info(self.log_msg('data forum not found'))
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
        for each in ['posted_date','title','et_author_name','ei_thread_replies_count','ei_thread_views_count','edate_last_post_date']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('%s cannot be extracted'%each))
        try:
            page['et_thread_hierarchy'] = [stripHtml(each) for each in  re.split('<img.*?>',self.soup.find('div',id='eve_bc_top').renderContents())]
            page['title']= page['et_thread_hierarchy'][-1]            
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            page['title']=''
        try:
            page['et_thread_id'] = self.parent_uri.split('/')[-1]
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
