'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Pooja

import re
from datetime import datetime, timedelta
import logging
from urllib2 import urlparse
from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('MoneyCentralConnector')
class MoneyCentralConnector(BaseConnector):
    '''
    Sample url http://moneycentral.msn.com/community/message/board.asp?Board=YourMoney
    '''
    @logit(log,'fetch')
    def fetch(self):
        self.genre = 'Forum'
        try:
            self.parent_uri = self.currenturi
            if urlparse.urlparse(self.currenturi)[2].split('/')[-1].startswith('board'):
                self.total_posts_count = 0
                self.last_timestamp = datetime(1980, 1, 1)
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set, Returning False from Fetch'))
                    return False
                else:
                    self.currenturi = str(self.soup.find('frame',{'name':'boarddata'})['src'])
                    if not self.__setSoup():
                        log.info(self.log_msg('Soup not set, Returning False from Fetch'))
                        return False                
                while True:
                    if not self.__getThreads():
                        break
                    try:
                        self.currenturi = str(self.soup.find('a',text='Next').parent['href'])
                        if not self.__setSoup():
                            break
                        else:
                            self.currenturi = str(self.soup.find('frame',{'name':'boarddata'})['src'])
                            if not self.__setSoup():
                                break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                if self.linksOut:
                    updateSessionInfo('Search',self.session_info_out, self.last_timestamp, None, 'ForumThreadsPage', self.task.instance_data.get('update'))
                return True
            elif urlparse.urlparse(self.currenturi)[2].split('/')[-1].startwith('thread'):
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set, Returning False from Fetch'))
                    return False
                else:
                    self.currenturi = str(self.soup.find('frame',{'name':'boardframe'})['src'])
                    if not self.__setSoup():
                        log.info(self.log_msg('Soup not set, Returning False from Fetch'))
                        return False
                self.__getParentPage()
                self.post_type = True
                while True:
                    self.__addPosts()
                    try:
                        self.currenturi = str(self.soup.find('a',text='Next').parent['href'])
                        if not self.__setSoup():
                            log.info(self.log_msg('Soup not set, Returning False from Fetch'))
                            return False
                        else:
                            self.currenturi = str(self.soup.find('frame',{'name':'boardframe'})['src'])
                            if not self.__setsoup():
                                log.info(self.log_msg('Soup not set, Returning False from Fetch'))
                                return False
                    except:
                        log.exception(self.log_msg('Next page not set'))
                        break
                return True
        except:
            log.exception(self.log_msg('Exception in Fetch'))
            return False

    @logit(log, '__addPosts')
    def __addPosts(self):
        '''
        It will add Post for a particular thread
        '''
        try:
            reviews = self.soup.find('table',{'id':'results'}).findAll('tr')[1:]
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        for i,review in enumerate(reviews[:]):
            post_type = 'Question'
            if i==0 and self.post_type:
                post_type = 'Question'
                self.post_type = False
            else:
                post_type = 'Answer'
            try:
                uniq_key = striHtml(reviews.find('div',{'class':'details'})['id'])
                if checkSessionInfo(self.genre, self.session_info_out, uniq_key, self.task.instance_data.get('update'),parent_list=[self.parent_uri]):
                    log.info(self.log_msg('Session info return True'))
                    continue
                page = self.__getData(review, post_type)
                log.info(page)
                review_hash = get_hash(page)
                result = updateSessionInfo(self.genre, self.session_info_out, uniq_key,review_hash, 'Review', self.task.instance_data.get('update'),parent_list=[self.parent_uri])
                if not result['updated']:
                    continue
                parent_list = [self.parent_uri]
                page['parent_path'] = copy.copy(parent_list)
                parent_list.append(uniq_key)
                page['path'] = parent_list
                page['priority'] = self.task.priority
                page['level'] = self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
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
    @logit(log, '__getThreads')
    def __getThreads(self):
        '''
        It will fetch thread info and create tasks
        '''
        try:
            threads = self.soup.find('table',{'id':'results'}).findAll('tr',{'class':'read'})
        except:
            log.exception(self.log_msg('No thread found, cannot processed'))
            return False
        log.info(self.currenturi)
        for thread in threads:
            self.total_posts_count = self.total_posts_count + 1
            try:
                thread_info = thread.findAll('td')
                if not len(thread_info) == 7:
                    log.info(self.log_msg('Not enough info from thread'))
                    continue
                try:
                    date_str = stripHtml(thread_info[2].find('span',{'class':'timestamp'}).renderContents())
                    thread_time = datetime.strptime(date_str,'%m/%d/%y %H:%M %p')
                except:
                    log.info(self.log_msg('Posted date not found'))
                    continue
                if checkSessionInfo('Search',self.session_info_out, thread_time, self.task.instance_data.get('update')):
                    log.info(self.log_msg('Session Info return True'))
                    continue
                self.last_timestamp = max(thread_time, self.last_timestamp)
                temp_task = self.task.clone()
                temp_task.instance_data['uri'] = str(thread_info[2].find('a')['href'])
                temp_task.pagedata['et_author_name'] = stripHtml(thread_info[2].find('a',{'class':'author'}).renderContents())
                temp_task.pagedata['title'] = stripHtml(thread_info[2].find('a',{'class':'subject'}).renderContents())
                temp_task.pagedata['posted_date'] = datetime.strftime(thread_time,'%Y-%m-%dT%H:%M:%SZ')
                temp_task.pagedata['ei_thread_replies_count'] = stripHtml(thread_info[3].renderContents())
                temp_task.pagedata['ei_thread_view_count'] = stripHtml(thread_info[5].renderContents())
                temp_task.pagedata['ei_author_count'] = stripHtml(thread_info[4].renderContents())
                temp_task.pagedata['et_last_post_author'] = stripHtml(thread_info[6].find('a',{'class':'author'}).renderContents())
                last_date_str = stripHtml(thread_info[6].find('span',{'class':'timestamp'}).renderContents())
                last_date = datetime.strptime(last_date_str,'%m/%d/%y %H:%M %p')
                temp_task.pagedata['edate_last_post_date'] = datetime.strftime(last_date,'%Y-%m-%dT%H:%M:%SZ')
                log.info(temp_task.pagedata)
                log.info('TaskAdded')
                self.linksOut.append(temp_task)
            except:
                log.exception(self.log_msg('Task cannot be added'))
                continue
        return True

    @logit(log, '__getData')
    def __getData(self, review, post_type):
        '''
        This will return Page dict
        '''
        page = {}
        try:
            page['et_author_name'] = stripHtml(review.find('span',{'class':'author'}).renderContents())
            page['et_user_profile'] = review.find('span',{'class':'author'}).find('a')['href']
        except:
            log.info(self.log_msg('author info not found'))
        try:
            date_str = stripHtml(review.find('span',{'class':'timestamp'}).renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%m/%d/%y %H:%M %p'), '%Y-%m-%dT%H:%M:%SZ' )
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),'%Y-%m-%dT%H:%M:%SZ')
            log.info(self.log_msg('Posted Date Not Found'))
        try:
            page['data'] = striHtml(review.find('div',{'class':'msgBody inlineimg'}).renderContents())
        except:
            page['data'] = ''
            log.info(self.log_msg('Data not found for post'))
        try:
            if len(page['data']) > 50:
                page['title'] = page['data'][:50] + '...'
            else:
                page['tile'] = page['data']
        except:
            log.exception(self.log_msg('Title not Found'))
            page['title'] = ''
        try:
            page['et_data_post_type'] = post_type
        except:
            log.info(self.log_msg('Page info missing'))
        try:
            page['et_forum_title'] = self.forum_title
        except:
            log.info(self.log_msg('Data forum not found'))
        return page

    @logit(log, '__getParentPage')
    def __getParentPage(self):
        '''
            This will get the Parent Page info
        '''
        page = {}
        try:
            self.hierarchy = page['et_thread_hierarchy'] = [stripHtml(x.renderContents()) for x in self.soup.find('div',{'class':'rd Microsoft_Msn_Boards_Read_List Web_Bindings_Base'}).findAll('li')]
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
        try:
           self.forum_title = page['title'] = stripHtml(self.soup.find('h2').renderContents())
        except:
            log.info(self.log_msg('Title Not Found'))
            page['title'] = ''
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri, self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True'))
            return False
        for each in ['et_author_name','ei_thread_replies_count','ei_thread_view_count','ei_author_count','et_last_post_author','edate_last_post_date','posted_date']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('Page data cannot be extracted for %s'%each))
        try:
            page['ei_thread_id'] = int(urlparse.urlparse(self.currenturi)[4].split('&')[0].split('ThreadId=')[1])
        except:
            log.info(self.log_msg('Thread id not found'))
        try:
            post_hash = get_hash(page)
            id = None
            if self.session_info_out == {}:
                id = self.task.id
            result = updateSessionInfo(self.genre, self.session_info_out, self.parent_uri, post_hash, 'Post', self.task.instance_data.get('update'),Id=id)
            if not result['updated']:
                return False
            page['path'] = [self.parent_uri]
            page['parent_path'] = []
            page['uri'] = normalize(self.currenturi)
            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
            page['priority'] = self.task.priority
            page['level'] = self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow(),'%Y-%m-%dT%H:%M:%SZ')
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

