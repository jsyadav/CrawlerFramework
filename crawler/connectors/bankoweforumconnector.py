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
from urllib import urlencode
from cgi import parse_qsl
import copy

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('BankOweForumConnector')
class BankOweForumConnector(BaseConnector):
    '''

    Sample uris is
    http://www.forum-bankowe.pl/tematy1/
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of forum page
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            self.total_posts_count = 0
            self.last_timestamp = datetime( 1980,1,1 )
            self.max_posts_count = int(tg.config.get(path='Connector',key='bankowe_forum_numresults'))
            try:
                search_type=False
                if self.currenturi.startswith('http://www.forum-bankowe.pl/search.php'):
                    search_type = True
            except:
                log.info(self.log_msg('Not a Search type'))
            if search_type:
                if not self.__setSoup():
                    return False
                while True:
                    if not self.__getSearchResults():
                        break
                    try:
                        self.currenturi =  'http://www.forum-bankowe.pl' + self.soup.find('a',text=re.compile('Nast.*?pna strona')).parent['href'][1:]
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                return True
            if self.currenturi.endswith('/'):
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                while True:
                    if not self.__getThreads():
                        break
                    try:
                        self.currenturi = self.soup.find('a',text=re.compile('Nast.*?pna strona')).parent['href']
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
                self.post_type = True
                while True:
                    self.__addPosts()
                    try:
                        self.currenturi = self.soup.find('a',text=re.compile('Nast.*?pna strona')).parent['href']
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
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
            reviews = self.soup.find('div',id='page-body').findAll('div',attrs={'class':re.compile('post bg\d'),'id':True})
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        for i, review in enumerate(reviews):
            post_type =''
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
                page = self.__getData( review,post_type)
                review_hash = get_hash( page )
                log.info(page)
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            review_hash,'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    continue
                #page['first_version_id']=result['first_version_id']
                #page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                #page['id'] = result['id']
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
                threads = self.soup.find('ul','topiclist topics').findAll('li')
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False
            for thread in threads:
                if  self.total_posts_count >= self.max_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    last_post = thread.find('dd','lastpost').find('dfn').nextSibling
                    date_str = last_post.next.next
                    thread_time = datetime.strptime(date_str,'%Y-%m-%d, %H:%M')
                    if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')):
                        log.info(self.log_msg('Session info return True or Reaches max count'))
                        return False
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    title_tag = thread.find('a','topictitle')
                    temp_task.instance_data[ 'uri' ] = title_tag['href']
                    log.info(temp_task.instance_data[ 'uri' ])
                    try:
                        temp_task.pagedata['title'] = stripHtml(title_tag.renderContents())
                        temp_task.pagedata['et_last_post_author_name'] = stripHtml(last_post.replace(u'napisa\u0139&sbquo;','').strip())
                        post_info = copy.copy(thread.find('dd','posts') )
                        date_info = post_info.previousSibling.previous.strip().split('&raquo;')
                        post_info.find('dfn').extract()
                        temp_task.pagedata['et_author_name'] = stripHtml(date_info[0].replace(u'napisa\u0139&sbquo;','').strip())
                        last_post = thread.find('dd','lastpost')
                        views = thread.find('dd','views')
                        views.find('dfn').extract()
                        temp_task.pagedata['ei_thread_replies_count'] = int(stripHtml(post_info.renderContents()))
                        temp_task.pagedata['ei_thread_views_count'] = int(stripHtml(views.renderContents()))
                        date_str = last_post.find('span').findNext('br').next.extract()
                        temp_task.pagedata['edate_last_post_date']=  datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.exception(self.log_msg('page data not found'))
                    log.info(temp_task.pagedata)
                    log.info('taskAdded')
                    self.linksOut.append( temp_task )
                except:
                    log.exception( self.log_msg('Task Cannot be added') )
            return True
    @logit(log, '__getData')
    def __getData(self, review, post_type ):
        """ This will return the page dictionry
        """
        page = {'et_data_post_type':post_type}
        try:
            page['title'] = stripHtml(review.find('h3').renderContents())
        except:
            log.info(self.log_msg('title not found'))
            page['title'] =''
        try:
            post_info = review.find('p','author')
            aut_info = post_info.find('strong')
            page['et_author_name'] = stripHtml(aut_info.renderContents())
            date_str = stripHtml(aut_info.nextSibling)
            page['posted_date'] = datetime.strftime( datetime.strptime(date_str,'%Y-%m-%d, %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('post info not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['data'] =  stripHtml(review.find('div','content').renderContents())
        except:
            log.exception(self.log_msg('Posted date not found for this post'))
            page['data'] = ''
        try:
            if page['title'] =='':
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
        return page

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
        page = {'title':''}
        for each in ['et_author_name','edate_last_post_date','ei_thread_replies_count','et_last_post_author_name','ei_thread_views_count','title']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('page data cannot be extracted for %s'%each))
        try:
            page['et_thread_hierarchy'] =  [stripHtml(x.renderContents()) for x in  self.soup.find('ul','linklist navlinks').find('li','icon-home').findAll('a')]
            page['title']= stripHtml(self.soup.find('div',id='page-body').find('h2').renderContents())
            page['et_thread_hierarchy'].append(page['title'])
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))

        
##        try:
##            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
##            review = self.soup.find('div','tab5')
##            post_info = [x.strip() for x in stripHtml(review.find('div','dt').renderContents()).split('|')]
##            page['posted_date'] = datetime.strftime(datetime.strptime(post_info[0],'%Y-%m-%d %H:%M:%S'),"%Y-%m-%dT%H:%M:%SZ")
##            page['et_author_ipaddress'] = post_info[1]
##            page['ei_thread_replies_count'] = int(re.search('\d+',soup.find('div','tab5').find('span','ns5').nextSibling.strip()).group())
##        except:
##            log.info(self.log_msg('post info not found'))
        try:
            post_hash = get_hash( page )
            log.info(page)
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo( self.genre, self.session_info_out, self.\
                   parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
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
            log.info(self.log_msg('Parent Page added'))
            return True
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False
    @logit(log,"__createSiteUrl")
    def __createSiteUrl( self, review_url = None):
        '''it will create url
        '''
        if not review_url:
            return False
        url_part = review_url.split('?')
        review_parameters = dict(parse_qsl(url_part[-1]))
        if review_parameters.has_key('a'):
            review_parameters.pop('a')
        if not review_parameters.has_key('s'):
            review_parameters['s']=0
        review_parameters['v']=2
        return  url_part[0] + '?' + urlencode(review_parameters)

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
        
    @logit(log,'__getSearchResults')
    def __getSearchResults(self):
        '''It will fetch the search results and and add the tasks
        '''
        try:
            results = self.soup.findAll('dl','postprofile')
            for result in results:
                try:
                    if self.total_posts_count >= self.max_posts_count:
                        log.info(self.log_msg('Reaching maximum post,Return false'))
                        return False
                    self.total_posts_count = self.total_posts_count + 1
                    date_str =  stripHtml(result.find('dd').renderContents())
                    try:
                        thread_time = datetime.strptime(date_str,'%Y-%m-%d, %H:%M')
                    except:
                        log.info(self.log_msg('Cannot find the thread time, task not added '))
                        continue
                    if checkSessionInfo('search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        log.info(self.log_msg('Session info return True'))
                        continue
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] =  result.findAll('dd')[-3].find('a')['href']
                    log.info('taskAdded')
                    self.linksOut.append( temp_task )

                except:
                    log.exception(self.log_msg('task not added'))
            return True
        except:
            log.info(self.log_msg('cannot get the search results'))