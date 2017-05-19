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

log = logging.getLogger('MoneyGrupyDyskusyjneConnector')
class MoneyGrupyDyskusyjneConnector(BaseConnector):
    '''

    Sample uris is
    http://grupy-dyskusyjne.money.pl/pl;biznes;banki,grupa,2.html
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of forum page
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            self.base_url = 'http://grupy-dyskusyjne.money.pl'
            self.total_posts_count = 0
            self.last_timestamp = datetime( 1980,1,1 )
            self.max_posts_count = int(tg.config.get(path='Connector',key='money_group_discussion_numresults'))
            try:
                search_type=False
                if dict(parse_qsl(self.currenturi.split('?')[-1])).get('slowo'):
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
                        self.currenturi = self.base_url + self.soup.find('a','next ns8')['href']
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                return True
            if search_type:
                pass
            if not self.currenturi.find('grupa,')==-1:
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                while True:
                    if not self.__getThreads():
                        break
                    try:
                        self.currenturi = self.base_url + self.soup.find('a','next ns8')['href']
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
                #self.currenturi = self.base_url + self.soup.find('div','nawigacja').find('strong',text=re.compile('Poka. tre.{2} wszystkich')).findParent('a')['href']
                while True:
                    self.__addPosts()
                    try:
                        self.currenturi = self.base_url + self.soup.find('a','next ns8')['href']
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next page not found'))
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
            #reviews = [x.findParent('table') for x in self.soup.findAll('tr','trg')]
            reviews = [x.findParent('tr') for x in self.soup.findAll('td','red bld th3')]
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        for i, review in enumerate(reviews):
            post_type = ""
            if i==0 and self.post_type:
                post_type = "Question"
                self.post_type = False
            else:
                post_type = "Suggestion"
            try:
                unique_key = review.find('h3')['id']
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    log.info(self.log_msg('session info return True, with hash'))
                    continue
                page = self.__getData( review , post_type )
                review_hash = get_hash( page )
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            review_hash,'review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    log.info(self.log_msg('result not updated'))
                    continue
                log.info(page)
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
                page['entity'] = 'thread'
                page['category'] = self.task.instance_data.get('category','')
                page['task_log_id']=self.task.id
                page['uri'] = self.currenturi
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append( page )
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))

    @logit(log , '__getThreads')
    def __getThreads( self ):
            """ It will fetch the thread info and create tasks
            """
            try:
                threads=threads = [x.findParent('tr') for x in self.soup.findAll('td','th3')]
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False
            for thread in threads:
                if  self.total_posts_count >= self.max_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    thread_info = thread.findAll('td')
                    if not len(thread_info)==3:
                        log.info(self.log_msg('Not enough data , to proceed'))
                        continue
                    try:
                        date_str = stripHtml(thread_info[0].renderContents())
                        thread_time = datetime.strptime(date_str,'%Y-%m-%d %H:%M')
                    except:
                        log.info(self.log_msg('Cannot continue, thread time not found '))
                        continue
                    if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                    #if checkSessionInfo('search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        log.info(self.log_msg('Session info return True or Reaches max count'))
                        continue
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    title_tag = thread_info[1].find('a','red')
                    temp_task.instance_data[ 'uri' ] =  self.base_url + title_tag['href']
                    temp_task.pagedata['et_author_name'] =  stripHtml(thread_info[2].renderContents())
                    temp_task.pagedata['title'] =  stripHtml(title_tag.renderContents())
                    temp_task.pagedata['edate_last_post_date']=  datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
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
        page = {'title':'','posted_date':datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")}
        try:
            page['et_data_post_type'] = post_type
            title_info = review.findAll('td')
            page['title'] = stripHtml(title_info[1].renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(stripHtml\
                            (title_info[0].renderContents()),'%Y-%m-%d %H:%M'),\
                                                            "%Y-%m-%dT%H:%M:%SZ")

            page['et_author_name'] = stripHtml(title_info[2].renderContents())
        except:
            log.info(self.log_msg('title or posted date not found'))
        try:
            td_tag = review.findNext('tr')
            div_tag = td_tag.find('div')
            if div_tag:
                div_tag.extract()
            page['data'] = '\n'.join([x for x in  stripHtml(td_tag.renderContents()).split('\n') if not x.strip()=='' and not x.strip().startswith('>') and not re.match('.*wrote:$',x.strip()) and not re.search('napisa.a:$',x.strip()) and not re.search('napisa.\(a\):$',x.strip())])
        except:
            log.exception(self.log_msg('Posted date not found for this post'))
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
        page = {}
        try:
            self.hierarchy =page['et_thread_hierarchy'] = [ stripHtml(x.renderContents()) for x in self.soup.findAll('h1')]
            page['title']= page['et_thread_hierarchy'][-1]
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            page['title']=''
        for each in ['et_author_name','edate_last_post_date']: # ,'ei_thread_views_count','edate_last_post_date']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('page data cannot be extracted for %s'%each))
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
            results = [x.findParent('tr') for x in self.soup.findAll('td','th3')]
            for result in results:
                try:
                    if self.total_posts_count >= self.max_posts_count:
                        log.info(self.log_msg('Reaching maximum post,Return false'))
                        return False
                    self.total_posts_count = self.total_posts_count + 1
                    td_tags =  result.findAll('td')
                    if not len(td_tags)==3:
                        log.info(self.log_msg('3 td tags not availble, something is missing'))
                        continue
                    date_str =  stripHtml(td_tags[0].renderContents())
                    try:
                        thread_time = datetime.strptime(date_str,'%Y-%m-%d %H:%M')
                    except:
                        log.info(self.log_msg('Cannot find the thread time, task not added '))
                        continue
                    if checkSessionInfo('search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        log.info(self.log_msg('Session info return True'))
                        continue
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    self.currenturi = 'http://grupy-dyskusyjne.money.pl' + td_tags[1].find('a')['href']
                    if not self.__setSoup():
                        continue
                    temp_uri = self.soup.find('div','ar sep5').find('a')['href']
                    if not temp_uri.startswith('http:'):
                        temp_uri = self.base_url + temp_uri
                    elif not self.currenturi.startswith('http://grupy-dyskusyjne.money.pl'):
                        continue
                    temp_task.instance_data[ 'uri' ] = temp_uri
                    log.info(temp_task.instance_data[ 'uri' ])
                    
                    log.info('taskAdded')
                    self.linksOut.append( temp_task )
                    
                except:
                    log.exception(self.log_msg('task not added'))
            return True
        except:
            log.info(self.log_msg('cannot get the search results'))