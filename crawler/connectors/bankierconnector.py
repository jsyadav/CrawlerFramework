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
import copy

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('BankierConnector')
class BankierConnector(BaseConnector):
    '''
    This will fetch the info for msexchange forums
    Sample uris is
    http://www.bankier.pl/forum/forum_banki,9,1.html
    or
    http://www.bankier.pl/narzedzia/znajdz/?query=%22Bank+Handlowy%22&typ=4
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of forum page
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            self.base_url = 'http://www.bankier.pl/forum/'
            self.total_posts_count = 0
            self.last_timestamp = datetime( 1980,1,1 )
            self.max_posts_count = int(tg.config.get(path='Connector',\
                                                key='bankier_forum_numresults'))
            if self.currenturi.startswith('http://www.bankier.pl/narzedzia/znajdz/'):
                self.currenturi = self.currenturi + '&sort=1&nm=50'
                if not self.__setSoup():
                    return False
                while True:
                    if not self.__getSearchResults():
                        break
                    try:
                        self.currenturi = self.base_url + self.soup.find('a',text\
                                =re.compile('nast.pna strona &raquo;')).parent['href']
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                return True
            if self.currenturi.split('/')[-1].startswith('forum_'):
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                while True:
                    if not self.__getThreads():
                        break
                    try:
                        self.currenturi = 'http://www.bankier.pl/narzedzia/znajdz/' + self.soup.find('a','WyszE')['href']
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                if self.linksOut:
                    updateSessionInfo('Search', self.session_info_out,self.last_timestamp\
                             , None,'ForumThreadsPage', self.task.instance_data.get('update'))
                return True
            else:
                if not self.__setSoup():
                    return False
                self.__getParentPage()
                self.post_type = True
                self.currenturi = self.base_url + self.soup.find('div','nawigacja')\
                        .find('strong',text=re.compile('Poka. tre.{2} wszystkich'))\
                                                        .findParent('a')['href']
                if self.__setSoup():
                    while True:
                        self.__addPosts()
                        try:
                            self.currenturi = self.base_url + self.soup.find('a',text=re.compile('nast.pna strona &raquo;')).parent['href']
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
            reviews = self.soup.findAll('div','post')
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
            page = self.__getData( review , post_type )
            try:
                review_hash = get_hash( page )
                log.info(page)
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    continue
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            review_hash,'Thread', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    continue
                #page['id'] = result['id']
                #page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                #page['first_version_id']=result['first_version_id']
                parent_list = [self.parent_uri]
                page['parent_path'] = copy.copy(parent_list)
                parent_list.append( unique_key )
                page['path'] = parent_list
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
                page['entity'] = 'Thread'
                page['category'] = self.task.instance_data.get('category','')
                page['task_log_id']=self.task.id
                page['uri'] = self.currenturi
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append( page )
                #log.info(page)
                log.info(self.log_msg('Thread Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))

    @logit(log , '__getThreads')
    def __getThreads( self ):
            """ It will fetch the thread info and create tasks
            """
            try:
                threads=self.soup.find('div','fthread').findAll('tr')
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False
            for thread in threads:
                if  self.total_posts_count > self.max_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    thread_info = thread.findAll('td')
                    if not len(thread_info)==6:
                        log.info(self.log_msg('not enough info from thread'))
                        continue
                    date_str = stripHtml(thread_info[-1].renderContents())
                    log.info(self.log_msg(date_str))
                    try:
                        thread_time = datetime.strptime(date_str,'%Y-%m-%d %H:%M')
                    except:
                        log.info(self.log_msg('Cannot continue, thread time not found '))
                        continue
                    if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        log.info(self.log_msg('Session info return True or Reaches max count'))
                        continue
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] = self.base_url + thread.findAll('a')[-1]['href']
                    author_name = stripHtml(thread_info[-3].renderContents())
                    if author_name.startswith('~'):
                        author_name = author_name[1:]
                    temp_task.pagedata['et_author_name'] =  author_name
                    temp_task.pagedata['ei_thread_posts_count'] = int(stripHtml(thread_info[-2].renderContents()))
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
        page = {}
        try:
            page['et_data_post_type'] = post_type
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['data'] = ''
            author_tag = review.find('div','autoridata')
            aut_ipaddr_date_str = stripHtml(author_tag.renderContents())
            match_object = re.search('^Autor:(.*?)\[(.*?)\],(.*)', aut_ipaddr_date_str)
            aut_name = match_object.group(1).strip()
            if aut_name.startswith('~'):
                aut_name = aut_name[1:]
            page['et_author_name'] = aut_name
            page['et_author_ipaddress'] = match_object.group(2).strip()
            date_str = match_object.group(3).strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%Y-%m-%d %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
            if self.post_type=='Question':
                page['data'] =  self.question_content
            else:
                review.find('h3').extract()
                [x.extract() for x in review.findAll('div')]
##                review_str = review.__str__()
##                author_str = author_tag.__str__()
##                data_str = review_str[review_str.find( author_str ) + len(author_str):]
                page['data']= stripHtml( review.renderContents() )
        except:
            log.exception(self.log_msg('Posted date not found for this post'))
        try:
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
        page = {}
        try:
            post_tag = self.soup.find('div','fpost')
            page['title']= stripHtml(post_tag.find('h1').renderContents())
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            page['title']=''
        try:
            self.question_content = stripHtml(post_tag.find('div','tresc').renderContents())
        except:
            log.info(self.log_msg('Main post not found'))
            self.question_content = ''
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        for each in ['et_author_name','et_last_post_author_name','ei_thread_posts_count']: # ,'ei_thread_views_count','edate_last_post_date']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('page data cannot be extracted for %s'%each))
        try:
            aut_str = stripHtml(post_tag.find('span','autor').renderContents())
            match_object = re.search('Autor\s*?\|\s*(.*?)\[(.*?)\]',aut_str)
            page['et_author_ipaddress'] = match_object.group(2).strip()
        except:
            log.info(self.log_msg('Author Ip address not found'))
        try:
            date_str = stripHtml(post_tag.find('div','autoridata').find('span','data').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%Y-%m-%d %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Posted date not found'))
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
            page['path'] = [self.parent_uri]
            page['parent_path'] = []
            page['uri'] = normalize( self.currenturi )
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
            log.info(self.log_msg('Parent Page added'))
            return True
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False

    @logit(log, "_setSoup")
    def __setSoup( self, url = None, data = None, headers = {} ):
        """
            It will set the uri to current page, 
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
            results = self.soup.findAll('a','w_tyt')
            for result in results:
                try:
                    if self.total_posts_count >= self.max_posts_count:
                        log.info(self.log_msg('Reaching maximum post,Return false'))
                        return False
                    self.total_posts_count = self.total_posts_count + 1
                    date_str = stripHtml(result.findNext('div').findAll('a')[-1].renderContents()).split('\n')[-1][1:].strip()
                    try:
                        thread_time = datetime.strptime(date_str, '%d-%m-%Y')
                    except:
                        log.info(self.log_msg('Cannot find the thread time, task not added '))
                        continue
                    if checkSessionInfo('search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        log.info(self.log_msg('Session info return True or Reaches max count'))
                        return False
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] = result['href']
                    log.info('taskAdded')
                    self.linksOut.append( temp_task )
                except:
                    log.exception(self.log_msg('task not added'))
            return True
        except:
            log.info(self.log_msg('cannot get the search results'))