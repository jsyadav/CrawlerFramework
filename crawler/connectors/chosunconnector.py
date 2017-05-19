'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
import copy
from datetime import datetime
import logging
from urllib2 import urlparse
from cgi import parse_qsl

from utils.httpconnection import HTTPConnection
from BeautifulSoup import BeautifulSoup
from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('ChosunConnector')
class ChosunConnector(BaseConnector):
    '''
    This will fetch the info from chosun forum
    Sample uris is
    http://forum.chosun.com/bbs.message.list.screen?bbs_id=10134 ( forum type )
    or
    http://search.chosun.com/search/communitySearch.jsp?searchTerm=CitiBank&sTarget=community&turn=tab ( searching forum type)
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of forum page
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            self.base_url = 'http://forum.chosun.com'
            self.total_posts_count = 0
            self.last_timestamp = datetime( 1980,1,1 )
            self.max_posts_count = int(tg.config.get(path='Connector',key='chosun_forum_numresults'))
            forum_search = self.currenturi.startswith('http://search.chosun.com/search/communitySearch.jsp')
            forum = self.currenturi.startswith('http://forum.chosun.com/bbs.message.list')
            topic = self.currenturi.startswith('http://forum.chosun.com/bbs.message.view.screen')
            if forum_search:
                headers = {}
                self.currenturi =self.currenturi.replace('&turn=tab','&bbsSortType=recent')
                headers['Host'] = 'search.chosun.com'
                log.info(self.currenturi)
                data1= dict(parse_qsl(str(self.currenturi).split('?')[-1]))
                log.info(data1)
                if not self.__setSoup():
                    return False
                if not self.__setSoup():
                    return False
                log.info(len(self.soup.findAll('div','eachResult')))
                #f=open('test.txt','w')
                #f.write(self. soup.prettify())
                #f.close()
                next_page_no = 2
                while True:
                    try:
                        if not self.__getSearchForumResults():
                            break
                        self.currenturi = 'http://search.chosun.com/search/' + self.soup.find('div',id='number_navi').find('a',text=(next_page_no)).parent['href']
                        if not self.__setSoup():
                            break
                        if not self.__setSoup():
                            break
##                        data = dict(parse_qsl(self.currenturi.split('?')[-1]))
##                        conn = HTTPConnection()
##                        conn.createrequest(self.currenturi,data=dict(parse_qsl(self.currenturi.split('?')[-1])))
##                        self.soup = BeautifulSoup( conn.fetch().read())
                        next_page_no= next_page_no + 1
                    except:
                        log.info(self.log_msg('Next page not found'))
                        break
                return True
            elif forum:
                self.currenturi = self.currenturi + '&sort=write_time'
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                next_page_no = 2
                while True:
                    if not self.__getThreads():
                        break
                    try:
                        self.currenturi = self.currenturi + '&current_page=%s'%str(next_page_no)
                        if not self.__setSoup():
                            break
                        next_page_no = next_page_no + 1
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                if self.linksOut:
                    updateSessionInfo('search', self.session_info_out,self.last_timestamp , None,'ForumThreadsPage', self.task.instance_data.get('update'))
                return True
            elif topic:
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                next_page_no = 2
                while True:
                    self.__addPosts()
                    try:
                        self.currenturi = 'http://kin.naver.com' + self.soup.find('a',id=re.compile('pagearea_\d+'),text=str(next_page_no)).parent['href']
                        if not self.__setSoup():
                            break
                        next_page_no = next_page_no + 1
                    except:
                        log.info(self.log_msg('Next page not found'))
                        break
                return True
            else:
                log.info(self.log_msg('Wrong url feeded'))
                return False
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log , '__addPosts')
    def __addPosts(self):
        """ It will add Post for a particular thread
        """
        try:
            reviews = self.soup.findAll('div',id=re.compile('text(_best)?_box'))
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        for i,review in enumerate(reviews):
            post_type = 'Suggestion'
            page = self.__getData( review,post_type )
            try:
                review_hash = get_hash( page )
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                if not checkSessionInfo('Review', self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]) or self.task.instance_data.get('pick_comments') :
                    
                    result=updateSessionInfo('Review', self.session_info_out, unique_key, \
                                review_hash,'Review', self.task.instance_data.get('update'),\
                                                            parent_list=[self.parent_uri])
                    if result['updated']:
                        
                        #page['id'] = result['id']
                        #page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                        #page['first_version_id']=result['first_version_id']
                        parent_list =[self.parent_uri]
                        page['parent_path'] = copy.copy(parent_list)
                        parent_list.append(unique_key)
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
                        page['entity'] = 'thread'
                        page['category'] = self.task.instance_data.get('category','')
                        
                        page['task_log_id']=self.task.id
                        page['uri'] = self.currenturi
                        page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                        self.pages.append( page )
                        log.info(page)
                        log.info(self.log_msg('Review Added'))
                    else:
                        log.info(self.log_msg('result not updated'))
            except:
                log.exception(self.log_msg('Error while adding session info'))
                
    @logit(log,'__getSearchForumResults')
    def __getSearchForumResults(self):
        '''It will fetch the search results and and add the tasks
        '''
        try:
            results = self.soup.findAll('div','eachResult')
            log.info(self.log_msg('Total Results found is %d'%len(results)))
            for result in results:
                try:
                    if self.total_posts_count >= self.max_posts_count:
                        log.info(self.log_msg('Reaching maximum post,Return false'))
                        return False
                    self.total_posts_count = self.total_posts_count + 1
                    date_str = stripHtml(result.find('span','grayText12').renderContents())
                    try:
                        thread_time = datetime.strptime(date_str, '%Y.%m.%d')
                    except:
                        log.info(self.log_msg('Cannot find the thread time, task not added '))
                        continue
                    if checkSessionInfo('search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        log.info(self.log_msg('Session info return True or Reaches max count'))
                        return False
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] = result.find('span','linkedBlueText13').find('a')['href']
                    log.info('taskAdded')
                    self.linksOut.append( temp_task )
                except:
                    log.exception(self.log_msg('task not added'))
                    continue
            return True
        except:
            log.exception(self.log_msg('cannot get the search results'))
            return False

                

    @logit(log , '__getThreads')
    def __getThreads( self ):
            """ It will fetch the thread info and create tasks
            """
            try:
                threads = [x.findParent('tr') for x in self.soup.findAll('td','sisa_sub_title')[1:]]
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False
            for thread in threads:
                thread_info = thread.findAll('td')
                if not len(thread_info)==7:
                    log.info(self.log_msg('not enough info, cannot proceed'))
                if  self.total_posts_count > self.max_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    date_str = stripHtml(thread_info[-1].renderContents())
                    try:
                        thread_time = datetime.strptime ( str(datetime.today().year) + '-' + date_str,'%Y-%m-%d')
                    except:
                        log.info(self.log_msg('Cannot add the post continue'))
                        continue
                    if checkSessionInfo('search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        log.info(self.log_msg('Session info return True or Reaches max count'))
                        continue
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    try:
                        thread_info[1].find('span').extract()
                    except:
                        pass
                    div_content = thread.find('div','title_area')
                    temp_task.instance_data[ 'title' ] = stripHtml(thread_info[1].renderContents())
                    temp_task.instance_data[ 'uri' ] = 'http://forum.chosun.com' + thread_info[1].find('a')['href']
                    try:
                        temp_task.pagedata['et_author_name'] =  stripHtml(thread_info[2].renderContents())
                    except:
                        log.info(self.log_msg('author name not found'))
                    count_dict = {'ei_thread_helpful_yes':3,'ei_thread_helpful_no':4,'ei_thread_views_count':5}
                    for each in count_dict.keys():
                        try:
                            temp_task.pagedata[each] = int(stripHtml(thread_info[count_dict[each]].renderContents()))
                        except:
                            log.info(self.log_msg('data abt thread not found'))
                    temp_task.pagedata['edate_last_post_date']=  datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    log.info(temp_task.pagedata)
                    log.info('taskAdded')
                    self.linksOut.append( temp_task )
                except:
                    log.exception( self.log_msg('Task Cannot be added') )
            return True
    @logit(log, '__getData')
    def __getData(self, review,post_type ):
        """ This will return the page dictionry
        """
        page={}
        page = {'title':'','data':''}
        page['et_data_post_type'] = post_type
        try:
            if review.get('id'):
                page['et_data_best_reply'] ='yes'
        except:
            log.info(self.log_msg('it is not rhe best reply'))
        try:
            page['et_author_id'] = stripHtml(review.find('p','id').renderContents())
        except:
            log.info(self.log_msg('author id not found'))
        try:
            page['et_author_name'] = stripHtml(review.find('dd','left').find('b').renderContents())
        except:
            log.info(self.log_msg('author name not found'))
        try:
            page['data'] = stripHtml(review.find('div',attrs = {'class':re.compile('reply_text.*')}).renderContents())
        except:
            log.info(self.log_msg('Data not found'))
            page['data'] =''
        try:
            date_str = stripHtml(review.find('div','reply_date').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%Y.%m.%d %H:%M:%S'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('posted_date not found'))
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
        """This will get the parent info
        """
        page = {}
        if checkSessionInfo('Review', self.session_info_out, self.parent_uri,\
                                        self.task.instance_data.get('update')):
            return False
        for each in ['ei_thread_helpful_yes','edate_last_post_date','ei_thread_helpful_no','ei_thread_views_count']:#,'ei_thread_views_count','title']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('page data cannot be extracted for %s'%each))
    
        try:
            title_tag  = self.soup.find('td','sisa_title')
            title_tag.find('font').extract()
            page['title']= stripHtml(title_tag.renderContents())
        except:
            log.info(self.log_msg('Title not found'))
            page['title'] = ''
        try:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            date_tag = self.soup.find('span','sisa_number')
            date_str = re.search('\[(.*)\]',stripHtml(date_tag.renderContents())).group(1)
            page['posted_date'] =  datetime.strftime(datetime.strptime(date_str,'%Y-%m-%d %H:%M:%S'),"%Y-%m-%dT%H:%M:%SZ")
            page['ei_thread_views_count'],page['ei_thread_votes_count'],page['ei_thread_helpful_yes_count'],page['ei_thread_helpful_no_count'] = [int(re.search('\d+$',x.strip()).group()) for x in  stripHtml(date_tag.findParent('tr').findAll('td')[-1].renderContents()).split('|')]
            author_name = stripHtml(date_tag.findParent('div').next.__str__()).split('(')
            page['et_author_name'] = author_name[0]
            page['et_author_userid'] = author_name[1][:-1]
        except:
            log.info(self.log_msg('Author thread views count not found'))
        try:
            page['et_data_post_type'] = 'Question'
            page['data'] = stripHtml(self.soup.find('div','view_text').renderContents())
        except:
            log.info(self.log_msg('Data not found'))
            page['data'] =''
        try:
            post_hash = get_hash( page )
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result= updateSessionInfo( 'Review', self.session_info_out, self.\
                   parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
            if result['updated']:
                page['parent_path'] = []
                page['path'] = [self.parent_uri]
                page['uri'] = normalize( self.currenturi )
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
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
        except:
            log.exception(self.log_msg("parent post couldn't be parsed"))
        
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