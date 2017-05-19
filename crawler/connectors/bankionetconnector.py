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

log = logging.getLogger('BankiOnetConnector')
class BankiOnetConnector(BaseConnector):
    '''
    This will fetch the info for msexchange forums
    Sample uris is
    #http://banki.onet.pl/1969390,wiadomosci.html # may change to
    http://banki.onet.pl/4,61,8,6606181,0,0,0,forum2.html
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        fetches all the information contanined in the given url
        sample
        thread uri = http://banki.onet.pl/4,61,8,6602845,0,0,0,forum2.html
        """
        self.genre="Review"
        try:
            #self.currenturi = 'http://banki.onet.pl/4,61,8,6551990,0,0,0,forum2.html'
            self.parent_uri = self.currenturi
            next_page_number = 2
            if not self.__setSoup():
                return False
            if len(re.findall('\d+,',self.parent_uri))==4:
                self.total_posts_count = 0
                self.last_timestamp = datetime( 1980,1,1 )
                self.max_posts_count = int(tg.config.get(path='Connector',key='bankionet_forum_numresults'))
                while True:
                    if not self.__getThreads():
                        break
                    try:
                        self.currenturi = self.soup.find('td','forumauthor',align='right').find('a',text=str(next_page_number)).parent['href']
                        if not self.__setSoup():
                            break
                    except:
                        log.exception(self.log_msg('Next page not found'))
                        break
                    next_page_number = next_page_number + 1
                if self.linksOut:
                    updateSessionInfo('search', self.session_info_out,self.last_timestamp , None,'ForumThreadsPage', self.task.instance_data.get('update'))
                return True
            parent_soup = copy.copy( self.soup )
            self.__getParentPage()
            while True:
                posts_tags = [x.findParent('table') for x in parent_soup.findAll('img',src=re.compile('.*ico_wtk.gif$'))]
                for post_tag in posts_tags:
                    self.__addPosts(post_tag)
                try:
                    self.currenturi = parent_soup.find('td','forumauthor',align='right').find('a',text=str(next_page_number)).parent['href']
                    if not self.__setSoup():
                        break
                    parent_soup = copy.copy( self.soup )
                except:
                    log.exception(self.log_msg('Next page not found'))
                    break
                next_page_number = next_page_number + 1
            return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log , '__addPosts')
    def __addPosts(self, post_tag):
        """ It will add Post for a particular thread
        """
        try:
            self.currenturi = post_tag.findAll('a')[-1]['href']
            if not self.__setSoup():
                return False
            replies = [ x for x in self.soup.find('td','forumlight').findParent('table').findAllNext('table','forumtext') if x.find('img',src=re.compile('.*ico_wtk.gif$'))]
            page = self.__getData(stripHtml(post_tag.findAll('span','forumauthor')[-1].renderContents()))
            self.__addSessionInfo(page)
            for reply in replies:
                try:
                    self.currenturi =  reply.findAll('a')[-1]['href']
                    if not self.__setSoup():
                        continue
                    page = self.__getData(stripHtml(reply.findAll('span','forumauthor')[-1].renderContents()))
                    self.__addSessionInfo(page)
                except:
                    log.exception(self.log_msg('Cannot get data data'))
        except:
            log.exception(self.log_msg('cannot proceed, review soup not set'))
            return False

    def __addSessionInfo(self,page):
        ''' This will add the session info
        '''
        try:
            review_hash = get_hash( page )
            if checkSessionInfo(self.genre, self.session_info_out, self.currenturi,\
                         self.task.instance_data.get('update'),parent_list\
                                                        =[self.parent_uri]):
                return False
            result=updateSessionInfo(self.genre, self.session_info_out, self.currenturi, \
                        review_hash,'Review', self.task.instance_data.get('update'),\
                                                    parent_list=[self.parent_uri])
            if not result['updated']:
                return False
            parent_list = [self.parent_uri]
            page['parent_path']=copy.copy(parent_list)
            parent_list.append(self.currenturi)
            page['path']=parent_list
            #page['id'] = result['id']
            #page['first_version_id']=result['first_version_id']
            #page['parent_id']= '-'.join(result['id'].split('-')[:-1])
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

    @logit(log , '__getThreads')
    def __getThreads( self ):
            """ It will fetch the thread info and create tasks
            """
            try:
                threads=self.soup.find('td','forumbigsubject').findAllNext('table','forumtext')
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False
            for thread in threads[:3]:
                if  self.total_posts_count > self.max_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    date_str = stripHtml(thread.find('td',align='right').renderContents())
                    try:
                        date_str = stripHtml(thread.find('td',align='right').renderContents())
                        thread_time = datetime.strptime(date_str,'%Y-%m-%d')
                    except:
                        log.exception(self.log_msg('Cannpt find the time stamp'))
                        continue
                    if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        log.info(self.log_msg('Session info return True or Reaches max count'))
                        continue
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] = thread.find('a')['href']
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
    def __getData(self,author_name):
        """ This will return the page dictionry
        """
        page = {'et_data_post_type':'Suggestion'}
        try:
            title_tag = self.soup.find('td','forumbigsubject')
            page['title'] =  stripHtml(title_tag.renderContents())
        except:
            page['title'] = ''
            log.info(self.log_msg('title not found'))
        try:
            data_str = ''
            date_str = ''
            tds = title_tag.findParent('table','forumtext').findAll('td')
            start=False
            data_str = ''
            for td in tds:
                text = stripHtml(td.renderContents())
                if td.has_key('class'):
                    if td['class'] == 'forumbigsubject':
                        start = True
                        continue
                if text.startswith(author_name):#and re.match('^' +author_name+ '.*?\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}',date_str,re.DOTALL):
                    date_str = text
                    break
                if start:
                    data_str = data_str + text + '\n'
            page['data'] = data_str.strip()
        except:
            log.info(self.log_msg('data not found'))
            page['data'] =''
        try:
            if author_name.startswith('~'):
                author_name = author_name[1:]
            page['et_author_name'] = author_name
            date_str = date_str.split(',')[-1].strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%d.%m.%Y %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            log.exception(self.log_msg('Posted date not found'))
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
        page['data'] = ''
        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['edate_last_post_date'] = self.task.pagedata['edate_last_post_date']
        except:
            log.info(self.log_msg('edate_last_post_date not extracted from page data'))
        page['et_data_post_type']= 'Question'
        title_td = self.soup.find('td','forumbigsubject')
        try:
            page['title'] = stripHtml(title_td.renderContents())
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            page['title']=''
        try:
            self.currenturi = title_td.find('a')['href']
            if self.__setSoup():
                font_tag = self.soup.find('font',color='#999999')
                try:
                    data_div = font_tag.findParent('table').findParent('table')
                    data_str = data_div.__str__()
                    font_str = font_tag.__str__()
                    font_str_ind = data_str.find(font_str )
                    data_str = data_str[font_str_ind+len(font_str):]
                    unwanted_tables = data_div.findAll('table',width=None)
                    for table in unwanted_tables:
                        data_str = data_str.replace(table.__str__(),'')
                    div_tag = data_div.find('div','a2')
                    if div_tag:
                        data_str = data_str[:(data_str.find(div_tag.__str__()))]
                    page['data'] = stripHtml(data_str)
                except:
                    log.info(self.log_msg('Data not found'))
                    page['data'] = ''
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                try:
                    font_str = stripHtml(font_tag.renderContents()).split('/')
                    page['posted_date'] = datetime.strftime(datetime.strptime(font_str[-1],'%d.%m.%Y, godz. %H:%M)'),"%Y-%m-%dT%H:%M:%SZ")
                    page['et_data_source'] = font_str[0][1:]
                except:
                    log.info(self.log_msg('time string not found'))
        except:
            log.info(self.log_msg('Post info not found'))
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
            #page['first_version_id']=result['first_version_id']
            #page['id'] = result['id']
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
            #page['data'] = ''
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