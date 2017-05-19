
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
from cgi import parse_qsl
import copy

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('CsharpCornerConnector')
class CsharpCornerConnector(BaseConnector):
    '''
    This will fetch the info for c sharp corner
    Sample uris is
    http://www.c-sharpcorner.com/Forums/ShowPost.aspx?PostID=2
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of c sharp corner
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            self.total_posts_count = 0
            self.last_timestamp = datetime( 1980,1,1 )
            self.max_posts_count = int(tg.config.get(path='Connector',key='csharp_corner_max_threads_to_process'))
            headers={'Host':'www.c-sharpcorner.com'}
            headers['Referer'] = self.currenturi
            data = dict(parse_qsl(self.currenturi.split('?')[-1]))
            if self.currenturi.startswith('http://www.c-sharpcorner.com/Forums/ShowMessages'):
                if not self.__setSoup(data=data,headers=headers):
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                self.post_type = True
                while True:
                    self.__addPosts()
                    try:
                        data = dict(parse_qsl(self.currenturi.split('?')[-1]))
                        data['ctl00$ContentPlaceHolder1$ddlMessageCount'] = '20'
                        data['ctl00$ContentPlaceHolder1$ddlOrder'] ='Desc'
                        data['__EVENTTARGET'] = self.soup.find('a',id=re.compile('LinkButtonNext'))['id'].replace('_','$')
                        jscript_arg = ['__EVENTVALIDATION','__VIEWSTATE']
                        for each in jscript_arg:
                            data[each] =  self.soup.find('input',id=each)['value']
                        if not self.__setSoup(data=data,headers=headers):
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                return True
            else:
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                while True:
                    if not self.__getThreadPage():
                        break
                    try:
                        data = dict(parse_qsl(self.currenturi.split('?')[-1]))
                        data['ctl00$ContentPlaceHolder1$ddlMessageCount'] = '20'
                        data['ctl00$ContentPlaceHolder1$ddlOrder'] ='Desc'
                        data['__EVENTTARGET'] = self.soup.find('a',id=re.compile('LinkButtonNext'))['id'].replace('_','$')
                        jscript_arg = ['__EVENTVALIDATION','__VIEWSTATE']
                        for each in jscript_arg:
                            data[each] =  self.soup.find('input',id=each)['value']
                        if not self.__setSoup(data=data,headers=headers):
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
            reviews = self.soup.findAll('table',id='tblTitle')
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        for i, review in enumerate(reviews):
            if i==0 and self.post_type:
                post_type = "Question"
                self.post_type = False
            else:
                post_type = "Suggestion"
            try:
                page = self.__getData( review, post_type )
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                #unique_key = stripHtml(review.findNext('a',id=re.compile('PostLink')).renderContents()).split('#')[-1]
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    log.info(self.log_msg('Session info returns True'))
                    continue
                
            except:
                log.info(self.log_msg('unique key not found'))
                continue
            try:
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            get_hash( page ),'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    continue
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
            and add the tasks
            """
            threads = [x.findParent('tr') for x in self.soup.find('table',id=re.compile('ViewAllThread')).findAll('a','ForumPostHead')]
            for thread in threads:
                try:
                    thread_info = thread.findAll('td',recursive=False)
                    if not len(thread_info)==6:
                        log.info(self.log_msg('Not enough fiels'))
                        continue
                    last_post_info = [x.strip() for x in stripHtml(thread_info[-1].renderContents()).split('\n')]
                    thread_time = datetime.strptime( last_post_info[0],'%m/%d/%Y %I:%M:%S %p   by')
                    #page['edate_thread_last_post_date'] = datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                except:
                    log.exception(self.log_msg('Todays Post, so ignoring'))
                    continue
                if  self.total_posts_count > self.max_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    if checkSessionInfo('Search',self.session_info_out, thread_time,\
                                        self.task.instance_data.get('update')) and \
                                        self.max_posts_count >= self.total_posts_count:
                        continue
                    temp_task=self.task.clone()
                    try:
                        title_tag = thread_info[1].find('a','ForumPostHead')
                        temp_task.pagedata['title']= stripHtml(title_tag.renderContents())
                        temp_task.instance_data[ 'uri' ] =  title_tag['href']
                        log.info(stripHtml(title_tag.renderContents()))
                    except:
                        log.info(self.log_msg('Cannot find the uri'))
                        continue
                    try:
                        temp_task.pagedata['et_author_name'] = stripHtml(thread_info[2].renderContents())
                    except:
                        log.info(self.log_msg('Cannot find author name'))
                    try:
                        temp_task.pagedata['et_thread_last_post_author']  = last_post_info[-1]
                    except:
                        log.info(self.log_msg('Cannot find the replies count'))
                    try:
                        view_reply = {'ei_thread_replies_count':3,'ei_thread_views_count':4}
                        for each in view_reply.keys():
                            temp_task.pagedata[each] = int(stripHtml(thread_info[view_reply[each]].renderContents()))
                    except:
                        log.info(self.log_msg('Cannot find the views count'))
                    try:
                        temp_task.pagedata['edate_last_post_date']=  datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg('Cannot find the last posted'))
                    self.linksOut.append( temp_task )
                    log.info(self.log_msg('Task Added'))
                except:
                    log.info(self.log_msg('Cannot add the Task'))
            return True

    @logit(log, '__getData')
    def __getData(self, review, post_type ):
        """ This will return the page dictionry
        """
        page = {'title':''}
        try:
            author_tag = review.find('a',id=re.compile('lnkBtnAuthor'))
            page['et_author_name'] = stripHtml(author_tag.renderContents())
            author_info_tag = author_tag.findParent('td')
            #log.info ([stripHtml(x.renderContents()) for x in  author_tag.findParent('td').findAll('span',recursive=False)])
            for each in [stripHtml(x.renderContents()) for x in  author_tag.findParent('td').findAll('span',recursive=False)]:
                try:
                    if each.startswith('posted'):
                        page['ei_author_posts_count'] = int(re.search('\d+',each.split('\n')[-1]).group())
                    if each.startswith('since'):
                        page['edate_author_member_since'] = datetime.strftime(datetime.strptime(each.split('\n')[-1].strip(),'%b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
                    if each.startswith('from'):
                        author_location = each.replace('from','').strip()
                        if not author_location=='':
                            page['et_author_location'] = author_location
                except:
                    log.info(self.log_msg('Author info not found'))
        except:
            log.info(self.log_msg('author name not found'))
        try:
            page['et_author_title'] = stripHtml(review.find('a',id=re.compile('AuthorAwardImage'))['title'])
        except:
            log.exception(self.log_msg('Author title not found'))
        try:
            page['data'] = stripHtml(review.find('table',id=re.compile('tableTitle')).find('td','VAlignTop').renderContents())
            #page['data'] = stripHtml(review.find('table',id=re.compile('tableTitle')).findAll('span','ForumNormalText')[-2].renderContents())
        except:
            log.info(self.log_msg('data not found'))
            page['data'] =''
        try:
            page['title']  = stripHtml(review.find('td',id=re.compile('ColTitle')).renderContents())
            log.info(page['title'])
        except:
            log.info(self.log_msg('Totle not found'))
            page['title']=''
        try:
            if page['title']=='':
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
        try:
            page['et_data_reply_to'] = self.thread_id
        except:
            log.info(self.log_msg('data reply to is not found'))
        try:
            date_str = stripHtml(review.find('span',id=re.compile('lblPosted')).parent.renderContents()).split('\n')[-1].strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%m/%d/%Y %I:%M:%S %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Posted date not found for this post'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['et_data_post_type'] = post_type
        except:
            log.info(self.log_msg('Page info is missing'))
        try:
            page['et_data_forum'] = self.hierarchy[0]
            page['et_data_subforum'] = self.hierarchy[1]
            page['et_data_topic'] = self.hierarchy[2  ]
        except:
            log.exception(self.log_msg('data forum not found'))
        return page

    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will get the parent info
        """
        page = {}
        try:
            self.hierarchy =  page['et_thread_hierarchy'] = ['Csharp Corner Forums'] + [x.replace(u'\xbb','').strip() for x in stripHtml(self.soup.find('table',id='Table2').renderContents()).split('\n')][1:]
            page['title']= page['et_thread_hierarchy'][-1]
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            page['title']=''
        try:
            self.thread_id =  page['et_thread_id'] = dict(parse_qsl(self.currenturi.split('?')[-1]))['ThreadID']
        except:
            log.info(self.log_msg('Thread id not found'))
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        for each in ['et_author_name','et_thread_last_post_author','ei_thread_views_count','ei_thread_replies_count','edate_last_post_date']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('page data cannot be extracted for %s'%each))
        try:
            post_hash = get_hash( page )
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo( self.genre, self.session_info_out, self.\
                   parent_uri, post_hash,'Forum',self.task.instance_data.get('update'), Id=id)
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