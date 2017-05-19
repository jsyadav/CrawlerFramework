'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
import copy
from datetime import datetime,timedelta
import logging
from urllib2 import urlparse
from tgimport import tg

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('VmWareCommunitiesConnector')
class VmWareCommunitiesConnector(BaseConnector):
    '''
    This will fetch the info for vmware community forums
    Sample uris is 
    http://communities.vmware.com/community/vmtn/server
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of http://communities.vmware.com
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            remove_session_re = re.compile(';jsessionid=.*?\?')
            if self.currenturi.startswith('http://communities.vmware.com/thread'):
                try:
                    # can be used for date filter, it should be in the format of year/mm/dd
                    self.least_posted_date = tg.config.get(path='Connector',key='vmwareforum_least_posted_date')
                    if  self.least_posted_date:
                        self.least_posted_date = datetime.strptime(self.least_posted_date,'%Y/%m/%d')
                except:
                    log.info(self.log_msg('no least posted date is given'))
                    self.least_posted_date = datetime.strptime('1980/01/01','%Y/%m/%d')
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                self.post_type= True
                while True:
                    self.__addPosts()
                    try:
                        self.currenturi = 'http://communities.vmware.com' + \
                            remove_session_re.sub('?',self.soup.find('a',text='Next').parent['href'])
                    except:
                        log.info(self.log_msg('Next page not set'))
                        break
                    if not self.__setSoup():
                        log.info(self.log_msg('cannot continue'))
                        break
                try:
                    if self.least_posted_date:
                        for page in pages[:]:
                            if datetime.strptime(page['posted_date'],"%Y-%m-%dT%H:%M:%SZ")> self.least_posted_date:
                                pages.remove(page)
                except:
                    log.info(self.log_msg('Cannot remove page'))
                return True
            else:
                try:
                    max_posts_count = int(tg.config.get(path='Connector',key='vmwareforum_threads'))
                except:
                    max_posts_count = 50
                self.currenturi +='?view=discussions&numResults=%s&filter=all'%str(max_posts_count)
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                urls = [remove_session_re.sub('?',x.find('a')['href']) for x in self.soup.findAll('td','jive-table-cell-subject')]
                for url in urls:
                    temp_task = self.task.clone()
                    temp_task.instance_data['uri']=url
                    self.linksOut.append(temp_task)
                    log.info(self.log_msg('Task Added'))
                return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False
        
    @logit(log , '__addPosts')
    def __addPosts(self):
        """this will add the post and reply
        """
        try:
            reviews =self.soup.find('div','jive-thread-messages').findAll('div',recursive=False)
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
            if post_type=='Question':
                page= self.__getDataForQuestion(review)
            else:
                page = self.__getData(review )
            try:
                review_hash = get_hash( page )
                try:
                    if not post_type=='Question':
                        unique_key = review.find('a').get('name')
                        if not unique_key:
                            unique_key = get_hash( {'data':page['data'],'title':page['title']})
                    else:
                        unique_key = self.parent_uri
                except:
                    log.exception(self.log_msg('unique key not found'))
                    unique_key=review_hash
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    continue
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            review_hash,page['entity'], self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    continue
                parent_list = [ self.parent_uri ]
                page['parent_path'] = parent_list[:]
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
                page['category'] = self.task.instance_data.get('category','')
                page['task_log_id']=self.task.id
                page['uri'] = self.currenturi
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append( page )
                #log.info(page)
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))
    
    @logit(log, '__getDataForQuestion')
    def __getDataForQuestion(self, reply):
        '''This will return the question details
        '''
        page={'entity':'question','et_data_post_type':'Question'}
        try:
            page['title']=stripHtml(self.esoup.find('h2').renderContents())
            page['et_data_hierarchy'] = self.hierarchy
        except:
            log.info(self.log_msg('title not found'))
            page['title']=''
        try:
            author_info = reply.find('div','jive-author').find('em')
            page['et_author_name'] = stripHtml(author_info.findPrevious('a').renderContents())
            author_info_str = [x.strip() for x in stripHtml(author_info.renderContents()).split('\n')]
            page['ei_author_posts_count']= int(re.search('\d+',author_info_str[0]).group())
            page['edate_author_member_since'] = datetime.strftime(datetime.strptime(author_info_str[1].replace('>',''),'%b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('Author name not found'))
        try:
            page['posted_date'] = datetime.strftime(datetime.strptime(stripHtml(\
                    self.soup.find('h3').renderContents()),'%b %d, %Y %I:%M %p')\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['data']=stripHtml(reply.find('div','jive-thread-post-message').renderContents())
        except:
            log.info(self.log_msg('No data found'))
            page['data']=''
        return page

        
            
            
    
    @logit(log, '__getData')
    def __getData(self, reply):
        """ This will fetch the reply data
        """
        page = {'title':'','et_data_post_type':'Suggestion','entity':'answer'}
        
        remove_session_re = re.compile(';jsessionid=.*?#')
        try:
            author_info = reply.find('div','jive-author').find('em')
            page['et_author_name'] = stripHtml(author_info.findPrevious('a').renderContents())
            author_info_str = [x.strip() for x in stripHtml(author_info.renderContents()).split('\n')]
            page['ei_author_posts_count']= int(re.search('\d+',author_info_str[0]).group())
            page['edate_author_member_since'] = datetime.strftime(datetime.strptime(author_info_str[1].replace('>',''),'%b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Author name not found'))
        try:
            class_name = reply.get('class').strip()
            if 'correct' in class_name:
                page['et_answer_type']='Correct Answer'
            elif 'helpful' in class_name:
                page['et_answer_type'] = 'Helpful Answer'
        except:
            log.info(self.log_msg('cannot get the answer type'))
        try:
            page['uri'] = self.currenturi
            link_tag = reply.find('a',title='Link to reply')
            page['uri'] = 'http://communities.vmware.com' + remove_session_re.sub('#',link_tag['href'])
            date_str  = stripHtml(link_tag.renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%b %d, %Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")            
        except:
            log.info(self.log_msg('Posted date not found for this post'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['data']  =  stripHtml(reply.find('div','jive-thread-reply-message').renderContents())
            if page['data']=='':
                return False
        except:
            page['data'] = ''
            log.info(self.log_msg('Data not found for this post'))
        try:
            page['title'] = stripHtml(reply.find('div','thread-reply-bar').find('strong').renderContents())            
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
        try:
            if len(page['data']) > 50:
                page['title'] = page['data'][:50] + '...'
            else:
                page['title'] = page['data']
        except:
            log.info(self.log_msg('Title not found'))
            page['title']=''
        try:
            page['et_data_hierarchy'] = self.hierarchy
        except:
            log.info(self.log_msg('data forum not found'))
        return page

    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will get the parent info
        thread url.split('&')[-1].split('=')[-1]

        """
        page = {}
        try:
            self.hierarchy =  page['et_thread_hierarchy'] = ' > '.join([x.strip() for x in stripHtml(self.soup.find('div',id='jive-breadcrumb-custom').renderContents()).split('>')][:-1])            
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            
        try:
            answer_header = self.soup.find('div',id='jive-answer-bar')
            page['et_question_answered'] = stripHtml(answer_header.h4.span.renderContents())
        except:
            log.info(self.log_msg('Question answered not found'))
        info_dict = {'correct':'jive-answer-correct','helpful':'jive-answer-helpful'}
        for each in info_dict.keys():
            try:
                pts_answer_tag = answer_header.find('span',info_dict[each])
                if pts_answer_tag:
                    try:
                        page['et_question_%s_answers_count'%each] = int(stripHtml(pts_answer_tag.strong.renderContents()))
                        page['et_question_%s_answers_points'%each] = int(re.search('\((\d+) pts\)',stripHtml(pts_answer_tag .renderContents())).group(1))
                    except:
                        log.info(self.log_msg('Correct Questions and Answers available'))
            except:
                log.info(self.log_msg('no Corrent answers available'))
        try:
            page['title']=stripHtml(self.soup.find('h2').renderContents())
        except:
            log.info(self.log_msg('title not found'))
            page['title']=''
        try:
            page['et_last_post_author_name'] = stripHtml(self.soup.find('span','jive-thread-info').renderContents()).split('\n')[-1]
        except:
            log.info(self.log_msg('Author name not found'))
        try:
            page['posted_date'] = datetime.strftime(datetime.strptime(stripHtml(\
                    self.soup.find('h3').renderContents()),'%b %d, %Y %I:%M %p')\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")        
        try:
            if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info return True, Already exists'))
                return False
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
            page['versioned'] = self.task.instance_data.get('versioned',False)
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