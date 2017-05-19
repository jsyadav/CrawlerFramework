'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Rakesh Soni
'''
To get sample URL, go to http://collegenet.com/elect/app/app and specify keyword (For E.g. maryland) 
   in "Quick Search". Resulted URL will be our sample URL.
Sample URL will give posts as a result. We are capturing all posts and if post is a reply then we are 
    capturing Question(First) post from Forum(parent) page.

'''
import re
import time 
import random 
from BeautifulSoup import BeautifulSoup
from datetime import datetime,timedelta
from utils.httpconnection import HTTPConnection
import logging
from urllib2 import urlparse
from tgimport import tg
import copy
from cgi import parse_qsl
import md5

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

baseuri = 'http://collegenet.com'

log = logging.getLogger('CollegeNetConnector')

class CollegeNetConnector(BaseConnector):
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch information from collegenet.com Forum
        http://www.collegenet.com/elect/app/app?service=external/MessageSearchResults&sp=S280914
        """
        
        self.genre="Review"
    
        try:
            self.parent_uri = self.currenturi
            
            self.total_threads_count = 0
            self.last_timestamp = datetime( 1980,1,1 )
            if tg.config.get(path='Connector',key='collegenet_max_threads_to_process'):
                self.max_threads_count = int(tg.config.get(path='Connector',key='collegenet_max_threads_to_process'))
            else:
                self.max_threads_count = None
            
            self.collegenetTimelag_max = tg.config.get(path='Connector', key='collegenet_search_timeLag_max')
            self.collegenetTimelag_min = tg.config.get(path='Connector', key='collegenet_search_timeLag_min')
            log.info(self.log_msg('Time Logs :::::::'))
            log.info(self.collegenetTimelag_max)
            log.info(self.collegenetTimelag_min)
            
##            if self.collegenetTimelag_min and self.collegenetTimelag_max:
##                randomTimeLag = random.randint(self.collegenetTimelag_min,self.collegenetTimelag_max) / 1000.0000
##                log.info('sleeping for %s seconds between requests'%randomTimeLag)
##                time.sleep(randomTimeLag)
            
            if not self.__setSoup():
                log.exception(self.log_msg('Soup not set ..... '))
                return False
            
            #post_no = 0 #Remove
            while True:
                #if post_no==3: #Remove
                #    break   #Remove
                #post_no = post_no + 1 #Remove
                
                currenturi = self.currenturi
                if not self.__addPosts():
                    break                
                #Get "post" information
                #break #Remove
                self.currenturi = currenturi
                if self.collegenetTimelag_min and self.collegenetTimelag_max:
                    randomTimeLag = random.randint(self.collegenetTimelag_min,self.collegenetTimelag_max) / 1000.0000
                    log.info('sleeping for %s seconds between requests'%randomTimeLag)
                    time.sleep(randomTimeLag)
                #We need to set soup again as addPosts() method is changing self.currenturi and self.soup
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set.... break while loop.'))
                    break
                
                try:
                    next_uri = baseuri + self.soup.find('div',id='search_results_controls').find('a',text=re.compile('&gt;')) \
                                .parent['href']
                    self.currenturi = next_uri
                    log.info(self.log_msg('Next URI :::::::;'))
                    log.info(next_uri)
                except:
                    log.exception(self.log_msg('Next Post link not found'))
                    break
                
##                if self.collegenetTimelag_min and self.collegenetTimelag_max:
##                    randomTimeLag = random.randint(self.collegenetTimelag_min,self.collegenetTimelag_max) / 1000.0000
##                    log.info('sleeping for %s seconds between requests'%randomTimeLag)
##                    time.sleep(randomTimeLag)
                    
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set.... break while loop.'))
                    break
                
            return True
            
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False
        
    @logit(log , '__addPosts')
    def __addPosts(self):
        """ 
            Get post informations
        """
        currenturi = self.currenturi
        
        try:
            reviews = self.soup.find('div',id='search_results_content').findAll('div','forum_post')
        except:
            log.exception(self.log_msg('Reviews not found'))
            return False
        
        for review in reviews:
            
            if self.max_threads_count and self.total_threads_count > self.max_threads_count:
                log.info(self.log_msg('Reaching maximum post,Return false'))
                return False
            self.total_threads_count = self.total_threads_count + 1
            
            try:
                parent_uri = baseuri + review.find('div','forum_post_topic').find('a')['href']
                self.parent_uri = parent_uri
            except:
                parent_uri = None
                self.parent_uri = self.currenturi

            post_type = "Question"
            
            try:
                subject = stripHtml(review.find('div','forum_post_subject').find('a').renderContents().strip())
            except:
                subject = None
                log.info(self.log_msg('Subject not found in addPost() method'))
                
            if subject:
                #Check if Subject is available
                if (subject.split(" ")[0].strip()=="Re:" and parent_uri):
                    #Check that it is reply and parent_uri is available
                    log.info(self.log_msg('Reply post ::::::;'))
                    post_type = "Reply"                
                    if not self.__addQuestionPost(self.parent_uri):
                        log.info(self.log_msg('addQuestionPost() method returned False'))
                    
            try:
                page = {}
                try:
                    page['data'] = stripHtml(review.find('div','forum_post_message').renderContents().strip())
                except:
                    log.info(self.log_msg('data not available in addPosts()'))
                    continue
                
                try:
                    page['title'] = stripHtml(review.find('div','forum_post_subject').find('a') \
                                    .renderContents().strip())
                except:
                    try:
                        if len(page['data']) > 50:
                            page['title'] = page['data'][:50] + '...'
                        else:
                            page['title'] = page['data']
                    except:
                        page['title'] = ''
                        log.exception(self.log_msg('title not found'))
                        continue
                    
                try:
                    unique_key = get_hash( {'data':page['data'],'title':page['title']})
                except:
                    log.exception(self.log_msg('unique_key not found'))
                    continue
                
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                    self.task.instance_data.get('update'),parent_list\
                        =[self.parent_uri]):
                    log.info(self.log_msg('session info return True'))
                    continue
                
                try:
                    date_str = stripHtml(review.find('div','forum_post_date').renderContents().strip())
                    thread_time = self.__getPostDateTime(date_str)
                    if thread_time:
                        page['posted_date'] = datetime.strftime(thread_time,"%Y-%m-%dT%I:%M:%SZ")
                    else:
                        log.info(self.log_msg('posted date returned None'))
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                except:
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    log.info(self.log_msg('posted date not found, taking current date.'))
                    
                try:
                    page['et_author_name'] = stripHtml(review.find('span','forum_post_author_name') \
                                            .renderContents().strip())
                except:
                    log.info(self.log_msg('Author name not available'))    
                    
                try:
                    if review.find('div','forum_post_author_info').find('span'):
                        joining_date = stripHtml(review.find('div','forum_post_author_info').findAll('br')[1] \
                                        .next.strip()).split(":")[-1].split("<")[0].strip()
                        joining_date = self.__getAuthorJoiningDate(joining_date)
                        if joining_date:
                            page['edate_author_joining_date'] = joining_date
                        else:
                            log.info(self.log_msg('Author joining date not available'))
                    else:
                        joining_date = stripHtml(review.find('div','forum_post_author_info').next.strip()) \
                                        .split(":")[-1].strip()
                        joining_date = self.__getAuthorJoiningDate(joining_date)
                        if joining_date:
                            page['edate_author_joining_date'] = joining_date
                        else:
                            log.info(self.log_msg('Author joining date not available'))
                except:
                    log.info(self.log_msg('Author joining date not available'))
                        
                try:
                    if review.find('div','forum_post_author_info').find('span'):
                        page['et_author_location'] = stripHtml(review.find('div','forum_post_author_info').findAll('br')[1] 
                                                    .next.next.strip()).split(":")[-1].split("<")[0].strip()
                    else:
                        page['et_author_location'] = stripHtml(review.find('div','forum_post_author_info') \
                                                    .find('br').next.strip()).split(":")[-1].strip()
                except:
                    log.info(self.log_msg('Author location not available'))
                
                try:
                    page['ei_author_post_counts'] = int(stripHtml(review.find('div','forum_post_author_info').findAll('br')[-1] \
                                                .next.strip()).split(":")[-1].strip())
                except:
                    log.info(self.log_msg('Author post count not available'))
                    
                    
                try:
                    page['entity'] = page['et_data_post_type'] = post_type
                except:
                    log.info(self.log_msg('Post type not available'))
                
                review_hash = get_hash( page )
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
                #page['entity'] = 'Review'
                page['category'] = self.task.instance_data.get('category','')
                page['task_log_id']=self.task.id
                page['uri'] = currenturi
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                
                self.pages.append( page )
                log.info(page)
                log.info(self.log_msg('Post Added'))
            except:
                log.info(self.log_msg('Error while adding post'))
                
        return True
                    
    @logit(log , '__addQuestionPost')
    def __addQuestionPost(self,currenturi):
        """ 
            Get Question(parent) post informations
        """
##        if self.collegenetTimelag_min and self.collegenetTimelag_max:
##            randomTimeLag = random.randint(self.collegenetTimelag_min,self.collegenetTimelag_max) / 1000.0000
##            log.info('sleeping for %s seconds between requests'%randomTimeLag)
##            time.sleep(randomTimeLag)
##                
        self.currenturi = currenturi
        if not self.__setSoup():
            log.exception(self.log_msg('Soup not set ..... '))
            return False
        
        
        try:
            review = self.soup.find('div',id='forum_content').find('div','forum_post')
        except:
            log.exception(self.log_msg('Reviews not found'))
            return False
        
        page = {}
        post_type = "Question"
        try:
            try:
                page['data'] = stripHtml(review.find('div','forum_post_message').renderContents().strip())
            except:
                log.info(self.log_msg('data not available in addPosts()'))
                return False
            try:
                page['title'] = stripHtml(review.find('div','forum_post_subject').find('a').renderContents().strip())
            except:
                try:
                    if len(page['data']) > 50:
                        page['title'] = page['data'][:50] + '...'
                    else:
                        page['title'] = page['data']
                except:
                    page['title'] = ''
                    log.exception(self.log_msg('title not found'))
                    return False
            try:
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
            except:
                log.exception(self.log_msg('unique_key not found'))
                return False
            
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                self.task.instance_data.get('update'),parent_list\
                    =[self.parent_uri]):
                log.info(self.log_msg('session info return True'))
                return False
            try:
                date_str = stripHtml(review.find('div','forum_post_date').renderContents().strip())
                thread_time = self.__getPostDateTime(date_str)
                if thread_time:
                    page['posted_date'] = datetime.strftime(thread_time,"%Y-%m-%dT%I:%M:%SZ")
                else:
                    log.info(self.log_msg('posted date returned None'))
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                log.info(self.log_msg('posted date not found, taking current date.'))
                
            try:
                page['et_author_name'] = stripHtml(review.find('span','forum_post_author_name') \
                                        .renderContents().strip())
            except:
                log.info(self.log_msg('Author name not available'))
                
            try:
                if review.find('div','forum_post_author_info').find('span'):
                    joining_date = stripHtml(review.find('div','forum_post_author_info').findAll('br')[1] \
                                    .next.strip()).split(":")[-1].split("<")[0].strip()
                    joining_date = self.__getAuthorJoiningDate(joining_date)
                    if joining_date:
                        page['edate_author_joining_date'] = joining_date
                    else:
                        log.info(self.log_msg('Author joining date not available'))
                else:
                    joining_date = stripHtml(review.find('div','forum_post_author_info').next.strip()) \
                                    .split(":")[-1].strip()
                    joining_date = self.__getAuthorJoiningDate(joining_date)
                    if joining_date:
                        page['edate_author_joining_date'] = joining_date
                    else:
                        log.info(self.log_msg('Author joining date not available'))
            except:
                log.info(self.log_msg('Author joining date not available'))
                    
            try:
                if review.find('div','forum_post_author_info').find('span'):
                    page['et_author_location'] = stripHtml(review.find('div','forum_post_author_info').findAll('br')[1] 
                                                .next.next.strip()).split(":")[-1].split("<")[0].strip()
                else:
                    page['et_author_location'] = stripHtml(review.find('div','forum_post_author_info') \
                                                .find('br').next.strip()).split(":")[-1].strip()
            except:
                log.info(self.log_msg('Author location not available'))
            
            try:
                page['ei_author_post_counts'] = int(stripHtml(review.find('div','forum_post_author_info').findAll('br')[-1] \
                                            .next.strip()).split(":")[-1].strip())
            except:
                log.info(self.log_msg('Author post count not available'))
                
            try:
                page['entity'] = page['et_data_post_type'] = post_type
            except:
                log.info(self.log_msg('Post type not available'))
            
            review_hash = get_hash( page )
            result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                        review_hash,'Review', self.task.instance_data.get('update'),\
                                                    parent_list=[self.parent_uri])
            if not result['updated']:
                return False
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
            #page['entity'] = 'Review'
            page['category'] = self.task.instance_data.get('category','')
            page['task_log_id']=self.task.id
            page['uri'] = self.currenturi
            page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
            
            self.pages.append( page )
            log.info(page)
            log.info(self.log_msg('Question Post Added'))
            
        except:
            return False
            log.exception(self.log_msg('Error in addPosts'))
            
        return True
        
        
    @logit(log , '__getPostDateTime')
    def __getPostDateTime(self,date_str):
        try:
            date_time_list = date_str.split(' ')
            date_str = date_time_list[1] + " " + date_time_list[2] + " " + date_time_list[3] + " " \
                        + date_time_list[5] + " " + date_time_list[6]
            thread_time = datetime.strptime(date_str,"%b %d, %Y %H:%M %p")
            '''
            if date_time_list[-1] == 'PDT':
                interval = timedelta(seconds=0,minutes=0,hours=7,days=0)
                thread_time = thread_time-interval
                #substracting 7 hours from thread_time as PDT is 7 hours behind of UTC
            '''
        except:
            log.info(self.log_msg('Error while getting posted date'))
            return None
        
        return thread_time
    
    @logit(log , '__getAuthorJoiningDate')
    def __getAuthorJoiningDate(self,date_str):
        try:
            thread_time = datetime.strftime(datetime.strptime(date_str,"%b %d, %Y"),"%Y-%m-%dT%I:%M:%SZ")
        except:
            log.info(self.log_msg('Error while getting author joining date'))
            return None
        
        return thread_time
    
    @logit(log, "__setSoup")
    def __setSoup( self, url = None, data=None, headers={}):        
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
   