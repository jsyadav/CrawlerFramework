'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Rakesh Soni

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


initial_uri = 'http://www.ratemyprofessors.com/jive/vodka/viewForum.jsp?forum=2&range=100'
baseuri = 'http://www.ratemyprofessors.com/jive/vodka/' # Strange as it seems, this IS the baseuri

log = logging.getLogger('RateMyProfessorsConnector')


class RateMyProfessorsConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch information from gocollegeforums Forum
        http://www.ratemyprofessors.com/jive/vodka/viewForum.jsp?forum=2&range=100
        """
        
        self.genre="Review"
        log.info(self.log_msg('Within fetch :::::::::::::::::::::::::::::::::'))
        
        try:
            self.parent_uri = self.currenturi
            if self.currenturi==initial_uri:
                log.info(self.log_msg('Within If :::::::::::::::::::::::::::::::::'))
                self.total_threads_count = 0
                self.last_timestamp = datetime( 1980,1,1 )
                self.max_threads_count = int(tg.config.get(path='Connector',key='ratemyprofessors_max_threads_to_process'))
                
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                page_no = 1
                while True:
                    log.info(self.log_msg('Page no ========== ' + str(page_no)+'  :::::::::::   '))
                    if not self.__getThreads():
                        log.info(self.log_msg('Error in getThreads(), returned False'))
                        break
                    
                    #break #Remove
                
                    try:
                        next_uri = baseuri + self.soup.find('a',text=re.compile('Next 100 threads')).parent['href']
                        self.currenturi = next_uri
                        log.info(self.log_msg('Next URI == ' + next_uri + ' :::::::::::'))
                    except:
                        log.info(self.log_msg('Next URI == ' + next_uri + ' :::::::::::'))
                        log.info(self.log_msg('Next Page link not found'))
                        break
                    
                    if not self.__setSoup():
                        log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                        break
                    page_no += 1
                    
                    
                    
                    
                #self.linksOut = self.linksOut[:1] #Remove it after testing
                if self.linksOut:
                    updateSessionInfo('Search', self.session_info_out,self.last_timestamp , None, \
                            'ForumThreadsPage', self.task.instance_data.get('update'))
                log.info(self.log_msg('Out of while loop for getThreads'))
                
                return True
            else:
                log.info(self.log_msg('In else part of fetch() method'))
                log.info(self.log_msg('Current URI...................'))
                log.info(self.log_msg(self.currenturi))
                
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                
                self.__getParentPage()
                
                log.info(self.log_msg('getParentPage Over ...............................'))
                self.post_type= True
                
                post_page = 0 #Remove
                
                log.info(self.log_msg('Entering into addPosts()'))
                self.__addPosts()                    
                #Get "post" information
                
                log.info(self.log_msg('Returned from addPosts()'))
                
                    
                return True
            
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False
    
        
    @logit(log , '__addPosts')
    def __addPosts(self):
        """ 
            Get post informations
        """
        try:
            reviews = self.soup.findAll('table',bgcolor='#cccccc',cellpadding='0',cellspacing='0',border='0',width='100%')
            log.info(self.log_msg('reviews found'))
            #log.info(len(reviews))
            #log.info(reviews)
        except:
            log.exception(self.log_msg('Reviews not found'))
            return False
        
        
        for i, review in enumerate(reviews[:]):
            
            page = {}
            
            post_type = "Question"
            if i==0 and self.post_type:
                post_type = "Question"
                self.post_type = False
            else:
                post_type = "Suggestion"
                
            try:
                try:
                    page['data'] = stripHtml(review.find('span','messageHeader').findParent('table').findAll('tr')[1] \
                                    .renderContents().strip())
                except:
                    page['data'] = ''
                    log.exception(self.log_msg('Data not found for this post'))
                    
                log.info(page['data'])
            
                try:
                    page['title'] = stripHtml(review.find('span','messageHeader').renderContents().strip())
                except:
                    try:
                        if len(page['data']) > 50:
                            page['title'] = page['data'][:50] + '...'
                        else:
                            page['title'] = page['data']
                    except:
                        page['title'] = ''
                        log.exception(self.log_msg('title not found'))            
                        
                log.info(page['title'])
                
                
                try:
                    unique_key = get_hash({'data':page['data'],'title':page['title']})
                    log.info(self.log_msg('unique_key not available, using hash of data and title as unique key ::'))
                    log.info(unique_key)                    
                except:
                    log.exception(self.log_msg('unique_key not found'))
                    continue
                
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                                 self.task.instance_data.get('update'),parent_list\
                                                                =[self.parent_uri]):
                    log.info(self.log_msg('session info return True'))
                    continue
              
                try:
                    page['et_author_name'] = stripHtml(review.find('span','messageAuthor').renderContents()).split(':')[1].strip()
                    log.info(self.log_msg(page['et_author_name']))
                except:
                    log.info(self.log_msg('Author name not available'))
               
                try:
                    date_str = review.find('small','date').next.string.strip()
                
                    try: #Remove
                        date_time_list = date_str.split(' ')
                        time_value = date_time_list[1]
                        time_var = date_time_list[2]
                    except e: #Remove
                        log.info(self.log_msg('time_value and time_var not found')) #Remove
                        #raise e
                    
                    try: #Remove    
                        today = datetime.utcnow()              
                    except e: #Remove
                        log.info(self.log_msg('today date not found')) #Remove
                        #raise e
                    
                    
                    if time_var=='seconds' or time_var=='second':
                        log.info(self.log_msg('Within "seconds..........."'))
                        time_value = int(time_value)
                        interval = timedelta(seconds=time_value,minutes=0,hours=0,days=0)
                        diff = today-interval
                        thread_time = datetime.strftime(diff,"%m-%d-%Y %I:%M %p")
                        thread_time =  datetime.strptime(thread_time,'%m-%d-%Y %I:%M %p')                    
                    elif time_var=='minutes' or time_var=='minute':
                        log.info(self.log_msg('Within "minutes..........."'))
                        time_value = int(time_value)
                        try:
                            interval = timedelta(seconds=0,minutes=time_value,hours=0,days=0)
                        except e:
                            log.info(self.log_msg('interval not found .......................')) #Remove
                            #raise e
                        try:
                            diff = today-interval
                        except e:
                            log.info(self.log_msg('diff not found .................')) #Remove
                            #raise e
                        try:
                            thread_time = datetime.strftime(diff,"%m-%d-%Y %I:%M %p")
                        except e:
                            log.info(self.log_msg('problem in strftime ...................')) #Remove
                            #raise e
                        try:
                            thread_time =  datetime.strptime(thread_time,'%m-%d-%Y %I:%M %p')
                        except e:
                            log.info(self.log_msg('problem in strptime ....................')) #Remove
                            #raise e
                    elif time_var=='hours' or time_var=='hour':
                        log.info(self.log_msg('Within "hours ..........."'))
                        time_value = int(time_value)
                        interval = timedelta(seconds=time_value,minutes=0,hours=0,days=0)
                        diff = today-interval
                        thread_time = datetime.strftime(diff,"%m-%d-%Y %I:%M %p")
                        thread_time =  datetime.strptime(thread_time,'%m-%d-%Y %I:%M %p')                    
                        
                    elif date_time_list[0]=='Today':
                        log.info(self.log_msg('Within "Today ..........."'))
                        current_date = datetime.today()
                        date_str = str(current_date.month)+'-'+str(current_date.day)+'-'+str(current_date.year) \
                                    +' '+date_time_list[2]
                        thread_time = datetime.strptime(date_str,"%m-%d-%Y %I:%M%p")                        
                    else:
                        log.info(self.log_msg('Within date ...........'))
                        splitted_date = date_str.split(' ')
                        today = datetime.utcnow()

                        date_str = splitted_date[2]+' '+splitted_date[3]+' '+ str(today.year)+ \
                                    ' '+splitted_date[5]+' ' +splitted_date[6]
                                    
                        thread_time = datetime.strptime(date_str,'%b %d %Y %I:%M %p')
                        log.info(self.log_msg('Within date, Thread time =============='))
                        log.info(thread_time)
                        
                    page['posted_date'] = datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                except:
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    log.exception(self.log_msg('posted date not found, taking current date.'))
                    
                log.info(self.log_msg('Posted date :::::::::::'))
                log.info(page['posted_date'])
                
                try:
                    page['et_data_post_type'] = post_type
                except:
                    log.info(self.log_msg('Page info is missing'))
                    
                try:
                    page['et_data_forum'] = self.hierarchy[0]
                    page['et_data_subforum'] = self.hierarchy[1]
                    page['et_data_topic'] = self.forum_title
                except:
                    log.info(self.log_msg('data forum not found'))
                    
                log.info(page)
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
                page['entity'] = 'Review'
                page['category'] = self.task.instance_data.get('category','')
                page['task_log_id']=self.task.id
                page['uri'] = self.currenturi
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                
                self.pages.append( page )
                
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))
                
                
            
    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
            Get the parent information
        """
        
        page = {}
        
        try:
            hierarchy = [stripHtml(x.renderContents().strip() for x in \
                                self.soup.find('div',id='breadcrumb_links').findAll('a'))]
            temp = stripHtml(self.soup.find('div',id='breadcrumb_links').findAll('a')[-1].nextSibling)
            
            hierarchy.append(temp)
            self.hierarchy = page['et_thread_hierarchy'] = hierarchy
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            
        for each in ['title','ei_thread_replies_count','et_thread_author_name']:
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
            if not page.has_key('posted_date'):
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
    
    
    @logit(log , '__getThreads')
    def __getThreads( self ):
        """
            Get thread information and create tasks.
        """
        try:
            threads = self.soup.find('table','forumBkg').findAll('tr','rowBkg')
        except:
            log.exception(self.log_msg('No thread found, cannot proceed'))
            return False
        
        for thread in threads:
            if self.total_threads_count > self.max_threads_count:
                log.info(self.log_msg('Reaching maximum post,Return false'))
                return False
            self.total_threads_count = self.total_threads_count + 1
            
            log.info(self.log_msg('No.of Threads ==========='))
            log.info(self.total_threads_count)
    
            try:
                thread_uri = baseuri + thread.find('a','forum2')['href']
                log.info(self.log_msg('Thread URI .............................'))
                log.info(self.log_msg(thread_uri))
            except:
                log.info(self.log_msg('Thread uri not available'))
                continue
            
            try:
                thread_title = stripHtml(thread.find('a','forum2').renderContents().strip())
                log.info(self.log_msg('Thread Title .............................'))
                log.info(self.log_msg(thread_title))
            except:
                log.info(self.log_msg('Thread title not available'))
                continue
            
            try:
                thread_replies_count =  int(thread.findAll('td','center')[1].renderContents().strip())
                log.info(self.log_msg('Thread replies count .............................'))
                log.info(thread_replies_count)
            except:
                log.info(self.log_msg('Thread replies count not available'))
                #continue
    
            try:
                date_str =  thread.find('td','date').renderContents().strip()                   
                #log.info(self.log_msg('Thread replies count .............................'))
                #log.info(self.log_msg(thread_replies_count))
            except:
                log.info(self.log_msg('Thread replies count not available'))
                continue
    
            try: #Remove
                date_time_list = date_str.split(' ')
                time_value = date_time_list[0]
                time_var = date_time_list[1]
            except: #Remove
                log.info(self.log_msg('time_value and time_var not found')) #Remove
                continue #Remove
            
            try: #Remove    
                today = datetime.utcnow()              
            except: #Remove
                log.info(self.log_msg('today date not found')) #Remove
                continue #Remove
            
            try: #Remove
                if time_var=='hours' or time_var=='hour':
                    log.info(self.log_msg('Within "hours..........."'))
                    time_value = int(time_value)
                    interval = timedelta(seconds=0,minutes=0,hours=time_value,days=0)
                    diff = today-interval
                    thread_time = datetime.strftime(diff,"%m-%d-%Y %I:%M %p")
                    thread_time =  datetime.strptime(thread_time,'%m-%d-%Y %I:%M %p') 
                
                else:
                    log.info(self.log_msg('Within "date ..........."'))
                    try:
                        date_str_splitted = date_str.split(' ')
                    except:
                        log.info(self.log_msg('problem in splitting ....................')) #Remove
                        continue #Remove
                    
                    try:
                        date_str = date_str_splitted[1]+' ' +date_str_splitted[2]+' '+str(today.year) \
                                    +' '+date_str_splitted[4]+' '+date_str_splitted[5]
                    except:
                        log.info(self.log_msg('problem in joining ....................')) #Remove
                        continue #Remove
                    
                    try:
                        thread_time = datetime.strptime(date_str,"%b %d %Y %I:%M %p")
                    except:
                        log.info(self.log_msg('problem in strptime ....................')) #Remove
                        continue #Remove
                
            except:
                log.info(self.log_msg('Last post date not found'))
                continue
            
            
            log.info(self.log_msg('Last Posted Date :::::::::::::::::::::'))
            log.info(thread_time)
            
            if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')) \
                and self.max_threads_count >= self.total_threads_count:
                log.info(self.log_msg('Session info return True or Reaches max count'))
                continue
            
            self.last_timestamp = max(thread_time , self.last_timestamp )
            #Updating last_timestamp
            
            temp_task =  self.task.clone()
            temp_task.instance_data[ 'uri' ] = thread_uri
            temp_task.pagedata['title'] = thread_title
            temp_task.pagedata['edate_last_post_date'] = datetime.strftime(thread_time,"%Y-%m-%dT%I:%M:%SZ")
            temp_task.pagedata['ei_thread_replies_count'] = thread_replies_count
            
            log.info(self.log_msg('Thread ::::::::::'))
            log.info(thread)
            try:
                temp_task.pagedata['et_thread_author_name'] = stripHtml(thread.findAll('td')[3].renderContents().strip())
                log.info(self.log_msg('Author name ::::::::::::::'))
                log.info(temp_task.pagedata['et_thread_author_name'])
            except:
                
                log.exception(self.log_msg('Thread author name not available'))
                #continue
            
            log.info(temp_task.pagedata)
            log.info(self.log_msg('taskAdded'))
            self.linksOut.append( temp_task )
        
        log.info(self.log_msg('Thread count = ' + str(self.total_threads_count)))
        return True
        

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











