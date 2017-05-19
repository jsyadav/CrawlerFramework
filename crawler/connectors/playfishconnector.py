'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Rakesh Soni

import re
from BeautifulSoup import BeautifulSoup
from utils.httpconnection import HTTPConnection
from urllib2 import urlopen
from datetime import datetime,timedelta
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

baseurl = 'http://forum.playfish.com/'
#We are adding base url to those href which have only part of url

log = logging.getLogger('PlayfishConnector')
class PlayfishConnector(BaseConnector):
    '''
        Get information for www.playfish.com
        Sample uris is
        http://forum.playfish.com/forumdisplay.php?f=158
    '''
    
    @logit(log , 'fetch')
    def fetch(self):
        """
            Fetch of playfish forums
            sample uri : http://forum.playfish.com/forumdisplay.php?f=158
        
        """
        self.genre="Review"
        
        try:
            self.parent_uri = self.currenturi
            
            if urlparse.urlparse(self.currenturi)[2]=='/forumdisplay.php':
                                
                self.total_threads_count = 0
                self.last_timestamp = datetime( 1980,1,1 )
                self.max_threads_count = int(tg.config.get(path='Connector',key='playfish_max_threads_to_process'))                
                
                headers = {'Accept-encoding':''}
                #It is required because getHtml() is returning bnary data.
                if not self.__setSoup(headers=headers):
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                
                while True:
                    #Get all threads available in multiple pages. Traverse throul "Next" link
                    if not self.__getThreads():
                        break
                    
                    try:
                        new_thread_uri = self.soup.find('a',rel='next')
                        #Get "Next" link
                        
                        self.currenturi = baseurl + new_thread_uri['href']
                        
                    except:
                        log.exception(self.log_msg('No more thread links'))
                        break
                    
                    headers = {'Accept-encoding':''}
                    if not self.__setSoup(headers=headers):
                        break
                    
                #self.linksOut = self.linksOut[:1] #Remove it after testing
                if self.linksOut:
                    updateSessionInfo('Search', self.session_info_out,self.last_timestamp , None, \
                            'ForumThreadsPage', self.task.instance_data.get('update'))
                log.info(self.log_msg('Out of while loop for getThreads'))
                
                return True
            else:
                
                headers = {'Accept-encoding':''}
                if not self.__setSoup(headers=headers):
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                
                self.__getParentPage()
                self.post_type= True
                
                while True:
                    
                    self.__addPosts()                    
                    #Get "post" information
                    
                    try:                        
                        new_post_uri = self.soup.find('a',rel='next')
                        
                        self.currenturi = baseurl + new_post_uri['href']
                        
                        headers = {'Accept-encoding':''}
                        if not self.__setSoup(headers=headers):
                            log.info(self.log_msg("Soup didn't get created..........."))
                            break
                    except:
                        log.info(self.log_msg('Next post page not found'))
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
        
        try:
            reviews = self.soup.find('div',id='posts').findAll('div',attrs={'id':re.compile('edit[09]*')})
        except:
            log.exception(self.log_msg('Reviews not found'))
            return False
        
        for i, review in enumerate(reviews[:]):
            post_type = "Question"
            if i==0 and self.post_type:
                post_type = "Question"
                self.post_type = False
            else:
                post_type = "Suggestion"

            try:
                unique_key =  review.find('a',attrs={'id':re.compile('postcount[0-9]*')})['href']
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                                 self.task.instance_data.get('update'),parent_list\
                                                                =[self.parent_uri]):
                    log.info(self.log_msg('session info return True'))
                    continue
                        
                page = self.__getData( review , post_type )
                #Get post data
                
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

    @logit(log, '__getData')
    def __getData(self, review, post_type ):
        """ 
            Get a page which contains post data 
        """
        page = {}    
        
        try:
            author_info = review.find('td','alt2').findAll('div')
        except:
            log.info(self.log_msg('Author Information not available'))
            
        #Author information
        try:
            page['et_author_name'] = stripHtml(author_info[0].find('a').renderContents())
        except:
            log.info(self.log_msg('Author name not available'))
        
        try:
            page['et_author_type'] = stripHtml(author_info[1].renderContents())
        except:
            log.info(self.log_msg('Author type not available'))
        
        date_str = stripHtml(author_info[-4].renderContents().split(':')[1].strip())
        
        date_str = '1 '+date_str
        #Since date is not availble, we are assuming from 1st of the month
        
        try:
            page['edate_author_member_since'] = datetime.strftime(datetime.strptime(date_str, '%d %b %Y'), \
                                                    "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Author member since info is not available'))
          
        try:
            page['et_author_location'] = stripHtml(author_info[-3].renderContents().split(':')[1].strip())
        except:
            log.info(self.log_msg('Author location not available'))
            
        try:
            page['et_author_posts_count'] = stripHtml(author_info[-2].renderContents().split(':')[1].strip())
        except:
            log.info(self.log_msg('Author posts count info not available'))
            
        try:
            posted_date_str = stripHtml(review.find('a',attrs={'name':re.compile('post[0-9]*')}) \
                                .next.next.string.strip())  
            page['posted_date'] = datetime.strftime(datetime.strptime(posted_date_str,'%m-%d-%Y, %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('posted date not found, taking current date.'))
        
        try:
            page['data'] = stripHtml(review.find('div',attrs={'id':re.compile('post_message_[0-9]*')}) \
                            .renderContents())
        except:
            page['data'] = ''
            log.info(self.log_msg('Data not found for this post'))
            
        try:
            if len(page['data']) > 50:
                page['title'] = page['data'][:50] + '...'
            else:
                page['title'] = page['data']
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
            
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
        return page
            
        
    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
            Get the parent information
        """
        
        page = {}
        
        try:
            self.hierarchy = page['et_thread_hierarchy'] = [stripHtml(x.find('a').renderContents()) for x in self.soup.findAll('span','navbar')]
            #hierarchy of forum and subforums
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
        
        try:
            self.forum_title = page['title'] = stripHtml(self.soup.find('td','navbar').find('strong') \
                                    .renderContents())
            
        except:
            log.info(self.log_msg('title not found'))
            
        for each in ['et_last_post_author_name','ei_thread_replies_count','edate_last_post_date', \
                            'et_thread_author_name','et_views']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('page data cannot be extracted for %s'%each))
        try:
            page['et_thread_id'] = urlparse.urlparse(self.currenturi)[4].split('=')[-1]
        except:
            log.info(self.log_msg('Thread id not found'))    
            
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
        Get thread links and create tasks.
        """
        
        try:
            threads = self.soup.find('table',id='threadslist').findAll('tr')[1:]
        except:
            log.exception(self.log_msg('No thread found, cannot proceed'))
            return False
        
        for thread in threads:
            if self.total_threads_count > self.max_threads_count:
                log.info(self.log_msg('Reaching maximum post,Return false'))
                return False
            self.total_threads_count = self.total_threads_count + 1
            
            try:
                thread_info = thread.findAll('td')
                
                try:
                    date = thread_info[3].find('div').next.string.strip()
                except:
                    log.info(self.log_msg('Date not available, thread moved from page.'))
                    continue
                
                #log.info(self.log_msg('Current Date::::::::::::::::::::::::::::::'))
                #log.info(self.log_msg(date))
                
                if date=='Yesterday':
                    #Get yesterday date using current date
                    try:
                        today = datetime.today()
                        yesterday = today - timedelta(1)
                        date = str(yesterday.month) + '-' + str(yesterday.day) + '-' + str(yesterday.year)
                    except:
                        log.info(self.log_msg('Problem in getting date for Yesterday.'))
                        continue
                
                if date=='Today':
                    try:
                        date = str(datetime.strftime(datetime.utcnow(),"%m-%d-%Y"))
                    except:
                        log.info(self.log_msg('Problem in getting date for Today.'))                     
                        continue
                        
                time = thread_info[3].find('span').renderContents().strip()
                date_str = date + ' ' + time
                
                try:
                    thread_time =  datetime.strptime(date_str,'%m-%d-%Y %H:%M %p')
                except:
                    log.info(self.log_msg('Last post date not found'))
                    continue
                #log.info(self.log_msg('Thread time ===================='))
                #log.info(thread_time)
                if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')) \
                    and self.max_threads_count >= self.total_threads_count:
                    log.info(self.log_msg('Session info return True or Reaches max count'))
                    continue
                
                self.last_timestamp = max(thread_time , self.last_timestamp )
                #Updating last_timestamp
                
                temp_task=self.task.clone()
                
                temp_task.instance_data[ 'uri' ] = baseurl + thread_info[2].find('div').find('a')['href']
                temp_task.pagedata['et_thread_author_name'] =  thread_info[2].find('div','smallfont').findAll('span')[-1] \
                                                            .renderContents().strip()
                temp_task.pagedata['title']= stripHtml(thread_info[2].find('div').find('a').renderContents().strip())
                temp_task.pagedata['ei_thread_replies_count'] = int(stripHtml(thread_info[4].find('a') \
                                                        .renderContents().strip()))
                
                temp_task.pagedata['edate_last_post_date']=  datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                temp_task.pagedata['et_last_post_author_name'] = thread_info[3].find('div','smallfont').find('a') \
                                                        .renderContents().strip()
                                                        
                temp_task.pagedata['et_views'] = thread_info[5].renderContents().strip()
                
                log.info(temp_task.pagedata)
                log.info('taskAdded')
                self.linksOut.append( temp_task )
            except:
                log.exception( self.log_msg('Task Cannot be added') )
                continue
            
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
