'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Rakesh Soni

import re
import time #Added on Sep 22, 2009
import random #Added on Sep 22, 2009
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

baseuri = 'http://www.fool.com/search/'

log = logging.getLogger('FoolConnector')

class FoolConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch information from fool.com Forum
        #http://www.fool.com/search/index.aspx?go=1&site=USBoards&q=citibank&source=ignsittn0000001&filter=p
        http://www.fool.com/search/index.aspx?q=citibank&filter=p&site=USBoards&sort=date%3AD%3AR%3Ad1
        """
        
        self.genre="Review"
        log.info(self.log_msg('  :::::::::::::::::::::::::::::::::'))
        
        try:
            self.parent_uri = self.currenturi
            
            self.foolTimelag_max = tg.config.get(path='Connector', key='fool_search_timeLag_max')
            self.foolTimelag_min = tg.config.get(path='Connector', key='fool_search_timeLag_min')
                                                        
            if not self.task.instance_data.get('already_parsed'): #a flag to distinguish between tasks created by me , and original tasks
                #log.info(self.log_msg('Within If :::::::::::::::::::::::::::::::::'))
                
                #log.info(self.log_msg('Current URL :::::::::'))
                #log.info(self.currenturi)
                
##                if self.foolTimelag_min and self.foolTimelag_max:
##                    randomTimeLag = random.randint(self.foolTimelag_min,self.foolTimelag_max) / 1000.0000
##                    log.info('sleeping for %s seconds between requests'%randomTimeLag)
##                    time.sleep(randomTimeLag)
                
                #if not self.__setSoup():
                #    log.info(self.log_msg('Return from setSoup: 1 ::::'))
                data = dict(parse_qsl(self.currenturi.split('?')[-1]))
                headers ={'Host':'www.fool.com'}
                headers['Referer'] = self.currenturi
                try:
                    conn = HTTPConnection()
                    conn.createrequest(self.currenturi,headers=headers,data=data)
                    self.rawpage = conn.fetch().read()
                    self._setCurrentPage()
                except:
                        log.exception(self.log_msg('Soup not set ..... OK OK'))
                        return False
            
                self.total_threads_count = 0
                self.last_timestamp = datetime( 1980,1,1 )
                if tg.config.get(path='Connector',key='fool_max_threads_to_process'):
                    self.max_threads_count = int(tg.config.get(path='Connector',key='fool_max_threads_to_process'))
                else:
                    self.max_threads_count = None
    
                #page_no = 1 #Remove
                while True:
                    #if page_no==5: #Remove
                    #    break       #Remove
                    #page_no += 1 #Remove
                    
                    #log.info(self.log_msg('Page no ========== ' + str(page_no)+'  :::::::::::   ')) #Remove
                    if not self.__getThreads():
                        log.info(self.log_msg('Error in getThreads(), returned False'))
                        break
                    
                    #break #Remove
                    
                    try:
                        next_uri = self.soup.find('div',id='resultsNav').find('a',text=re.compile('Next')).parent['href']
                        #next_uri = baseuri + self.soup.find('div',id='resultsNav').find('a',text=re.compile('Next')).parent['href']
                        self.currenturi = next_uri
                        log.info(self.log_msg('Next URI == '))
                        log.info(next_uri)
                    except:
                        try:
                            #next_uri = self.soup.find('div',id='resultsNav').find('a',text=re.compile('Next &raquo;')).parent['href']
                            next_uri = baseuri + self.soup.find('div',id='resultsNav').find('a',text=re.compile('Next &raquo;')).parent['href']
                            self.currenturi = next_uri
                            log.info(self.log_msg('Next URI in except == '))
                            log.info(next_uri)
                        except:
                            log.info(self.log_msg('Next Page link not found'))
                            break
                        
                    if self.foolTimelag_min and self.foolTimelag_max:
                        randomTimeLag = random.randint(self.foolTimelag_min,self.foolTimelag_max) / 1000.0000
                        log.info('sleeping for %s seconds between requests'%randomTimeLag)
                        time.sleep(randomTimeLag)
                    
                    #if not self.__setSoup():
                    #    log.info(self.log_msg('Return from setSoup: 2 ::::'))
                    data = dict(parse_qsl(self.currenturi.split('?')[-1]))
                    headers ={'Host':'www.fool.com'}
                    headers['Referer'] = self.currenturi
                    try:
                        conn = HTTPConnection()
                        conn.createrequest(self.currenturi,headers=headers,data=data)
                        self.rawpage = conn.fetch().read()
                        self._setCurrentPage()
                    except:
                            log.exception(self.log_msg('Soup not set ..... '))
                            break
                    
                    
                #self.linksOut = self.linksOut[:1] #Remove it after testing
                if self.linksOut:
                    updateSessionInfo('Search', self.session_info_out,self.last_timestamp , None, \
                            'ForumThreadsPage', self.task.instance_data.get('update'))
                log.info(self.log_msg('Out of while loop for getThreads'))
                
                
                return True
            else:
                #log.info(self.log_msg('In else part of fetch() method'))
                #log.info(self.log_msg('Current URI...................'))
                #log.info(self.log_msg(self.currenturi))
                                
                if self.foolTimelag_min and self.foolTimelag_max:
                    randomTimeLag = random.randint(self.foolTimelag_min,self.foolTimelag_max) / 1000.0000
                    log.info('sleeping for %s seconds between requests'%randomTimeLag)
                    time.sleep(randomTimeLag)
                        
                if not self.__setSoup():
                    log.info(self.log_msg('Return from setSoup: 3 ::::'))
                    try:
                        conn = HTTPConnection()
                        conn.createrequest(self.currenturi)
                        self.rawpage = conn.fetch().read()
                        self._setCurrentPage()
                    except:
                        log.exception(self.log_msg('Soup not set ..... '))
                        return False
                
                #getParentPage() is commented on Oct 08, 2009 as we were storing link info as a posts 
                #   which is not useful.
                #if not self.__getParentPage():
                #    return False
                
                self.post_type= True
                
                #self.__addPosts()
                #Updated Sep 23, 2009 by Rakesh
                self.__getPosts() 
                
                return True
            
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False
        
    #Added Sep 23, 2009 by Rakesh
    @logit(log , '__getPosts')
    def __getPosts(self):
        """ 
            Get post informations
        """
        is_reply = False #To check post is question or reply
        
        try:
            #reviews = self.soup.findAll('table','messageMeta')
            subject = stripHtml(self.soup.findAll('table','messageMeta')[0].findAll('tr')[-1].findAll('td')[0]. \
                        renderContents().strip())
            sub_splitted = subject.split(" ")
            for each in sub_splitted:
                if each=='Re:':
                    is_reply = True
        except:
            log.exception(self.log_msg('Subject not found'))
            
        is_soup_set = True #Flag to check whether soup is set or not
        
        if is_reply:
            try:
                parent_uri = 'http://boards.fool.com' + self.soup.find('table','pbnavtable').find('tr'). \
                            find('td').findAll('a')[1]['href']
                #parent_uri is page in which we will get Question post
            except:
                log.exception(self.log_msg('Parent uri not found in getPosts()'))
            
            currenturi = self.currenturi            
            self.currenturi = parent_uri
            self.parent_uri = parent_uri
            
            if not self.__setSoup():
                log.info(self.log_msg('Return from setSoup: 11 ::::'))
                data = dict(parse_qsl(self.currenturi.split('?')[-1]))
                headers ={'Host':'www.fool.com'}
                headers['Referer'] = self.currenturi
                try:
                    conn = HTTPConnection()
                    conn.createrequest(self.currenturi,headers=headers,data=data)
                    self.rawpage = conn.fetch().read()
                    self._setCurrentPage()
                except:
                        log.exception(self.log_msg('Soup not set ..... getPost'))
                        is_soup_set = False
                        
            
            if is_soup_set:
                self.__addPosts(False)
                
            self.currenturi = currenturi
            self.parent_uri = parent_uri #uri of Question page
            
            is_soup_set = True
            if not self.__setSoup():
                log.info(self.log_msg('Return from setSoup: 12 ::::'))
                data = dict(parse_qsl(self.currenturi.split('?')[-1]))
                headers ={'Host':'www.fool.com'}
                headers['Referer'] = self.currenturi
                try:
                    conn = HTTPConnection()
                    conn.createrequest(self.currenturi,headers=headers,data=data)
                    self.rawpage = conn.fetch().read()
                    self._setCurrentPage()
                except:
                        log.exception(self.log_msg('Soup not set ..... getPost'))
                        is_soup_set = False
        
        if is_soup_set:
            self.__addPosts(is_reply)
            
     
    @logit(log , '__addPosts')
    def __addPosts(self,is_reply):
        """ 
            Get post informations
        """
        
        try:
            reviews = self.soup.findAll('table','messageMeta')[:1]
            #Sep 23, To pick all posts us, reviews = self.soup.findAll('table','messageMeta') 
        except:
            log.exception(self.log_msg('Reviews not found'))
            return False
        
        for i, review in enumerate(reviews[:]):
            
            page = {}
            #Sep 23, Unccoment following commented lines to pick all posts.
##            post_type = "Question"
##            if i==0 and self.post_type:
##                post_type = "Question"
##                self.post_type = False
##            else:
##                post_type = "Suggestion"
            #Comment following if:else condition if want to pick all posts
            if not is_reply:
                post_type = 'Question'
            else:
                post_type = 'Suggestion/Reply'
                
            try:
                try:
                    page['et_author_name'] = stripHtml(review.find('a',title='View this Fool\'s profile').renderContents().strip())
                    #log.info(self.log_msg(page['et_author_name']))
                except:
                    log.info(self.log_msg('Author name not available'))
                    
                try:
                    date_str = stripHtml(review.findAll('tr')[-1].findAll('td')[-1].find('b').nextSibling.string)
                    page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%m/%d/%y %I:%M %p'), \
                                            "%Y-%m-%dT%H:%M:%SZ")
                except:
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    log.info(self.log_msg('posted date not found, taking current date.'))
                    
                try:
                    page['data'] = stripHtml(review.findNext('table',id='tableMsg').find('td',id='tdMsg') \
                                    .renderContents().strip())
                except:
                    page['data'] = ''
                    log.info(self.log_msg('Data not available'))
                    
                log.info(self.log_msg(page['data']))  
                
                try:
                    page['title'] = stripHtml( review.findAll('tr')[-1].findAll('td')[0].find('b') \
                                     .findNext('a','pnvalink').renderContents().strip())
                    #log.info(self.log_msg('Title ================'))
                    #log.info(page['title'])
                except:
                    try:
                        page['title'] = stripHtml(review.findAll('tr')[-1].findAll('td')[0] \
                                        .find('b').nextSibling.string)
                    except:
                        try:
                            if len(page['data']) > 50:
                                page['title'] = page['data'][:50] + '...'
                            else:
                                page['title'] = page['data']
                        except:
                            page['title'] = ''
                            log.exception(self.log_msg('title not found'))
                            
                    log.info(self.log_msg(page['title']))
                    
                if page['title']=='' and page['data']=='':
                    continue
                
                try:
                    unique_key = get_hash({'title':page['title'],'data':page['data']})
                except:
                    log.exception(self.log_msg('unique_key not found'))
                    continue
                
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                                     self.task.instance_data.get('update'),parent_list\
                                                                    =[self.parent_uri]):
                        log.info(self.log_msg('session info return True'))
                        continue
                    
                try:
                    page['ei_recommendations_count'] = int(review.findNext('table','pbnavtable').findNext('div') \
                                                        .renderContents().strip().split(' ')[-1])
                except:
                    log.info(self.log_msg('Recommendations count not found'))
                    
                try:
                    page['et_data_post_type'] = post_type
                except:
                    log.info(self.log_msg('Page info is missing'))
                 
                #Commented on Oct 08, 2009   
                #try:
                #    hier = self.hierarchy.split("/")
                #    page['et_data_forum'] = hier[0].strip()
                #    page['et_data_subforum'] = hier[1].strip()
                #    page['et_data_topic'] = self.forum_title
                #except:
                #    log.info(self.log_msg('data forum not found'))
                    
                #Following try..except included on Oct 08, 2009
                try:
                    hier = self.soup.find('table',id='tblBreadCrumb').find('tr').find('td').findAll('b')
                    page['et_data_forum'] = stripHtml(hier[0].find('a').renderContents().strip())
                    page['et_data_subforum'] = stripHtml(hier[1].find('a').renderContents().strip())
                except:
                    log.info(self.log_msg('data forum-subforum not found'))
                    
                #log.info(page)
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
                log.info(page)
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))
                
        return True
    
    
    
            
    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
            Get the parent information
        """
        page = {}
        
        try:
            hierarchy = '' #Updated, Sep 22      
            tmp_dict = self.soup.find('table',id='tblBreadCrumb').find('tr').findAll('b')
            for each in tmp_dict:
                #hierarchy.append(each.find('a').renderContents().strip())
                hierarchy = hierarchy + each.find('a').renderContents().strip() + " / "
                
            #log.info(self.log_msg('Hierarchy :::::::::::::::::::::'))
            #log.info(hierarchy)
            self.hierarchy = page['et_thread_hierarchy'] = hierarchy
        except:
            log.exception(self.log_msg('Thread hierarchy is not found'))
    
        #Following 2 lines are updated on Oct, 08, 2009
        #for each in ['title','et_thread_board_name','et_thread_board_uri','edate_last_post_date']:
        for each in ['title','posted_date','et_thread_board_name','et_thread_board_uri']:
            try:
                page[each] = self.task.pagedata[each]
                #log.info(each)
                #log.info(page[each])
                #log.info(self.log_msg('--------------------'))
                
            except:
                log.exception(self.log_msg('page data cannot be extracted for %s'%each))
                
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
                log.info(self.log_msg('Posted Date not picked up from getThread() method'))
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
            #log.info(page)
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
            threads = self.soup.find('dl','results').findAll('dt')
        except:
            log.exception(self.log_msg('No thread found, cannot proceed'))
            return False
        
        for thread in threads:
            #if self.total_threads_count > self.max_threads_count:
            if self.max_threads_count and self.total_threads_count > self.max_threads_count:
                log.info(self.log_msg('Reaching maximum post,Return false'))
                return False
            self.total_threads_count = self.total_threads_count + 1
            
            #log.info(self.log_msg('No.of Threads ==========='))
            #log.info(self.total_threads_count)
            
            try:
                thread_uri = thread.find('a')['href']
            except:
                log.exception(self.log_msg('Thread uri not available'))
                continue
            
            try:
                thread_title = stripHtml(thread.find('a').renderContents().strip())
                #log.info(self.log_msg('Thread Title .............................'))
                #log.info(self.log_msg(thread_title))
            except:
                log.exception(self.log_msg('Thread title not available'))
                continue
            try:
                boards = thread.findNext('dd','resultDetail').findAll('a')[1:]
                thread_board_name = ''
                for board in boards:
                    thread_board_name = thread_board_name + stripHtml(board.renderContents().strip()) + " / "
                log.info(self.log_msg('thread_board_name ==='))
                log.info(thread_board_name)
            except:
                #Earlier used tags, tags changed
                try:
                    thread_board_name = stripHtml(thread.findNext('dd','resultDetail boardsDetail').find('a').renderContents().strip())
                except:
                    log.exception(self.log_msg('Thread board not available'))
                    thread_board_name = ''
            try:
                thread_board_uri = thread.findNext('dd','resultDetail').findAll('a')[-1]['href']
            except:
                #Earlier used tags, tags changed
                try:
                    thread_board_uri = stripHtml(thread.findNext('dd','resultDetail boardsDetail').find('a')['href'])
                    #log.info(self.log_msg('thread_board_uri'))
                    #log.info(thread_board_uri)
                except:
                    log.exception(self.log_msg('Thread board uri not available'))
                    thread_board_uri = ''
                
            try:
                date_str = stripHtml(thread.findNext('dd','resultDetail').find('span').renderContents().strip())
                thread_time = datetime.strptime(date_str,'%B %d, %Y')
                #log.info(self.log_msg('date_str::::::::'))
                #log.info(date_str)
            except:
                #Earlier used tags, tags changed
                try:
                    log.info(self.log_msg('resultDetail did not work'))
                    date_str = thread.findNext('dd','resultDetail boardsDetail').find('a').nextSibling.string
                    splitted_date = date_str.split(' ')
                    tmp_date = splitted_date[2]+' '+splitted_date[3]+' '+splitted_date[4]
                    thread_time = datetime.strptime(tmp_date,'%B %d, %Y')                
                except:                
                    log.exception(self.log_msg('posted date not found, taking current date.'))
                    #thread_time = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    thread_time = datetime.utcnow()
            
##            if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')) \
##                and (self.max_threads_count and self.max_threads_count >= self.total_threads_count):
            #Updated: Rakesh, Sep 22
            if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')) \
                and (self.max_threads_count and self.max_threads_count >= self.total_threads_count):
                log.info(self.log_msg('Session info return True or Reaches max count'))
                continue
            
            self.last_timestamp = max(thread_time , self.last_timestamp )
            #Updating last_timestamp
            
            temp_task =  self.task.clone()
            temp_task.instance_data[ 'uri' ] = thread_uri
            temp_task.pagedata['title'] = thread_title
            
            #Following two lines are updated on Oct 08,2009            
            #temp_task.pagedata['edate_last_post_date'] = datetime.strftime(thread_time,"%Y-%m-%dT%I:%M:%SZ")
            temp_task.pagedata['posted_date'] = datetime.strftime(thread_time,"%Y-%m-%dT%I:%M:%SZ")
        
            temp_task.pagedata['et_thread_board_name'] = thread_board_name
            temp_task.pagedata['et_thread_board_uri']= thread_board_uri
            
            temp_task.instance_data['already_parsed'] = True
            
            #log.info(temp_task.pagedata)
            log.info(self.log_msg('taskAdded'))
            self.linksOut.append( temp_task )
        
        log.info(self.log_msg('Thread count = ' + str(self.total_threads_count)))
        return True
            
            
    @logit(log, "_setSoup")
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
        