'''
Copyright (c)2008-2010 Serendio Software Private Limited
All Rights Reserved
This software is confidential and proprietary information of Serendio Software.
It is disclosed pursuant to a non-disclosure agreement between the recipient
and Serendio. This source code is provided for informational purposes only, and
Serendio makes no warranties, either express or implied, in this. Information 
in this program, including URL and other Internet website references, is 
subject to change without notice. The entire risk of the use or the results of 
the use of this program  with the user. Complying with all applicable 
copyright laws is the responsibility of the user. Without limiting the rights 
under copyright, no part of this program may be reproduced, stored in, or 
introduced into a retrieval system, or distributed or transmitted in any form 
or by any means (electronic, mechanical, photocopying, recording, on a website,
or otherwise) or for any purpose, without the express written permission of 
Serendio Software.
Serendio may have patents, patent applications, trademarks, copyrights, or 
other intellectual property rights covering subject matter in this program. 
Except as expressly provided in any written license agreement from Serendio, 
the furnishing of this program does not give you any license to these patents, 
trademarks, copyrights, or other intellectual property.
'''
#Skumar

import re
import logging
from urllib2 import urlparse
from datetime import datetime

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('CureZoneConnector')
class CureZoneConnector(BaseConnector):
    '''
    This will fetch the info for curezone.com
    Sample uris is
    http://curezone.com/forums/f.asp?f=869
    '''        
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of curezone.com
        """
        try:
            self.__genre = 'review'
            self.__baseuri = 'http://curezone.com'
            self.__setSoupForCurrentUri()
            if not self.currenturi.startswith('http://curezone.com/forums/am.asp'):
                return self.__createTasksForThreads()
            else:
                return self.__addThreadAndPosts()
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'%self.currenturi))
            return False
        
    @logit(log, '__addThreadAndPosts')
    def __addThreadAndPosts(self):
        """
        This will add the Thread info from setParentPage method and 
        Add the posts of the thread in addPosts mehtod
        """        
        self.__hierarchy = []
        self.__task_elements_dict = {
                        'priority':self.task.priority,
                        'level': self.task.level,
                        'last_updated_time':datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ"),
                        'pickup_date':datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ"),
                        'connector_instance_log_id': self.task.connector_instance_log_id,
                        'connector_instance_id':self.task.connector_instance_id,
                        'workspace_id':self.task.workspace_id,
                        'client_id':self.task.client_id,
                        'client_name':self.task.client_name,
                        'versioned':False,
                        'category':self.task.instance_data.get('category',''),
                        'task_log_id':self.task.id }
        try:
            self.currenturi = 'http://curezone.com/forums/' + self.soup.find('a', href=re.compile('^fm\.asp\?i=\d+'))['href'].split('#')[0]
            self.__setSoupForCurrentUri()
            post_links = [ 'http://curezone.com/forums/' + x['href'].split('#')[0] for x in  self.soup.findAll('a', href=re.compile('^fm\.asp\?i=\d+'))]
            if not post_links:
                log.info(self.log_msg('No posts found in %s'%self.currenturi))
                return False
            self.__addPost(post_links[0], True)
            replies_links = post_links[1:]
            replies_links.reverse()
            for reply_link in replies_links:
                if not self.__addPost(reply_link):
                    log.info(self.log_msg('Post not added to self.pages for url\
                                                            %s'%self.currenturi))
                    break
        except:
            log.info(self.log_msg('Error while getting Posts'))
        return True
    
        
    @logit(log, '__createTasksForThreads')
    def __createTasksForThreads(self):
        """
        This will create Tasks for the threads found on the given url
        The # of Tasks are limited by Config Variable
        """
        self.__current_thread_count = 0
        self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                            'curezone_maxthreads'))
        while self.__getThreads():
            try:
                self.currenturi = self.__baseuri + self.soup.find('a', text=re.compile('^\.\.\. continue reading: Page')).parent['href']
                self.__setSoupForCurrentUri()
            except:
                log.info(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                break
            
        log.info('Total # of tasks found is %d'%len(self.linksOut))
        #self.linksOut = [] #None #To Remove
        return True
    
    @logit(log, '__getThreads')
    def __getThreads(self):
        """
        It will fetch each thread and its associate infomarmation
        and add the tasks
        """
        try:
            threads = ['http://curezone.com/forums/' + x.find('a')['href'].replace('fm.asp?', 'am.asp?').split('#')[0] for x in self.soup.find('ul').findAll('li', recursive=False) if x.find('a', href=re.compile('fm[.]asp'))]
            if not threads:
                log.info(self.log_msg('No threads are found for url %s'%\
                                                            self.currenturi))
                return False
        except:
            log.exception(self.log_msg('Threads not found'))
            return False
        for each_link in threads:
            if  self.__current_thread_count >= self.__max_threads_count:
                log.info(self.log_msg('Reaching maximum post,Return false \
                                        from the url %s'%self.currenturi))
                return False
            self.__current_thread_count += 1
            try:
                temp_task = self.task.clone()                    
                temp_task.instance_data['uri'] = each_link
                self.linksOut.append(temp_task)
                log.info(self.log_msg('Task Added'))
            except:
                log.exception(self.log_msg('Cannot find the thread url \
                                            in the uri %s'%self.currenturi))        
        return True
    
    @logit(log, '__addPost')
    def __addPost(self, post_link, is_question=False):
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            self.currenturi = post_link
            self.__setSoupForCurrentUri()
            if is_question:
                my_parent_list = []
            else:
                my_parent_list = [self.task.instance_data['uri']]            
            if checkSessionInfo(self.__genre, self.session_info_out, self.currenturi, \
                             self.task.instance_data.get('update'),parent_list\
                                        = my_parent_list):
                log.info(self.log_msg('Session info returns True for uri %s'%self.currenturi))
                return False
            post = self.soup.find('b', text='Subject:').findParent('table').findParent('table')
            page = self.__getData(post, is_question)
            if not page:
                log.info(self.log_msg('page contains empty data, getdata \
                                    returns  False for uri %s'%self.currenturi))
                return True
            result = updateSessionInfo(self.__genre, self.session_info_out, post_link, \
                get_hash( page ),'forum', self.task.instance_data.get('update'), \
                                parent_list=my_parent_list)
            if result['updated']:
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [self.task.instance_data['uri'], self.currenturi]
                page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
                page.update(self.__task_elements_dict)
                self.pages.append(page)
                log.info(self.log_msg('Page added'))
            else:
                log.info(self.log_msg('Update session info returns False for \
                                                    url %s'%self.currenturi))
                return True
        except:
            log.exception(self.log_msg('Cannot add the post for the uri %s'%self.currenturi))
        return True
    
    @logit(log, '__getData')
    def __getData(self, post, is_question):
        """ This will return the page dictionry
        """
        page = {'entity':'question' if is_question else 'answer'}
        try:
            post_info =  dict([(x.strip().lower(), y.strip()) for x,y in  [x.strip().split(':', 1) for x in stripHtml(post.find('b', text='Subject:').findParent('td').renderContents()).splitlines() if x.strip()]])
            page['uri'] = post_info['url']
            page['title'] = post_info['subject']
        except:
            log.info(self.log_msg('Cannot find the title, uri details'))
            return True
        try:
            page['data'] = stripHtml(post.findAll('table')[1].find('p').renderContents())
        except:
            log.info(self.log_msg('Data not found for the url %s'%self.currenturi))
            return True
        try:
            page['posted_date'] = datetime.strftime(datetime.strptime( post_info['date'].split(' ( ')[0].strip(), '%m/%d/%Y %I:%M:%S %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Date not be found in %s'% self.currenturi))
        try:
            page['et_author_name'] = post_info['username'].split(' contact ')[1].strip()
        except:
            log.info(self.log_msg('Ratings count not found'))
        try:
            page['ei_data_views_count'] = int(post_info['hits'])
        except:
            log.exception(self.log_msg('uri not found'))            
        try:
            if is_question:
                page['ei_data_replies_count'] = int(post_info['replies'])
        except:
            log.info(self.log_msg('data replies not found'))
        return page
            
    @logit(log,'__setSoupForCurrentUri')
    def __setSoupForCurrentUri(self, data=None, headers={}):
        """It will set soup object for the Current URI
        """
        res = self._getHTML(data=data, headers=headers)
        if res:
            self.rawpage = res['result']
        else:
            log.info(self.log_msg('Page Content cannot be fetched for the url: \
                                                            %s'%self.currenturi))
            raise Exception('Page content not fetched for th url %s'%self.currenturi)
        self._setCurrentPage()
