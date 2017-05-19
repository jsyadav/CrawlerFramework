'''
Copyright (c)2008-2009 Serendio Software Private Limited
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
#prerna

import re, copy, logging
from urllib2 import urlparse, urlopen
from urllib import urlencode
from datetime import datetime

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from BeautifulSoup import BeautifulSoup
log = logging.getLogger('AmplicateConnector')
class AmplicateConnector(BaseConnector):
    '''
    This will fetch the info for 
    Sample uris is
    http://amplicate.com/hate/bank-of-america
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of amplicate.com
        """
        try:
            self.__genre = 'review'
            self.basruri = 'http://www.amplicate.com'
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
            self.__setSoupForCurrentUri()
            self.__iteratePosts()
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'%self.currenturi))
        return True
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the links found in the Current URI
        """
        try:
            temp_posts = [each.findParent('li') for each in self.soup.\
                            findAll('div', id=re.compile('post\-less\-\d+')) if each.findParent('li')]
            list_of_unique_keys = []
            posts = []
            for each in temp_posts:
                u_key = each.find('div', id=re.compile('post\-less\-\d+'))['id']
                if u_key in list_of_unique_keys:
                    continue
                else:
                    list_of_unique_keys.append(u_key)
                    posts.append(each)
            
            if not posts:
                log.info(self.log_msg('No posts found in url %s'%self.currenturi))
                return False
            for post in posts:
                if not self.__addPost(post):
                    log.debug(self.log_msg('Post not added to self.pages for url\
                                                            %s'%self.currenturi))
                    break
            return True
        except:
            log.exception(self.log_msg('Reviews are not found for url %s'%self.currenturi))
            return True
    
    @logit(log, '__addPost')
    def __addPost(self, post):
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            unique_key = post.find('div', id=re.compile('post\-less\-\d+'))['id']
                             
            if checkSessionInfo(self.__genre, self.session_info_out, \
                        unique_key, self.task.instance_data.get('update'),\
                        parent_list = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'% \
                                                                unique_key))
                log.info(unique_key)
                log.info(str(unique_key))
                return False
            page = self.__getData(post)
            if not page:
                log.info(self.log_msg('page contains empty data, getdata \
                                    returns  False for uri %s'%self.currenturi))
                return True
            result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                get_hash( page ),'review', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if result['updated']:
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [self.task.instance_data['uri']]
                page['uri'] = self.currenturi + "#" + unique_key
                page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
                page ['entity'] = 'review'
                page.update(self.__task_elements_dict)
                self.pages.append(page)
                log.info(page)
            else:
                log.info(self.log_msg('Update session info returns False for \
                                                    url %s'%self.currenturi))
        except:
            log.exception(self.log_msg('Cannot add the post for the uri %s'%self.currenturi))
        return True
    
    @logit(log, '__getData')
    def __getData(self, post):
        """ This will return the page dictionry
        """
        page = {}
        try:
            user_info = post.find('div','post-user-info')
            try:
                date_str = stripHtml(user_info.find('abbr','timeago').renderContents()).strip()
                page['posted_date']= datetime.strptime(date_str,"%d %b %Y").strftime("%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception(self.log_msg('posted_date not be found in %s'% self.currenturi))
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            try:
                page['et_author_name'] = stripHtml(user_info.find('abbr','timeago').\
                                            findNext('a').renderContents())
            except:
                log.exception(self.log_msg('auth name not found'))   
        except:
            log.exception(self.log_msg('user_info  not found in %s'% self.currenturi))
        try:
            page['data'] = stripHtml(post.find('p','comment-body').renderContents())
            page['title'] = ''
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            return 
        try:
            rank = BeautifulSoup(post.__str__().replace('/>>','/>').replace('<span>','</span>'))
            page['et_data_rating'] = stripHtml(rank.find('span',id = re.compile('post-rank-\d+')).\
                                        renderContents())   
        except:
            log.exception(self.log_msg('data rating not found'))                                
        return page            
    
    @logit(log,'__setSoupForCurrentUri')
    def __setSoupForCurrentUri(self, data=None, headers={}):
        """It will set soup object for the Current URI
        """
        res = self._getHTML(data=data, headers=headers)
        if res:
            self.rawpage = res['result']
        else:
            raise Exception('Page content not fetched for th url %s'%self.currenturi)
        self._setCurrentPage()