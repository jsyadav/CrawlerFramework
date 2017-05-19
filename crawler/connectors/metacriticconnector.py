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
#Skumar

import re, copy, logging
from urllib2 import urlparse, urlopen
from urllib import urlencode
from datetime import datetime


from tgimport import tg
from utils.httpconnection import HTTPConnection
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('MetaCriticConnector')
class MetaCriticConnector(BaseConnector):
    '''
    This will fetch the info for 
    Sample uris is
    http://www.metacritic.com/game/pc/medal-of-honor-airborne/user-reviews
    '''
        
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of metacritic.com
        """
        try:
            self.__genre = 'review'
            self.currenturi = self.currenturi + '?sort-by=date&num_items=100'
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
            while True:
                if not self.__iteratePosts():
                    break
                try:
                    self.currenturi = 'http://www.metacritic.com' +  self.soup.find('span', 'text', text='next').parent.parent['href']
                    self.__setSoupForCurrentUri()
                except:
                    log.info(self.log_msg('Fetched all the contents'))
                    break
            #f=open('pages.dmp', 'w')
            #import pickle
            #pickle.dump(self.pages, f)
            #f.close()
            return True
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'%self.currenturi))
            return False
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = self.soup.findAll('li', id=re.compile('user_review_\d+'))
            if not posts:
                log.info(self.log_msg('No posts found in url %s'%self.currenturi))
                return False
            for post in posts:
                if not self.__addPost(post):
                    log.info(self.log_msg('Post not added to self.pages for url\
                                                            %s'%self.currenturi))
                    return False
            return True
        except:
            log.exception(self.log_msg('Reviews are not found for url %s'%self.currenturi))
            return False
    
    @logit(log, '__addPost')
    def __addPost(self, post):
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            unique_key = post['id'].replace('user_review_', '')
            if checkSessionInfo(self.__genre, self.session_info_out, \
                        unique_key, self.task.instance_data.get('update'),\
                        parent_list = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'% \
                                                                unique_key))
                return False
            page = self.__getData(post)
            if not page:
                log.info(self.log_msg('page contains empty data, getdata \
                                    returns  False for uri %s'%self.currenturi))
                return True
            result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                get_hash( page ),'forum', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if result['updated']:
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [self.task.instance_data['uri'], unique_key ]
                page['uri'] = self.currenturi
                page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
                page ['entity'] = 'review'
                page.update(self.__task_elements_dict)
                self.pages.append(page)
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
            date_str = stripHtml(post.find('div', 'date').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%b %d, %Y'),'%Y-%m-%dT%H:%M:%SZ')
        except:
            log.info(self.log_msg('posted_date not be found in %s'% self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            data_tag = post.find('div', 'review_body') 
            expand_tag = data_tag.find('a', 'toggle_expand_collapse toggle_expand', rel=True)
            if expand_tag:
                page['data'] = stripHtml(expand_tag['rel'].__str__())
            else:
                page['data'] = stripHtml(data_tag.renderContents())
            replacement_chars = {'&euro;':u'\u20ac', '&trade;':u'\u2122','&acirc;':u'\xe2', '&tilde;':'~'}
            for each in replacement_chars.keys():
                page['data'] = page['data'].replace(each, replacement_chars[each])
            page['title'] = ''
        except:
            log.info(self.log_msg('post  not found in %s'% self.currenturi))
            return
        try:
            page['et_data_helpful_count'] = int(stripHtml(post.find('span', 'total_ups').renderContents()))
        except:
            log.info(self.log_msg('author name be found in %s'% self.currenturi))
        try:
            page['et_author_name'] = stripHtml(post.find('div', 'name').renderContents())
        except:
            log.info(self.log_msg('Authro name not found'))
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