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

import re
import logging
from urllib2 import urlparse, urlopen
from urllib import urlencode
from cgi import parse_qsl
from datetime import datetime
from BeautifulSoup import BeautifulStoneSoup

from tgimport import tg
from utils.httpconnection import HTTPConnection
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('GamespyConnector')
class GamespyConnector(BaseConnector):
    '''
    This will fetch the info for 
    Sample uris is
    http://xbox360.gamespy.com/xbox-360/call-of-duty-6/1043734c.html
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of gamespy.com
        """
        try:
            self.__genre = 'review'
            total_replies_count = 0
            self.__total_pages_to_process = 1
            self.unique_keys = []
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
            headers = {'Host':unicode(urlparse.urlparse(self.currenturi)[1])}
            headers['Referer'] = self.currenturi
            #log.info(self.currenturi)
            #res = urlopen(self.currenturi).read()
            self.__setSoupForCurrentUri()
            self.__setSoupForCurrentUri()
            topic_id = re.search("var topicID = '(\d+)'", self.soup.__str__()).group(1)
            current_page_no = 1
            uri_template = 'http://forums.gamespy.com/comments/commentservices.asmx/GetArticleComments?topicid=%s&page=%s&perPage=100&sortorder=DESC'            
            xml_uri = uri_template%(topic_id, str(current_page_no))
            headers = {'Host':'forums.gamespy.com'}
            data = dict(parse_qsl(xml_uri.split('?')[-1]))
            conn = HTTPConnection()
            conn.createrequest(xml_uri, headers=headers, data=data)
            res = conn.fetch().read()
            self.soup = BeautifulStoneSoup(res)
            comment = self.soup.find('boardcomment').extract()
            total_replies_count = int(stripHtml(comment.find('totalreplies').renderContents()))
            self.__total_pages_to_process =  ((total_replies_count / 50 ) + 1 )
            while True:
                if not self.__iteratePosts():
                    log.info(self.log_msg('Iterteposts:fetched all posts'))
                    break
                try:
                    if current_page_no >= self.__total_pages_to_process:
                        log.info(self.log_msg('Reached Last page of the review'))
                        break
                    current_page_no += 1
                    xml_uri = uri_template%(topic_id, str(current_page_no))
                    data = dict(parse_qsl(xml_uri.split('?')[-1]))
                    conn = HTTPConnection()
                    conn.createrequest(xml_uri, headers=headers, data=data)
                    log.info(self.log_msg('fetching %s'%xml_uri))
                    res = conn.fetch().read()
                    self.soup = BeautifulStoneSoup(res)
                    parent_comment = self.soup.find('boardcomment')
                    if parent_comment:
                        parent_comment.extract()
                except:
                    log.exception(self.log_msg('Fetched all the contents'))
                    break
            return True
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'%self.currenturi))
            return False
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = self.soup.findAll('boardcomment')
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
            page = self.__getData(post)
            unique_key = get_hash( {'data':page['data'],'title':page['title']})
            if checkSessionInfo(self.__genre, self.session_info_out, \
                        unique_key, self.task.instance_data.get('update'),\
                        parent_list = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'% \
                                                                unique_key))
                return False
            
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
            page['posted_date'] = stripHtml(post.find('dateadded', recursive=False).renderContents()).split('.')[0] +'Z'
            page['title'] = stripHtml(stripHtml(post.commentsubject.renderContents()))
            page['data'] = stripHtml(stripHtml(post.find('commentbody').renderContents()))
            page['et_author_name'] = stripHtml(post.authoruser.find('name').renderContents())
        except:
            log.exception(self.log_msg('Date not be found in %s'% self.currenturi))
            return 
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