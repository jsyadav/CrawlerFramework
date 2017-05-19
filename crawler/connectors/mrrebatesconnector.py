'''
Copyright (c)2008-2010 Serendio Software Private Limited
All Rights Reserved
This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
import logging

from urllib2 import urlparse
from datetime import datetime

from utils.decorators import logit
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('MrRebatesConnector')
class MrRebatesConnector(BaseConnector):
    '''Forum for wireless.att.com
    ''' 
    @logit(log , 'fetch')
    def fetch(self):
        """
        http://www.mrrebates.com/store_ratings/view_store_ratings.asp?merchant_id=1465
        """
        try:
            self.__setSoupForCurrentUri()
            self.__genre = "Review"
            self.__task_elements_dict = {
                            'priority':self.task.priority,
                            'level': self.task.level,
                            'last_updated_time':datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ"),
                            'pickup_date':datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ"),
                            'posted_date':datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ"),
                            'connector_instance_log_id': self.task.connector_instance_log_id,
                            'connector_instance_id':self.task.connector_instance_id,
                            'workspace_id':self.task.workspace_id,
                            'client_id':self.task.client_id,
                            'client_name':self.task.client_name,
                            'versioned':False,
                            'category':self.task.instance_data.get('category',''),
                            'task_log_id':self.task.id }
            while self.__iteratePosts():
                try:
                    self.currenturi = 'http://www.mrrebates.com' + self.soup.find('b', text='Next').parent.parent['href']
                    self.__setSoupForCurrentUri()
                except:
                    log.info(self.log_msg('No Previous URL found for url \
                                                        %s'%self.currenturi))
                    break
        except:
            log.exception(self.log_msg('Exception while fetching posts'))
        return True
    
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = self.soup.find('img', alt='Store Ratings').findParent('table').findAll('tr', recursive=False)[1].findAll('table')[1].findAll('tr', recursive=False)[1:]
            if not posts:
                log.info(self.log_msg('No posts found'))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(posts)))
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
            if not page:
                log.info(self.log_msg('page contains empty data, getdata \
                                    returns  False for uri %s'%self.currenturi))
                return True
            unique_key = get_hash(page)
            if checkSessionInfo(self.__genre, self.session_info_out, unique_key,\
                                        self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info returns True for uri %s'%unique_key))
                return False
            result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                get_hash( page ),'forum', self.task.instance_data.get('update'))
            if result['updated']:
                page['parent_path'] = []
                page['path'] = [unique_key]
                page['uri'] = self.currenturi
                page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
                log.info(page)
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
        page = {'entity':'review'}
        try:
            data_tag = post.findAll('td', recursive=False)[1]
            author_name_tag  = data_tag.font.b.extract()
            page['et_author_name'] = stripHtml(author_name_tag.renderContents())
            page['data'] =  stripHtml(data_tag.renderContents()).split('\n',1)[1].strip()
            page['title'] = ''
        except:
            log.info(self.log_msg("Data is not found for %s,discarding this Post"%(permalink)))
            return False 
        try:
            page.update(dict([('ef_rating_' + p.lower().replace('rating', '').strip(':').strip(),float(q.split('/')[0].strip())) for p,q, in dict([[y.strip() for y in stripHtml(x.renderContents()).splitlines() if y.strip()] for x in  post.find('td').findAll('tr')]).iteritems()]))
        except:
            log.info(self.log_msg('No Ratings found'))
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
    
    