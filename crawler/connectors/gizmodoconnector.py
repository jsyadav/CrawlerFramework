'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#SKumar

import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging. getLogger("GizmodoConnector")

class GizmodoConnector(BaseConnector):
    '''Connector for gizmodo.com
    '''
    
    @logit(log,'fetch')
    def fetch(self):
        """
        It fetches the data from the url mentioned
        sample urls
        http://gizmodo.com/5396168/motorola-droid-review
        """
        self.genre = "Review"
        try:
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
            self.__setParentPage()
            self.currenturi = 'http://gizmodo.com/index.php'
            data = {}
            data['cpage'] = '1'
            data['mode'] = 'postComments'
            data['op'] = 'commentlist'
            data['postId'] = self.task.instance_data['uri'].split('/')[-2]
            data['priority'] ='1'
            data['sort'] ='desc'
            self.__setSoupForCurrentUri(data=data)
            self.__addReviews()
            return True
        except:
            log.exception(self.log_msg("Exception in fetch"))
            return False
        
    @logit(log,'getparentpage')    
    def __setParentPage(self):
        """It fetches the product information
        """
        page = {}
        try:
            self.__title_str = page['title'] = re.sub(' Review$','', stripHtml(self.soup.find('div',\
                        'content permalink').find('h1').renderContents())).strip()
        except:
            log.info(self.log_msg('No Title is found in url %s'%self.currenturi))
            return
        try:
            content_tag = self.soup.find('div', 'content permalink')
            contact_info_tag = content_tag.find('p', 'contactinfo')
            if contact_info_tag:
                contact_info_tag.extract()
            page['data'] = page['data'] ='\n'.join([stripHtml(x.renderContents()) for x in content_tag.findAll('p')])        
        except:
            log.info(self.log_msg('No data is found in the url %s'%self.currenturi))
        
        if checkSessionInfo(self.genre, self.session_info_out, \
                self.task.instance_data['uri'],self.task.instance_data.get('update')):
                log.info(self.log_msg('Check Session info return True'))
                return 
        result = updateSessionInfo(self.genre, self.session_info_out,\
                    self.task.instance_data['uri'], get_hash(page) ,'Review',\
                                    self.task.instance_data.get('update'))
        if not result['updated']:
            log.info(self.log_msg('Update session info returns False for uri %s'%self.currenturi))
            return
        page['uri'] = self.task.instance_data['uri']
        page['data'] = ''
        page['path'] = [self.task.instance_data['uri']]
        page['parent_path'] = []
        page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])        
        page['entity'] = 'Review'
        page.update(self.__task_elements_dict)
        page['posted_date'] = page['pickup_date']
        self.pages.append(page)
        
    @logit(log, '_addreviews')
    def __addReviews(self):
        '''It will fetch the the reviews and append it  to self.pages
        '''
        reviews= [x.findParent('div').findParent('div')  for x in self.soup.findAll('span' ,'ctedit')]
        log.debug(self.log_msg('# Of Reviews found is %d'%len(reviews)))
        for review in reviews:
            try:
                unique_key = review.find('a')['name']
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                            =[ self.task.instance_data['uri'] ]):
                    log.info(self.log_msg('session info return True in url %s'%self.currenturi))
                    continue
                page = self.__getData(review)
                if not page:
                    log.info(self.log_msg('No data found in url %s'%self.currenturi))
                    continue                
                result = updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                    get_hash(page),'comment', self.task.instance_data.get('update'),\
                                    parent_list=[self.task.instance_data['uri']])
                if not result['updated']:
                    log.info(self.log_msg('result not updated'))
                    continue
                page['path'] = page['parent_path'] = [ self.task.instance_data['uri'] ]
                page['path'].append( unique_key )
                page['entity'] = 'comment'
                page['uri'] = self.task.instance_data['uri']
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                page.update(self.__task_elements_dict)                
                self.pages.append(page)
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Exception while adding session info in url %s'%self.currenturi))
            
    @logit(log, '__getData')
    def __getData(self, review):
        '''This will get review Div tag and return a Dictionary if all fields 
        captured, if no data found, it will return False'''
        page = {'title':self.__title_str}  # Title Changed
        author_tag = review.find('a', 'avatar_link', href=True)
        if author_tag:
            page['et_author_name'] = stripHtml(author_tag.renderContents())
            page['et_author_profile'] = author_tag['href']
        try:
            date_str = stripHtml(review.find('a', 'avatar_time').renderContents())
            date_obj = datetime.strptime(date_str,'%m/%d/%y')
        except:
            log.info(self.log_msg('posted date cannot be parsed in url %s'%self.currenturi))
            date_obj = datetime.utcnow()
        page['posted_date'] = datetime.strftime(date_obj,"%Y-%m-%dT%H:%M:%SZ")
        try:
            data_str = stripHtml(review.find('span' ,'ctedit').renderContents())
            reply_author_match = re.search('@\s*.+?:', data_str)
            if reply_author_match:
                author_name = reply_author_match.group() # Variable Fixed
                page['et_data_replied_author'] = author_name[1:-1].strip()
                data_str = data_str.replace(author_name,'',1).strip()
            page['data'] = data_str
        except:
            log.info(self.log_msg('Data not found in url %s'%self.currenturi)) # Url Fixed
            page['data'] = ''
        if not page['data']:
            log.info(self.log_msg('Empty data is found for url %s'%self.currenturi)) # URL Fixed
            return False
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
            raise Exception('Page content not fetched for the url %s'%self.currenturi)
        self._setCurrentPage()