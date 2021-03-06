'''
Copyright (c)2008-2010 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Packiaraj

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
log = logging. getLogger("GradmoneyMattersBlogconnector")

class GradmoneyMattersBlogconnector(BaseConnector):
    '''Connector for Gradmoneymatters blog
    '''
    
    @logit(log,'fetch')
    def fetch(self):
        """
        It fetches the data from the url mentioned
        sample urls
        forum url: http://gradmoneymatters.com/search/label/Credit%20Cards
        thread url: http://gradmoneymatters.com/2009/08/credit-card-debt-among-college-students.html
        """
        self.genre = "Review"
        try:
            self.__setSoupForCurrentUri()
            if self.currenturi.startswith('http://gradmoneymatters.com/search/label/'):
                while self.__getThreadPage():
                   try:
                        self.currenturi = self.soup.find('div',id='blog-pager').find('a',title='Older Posts')['href']
                        self.__setSoupForCurrentUri()
                   except:
                        log.info(self.log_msg('Next Page link not found in url %s'%self.currenturi))
                        break
            else:
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
                self.__setParentPage()
                self.__addReviews()
            return True
        except:
            log.exception(self.log_msg("Exception in fetch"))
            return False
        
    @logit(log , '__getThreadPage')
    def __getThreadPage(self):
        """
        It will fetch each thread and its associate infomarmation
        and add the tasks
        """
        threads = self.soup.find('div',id='blog-posts').findAll('div','post')
        for thread in threads:
            try:
                temp_task = self.task.clone()
                try:
                    temp_task.instance_data['uri'] = stripHtml(thread.find('h3','post-title').find('a')['href'])
                except:
                    log.exception(self.log_msg('Cannot find the uri %s'%self.currenturi))
                    continue
            
                self.linksOut.append( temp_task )
                log.info(self.log_msg('Task  added'))
            except:
                log.info(self.log_msg('Cannot add the Task'))
        return True
        
    @logit(log,'getparentpage')    
    def __setParentPage(self):
        """It fetches the product information
        """
        page = {}
        self.__title_str = ''
        try:
            title_tag = self.soup.find('div', id='blog-posts').find('h3','post-title')
            self.__title_str =  page['title'] = stripHtml(title_tag.find('a').renderContents())
            page['data'] = stripHtml(self.soup.find('div','post-body').renderContents())
        except:
            log.info(self.log_msg('No Title or Post is found in url %s'%self.currenturi))
            return
        try:
            product_info = self.soup.find('div','post-footer')
            categories = product_info.find('span','post-labels').findAll('a')
            page['et_data_category' ] = [stripHtml(each.renderContents()) for each in categories]
        except:
            log.info(self.log_msg('Data category not found in url %s'%self.currenturi))
        try:
            author_info = product_info.find('p','post-footer-line post-footer-line-1')
            author_name = author_info.find('span','post-author').renderContents().split('by')
            page['et_author_name'] = stripHtml(author_name[1])
        except:
            log.info(self.log_msg('Author name not found in the url %s'%self.currenturi))
        try:
            auth_post_date = author_info.find('span','post-timestamp').renderContents().split('\n')
            date_str = auth_post_date[2]
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str, '%A, %B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('posted_date not found in url %s'%self.currenturi))
            page['posted_date']= self.__task_elements_dict['pickup_date']
        try:
            comments_count = stripHtml(self.soup.find('div',id='comments').find('h4').renderContents())
            page['ei_num_comments'] = int(re.search('\d+',comments_count).group())
        except:
            log.info(self.log_msg('Cannot find the comments count in url %s'%self.currenturi))
        
        if checkSessionInfo(self.genre, self.session_info_out, \
                self.task.instance_data['uri'],self.task.instance_data.get('update')):
                log.info(self.log_msg('Check Session info return True'))
                return 
        result = updateSessionInfo(self.genre, self.session_info_out,\
                    self.task.instance_data['uri'], get_hash(page) ,'Post',\
                                    self.task.instance_data.get('update'))
        if not result['updated']:
            log.info(self.log_msg('Update session info returns False for uri %s'%self.currenturi))
            return
        page['uri'] = self.task.instance_data['uri']
        page['path'] = [self.task.instance_data['uri']]
        page['parent_path'] = []
        page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
        page['entity'] = 'Post'
        page.update(self.__task_elements_dict)
        self.pages.append(page)
        
    @logit(log, '_addreviews')
    def __addReviews(self):
        '''It will fetch the the reviews and append it  to self.pages
        '''
        comments_tag =  self.soup.find('div',id='comments').find('dl',id='comments-block')
        if not comments_tag:
            log.info(self.log_msg('No comments found'))
            return False
        authors = self.soup.findAll('dt','comment-author')
        comment_body = self.soup.findAll('dd', attrs={'class':re.compile('comment-body')})
        footer = self.soup.findAll('dd', 'comment-footer')
        comments = zip(authors,comment_body,footer)
        log.debug(self.log_msg('# Of comments found is %d'%len(comments)))
        for comment in comments:
            try:
                unique_key = comment[2].find('span','comment-timestamp').find('a')['href']
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                            =[ self.task.instance_data['uri'] ]):
                    log.info(self.log_msg('session info return True in url %s'%self.currenturi))
                    continue
                page = self.__getData(comment)
                if not page:
                    log.info(self.log_msg('No data found in url %s'%self.currenturi))
                    continue                
                result = updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                    get_hash(page),'Review', self.task.instance_data.get('update'),\
                                    parent_list=[self.task.instance_data['uri']])
                if not result['updated']:
                    log.info(self.log_msg('result not updated'))
                    continue
                page['parent_path'] = [ self.task.instance_data['uri'] ]
                page['path'] = [ self.task.instance_data['uri'] ]
                page['path'].append( unique_key )
                page['entity'] = 'Comment'
                page['uri'] = self.task.instance_data['uri']
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                page.update(self.__task_elements_dict)                
                self.pages.append(page)
                log.info(self.log_msg('Comment Added'))
            except:
                log.exception(self.log_msg('Exception while adding session info in url %s'%self.currenturi))
            
    @logit(log, '__getData')
    def __getData(self, comment):
        '''This will get review Div tag and return a Dictionary if all fields 
        captured, if no data found, it will return False'''
        page = {'title':self.__title_str}
        author_tag = comment[0].find('a',rel='nofollow')
        if author_tag:
            page['et_author_name'] = stripHtml(author_tag.renderContents())
        try:
            data_tags = comment[1].find('p')
            page['data'] = stripHtml(data_tags.renderContents())
        except:
            log.info(self.log_msg('Cannot find data in the url %s'%self.currenturi))
        try:
            date_str = stripHtml(comment[2].find('span','comment-timestamp').find('a').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%B %d, %Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Cannot find posted date in the url %s'%self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        if not page['data']:
            log.info(self.log_msg('No data is found'))
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
            raise Exception('Page content not fetched for th url %s'%self.currenturi)
        self._setCurrentPage()