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
log = logging. getLogger("YoungmoneyConnector")

class YoungmoneyConnector(BaseConnector):
    '''Connector for Youngmoney
    '''
    
    @logit(log,'fetch')
    def fetch(self):
        """
        It fetches the data from the url mentioned
        sample urls
        forum url: http://local.youngmoney.com/Technology-s1545.html
        thread url: http://www.youngmoney.com/cell_phones/iPhone_save_money/
        """
        self.genre = "Review"
        try:
            self.__setSoupForCurrentUri()
            if re.search('s\d+.html$',self.currenturi):
                self.__getThreadPage()
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
        base_url = 'http://local.youngmoney.com/'
        thread_urls = [ base_url + x.find('a')['href'] for x in self.soup.find('div','mainLeft').findAll('h2',recursive=False)]
        for thread_url in thread_urls:
            try:
                self.currenturi = thread_url
                self.__setSoupForCurrentUri()
                temp_task = self.task.clone()
                temp_task.instance_data['uri'] = self.soup.find('a',target='_blank')['href']
                self.linksOut.append( temp_task )
                log.info(self.log_msg('Task  added'))
            except:
                log.info(self.log_msg('Cannot add the Task'))
        return True

    def __setParentPage(self):
        """It fetches the product information
        """
        page = {}
        self.__title_str = ''
        try:
            title_tag = self.soup.find('div', id='article_container')
            self.__title_str =  page['title'] = stripHtml(title_tag.find('div','article_heading').renderContents())
            page['data'] = '\n'.join([stripHtml(x.renderContents()) for x in self.soup.find('div',id='article_container').findAll('p')[1:]])
        except:
            log.info(self.log_msg('No Title or Post is found in url %s'%self.currenturi))
            return False
        try:
            categories = self.soup.find('div','breadcrumbs').findAll('a')
            page['et_data_category' ] = stripHtml(categories[1].renderContents())
        except:
            log.info(self.log_msg('Data category not found in url %s'%self.currenturi))
        try:
            prod_info = self.soup.find('div',id='article_container').find('p')
            page['et_author_name'] = stripHtml(str(prod_info.find('span'))).replace('By','').strip()
        except:
            log.info(self.log_msg('Author name not found in the url %s'%self.currenturi))
        try:
            date_info = stripHtml(prod_info.renderContents()).split('\n')
            date_str = date_info[2].strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%d %B %Y'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('posted_date not found in url %s'%self.currenturi))
            page['posted_date']= self.__task_elements_dict['pickup_date']
        try:
            comments_count = stripHtml(self.soup.find('h3',id='comments').renderContents())
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
        '''It will fetch the the reviews and append it to self.pages
        '''
        comments_tag =  self.soup.find('table','comments margin')
        if not comments_tag:
            log.info(self.log_msg('No comments found'))
            return False
        comments = comments_tag.findAll('tr')
        log.debug(self.log_msg('# Of comments found is %d'%len(comments)))
        for comment in comments:
            try:
                page = self.__getData(comment)
                if not page:
                    log.info(self.log_msg('No data found in url %s'%self.currenturi))
                    continue
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                            =[ self.task.instance_data['uri'] ]):
                    log.info(self.log_msg('session info return True in url %s'%self.currenturi))
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
        try:
            page = {'title':self.__title_str}
            comment_info = comment.find('div','commenttext')
            try:
                author_tag = comment_info.find('cite').find('strong')
                page['et_author_name'] = stripHtml(author_tag.renderContents())
            except:
                log.info(self.log_msg('Cannot find author name in the url %s'%self.currenturi))
            try:
                comment_data = comment_info.findAll('p')
                page['data'] = '\n'.join([stripHtml(each.renderContents()) for each in comment_data])
            except:
                log.info(self.log_msg('Cannot find data in the url %s'%self.currenturi))
            try:
                date_info = stripHtml(comment.find('div','commentmetadata').renderContents()).replace('#','').strip()
                date_str = re.sub("(\d+)(st|nd|rd|th)",r"\1",date_info).strip()
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%d %B %Y at %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.info(self.log_msg('Cannot find posted date in the url %s'%self.currenturi))
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            if not page['data']:
                log.info(self.log_msg('No data is found'))
                return False
            return page
        except:
            log.info(self.log_msg('No Data found for this Post'%self.currenturi))

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