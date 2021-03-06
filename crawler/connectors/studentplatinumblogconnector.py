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
log = logging. getLogger("StudentPlatinumBlogconnector")

class StudentPlatinumBlogconnector(BaseConnector):
    '''Connector for studentplatinum blog
    '''
    
    @logit(log,'fetch')
    def fetch(self):
        """
        It fetches the data from the url mentioned
        sample urls
        forum url: http://blog.studentplatinum.com/category/credit-cards/
        thread url: http://blog.studentplatinum.com/credit-cards/myth-credit-cards-are-evil/
        """
        self.genre = "Review"
        try:
            self.__setSoupForCurrentUri()
            if '/category/' in self.currenturi:
                while self.__getThreadPage():
                   try:
                        self.currenturi = self.soup.find('div','navigation').find('div','alignleft').find('a')['href']
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
        threads = self.soup.find('div',id='right_content').findAll('div','post')
        for thread in threads:
            try:
                temp_task = self.task.clone()
                try:
                    temp_task.instance_data['uri'] = stripHtml(thread.find('h3','storytitle').find('a')['href'])
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
            title_tag = self.soup.find('div', 'post').find('h3')
            self.__title_str =  page['title'] = stripHtml(title_tag.find('a').renderContents())
            page['data'] = stripHtml(self.soup.find('div', 'storycontent').renderContents())
        except:
            log.info(self.log_msg('No Title or Post is found in url %s'%self.currenturi))
            return
        try:
            categories = self.soup.find('div' ,'meta').findAll('a')
            page['et_data_category' ] = [stripHtml(each.renderContents()) for each in categories]
        except:
            log.info(self.log_msg('Data category not found in url %s'%self.currenturi))
        try:
            page['et_author_name'] = re.sub('^by ','', stripHtml(str(categories[-1].next.next))).strip()
        except:
            log.info(self.log_msg('Author name not found in the url %s'%self.currenturi))
        try:
            date_str = stripHtml(title_tag.find('span').renderContents().replace('|','').strip())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%m.%d.%y'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('posted_date not found in url %s'%self.currenturi))
            page['posted_date']= self.__task_elements_dict['pickup_date']
        try:
            comments_count = stripHtml(self.soup.find('h4',id='comments').renderContents())
            page['ei_num_comments'] =int(re.search('\d+',comments_count).group())
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
        comments_tag =  self.soup.find('ol',id='commentlist')
        if not comments_tag:
            log.info(self.log_msg('No comments found'))
            return False
        comments = comments_tag.findAll('li',id=re.compile('comment\-\d+'))
        log.debug(self.log_msg('# Of comments found is %d'%len(comments)))
        for comment in comments:
            try:
                unique_key = comment.find('a', text='#').parent['href']
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
        author_tag = comment.find('strong')
        if author_tag:
            page['et_author_name'] = stripHtml(author_tag.renderContents())
        data_tags = comment.findAll('p')
        if not len(data_tags) >=2:
            return False
        page['data'] = stripHtml(data_tags[0].renderContents())
        try:
            date_str = re.sub("(\d+)(st|nd|rd|th)",r"\1",re.sub('[|#]','',stripHtml(data_tags[-1].renderContents()))).strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
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