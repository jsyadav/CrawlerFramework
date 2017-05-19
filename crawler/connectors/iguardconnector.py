'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#MOHIT RANKA
#SKumar
from urlparse import urlparse
import re
import logging
from datetime import datetime
from baseconnector import BaseConnector

from utils.sessioninfomanager import *
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import *

log = logging.getLogger('IGuardConnector')

class IGuardConnector(BaseConnector):
    @logit(log,'fetch')
    def fetch(self):
        """
        Fetched all the posts for a given self.currenturi and returns Fetched staus depending
        on the success and faliure of the task
        """
        try:
            self.genre="Review"
            res=self._getHTML(self.currenturi)
            self.rawpage=res['result']
            self._setCurrentPage()
            self.__getParentPage()
            next_page_no = 2
            while True:
                try:
                    self.__addReviews()
                    self.currenturi = 'http://www.iguard.org' + self.soup.find('div','comment_navigation').find('a',text=str(next_page_no)).parent['href']
                    res=self._getHTML(self.currenturi)
                    self.rawpage=res['result']
                    self._setCurrentPage()
                    next_page_no += 1
                except:
                    log.info(self.log_msg('No Next Page found, fetched all reviews'))
                    break
            return True
        except Exception,e:
            log.exception(self.log_msg("Exception occured in fetch()"))
            return False

    @logit(log,'_getParentPage')
    def __getParentPage(self):
        """
        Gets the data for the parent page and appends it to self.pages http://www.iguard.org/drugs/Singulair.htmlhttp://www.iguard.org/drugs/Singulair.htmlif the parent page has changed since the last crawl.
        An empty dictionay is added to self.pages if the parent page is not changed since the last crawl
        """
        page={}
        try:
            page['title'] = stripHtml(self.soup.find('h1').renderContents()).replace('Get Informed:','').strip()
            post_hash = self.task.instance_data['uri']
            if checkSessionInfo(self.genre, self.session_info_out,
                                    self.task.instance_data['uri'], self.task.instance_data.get('update')):
                log.info(self.log_msg('Check session info returns True'))
                return False
            result=updateSessionInfo(self.genre, self.session_info_out, self.task.instance_data['uri'], post_hash,
                                         'Post', self.task.instance_data.get('update'))
            if not result['updated']:
                log.info(self.log_msg('Result not updated'))
                return False
            page['data']=''
            page['path']=[ self.task.instance_data['uri'] ]
            page['parent_path']=[]
            page['task_log_id']=self.task.id
            page['versioned']=self.task.instance_data.get('versioned',False)
            page['category']=self.task.instance_data.get('category','generic')
            page['last_updated_time']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['client_name']=self.task.client_name
            page['entity']='post'
            page['uri'] = normalize(self.currenturi)
            page['uri_domain'] = unicode(urlparse(self.currenturi)[1])
            page['priority']=self.task.priority
            page['level']=self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            self.pages.append(page)
            log.debug(self.log_msg("Main page details stored"))
            return True
        except:
            log.info(self.log_msg('Cannot add the page'))
            return False
        
    @logit(log,'__addReviews')
    def __addReviews(self):
        '''This will fetch the data and other info and return a page dict
        '''
        try:
            reviews = []
            comments = self.soup.findAll('div','comment_block')
            if not comments:
                log.info(self.log_msg('No Comments found'))
                return False
            temp_block = [ comments[0] ]                
            for i,each in enumerate(comments[1:]):
                if stripHtml(each.find('h4').renderContents()).startswith('Question'):
                    reviews.append (temp_block)
                    temp_block = [each]
                else:
                    temp_block.append (each)
                if i==(len(comments)-2):
                    reviews.append (temp_block)
        except:
            log.exception(self.log_msg('Cannot get the reviews '))
        try:
            for review_and_comments in reviews:# review is a list containig Reviews and its comments
                try:
                    page = self.__getData(review_and_comments[0])
                    page['entity'] = 'review'
                    question_unique_key = get_hash( {'data':page['data'],'title':page['title']})
                    self.__addPage(page,question_unique_key, [ self.task.instance_data['uri'] ])
                    if len(review_and_comments)>1:
                        for answer in review_and_comments[1:]:
                            page = self.__getData(answer)
                            page['entity'] = 'comment'
                            unique_key = get_hash( {'data':page['data'],'title':page['title']})
                            self.__addPage( page,unique_key,[ self.task.instance_data['uri'], question_unique_key])
                except:
                    log.info(self.log_msg('Cannnot add the Review'))
        except:
            log.info(self.log_msg('Cannot find the comments'))
            return False
    
    @logit(log,'__addPage')
    def __addPage(self,page,unique_key,parent_list):
        '''This will add the page dict to pages
        '''
        try:
            if checkSessionInfo(self.genre, self.session_info_out,
                                    unique_key, self.task.instance_data.get('update'),
                                    parent_list = parent_list):
                log.info(self.log_msg('Check sessio info return True'))
                return False
            result=updateSessionInfo(self.genre, self.session_info_out, unique_key, get_hash(page),
                                         page['entity'], self.task.instance_data.get('update'), parent_list=parent_list)
            if not result['updated']:
                log.info(self.log_msg('Update session info returns False'))
                return False
            page['parent_path'] = parent_list[:]
            parent_list.append(unique_key)
            page['path'] = parent_list
            page['uri']=normalize(self.currenturi)
            page['task_log_id']=self.task.id
            page['versioned']=self.task.instance_data.get('versioned',False)
            page['category']=self.task.instance_data.get('category','generic')
            page['last_updated_time']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['client_name']=self.task.client_name
            page['uri_domain'] = urlparse(page['uri'])[1]
            page['priority']=self.task.priority
            page['level']=self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            self.pages.append(page)
            log.info(self.log_msg('Page added'))
            return True
        except:
            log.exception(self.log_msg('Error while adding the page'))
            return False
             
    
    @logit(log,'__getData')
    def __getData(self,review):
        '''This will fetch the data and other info and return a page dict
        '''
        page={'title':''}
        try:
            page['data']=stripHtml(review.find('p').renderContents())
            page['title'] = page['data'][:50]
        except:
            log.exception(self.log_msg("Error occured while fetching question data"))
            return False
        try:
            date_info = stripHtml(review.find('p','poster_information').renderContents()).split('\n')
            page['ei_author_age'] = int(re.findall('\d+',date_info[0])[0])
            page['et_author_gender']=date_info[-1].strip()
        except:
            log.info(self.log_msg("Could  not get author information"))
        try:
            date_str = stripHtml(review.find('p','post_information').renderContents()).replace('Posted: ','')
            page['posted_date']= datetime.strftime(datetime.strptime(date_str,"%Y-%m-%d %H:%M:%S"),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg("Could not fetch date information"))
        return page