
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

import re
import md5
import copy
import logging
import urlparse
from datetime import datetime
from BeautifulSoup import BeautifulSoup

from tgimport import *
from baseconnector import BaseConnector
from utils.task import Task
from utils.utils import stripHtml
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('digital camerahq Connector')

class DigitalCameraHqConnector(BaseConnector):
    """
    Fetches reviews through read more option and navigate through next links utill it reaches the last page
    """
    base_uri='http://www.digitalcamera-hq.com'

    @logit(log , 'fetch')
    def fetch(self):
        """
        sets the current page and fetches reviewLinks
        """
        self.genre="Review"
        try:
            if re.match("^http://www.digitalcamera-hq.com/digital-cameras(.+)_reviews.html$",self.currenturi):
                res=self._getHTML()
                self.rawpage=res['result']
                self._setCurrentPage()
                if self._getParentPage(self.currenturi):
                    self.parent_url = self.currenturi
                    self._getReviewLinks()
        except  Exception, e:
            log.exception(self.log_msg('Exception in fetching so returning False'))
            return False
        return True
    @logit(log , '_getParentPage')
    def _getParentPage(self,parent_uri):
        try:
            page={}
            if self.soup.html.head.title :
                page['title'] =  str(self.soup.html.head.title.renderContents()).split('-')[0].replace('\n', '').strip()
            else:
                page['title']=re.match('http://www.digitalcamera-hq.com/digital-cameras/(.+)(-|_)review.html', self.parent_url).group(1)
            try:
                if self.soup.find('p', {'class':'ratingInfo'}):
                    page['ef_overall_rating'] =float(self.soup.find('p', {'class':'ratingInfo'}).strong.renderContents().split('/')[0]) 
            except:
                page['ef_overall_rating'] =0.0
            try:
                post_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , 
                                                       page.values()))).encode('utf-8','ignore')).hexdigest()

            except:
                log.exception(self.log_msg('exception in building post_hash  moving onto next review'))
                log.debug('checking session info')
            if not checkSessionInfo(self.genre, self.session_info_out,
                                    parent_uri, self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug(id)
                result=updateSessionInfo(self.genre, self.session_info_out, parent_uri, post_hash, 'Post', self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    page['path']=[parent_uri]
                    page['parent_path']=[]
                    page['uri'] =self.currenturi
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id                                                        
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['data'] = ''
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')
                    page['task_log_id']=self.task.id
                    page['client_name'] = self.task.client_name
                    page['versioned']=self.task.instance_data.get('versioned',False)
                    self.pages.append(page)
        except Exception, e:
            log.exception(self.log_msg('Could not parse titl for the parent page'))
            raise e
            return False
        return True
 
    @logit(log, '_getReviewLinks')
    def _getReviewLinks (self):
        log.info(self.log_msg('going to get links for the page of ' + self.currenturi))
        try:
            if [each.find('a', {'href':True})['href'] for each in self.soup.findAll('div', {'class':'review'}) if each.find('a', {'href':True})]:
                temp=[each.find('a', {'href':True})['href'] for each in self.soup.findAll('div', {'class':'review'}) if each.find('a', {'href':True})]
                self.currenturi=self.base_uri + temp[0]
                res=self._getHTML(self.currenturi)
                self.rawpage=res['result']
                self._setCurrentPage()
                while True:
                    next = self.soup.find('div', {'class':'tabHeader'}).find('img', {'src':re.compile('.+next.png$')}) 
                    if self.addReviews() and next:
                        self.currenturi=self.base_uri+next.parent['href']
                        res=self._getHTML(self.currenturi)
                        self.rawpage=res['result']
                        self._setCurrentPage()
                    else:
                        break
            else:
                self.currenturi=self.parent_url
                self.addReviews()
        except  Exception, e:
            log.exception(self.log_msg('Exception occured while fetching reviewLinks'))
    @logit(log, 'addReviews')    
    def addReviews(self):
        """
        Navigate through reviewlinks --- add info to self.page, no comments section for these urls
        """
        log.info(self.log_msg('Going to fetch review for the page ' + self.currenturi))
        try:
            page={}
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    self.currenturi,  self.task.instance_data.get('update'), 
                                    parent_list=[self.parent_url]):

                try:
                    if [each.find('li', {'class':'currentRating'}) for each in self.soup.findAll('div', {'class': 'review'})[-1:]] :
                        page['ef_review_rating'] = float(''.join([each.find('li', {'class':'currentRating'}).renderContents().split('out')[0].strip() for each in self.soup.findAll('div', {'class': 'review'})[-1:]]))
                        print page['ef_review_rating']
                except:
                    page['ef_review_rating'] =0.0
                    log.exception(self.log_msg('could not parse rating for the review page ' + self.currenturi))
                try:
                    if self.soup.find('div', {'class': 'reviewText'}).h1:
                        page['title']=''.join([stripHtml(each.find('div', {'class': 'reviewText'}).h1.renderContents()).strip() for each in self.soup.findAll('div', {'class': 'review'})[-1:]])
                        
                    else:
                        page['title'] =''.join([stripHtml(each.find('div', {'class': 'reviewText'}).findNext('strong').renderContents()) for each in self.soup.findAll('div', {'class': 'review'})[-1:]])
                except:
                    page['title'] =''
                    log.exception(self.log_msg('could not parse title for the review page ' + self.currenturi))
                try:
                    page['et_author_name']= ''.join([each.find('i', {'class': True}).renderContents().split('<!--</a>-->')[0].lstrip().lstrip('(') for each in self.soup.findAll('div', {'class': 'review'})[-1:]])
                    
                except:
                    page['et_author_name'] =''
                try:
                    temp =[each.find('div', {'class': 'reviewText'}).findAll('p') for each in self.soup.findAll('div', {'class': 'review'})[-1:]]
                    if temp:
                        page['data'] = ''.join([stripHtml(str(tem)) for tem in temp[0]])
                    else: page['data'] =''    
                except:
                    page['data'] =''
                    log.exception(self.log_msg('could not parse DATA for the review page ' + self.currenturi))
                try:
                    review_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , page.values()))).encode('utf-8','ignore')).hexdigest()
                except:
                    log.exception(self.log_msg('exception in building review_hash'))
                result=updateSessionInfo(self.genre, self.session_info_out, 
                                         self.currenturi, review_hash, 'Review', 
                                         self.task.instance_data.get('update'), parent_list=[self.parent_url])
                if result['updated']:
                    parent_list = [self.parent_url]
                    page['parent_path']=copy.copy(parent_list)
                    parent_list.append(self.currenturi)
                    page['path']=parent_list
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['last_updated_time'] = page['pickup_date']
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project 
                    page['client_name'] = self.task.client_name
                    page['versioned'] = False
                    page['uri'] = self.currenturi
                    page['uri_domain'] = urlparse.urlparse(self.currenturi)[1]
                    page['task_log_id']=self.task.id
                    page['entity'] = 'Review'
                    page['category'] = self.task.instance_data.get('category' ,'')
                    try:
                        temp= ''.join([each.find('i', {'class': True}).renderContents().split('<!--</a>-->')[1].strip('-\n ').strip(')') for each in self.soup.findAll('div', {'class': 'review'})[-1:]])
                        page['posted_date']=datetime.strftime(datetime.strptime(temp.replace('EDT', ''),"%a %b %d %H:%M:%S %Y") ,"%Y-%m-%dT%H:%M:%SZ")
                        print page['posted_date']
                    except:
                        page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    self.pages.append(page)
                    log.info(self.log_msg('Process has been completed for ' + self.currenturi))
        except Exception, e:
            return False
        return True
