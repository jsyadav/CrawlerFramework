
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Latha

import re
from datetime import datetime
import logging
import copy
from urllib2 import urlparse

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('DrunkenDataConnector')

class DrunkenDataConnector (BaseConnector) :
    '''
    Fetches Data from http://www.drunkendata.com/. It is a blog site, uses self.genre='Review'
    sample url: http://www.drunkendata.com/?s=falconstor.
    '''
    ''' Given a url, it extracts links and enqueue all those links to temp_task '''
    uri_list=[]
    @logit(log , 'fetch')
    def fetch(self): 
        ''' 
        temp_task dict is used to store temp_task.instance_data[ 'uri' ], temp_task.pagedata['title'] for further use in _addReviews since no date and 
        title is available in review_page
        '''
        try:
            self.genre='Review'
            self.parent_url=self.currenturi
            self.parent_list=[self.currenturi]
            res=self._getHTML()
            if not res:
                return False
            self.rawpage=res['result']
            self._setCurrentPage()
            if re.search('http://www.drunkendata.com/\?s=.+', self.currenturi):
                while True:
                    for task in [each for each in self.soup.findAll('div', 'post')]:
                        log.info(self.log_msg('parentLink' + str(task.a['href']) ))
                        temp_task = self.task.clone()
                        temp_task.instance_data[ 'uri' ] = normalize( task.a['href'] )
                        temp_task.pagedata['title'] = stripHtml(task.find('h3').a.renderContents()).strip()
                        temp_task.pagedata['posted_date'] =datetime.strftime(datetime.strptime(re.sub(r'\b(\d+)(rd|th|st|nd\b)',\
                                                       r'\1', task.find('small').renderContents().strip()), '%A, %B %d, %Y') ,'%Y-%m-%dT%H:%M:%SZ')
                        self.linksOut.append(temp_task)
                    try:
                        if re.search ('Older Entries', self.soup.find('div', 'navigation').a.renderContents()) and\
                                self.soup.find('div', 'navigation').a['href']:
                            self.currenturi=self.soup.find('div', 'navigation').a['href']
                            res=self._getHTML()
                            if not res:
                                break
                            self.rawpage=res['result']
                            self._setCurrentPage()
                        
                        else:
                            break
                    except Exception, e:
                        log.exception(self.log_msg('No next page found for %s' %self.currenturi))
                        break
                return True
            self._getParentPage()
            self._addReviews()
            if not self._addComments():
                log.info(self.log_msg('comment information is not found '))
            return True
        
        except Exception, e:
            log.exception(self.log_msg("Exception in Fetch"))
            return False

    @logit (log, "_getParentPage")
    def _getParentPage(self):
        try:
            page={}
            if not checkSessionInfo(self.genre, self.session_info_out, self.parent_url \
                                        , self.task.instance_data.get('update')):
                try:
                    page['title'] = stripHtml(self.soup.find('h2').renderContents()).strip()
                except:
                    log.exception(self.log_msg('Title is not found'))
                    page['title'] =''
                try:    
                    post_hash = get_hash(page)
                except:
                    log.exception(self.log_msg("Exception in Building posthash in _getParentpage"))
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out,self.parent_url, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    page['path']=[self.parent_url]
                    page['parent_path']=[]
                    page['uri'] = self.parent_url
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['data'] = ''
                    page['task_log_id']=self.task.id
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')
                    self.pages.append(page)
                    log.info(self.log_msg('Parent Page title is added to the page dictionary'))
                    return True
                else:
                    log.info(self.log_msg('Parent Page title is not  added to the page dictionary'))
                    return False
                                                        
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            raise e

    @logit (log, "_addReviews")
    def _addReviews(self):
        '''page['posted_date'] is get from self.task.pagedata['posted_date'] ....same for review title '''
        try:
            page={}
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    self.currenturi,  self.task.instance_data.get('update'), 
                                    parent_list=[self.parent_url]):
                try:
                    page['data']=stripHtml(self.soup.find('div', 'entry').renderContents()).strip()
                except:
                    page['data']=''
                try:
                    page['title']= self.task.pagedata['title']
                except:
                    if len(page['data']) > 50:
                        page['title'] = page['data'][:50] + '...'
                    else:
                        page['title'] = page['data']

                try:
                    page['posted_date']=self.task.pagedata['posted_date']
                except:
                    page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    log.exception(self.log_msg('could not parse date for '+ self.currenturi))
                try:
                    review_hash = get_hash(page)
                except:
                    log.exception(self.log_msg('could not generate review_hash for '+ self.parent_url))
                    
            result=updateSessionInfo(self.genre, self.session_info_out, review_hash, \
                                         review_hash,'Review', self.task.instance_data.get('update'),\
                                         parent_list=[self.parent_url])
            if result['updated']:
                parent_list = [self.parent_url]
                page['parent_path']= copy.copy(parent_list)
                parent_list.append(review_hash)
                page['path']=parent_list
                page['uri'] = normalize(self.parent_url)
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                page['client_name'] = self.task.client_name
                page['last_updated_time'] = page['pickup_date']
                page['versioned'] = False
                page['entity'] = 'Review'
                page['category'] = self.task.instance_data.get('category','')
                page['task_log_id']=self.task.id
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append(page)
                self.parent_list.append(review_hash)
                log.info(self.log_msg('Adding review of %s ' % self.currenturi))
                return True
            else:
                log.info(self.log_msg('Not adding review of %s ' % self.currenturi))
                
        except Exception, e:
            log.exception(self.log_msg('Exception in addReviews'))
            raise e
            
    @logit (log, "_addComments")
    def _addComments(self):
        '''if comment is not found for the particular review, addComments returns false''' 
        try:
            if self.soup.find('ol', 'commentlist'):
                comments= self.soup.find('ol', 'commentlist').findAll('li')
            else:
                log.info(self.log_msg('No comments found for this page %s'% self.parent_url))
                return False
            for comment in comments:
                page={}
                try:
                    comment_id= comment['id']
                except:
                    comment_id=self.parent_url
                    log.exception(self.log_msg('Excepion in identifying comment id.. continue'))
                    
                if not checkSessionInfo(self.genre, self.session_info_out,
                                        comment_id, self.task.instance_data.get('update'),
                                        parent_list=self.parent_list):
                    try:
                        if comment.cite.a:
                            page['et_author_name'] =stripHtml(comment.cite.a.renderContents()).strip()
                        else:
                            page['et_author_name'] =stripHtml(comment.cite.renderContents()).strip()
                    except:
                        page['et_author_name'] =''
                        log.exception(self.log_msg("Exception in fetching author name"))
                    try:
                        pos=re.sub(r'\b(\d+)(rd|th|st|nd\b)', r'\1', comment.small.a.renderContents()).index('at')
                        page['posted_date']=datetime.strftime(datetime.strptime(re.sub(r'\b(\d+)(rd|th|st|nd\b)',\
                                              r'\1', comment.small.a.renderContents().strip())[0:pos], '%B %d, %Y ') ,'%Y-%m-%dT%H:%M:%SZ')
                    except :
                        page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        log.exception(self.log_msg('could not parse posted_date for the comment'))
                     
                    try:
                        page['data']=''.join([stripHtml(each.renderContents()).strip() for each in comment.findAll('p')])                        
                    except:
                        page['data']=''
                        log.exception(self.log_msg("Exception in fetching data from the comment"))
                    try:
                        hash=get_hash(page)
                    except:
                        log.exception(self.log_msg('Could not generate hash for the comment'))
                        continue
                    try:
                        if len(page['data']) > 50:
                            page['title'] = page['data'][:50] + '...'
                        else:
                            page['title'] = page['data']
                    except:
                        log.exception(self.log_msg('could not parse title'))


                    result=updateSessionInfo(self.genre, self.session_info_out, hash, hash, 
                                             'Comment', self.task.instance_data.get('update'), 
                                             parent_list=self.parent_list)
                    if result['updated']:
                        parent_list = copy.copy(self.parent_list)
                        page['parent_path']=copy.copy(parent_list)
                        parent_list.append(hash)
                        page['path']=copy.copy(parent_list)
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['last_updated_time'] = page['pickup_date']
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id  
                        page['client_name'] = self.task.client_name
                        page['versioned'] = False
                        page['uri'] = self.parent_url
                        page['uri_domain'] =  urlparse.urlparse(page['uri'])[1]
                        page['task_log_id']=self.task.id
                        page['entity'] = 'Comment'
                        page['category'] = self.task.instance_data.get('category' ,'')
                        self.pages.append(page)
                        log.info(self.log_msg('Adding comment of %s ' % self.currenturi))
                        return True
                    else:
                        log.info(self.log_msg('Not adding comment of %s ' % self.currenturi))
                        return False
            return True        
        except Exception, e:
            log.exception(self.log_msg('Exception in _addComments'))
            return False
