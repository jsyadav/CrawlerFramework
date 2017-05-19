
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#LATHA

from datetime import datetime, timedelta
import re
import md5
from BeautifulSoup import BeautifulSoup
from tgimport import *
import logging
import copy
from urllib2 import urlparse
from baseconnector import BaseConnector
from utils.task import Task
from utils.utils import stripHtml
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from utils.utils import get_hash

#logging.config.fileConfig('logging.cfg')
log = logging.getLogger('ConsumerComplaintsConnector')

class ConsumerComplaintsConnector (BaseConnector):
    '''
    currently consumercomplaints fetches complaints and comments ,
    checks review_hash and comment_hash if its new data will added
    to self.pages otherwise not, comment_hash depend on div_id which
    is unique
    '''
    base_url='http://www.consumercomplaints.in'

    @logit(log, '_createurl')
    def _createurl(self):
        ''' 
        this func replaces  ' ' +  from self.task.instance_data['queryterm']
        '''
        try:
            url= 'http://www.consumercomplaints.in/?search=%s' % (self.task.instance_data['queryterm'].replace(' ', '+'))
            log.debug(self.log_msg("seed url : %s" %(url)))
            return url
        except:
            log.exception(self.log_msg("Exception occured while creating url"))
    
    @logit(log, 'fetch')
    def fetch(self):
        try:
            self.genre = 'Review'
            RESULTS_ITERATIONS = tg.config.get(path='Connector',key='consumer_complaints_numresults')
            self.COMMENTS_ITERATIONS= tg.config.get(path='Connector',key='consumer_complaints_commentresults')
            self.iterator_count = 0
            done = False
            if self.currenturi:
                self.currenturi='http://www.consumercomplaints.in/?search=%s' % re.search(re.compile(r'^http://www.consumercomplaints.in/\?search=(.+)'), self.currenturi).group(1).replace(' ', '+')
                                
            if not self.currenturi:
                self.currenturi = self._createUrl()
                if not self.currenturi:
                    log.debug(self.log_msg("Not a consumer complaints url and No search term provided, Quitting"))
                    return False
            res=self._getHTML()
            if res:
                self.rawpage=res['result']
                self._setCurrentPage()
            else:
                return False
            self.parenturi=self.currenturi
            self._getparentPage()
            while self.iterator_count < RESULTS_ITERATIONS and not done:
                try:
                    next_page = self.soup.find('td', {'class':'categories'}).find('a', href=True, text='Next')
                    if self.addReviews() and next_page:
                        #self.iterator_count = self.iterator_count + 1
                        log.info(self.log_msg('Entering into Next page from main page reviews'))
                        self.currenturi =  self.base_url+next_page.parent['href']
                        log.debug(self.log_msg(self.currenturi))
                        res=self._getHTML()
                        if res:
                            self.rawpage=res['result']
                            self._setCurrentPage()
                        else:
                            break
                    else:
                        done= True
                        log.info(self.log_msg('Reached last page of reviews'))
                        break
                except Exception, e:
                    raise e

            return True
        except Exception, e:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log, '_getparentPage')
    def _getparentPage(self):
         try:
            page ={}
            page['uri']=self.currenturi
            #page['title']=self.soup.h1.renderContents()
            tem=self.soup.h1.renderContents().split('matching')
            page['title']=tem[1].strip().strip('"')
            try:
                post_hash=get_hash(page)
                #post_hash = md5.md5(''.join(sorted(page.values())).encode('utf-8', 'ignore')).hexdigest()
            except:
                log.exception(self.log_msg('exception in building post_hash  moving onto next review'))
            log.debug('checking session info')
            if not checkSessionInfo(self.genre, self.session_info_out,
                                    self.currenturi, self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug(id)
                result=updateSessionInfo(self.genre, self.session_info_out, self.currenturi, post_hash, 'Post', self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    page['path']=[self.currenturi]
                    page['parent_path']=[]
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
         except Exception,e:
            log.exception(self.log_msg('Exception  in _getparentpage'))
            raise e



    @logit(log, 'addReviews')
    def addReviews(self):
        #parent_uri=parenturi
        try:
            links=self.soup.findAll('td', {'class':'complaint'})
            review_links=[x.find('a', href=True)['href']  for x in links]
            for review_link in review_links:
                self.iterator_count = self.iterator_count + 1
                page={}
                try:
                    review_link = 'http://www.consumercomplaints.in' + review_link
                    self.currenturi = review_link
                    parent_review_uri= self.currenturi
                    res=self._getHTML()
                    if res:
                        self.rawpage=res['result']
                        self._setCurrentPage()
                    else:
                        log.info(self.log_msg('No htm page found for this link'))
                        continue
                #log.debug("PARENT_LIST:" + str(parenturi))
                    if not checkSessionInfo(self.genre, self.session_info_out, self.currenturi,  self.task.instance_data.get('update'), parent_list=[self.parenturi]):
                    #log.debug("PARENT_LIST:" + str(parent_list))
                        try:
                            page['title']=stripHtml(self.soup.find('td', {'class':'complaint'}).renderContents()).strip()
                            
                        except:
                            log.exception(self.log_msg(' title could not be parsed'))
                            page['title'] =''
                        try:
                            temp=self.soup.find('td', {'class':'small'}).renderContents().split('by')
                            try:
                                temps=temp[0].split('Posted:')
                                page['posted_date']= datetime.strftime(datetime.strptime(temps[1].strip(),'%Y-%m-%d'),'%Y-%m-%dT%H:%M:%SZ')
                            except:
                                log.exception(self.log_msg('date could not be parsed'))
                                page['posted_date'] =datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                            try:
                                tem=temp[1].split('[')
                                page['et_author_name']=tem[0].strip()
                            except:
                                log.exception(self.log_msg('autor name could not be parsed'))
                                page['et_author_name']  =''
                        except:
                            log.exception(self.log_msg('soup object for DATE and AUTHOR NAME could not be parsed'))
                        try:
                            t_data=stripHtml(self.soup.find('td', {'class': 'compl-text'}).renderContents()).strip()
                            data = re.sub('\x00[1-9]', '', t_data)
                        #page['data']=stripHtml(self.soup.find('td', {'class': 'compl-text'}).renderContents()).strip()
                            page['data'] =data
                        except:
                            log.exception(self.log_msg('Review Data could not be parsed'))
                            page['data']=''
                        
                        try:
                            review_hash=get_hash(page)
                        #review_hash = md5.md5(''.join(sorted(page.values())).encode('utf-8', 'ignore')).hexdigest()
                        except:
                            log.exception(self.log_msg('exception in building review_hash  moving onto next review'))
                            continue
                        result=updateSessionInfo(self.genre, self.session_info_out, self.currenturi, review_hash, 'Review', self.task.instance_data.get('update'), parent_list=[self.parenturi])

                        if result['updated']:
                            parent_list = [self.parenturi]
                            page['parent_path'] = copy.copy(parent_list)
                            parent_list.append(self.currenturi)
                            page['path'] = parent_list
                            page['uri']=self.currenturi
                            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                            page['priority']=self.task.priority
                            page['level']=self.task.level
                            page['connector_instance_log_id'] = self.task.connector_instance_log_id
                            page['connector_instance_id'] = self.task.connector_instance_id
                            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                            page['last_updated_time'] = page['pickup_date']
                            page['versioned'] = False
                            page['workspace_id'] = self.task.workspace_id
                            page['client_id'] = self.task.client_id          
                            page['client_name'] = self.task.client_name
                            page['entity'] = 'Review'
                            page['category'] = self.task.instance_data.get('category','')
                            page['task_log_id']=self.task.id
                            page['versioned']=self.task.instance_data.get('versioned',False)
                        
                            self.pages.append(page)
                            #parenturi = [self.parenturi, self.currenturi]
                        nxt=True
                        while nxt:            
                            try:
                                if self.addComments(self.currenturi, [self.parenturi, parent_review_uri]):
                                    try:
                                        next=self.soup.find('a', href=True, text='Next')
                                        if next:
                                            self.currenturi=self.base_url+ next.parent['href']
                                            log.info(self.log_msg('setting Next page as self.currenturi'+  self.currenturi))
                                        else:
                                            nxt=False
                                            break
                                    except:
                                        log.exception(self.log_msg("No next page for the review links"))
                                        nxt=False
                                        break
                                else:
                                    log.info(self.log_msg('addcoments method returned false so break from the loop'))
                                    break
                            except :
                                log.exception(self.log_msg('exception in add_comments'))
                                continue
                except:
                    log.exception(self.log_msg("Exception occured while fetching reviewlinks"))
                    continue
            return True
        except Exception, e:
            log.exception(self.log_msg("Exception occured in  fetch method"))
            raise e
        

    @logit(log, 'addComments')
    def addComments(self, link, parent_list ):
        self.currenturi=link
        self.comment_count=0
        try:
            res=self._getHTML()
            if res:
                self.rawpage=res['result']
                self._setCurrentPage()
            else:
                return False
            #self.soup = BeautifulSoup(self.rawpage)
            fixdata = re.sub(r'<a name="([A-z0-9]+)">', r'<a name="\1"></a>', self.rawpage) #raw page is modified to overcome the error
            fixsoup=BeautifulSoup(fixdata)
            comments=fixsoup.find('table', {'class':'grey-normal'}).findAll('div')
            for comment in comments:
                self.comment_count= self.comment_count+1
                page={}
                try:
                    div_id =comment['id']
                except:
                    log.exception(self.log_msg("could not parse div_id"))
                    div_id=''

                if not checkSessionInfo(self.genre, self.session_info_out,
                                        div_id, self.task.instance_data.get('update'),
                                        parent_list=parent_list):
                    
                    try:
                        if comment.find('td', {'class':'compl-text'}):
                            t_data=stripHtml(comment.find('td', {'class':'compl-text'}).renderContents())
                            data = re.sub('\x00[1-9]', '', t_data)
                    #page['data']=stripHtml(comment.find('td', {'class':'compl-text'}).renderContents())
                            page['data']= data
                    except:
                        page['data'] =''
                        log.exception(self.log_msg("could not parse comment data"))
                    try:
                        if comment.find('td', {'class':'comments'}):
                            page['et_author_name']=comment.find('td', {'class':'comments'}).contents[1].split ('by')[1].strip("[")
                    except:
                        page['et_author_name'] = ''
                        log.exception(self.log_msg("could not parse author name"))
                    try:
                        hash=get_hash(page)
                    #hash = md5.md5(''.join(sorted(page.values())).encode('utf-8','ignore')).hexdigest()
                    except:
                         log.exception(self.log_msg("could not generate commen hash"))
                         continue
                    try:
                        if len(page['data']) > 50:                                                                                                                                                              
                            page['title'] = page['data'][:50] + '...'
                        else:
                            page['title'] = page['data']
                    except:
                        log.info(self.log_msg('could not parse title'))
                    try:
                        temp=comment.find('td', {'class':'comments'}).contents[1].split ('days')[0].strip()
                        date=datetime.strftime(datetime.now()-timedelta(int(temp)), "%Y-%m-%dT%H:%M:%SZ")
                        page['posted_date']=date
                    except:
                        log.info(self.log_msg('could not parse date'))
                        page['posted_date'] =datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    log.debug("PARENT_LIST:" + str(parent_list))
                    result=updateSessionInfo(self.genre, self.session_info_out, div_id, hash,
                                             'Comment', self.task.instance_data.get('update'),
                                             parent_list=parent_list)
                    if result['updated']:
                        page['parent_path'] = copy.copy(parent_list)
                        parent_list.append(div_id)
                        page['path'] = parent_list
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
                        page['entity'] = 'Comment'
                        page['category'] = self.task.instance_data.get('category' ,'')
                        self.pages.append(page)
                        if self.comment_count >= self.COMMENTS_ITERATIONS:
                            break
            return True           
        except Exception, e:
            log.exception(self.log_msg("Exception occured while fetching comments"))
            raise e
