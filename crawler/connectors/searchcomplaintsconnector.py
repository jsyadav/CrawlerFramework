
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#LATHA
#Ashish

from datetime import timedelta
import re
import md5
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import traceback
import logging
import pickle
from urllib2 import urlparse,unquote,Request,urlopen
import urllib
import traceback
import simplejson
from xml.sax import saxutils
import copy
from tgimport import *
from baseconnector import BaseConnector
from utils.task import Task
from utils.utils import stripHtml
from utils.urlnorm import normalize
from knowledgemate import pysolr
from knowledgemate import model
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
#logging.config.fileConfig('logging.cfg')
log = logging.getLogger('ComplaintsConnector')

class SearchComplaintsConnector(BaseConnector):

    @logit(log, '_createurl')
    def _createurl(self):
        '''
        extracts query term from self.currenturi and create the url based on sort by date;
        '''
        try:
            key_term= re.search('q=([a-zA-Z0-9_\s?\+]+)',self.currenturi).group(1)
            url='http://search.complaints.com/search?q=%s&btnG=Search+Complaints.com&output=xml_no_dtd&sort=date3AD3AS3Ad1&client=complaints&y=0&oe=UTF-8&ie=UTF-8&proxystylesheet=complaints&x=0&site=complaints' % re.sub('\s+', '+', key_term)
            return  url.replace('3AD3AS3Ad1', '%3AD%3AS%3Ad1')
        except:
            log.exception(self.log_msg("Exception occured while creating url"))
    @logit(log, 'fetch') 
    def fetch(self):
        '''
        sets the current page. parse the next page complaints links if any ...
        sends the parenturi to self.addReviews, fetches contents, returns true or False
        '''
        try:
            self.genre = 'Review'
            self.session_info_out = copy.copy(self.task.session_info)
            self.review_count=0
            if self.currenturi:
                self.currenturi=self. _createurl()
                if not self.currenturi:
                    log.debug(self.log_msg("Not a consumer complaints url and No search term provided, Quitting"))
                    self.task.status['fetch_status']=False
                    return False
            res=self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            parenturi=normalize(self.currenturi)
            self.getparentPage()
            while True:
                try:
                    next_page=self.soup.find('a' , href=True, text='Next')
                    if self.addReviews(parenturi) and next_page:
                        log.info(self.log_msg('Entering into Next page from main Page '))
                        self.currenturi = 'http://search.complaints.com/'+next_page.parent['href']
                        res=self._getHTML()
                        self.rawpage=res['result']
                        self._setCurrentPage()
                    else:
                        log.info(self.log_msg('Reached last page of reviews'))
                        break
                except Exception, e:
                    raise e
            self.task.session_info = self.session_info_out ### copy session_info for next turn                    
            self.task.status['fetch_status']=True
            return True
        except:
            self.task.status['fetch_status']=False
            log.exception(self.log_msg("Exception Occured in Fetch"))
            return False
            
    @logit(log, 'getparentPage')
    def getparentPage(self):
        '''fetches title from the parent page'''
        try:
            page ={}
            page['title'] = stripHtml(self.soup.find('td', {'class':'t', 'nowrap':'1'}).find('b').renderContents()).strip()
            try:
                post_hash = md5.md5(''.join(sorted(page.values())).encode('utf-8', 'ignore')).hexdigest()
            except:
                log.exception(self.log_msg('exception in building post_hash'))
            log.debug('checking session info')
            page['uri'] =self.currenturi
            if not checkSessionInfo(self.genre, self.session_info_out,
                                    page['uri'], self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug(id)
                result=updateSessionInfo(self.genre, self.session_info_out, page['uri'], post_hash, 'Post', self.task.instance_data.get('update'), Id=id)
                if result['updated']:
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
                    page['first_version_id']=result['first_version_id']#self.session_info_out.get('first_version_id',self.task.id)
                    page['data'] = ''
                    page['id'] = result['id']#self.session_info_out.get('post_id' , self.task.id)
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')
                    page['task_log_id']=self.task.id
                    page['client_name'] = self.task.client_name
                    page['versioned']=self.task.instance_data.get('versioned',False)
                    self.pages.append(page)
        except Exception,e:
            log.debug(traceback.format_exc())
            raise e

    @logit(log, 'addReviews')                                                     
    def addReviews(self, parenturi):
        ''' checks for data, since All reviews and comments fall in same div, split operation is performed to split comments.
            kept 0th one as the review
        '''
        parent_uri=parenturi
        links=self.soup.findAll('span', {'class':'l'})
        review_links=[link.parent['href'] for link in links]
        for review_link in review_links:
            self.review_count= self.review_count +1
            self.currenturi= review_link
            res=self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            page={}
            try:
                if not checkSessionInfo(self.genre, self.session_info_out, normalize(self.currenturi),  self.task.instance_data.get('update'), parent_list=[parent_uri]):
                    try:
                        page['title'] = self.soup.find('div', {'class':'detailsHeader'}).h1.renderContents()
                    except:
                        page['title'] =''
                        log.info(self.log_msg("Could not parse title for the review"))
                    try:
                        s=self.soup.find('div', {'id':"content"}).find('td', {'width':"540"})
                        tem_=str(s).split('h2>Comment On This</h2>')
                        page['data']=stripHtml(tem_[0])
                    except:
                        page['data']=''
                        log.info(self.log_msg("Could not parse Data for the review"))
                    try:
                        review_hash = md5.md5(''.join(sorted(page.values())).encode('utf-8', 'ignore')).hexdigest()
                    except:
                        log.exception(self.log_msg('exception in building review_hash '))
                    result=updateSessionInfo(self.genre, self.session_info_out, normalize(self.currenturi), review_hash, 'Review', self.task.instance_data.get('update'), parent_list=[parent_uri])

                    if result['updated']:
                        page['uri']=normalize(self.currenturi)
                        page['id'] = result['id']
                        page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['last_updated_time'] = page['pickup_date']
                        page['versioned'] = False
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id  # TODO: Get the client from the project        
                        page['client_name'] = self.task.client_name
                        page['entity'] = 'Review'
                        page['category'] = self.task.instance_data.get('category','')
                        page['first_version_id']=result['first_version_id']
                        page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                        page['task_log_id']=self.task.id
                        page['versioned']=self.task.instance_data.get('versioned',False)
                        try:
                            s=self.soup.find('div')
                            date=re.search("Date:\s+\w+\,\s+(\d+-\w+-\d+)", str(s)).group(1)
                            page['posted_date']=datetime.strftime(datetime.strptime(date,'%d-%b-%y'),'%Y-%m-%dT%H:%M:%SZ')
                        except:
                            log.info(self.log_msg("Could not parese date")) 
                            page['posted_date']= page['pickup_date']

                        log.info(self.log_msg("Processing %dth review" % self.review_count))
                        self.pages.append(page)
                try:
                    self.addComments( [parent_uri, page['uri']])
                except:
                    log.exception(self.log_msg('exception in add_comments'))
                            
            except  : 
                log.exception(self.log_msg("Exception occured while fetching Review contents"))
        return True
                           
    @logit(log, 'addComments')            
    def addComments(self, parent_list):
        ''' entry id is kept as unique for checking session info
        '''
        pat=re.compile('https://account.complaints.com/mailto.php\?entry_id=(\d+).+')
        pat1=re.compile('https://post.complaints.com/post.php\?followup=6&amp;original_id=(\d+).+')
        try:
            s=self.soup.find('div', {'id':"content"}).find('td', {'width':"540"})
            comments=str(s).split('h2>Comment On This</h2>')
        except:
            log.info(self.log_msg("No Data found for the comments"))
        log.info(self.log_msg('processing the particular' + self.currenturi))
        if comments:
            id=0
            for comment in comments[1:]:
                page={}
                try:
                    page['data']=stripHtml(comment)
                except:
                    page['data']=''
                    log.exception(self.log_msg('could not Parse data for comment'))
                try:
                    hash = md5.md5(''.join(sorted(page.values())).encode('utf-8','ignore')).hexdigest()
                except:
                    log.exception(self.log_msg('could not Process hash for this comment and continue for next iteration'))
                    continue
                try:
                    if re.search(pat, comment):
                        uid=re.search(pat, comment).group(1)
                        log.info(self.log_msg( "UniQue" + uid))
                    else:
                        uid= hash
                        log.info(self.log_msg('UiD '+ uid))
                except:
                    uid =''
                    log.exception(self.log_msg('could not Parse unique Id for comment continue for next iteration'))
                    continue
                if not checkSessionInfo(self.genre, self.session_info_out,
                                        uid, self.task.instance_data.get('update'),
                                        parent_list=parent_list):
                    hash = md5.md5(''.join(sorted(page.values())).encode('utf-8','ignore')).hexdigest()
                    result=updateSessionInfo(self.genre, self.session_info_out, uid, hash,
                                             'Comment', self.task.instance_data.get('update'),
                                             parent_list=parent_list)
                    if result['updated']:
                        page['id']=result['id']
                        page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                        page['first_version_id'] = result['first_version_id']       
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
                        page['uri'] = self.currenturi
                        page['uri_domain'] = urlparse.urlparse(self.currenturi)[1]
                        page['task_log_id']=self.task.id
                        page['entity'] = 'Comment'
                        page['category'] = self.task.instance_data.get('category' ,'')
                        try:
                            date=re.search("Date:\s+\w+\,\s+(\d+-\w+-\d+)",comment ).group(1)
                            page['posted_date']=datetime.strftime(datetime.strptime(date,'%d-%b-%y'),'%Y-%m-%dT%H:%M:%SZ')
                        except:
                            log.exception(self.log_msg('could not parse date for comments'))
                            page['posted_date']=page['pickup_date']
                        try:
                            if len(page['data']) > 50:                                         
                                page['title'] = page['data'][:50] + '...'
                            else:
                                page['title'] = page['data']
                        except:
                            log.info(self.log_msg('could not parse title'))
                        self.pages.append(page)






                    
    
                                         
