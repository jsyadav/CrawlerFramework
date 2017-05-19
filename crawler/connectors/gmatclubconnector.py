'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.
    
Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''


#Rakesh Soni

import re
import md5
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import logging
from urllib2 import urlparse,unquote

from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash

from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('GMATClubConnector')

class GMATClubConnector(BaseConnector):
    '''
    Connector to extract reviwes.
    sample url:
    
    http://gmatclub.com/forum/advanced-search/?keywords=hass&terms=all&author=&sc=1&sk=t&sd=d&sr=posts&st=0&ch=-1&t=0&submit=Search
    '''
        
    @logit(log , 'fetch')
    def fetch(self):                                                                                         
        self.genre = "Review"
        try:
            self.parent_uri = self.currenturi
            self.__setSoup()#Remove, not required
            #Set soup
            
            while True:
                log.info(self.log_msg('Entering into addreviews'))
                self.addreviews()
                log.info(self.log_msg('Exiting into addreviews'))
                #break#Remove
            
                try:
                    next_uri = self.soup.find('div','nav').findAll('a')[-1]
                    if not next_uri.renderContents().strip()=='Next':
                        break
                    self.currenturi = self.parent_uri + next_uri['href'].strip(".")
                except:
                    log.info(self.log_msg('Next url link not found'))
                    break
                
                self.__setSoup()
                
            self.task.status['fetch_status']=True
            return True
            
        except:
            self.task.status['fetch_status']=False
            log.exception(self.log_msg('Exception in fetch'))
            return False
            
    
    @logit(log , 'addreviews')
    def addreviews(self):
        #add reviews in pages
        reviews = self.soup.findAll('table','tablebg')[1].findAll('tr','row2')
        log.info(self.log_msg('total # of Reviews is %d'%len(reviews)))
        for review in reviews:
            try:
                
                page = {}
                
                try:
                    page['et_data_forum'] = review.find('p','topictitle').findAll('a')[1] \
                                                .renderContents().strip()
                except:
                    log.info(self.log_msg('forum title not available'))
                    
                try:
                    page['et_data_topic'] = review.find('p','topictitle').findAll('a')[2] \
                                                .renderContents().strip()
                except:
                    log.info(self.log_msg('Topic not available'))
                    
                try:
                    author_info = review.nextSibling.nextSibling
                    
                    columns = author_info.findAll('td')
                    try:                    
                        page['et_author_name'] = columns[0].find('b','postauthor').find('a').renderContents().strip()
                    except:
                        log.info(self.log_msg('Author name not available'))
                        
                    
                    post_subject = columns[1].findAll('div')[0].find('a')
                    try:
                        page['uri'] = unique_key = post_subject['href']
                    except:
                        log.info(self.log_msg('No unique key available'))
                        continue
                except:
                    log.info(self.log_msg('Author info and unique key not available.'))
                    continue
                    
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list=[self.parent_uri]):
                
                    log.info(self.log_msg('session info return True'))
                    continue
                   
                try:
                    date_str = stripHtml(columns[1].findAll('div')[1].find('b').nextSibling.string)
                    page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%a %b %d, %Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.info(self.log_msg('Posted date not available, take current date.'))
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    
                try:
                    data_info = author_info.nextSibling.nextSibling
                    try:
                        page['ei_post_replies_count'] = int(data_info.find('span','postdetails').findAll('b')[0] \
                                                    .renderContents().strip())
                    except:
                        log.info(self.log_msg('Replies count not found'))
                        
                    try:
                        page['ei_post_views_count'] = int(data_info.find('span','postdetails').findAll('b')[-1] \
                                                    .renderContents().strip())
                    except:
                        log.info(self.log_msg('Views count not found'))    
                    
                    try:
                        page['data'] = stripHtml(data_info.find('td','postbody').renderContents().strip())
                        #log.info(page['data'])
                    except:
                        page['data'] = ''
                        log.info(self.log_msg('Data not found for this post'))
                    
                    
                    try:
                        page['title'] = stripHtml(post_subject.renderContents()).strip()
                        if page['title'].startswith('Re:'):
                            page['entity'] = 'Reply'
                        else:
                            page['entity'] = 'Question'
                    except:
                        page['title'] = ''
                        log.exception(self.log_msg('title not found'))
                except:
                    log.info(self.log_msg('data_info not available'))
                    continue
                    
                review_hash = get_hash( page )
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            review_hash,'review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    log.info(self.log_msg('updateSessionInfo returned False.'))
                    continue
                
                page['path'] = [self.parent_uri,unique_key]
                page['parent_path'] = [self.parent_uri]
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
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
                page['task_log_id']=self.task.id
                #page['entity'] = 'Post'
                page['category']=self.task.instance_data.get('category','')
                self.pages.append(page)
                log.debug(page)
                    
            except:
                log.exception(self.log_msg("exception in addreviews"))
                continue
    
    @logit(log, "_setSoup")
    def __setSoup( self, url = None, data = None, headers = {} ):
        
        """
            It will set the uri to current page, written in seperate
            method so as to avoid code redundancy
        """

        if url:
            self.currenturi = url
        try:
            log.info(self.log_msg( 'for uri %s'%(self.currenturi) ))
            res = self._getHTML( data = data, headers=headers  )
            if res:
                self.rawpage = res[ 'result' ]
            else:
                log.info(self.log_msg('self.rawpage not set.... so Sorry..'))
                return False
            self._setCurrentPage()
            return True
        except Exception, e:
            log.exception(self.log_msg('Page not for  :%s'%url))
            raise e

    
    
