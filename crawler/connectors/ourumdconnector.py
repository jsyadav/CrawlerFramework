'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.
Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''


# Sudharshan S

import re
import md5
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import logging
from urllib2 import urlparse,unquote,quote
import time

from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash

from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

baseurl = 'http://www.ourumd.com'

log = logging.getLogger('OurUMDConnector')
class OurUMDConnector(BaseConnector):
    '''
    Connector for ourumd.com
    '''
    @logit(log, 'fetch')
    def fetch(self):
        self.genre = 'Review'
        try:
            # Get review links
            #self.currenturi = 'http://www.ourumd.com/reviews/Wyss-Gallifent,%20J'
            if self.currenturi=='http://www.ourumd.com/viewreviews/?all':
                self.__addTasks()
                return False
            self.currenturi = self.currenturi.replace(" ", "%20")
            self.parenturi = self.currenturi
            if not self._setSoup():
                return False
            self._getParentPage(self.parenturi)
            self.addreviews()
            self.task.status['fetch_status'] = True
            return True
        except:
            self.task.status['fetch_status'] = False
            log.exception(self.log_msg('Exception in fetch'))
            return False


    @logit(log, '_getParentPage')
    def _getParentPage(self, parenturi):
        try:
            page = {}
            try:
                page['title'] = stripHtml(self.soup.find(id = "content").find({"p" : { "class" : "pageheading" }}).renderContents().decode('utf-8'))
            except:
                page['title'] = ''
                log.exception(self.log_msg("Couldn't parse title %s" %parenturi))

            try:
                page['ef_rating_overall'] = float(self.soup.findAll("img" , { "src" : re.compile("stars.*") })[0]['src'].split("=")[-1])
            except:
                page['ef_rating_overall'] = 0
                
            try:
                post_hash = get_hash(page)
            except Exception,e:
                log.exception(self.log_msg('could not build post_hash'))
                raise e
            log.debug(self.log_msg('checking session info'))

                #continue if returned true
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    self.parenturi, self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out, self.parenturi, post_hash, 
                                         'Post', self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    page['uri'] = normalize(self.currenturi)
                    page['path'] = [self.parenturi]
                    page['parent_path'] = []
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project       
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    #page['first_version_id'] = result['first_version_id']
                    #page['id'] = result['id']
                    page['versioned'] = False
                    page['data'] = ''
                    page['task_log_id']=self.task.id
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')                        
                    self.pages.append(page)
                    
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parse: " + self.currenturi))
            raise e



    @logit(log, 'addreviews')
    def addreviews(self):
        try:
            reviews = self.soup.find(id = "content").find("table").findAll("tr")
            #reviews.pop(0)
            log.info(self.log_msg("Number of reviews %d" %len(reviews)))
            for review in reviews:
                # Of the form, <b><a href="user/5191">Rahulm5000</a></b><br /><img src="stars/?avg=5" /><br />Course: MATH241<br />Grade Expected: A<br />June 5, 2009, 8:11 am
                page = {}
                page['uri'] = self.currenturi # all reviews in a single page
                try:
                    page['et_author_name'] = stripHtml(review.find("b").renderContents().decode('utf-8'))
                except:
                    log.info(self.log_msg("Couldn't get the user name"))
                    page['et_author_name'] = ''

                try:
                    page['ef_rating'] = float(review.find({"img" : {"src" : re.compile("stars/\?avg=(\d)")}})['src'].split("=")[-1])
                except:
                    log.info(self.log_msg("Couldn't get rating"))
                    page['ef_rating'] = ''

                try:
                    page['et_course'] = re.findall("<br />Course: (\w+)<br />", review.find("td").renderContents())[0]
                except:
                    log.info(self.log_msg("Couldn't get Course name"))
                    page['et_course'] = ''

                try:
                    page['et_grade_expected'] = re.findall("<br />Grade Expected: (\w+)<br />", review.find("td").renderContents())[0]
                except:
                    page['et_grade_expected'] = ''
                    log.info(self.log_msg(page['et_author_name']))
                    log.info(self.log_msg("Couldn't get Grade Expected " + page['uri']))

                try:
                    date = review.findAll("td")[0].renderContents().split("<br />")[-1]
                    date_s = datetime.strptime(date, "%B %d, %Y, %I:%M %p")
                    page['posted_date'] = datetime.strftime( date_s, "%Y-%m-%dT%H:%M:%SZ")
                except Exception,e:
                    log.info(self.log_msg("Couldn't pick up posted date " + str(e)  ))
                    log.info(self.log_msg(page['et_author_name'] + self.parenturi))
                    page['posted_date'] = datetime.strftime( datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")

                try:
                    page['data'] = stripHtml(review.findAll("td")[-1].renderContents().decode('utf-8'))
                except:
                    page['data'] = ''
                    log.info(self.log_msg("Couldn't get Data"))
                try:
                    if len(page['data']) > 50: #title is set to first 50 characters or the post whichever is less
                        page['title'] = page['data'][:50] + '...'
                    else:
                        page['title'] = page['data']
                except:
                    log.info(self.log_msg('could not parse title'))
                    
                parent_list = [self.parenturi]
                review_metadata = md5.md5(page['title'] + page['data']).hexdigest()
                if checkSessionInfo(self.genre, self.session_info_out, review_metadata,\
                                    self.task.instance_data.get('update'), parent_list\
                                    = [self.parenturi]):
                    continue
                
                try:
                    review_hash = get_hash(page)
                except:
                    log.exception(self.log_msg("exception in buidling review_hash , moving onto next comment"))
                    continue
                result=updateSessionInfo(self.genre, self.session_info_out, review_metadata, review_hash, 
                                                     'Review', self.task.instance_data.get('update'), parent_list=parent_list)
                if result['updated']:
                    page['parent_path'] = parent_list[:]
                    parent_list.append(review_metadata)
                    page['path'] = parent_list
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project 
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    #page['first_version_id'] = result['first_version_id']
                    #page['id'] = result['id']
                    #page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                    page['versioned'] = False
                    page['entity'] = 'Review'
                    page['category'] = self.task.instance_data.get('category','')
                    page['task_log_id']=self.task.id
                    page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                    self.pages.append(page)
        except Exception,e:
            log.exception(self.log_msg("Review post couldn't be parsed: ") + self.currenturi)
            raise e
        return True


    @logit(log, "_setSoup")
    def _setSoup( self, url = None, data = None, headers = {} ):
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

    def __addTasks(self):
        '''This will add the tasks
        '''
        try:
            if not self._setSoup():
                return False
##            res = self._getHTML( data = data, headers=headers  )
##            self.rawpage = res[ 'result' ]
##            self._setCurrentPage()
            review_links = ['http://www.ourumd.com/' + x['href'] for x in self.soup.findAll('a',href=re.compile('^reviews.*'))]
            for review_link in review_links:
                temp_task=self.task.clone()
                temp_task.instance_data[ 'uri' ] = normalize( review_link )
                self.linksOut.append( temp_task )
            log.info(self.log_msg('Tasks added'))
        except:
            log.info(self.log_msg('cannot add the tasks'))
                
        
        
            
