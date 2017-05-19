'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

import re
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import logging
from urllib2 import urlparse

from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

import cgi
import md5

log = logging.getLogger('SoftpediaConnector')


class SoftpediaConnector(BaseConnector):

        
    @logit(log, 'fetch')
    def fetch( self ):
        self.genre = 'Review'
        self.review_urls = []

        try:
            self.parenturi = self.currenturi
            res = self._getHTML()        
            if not res:
                return False        
            self.rawpage = res['result']
            #print self.rawpage
            self._setCurrentPage()            
            self._getParentPage(self.parenturi)
            reviewuri = self.soup.findAll("a" ,{"class" : "lineheighticon"})[-4].attrMap["href"]
            self.currenturi = reviewuri
            res = self._getHTML()
            if not res:
                return False
            self.rawpage = res['result']
            self._setCurrentPage()
            self.addreviews(reviewuri)

            self.task.status['fetch_status'] = True
            return True
        except:
            self.task.status['fetch_status']=False
            log.exception(self.log_msg('Exception in fetch'))
            return False


    @logit(log, '_getParentPage')
    def _getParentPage(self, parenturi):
        try:
            page = {}

            try:
                page['title'] = stripHtml(self.soup.find("td", {"class" : "pagehead2"}).find("h1").renderContents())
            except Exception, e:
                log.exception(self.log_msg("Couldn't parse Title"))

            try:
                page['ef_product_rating_overall'] = float(self.soup.find(id = "rater__upd").renderContents().partition("/")[0].split("(")[1])            
            except Exception, e:
                log.exception(self.log_msg('could not parse overall_rating'))

            try:
                post_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x ,\
                                                               page.values()))).encode('utf-8','ignore')).hexdigest()
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
                    page['path'] = [parenturi]
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
            log.exception(self.log_msg("parent post couldn't be parsed"))
            raise e
    

        
    @logit(log, 'addreviews')
    def addreviews( self, parenturi ):
        all_reviews = self.soup.find( id = "intelliTxt" ).findAll( id = re.compile(r"com_oid_(\d+)"))
        for review in all_reviews:

            review_table = review.findAll( "td", {"class" : "contentheadings"} )
            page = {}
            unique_key = review['id']
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                                        self.task.instance_data.get('update'),parent_list\
                                        = [self.parenturi]):
                
                continue

            page['uri'] = parenturi # All reviews are in a single page
            
            try:
                page['et_author_name'] = review_table[1].findAll("b")[0].renderContents()
            except:
                page['et_author_name'] = ''
                
            try:
                date = review_table[1].findAll("b")[1].nextSibling.strip()
                time_s = datetime.strptime(date, "%d %b %Y, %H:%M GMT")
                page['posted_date'] = datetime.strftime(time_s, "%Y-%m-%dT%H:%M:%SZ")
            except:
                page['posted_date'] = ''
                
            try:
                page['data'] = stripHtml(review.find("p").renderContents())
            except:
                page['data'] = ''
            

            try:
                if len(page['data']) > 50: #title is set to first 50 characters or the post whichever is less
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
            except:
                log.info(self.log_msg('could not parse title'))


            try:
                page['ef_rating_overall'] = len(review.findAll( "td", {"class" : "contentheadings"})[1].findAll("img", { "src" : "/base_img/smallstar_rx.gif" })) 
            except:
                page['ef_rating_overall'] = ''


            attributes = { 'GUI':'ef_rating_gui',
                           'Features':'ef_rating_features',
                           'Ease of use': 'ef_rating_ease_of_use',
                           'Value' : 'ef_rating_value'
                           }
            start_offset, end_offset = 1,6
            attribute_block = review.findAll( "td" , {"valign" :"top"})[1].contents[0:-4]
            attribute_block = [str(x).strip().strip('&nbsp;&nbsp;&nbsp;').strip(":") for x in attribute_block[0:-4]]
            for a in attributes:
                _ratings = attribute_block[start_offset:end_offset]
                start_offset = end_offset + 1
                end_offset   = end_offset + 6
                page[attributes[a]] = len([x for x in _ratings if x == """<img src="/base_img/smallstar_x.gif" width="12" height="10" hspace="0" />"""])

                
            try:
                review_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                             page.values()))).encode('utf-8','ignore')).hexdigest()
            except:
                log.exception(self.log_msg("exception in buidling review_hash , moving onto next comment"))
                continue
            parent_list=[self.parenturi]
            result=updateSessionInfo(self.genre, self.session_info_out, unique_key, review_hash, 
                                             'Review', self.task.instance_data.get('update'), parent_list=parent_list)
            if result['updated']:
                page['parent_path'] = parent_list[:]
                parent_list.append( unique_key )
                page['path'] = parent_list
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
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
                log.info(self.log_msg('Review Added'))
        return True
