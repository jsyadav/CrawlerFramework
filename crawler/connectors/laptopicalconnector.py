
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar
# Modified on 4th Dec, removed unused import, included get_hash, 
# removed redundant functions , spacing taken care

import re
import logging
from urlparse import urlparse
from datetime import datetime

from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.urlnorm import normalize
from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log=logging.getLogger('LaptopicalConnector')
class LaptopicalConnector(BaseConnector):

    @logit (log, "fetch")
    def fetch(self):
        """
        same fetch method, I need to write something for doc string
        So I m writing this doc string
        """
        try:
            self.parenturi = self.currenturi
            self.genre = "Review"
            if self.currenturi == 'http://www.laptopical.com/laptop-reviews.html':
                if not self._setSoup():
                    return False
                hrefs = [ 'http://www.laptopical.com' + div.find('a')['href'] \
                        for div in self.soup.find('div',{'id':'review-listing'})\
                        .find('ul').findAll('li') if not div.find('a') == None ]
                for href in hrefs:
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] = normalize( href )
                    self.linksOut.append( temp_task )
                log.info('Total uris are %d'%(len( hrefs )))
                return True
            if re.compile('http://www.laptopical.com/.+?\.html').match(self.currenturi):
                if not self._setSoup():
                    return False
                self._getParentPage()
                self._addReview()
                return True
        except:
            log.exception('error in fetch ')
            return False

    @logit( log, "_addReview")
    def _addReview(self):
        """This will add the Reviews and comments found on this page
        """
        review_comments = self.soup.find('div','comments').findAll('li')
        for review_comment in review_comments:
            page = {}
            page['uri'] = self.parenturi
            page['title'] = ''
            try:
                date_str = stripHtml(review_comment.find('small').renderContents())
                date_str = date_str[date_str.find(',')+1:].strip()
                page['posted_date'] = datetime.strftime(datetime.strptime\
                                    (date_str,"%B %d, %Y"),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception('posted date not found')
                page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                            ,"%Y-%m-%dT%H:%M:%SZ")
            try:
                page ['et_author_name'] = re.sub('^BY ','', stripHtml( \
                                review_comment.find('small').findNext('small')\
                                                            .renderContents() ) )
            except:
                log.exception('author name not found')
            try:
                review_str = review_comment.renderContents()
                small_tags = review_comment.findAll('small')
                for small_tag in small_tags:
                    review_str =  review_str.replace(small_tag.__str__(),'')
                page['data'] = stripHtml(review_str)
##                if len (page['data']) > 100:
##                    page['title'] = page['data'][:100]
##                else:
##                    page['title'] = page['data']
            except:
                log.exception('error with getting data')
                page['data'] = ''

            try:
                log.info(page)
                review_hash = get_hash(page)
                if not checkSessionInfo(self.genre, self.session_info_out, \
                            review_hash,self.task.instance_data.get('update'),\
                                                parent_list=[ self.parenturi ]):

                    id=None
                    if self.session_info_out=={}:
                        id=self.task.id
                        log.debug(id)

                    result=updateSessionInfo(self.genre, self.session_info_out,\
                         review_hash, review_hash,'Review', self.task.instance_data\
                                    .get('update'),parent_list=[self.parenturi])


                    if result['updated']:
                        page['id'] = result['id']
                        page['first_version_id'] = result[ 'first_version_id' ]
                        page['task_log_id'] = self.task.id
                        page['versioned'] = self.task.instance_data.get('versioned',False)
                        page['category'] = self.task.instance_data.get('category','generic')
                        page['last_updated_time'] = datetime.strftime(datetime.utcnow()\
                                                                ,"%Y-%m-%dT%H:%M:%SZ")
                        page['client_name'] = self.task.client_name
                        page['entity'] = 'Review'
                        page['uri'] = normalize( self.parenturi )
                        page['uri_domain'] = urlparse(page['uri'])[1]
                        page['priority'] = self.task.priority
                        page['level'] = self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                            ,"%Y-%m-%dT%H:%M:%SZ")
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id
                        log.info( page )
                        self.pages.append( page )
                        log.debug(self.log_msg("Review added"))
                    else:
                        log.debug(self.log_msg("Review Not added"))
            except:
                log.exception('Error with session info')

    @logit( log, "_getParentPage")
    def _getParentPage(self):
        """
        This will set the parent page details and add it to session info,
        Same thing, need to be written for all connectors
        """
        page = {}
        try:
            date_str = stripHtml ( self.soup.find('div','article-meta-data').\
                                        find('p').find('b').renderContents() )
            date_str = date_str[date_str.find(',')+1:].strip()
            page['posted_date'] = datetime.strftime(datetime.strptime\
                                    (date_str,"%B %d, %Y"),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception('posted date not found')
            page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['title'] = stripHtml( self.soup.find('div',{'id':'review-intro'})\
                            .find('h2').renderContents() ).replace('Review of ','')
        except:
            log.exception('title not found')
            page['title'] =''
        try:
            page['et_product_pros'] =  stripHtml( self.soup.find('div',\
                            {'id':'review-pros'}).find('ul').renderContents() )
        except:
            log.exception('pros not found')
        try:
            page['et_product_cons'] =  stripHtml(self.soup.find('div',{'id':'review-cons'})\
                                                            .find('ul').renderContents() )
        except:
            log.exception('cons not found')
        try:
            log.info(page)
            post_hash = get_hash( page )
            if not checkSessionInfo(self.genre, self.session_info_out,self.parenturi,\
                                                            self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug(id)
                result=updateSessionInfo(self.genre, self.session_info_out, \
                        self.parenturi, post_hash,'Post', self.task.instance_data\
                                                        .get('update'), Id=id)
                if result['updated']:
                    page['id'] = result['id']
                    page['first_version_id'] = result['first_version_id']
                    page['task_log_id'] = self.task.id
                    page['versioned'] = self.task.instance_data.get('versioned',False)
                    page['category'] = self.task.instance_data.get('category','generic')
                    page['last_updated_time'] = datetime.strftime(datetime.utcnow()\
                                                            ,"%Y-%m-%dT%H:%M:%SZ")
                    page['client_name'] = self.task.client_name
                    page['entity'] = 'post'
                    page['data'] = ''
                    page['uri'] = normalize(self.parenturi)
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    page['priority'] = self.task.priority
                    page['level'] = self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    self.pages.append( page )
                    log.debug(self.log_msg("Parent page added"))
                    return True
                else:
                    log.debug(self.log_msg("Parent page not added"))
                    return False
        except:
            log.exception('There is some problem with Session info')
            return False

    @logit(log, '_setSoup')
    def _setSoup( self ):
        """
            It will set the uri to current page, written in seperate
            method so as to avoid code redundancy
        """
        try:
            log.info( 'for uri %s'%( self.currenturi ) )
            res = self._getHTML()
            if res:
                self.rawpage = res[ 'result' ]
            else:
                log.info('self.rawpage not set.... so Sorry..')
                return False
            self._setCurrentPage()
            return True
        except Exception, e:
            log.exception(self.log_msg('soup not set for uri :%s'%uri))
            raise e
