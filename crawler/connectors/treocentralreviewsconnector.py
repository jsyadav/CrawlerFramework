
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
import logging
from urlparse import urlparse
from datetime import datetime
import copy

from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.urlnorm import normalize
from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log=logging.getLogger('TreocentralReviewsConnector')
class TreocentralReviewsConnector(BaseConnector):

    @logit (log, "fetch")
    def fetch(self):
        """
        For www.treocentral.com which has the format of the following url
        "http://www.treocentral.com/content/Products/factsheet-107-50.htm"
        and User Opinios has to be picked up.
        urlsegment is : treocentral.com
        source type : review
        comment is taken as reviews here.
        Main review
        Things to do
        ------------
        1) Check the format of uri
        2) setSoup()
        3) getParentPage()
            --It has title , ef_product_recommended_[yes/no]_count
            -- Review and Rating are found , but they are of experts
            --posted date, data not found
            -- unique key is the uri
        4) addReviews()
            --et_product_recommended_yes, data ,et_author_name, title
            -- posted_date is not found
            --use hash as the unique key and add review
            -- continue untill next page not found
        5) Start with the following uris
            http://www.treocentral.com/content/Products/factsheet-1-50.htm
            http://www.treocentral.com/content/Products/factsheet-307-50-.htm
            http://www.treocentral.com/content/Products/factsheet-359-50.htm
            http://www.treocentral.com/content/Products/factsheet-481-50.htm
            http://www.treocentral.com/content/Products/factsheet-652.htm
            
        """
        try:
            if re.match(r'http://www.treocentral.com/content/Products/factsheet-.+\.htm'\
                                                                     ,self.currenturi):
                log.info(self.log_msg ('Reviews  to be captured'))
                self.parenturi = self.currenturi
                self.genre = "Review"
                self.DEBUG = 0
                if not self._setSoup():
                    return False
                self._getParentPage()
                while True:
                    self._addReviews()
                    try:
                        self.currenturi = 'http://www.treocentral.com' +  self.\
                                            soup.find('a',text='Next &gt;&gt;').\
                                                        findParent('a')['href']
                        if not self._setSoup():
                            break
                    except:
                        log.exception( 'next page not found' )
                        break
                return True
            elif self.currenturi == 'http://www.treocentral.com/content/Products/index.htm':
                log.info(self.log_msg ( 'Need to be added'))
                self._setSoup()
                for href in  ['http://www.treocentral.com' + each['href'] for \
                                    each in self.soup.findAll('a',href=re.compile\
                                        (r'/content/Products/factsheet-.+\.htm'))]:
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] = normalize( href )
                    self.linksOut.append( temp_task )
                return True
            else:
                log.info( self.log_msg ( 'uri is not in the correct format' ) )
                return False
        except:
            log.exception ( self.log_msg ( 'Error in fetch '))
            return False
        
    @logit(log, "setSoup")
    def _setSoup( self ):
        """
            It will set the uri to current page, written in seperate
            method so as to avoid code redundancy
        """
        try:
            log.info(self.log_msg( 'for uri %s'%(self.currenturi) ))
            res = self._getHTML()
            if res:
                self.rawpage = res[ 'result' ]
            else:
                log.info(self.log_msg('self.rawpage not set.... so Sorry..'))
                return False
            self._setCurrentPage()
            return True
        except Exception, e:
            log.exception(self.log_msg('Page not for  :%s'%uri))
            raise e

    @logit(log,"_getParentPage")
    def _getParentPage( self ):
        """
        For gettting the info from Parent Page
        The page structure was not having any unique class name for every field
        For Title -- which has the font size 4, which is unique font size for title
        For et_product_manufacturer -- follwed immediablty by the title, which
           has the size of 1, <a> which has the text
        For ei_product_recommended_[yes/no], are the vert next to User Opinions
        which are in 4 tr tags, which are indentified by alt propery 'Thumps [ Up/Down ]
        session info is checked initially with the parent uri(unique key )

        """
        page = {}
        if checkSessionInfo( self.genre, self.session_info_out, self.parenturi,\
                                        self.task.instance_data.get( 'update' ) ):
            return False
        try:
            page [ 'title' ] = stripHtml( self.soup.find( 'font', size=4 )\
                                                            .renderContents() )
            if self.DEBUG == 1:
                log.info ( self.log_msg ('title :%s'%page [ 'title' ] ) )
        except:
            log.exception( self.log_msg ( 'Title is not found' ) )

        try:
            page[ 'et_product_manufacturer' ] = stripHtml( self.soup.find('font',\
                                    size=4 ).findNext('font',size=1).find('a').\
                                                            renderContents() )
            if self.DEBUG == 1:
                log.info ( self.log_msg ('et_product_manufacturer :%s'%page \
                                                [ 'et_product_manufacturer' ] ) )
        except:
            log.exception( self.log_msg ( 'et_product_manufacturer not found' ) )
        recommendation = {'Thumbs Up':'ei_product_recommended_yes','Thumbs Down':
                                                    'ei_product_recommended_no'}
        for each in recommendation.keys():
            try:
                page[ recommendation[ each ] ] = int ( stripHtml( self.soup.find\
                    ( 'font', text='User Opinions' ).findNext( 'img', alt= each )\
                        .findNext( 'td' ).renderContents() ).replace( '%','' ) )
                if self.DEBUG == 1:
                    log.info(self.log_msg ('key:%s'%page [recommendation[ each ]]))
            except:
                log.exception(self.log_msg('recommedation is not found %s'%each) )
        try:
            if self.DEBUG == 1:
                log.info ( page )
            post_hash = get_hash(page)
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo( self.genre, self.session_info_out, \
                                            self.parenturi, post_hash,'Post', \
                                    self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                log.debug(self.log_msg("Parent page not stored"))
                return False
            page['path']=[self.parenturi]
            page['parent_path']=[]
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
            page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            self.pages.append( page )
            log.debug(self.log_msg("Parent page added"))
            return True
        except:
            log.exception(self.log_msg('There is some problem with Session info'))
            return False
    @logit( log, "_addReviews")
    def _addReviews( self ):
        """
        This will add the reviews
        The review is identified the the title <td>, which has unique identified
        width = 7% and another td 93%
        """
        review_tags = [ each.findParent('table') for each in  self.soup.findAll\
                                                            ('td',width='7%') ]
        for review_tag in review_tags:
            page = {}
            try:
                if review_tag.find('img')['alt'] == 'Thumbs Down':
                    page['et_product_recommended'] ='no'
                if review_tag.find('img')['alt'] == 'Thumbs Up':
                    page['et_product_recommended'] ='yes'
                if self.DEBUG == 1:
                    log.info ( self.log_msg ('et_product_recommended :%s'%page \
                                                [ 'et_product_recommended' ] ) )
            except:
                log.exception(self.log_msg ( 'et_product_recommended not found'))
            try:
                page [ 'title' ] =  stripHtml( review_tag.find('b').renderContents() )
                if self.DEBUG == 1:
                    log.info ( self.log_msg ('title :%s'%page[ 'title' ] ) )
            except:
                log.exception (self.log_msg ( 'Title is not found' ) )
                page [ 'title' ] = ''
            try:
                page['data'] = stripHtml ( review_tag.find('tr').findNext('tr')\
                                        .find('font',size='2').renderContents() )
                if self.DEBUG == 1:
                    log.info ( self.log_msg ('data :%s'%page[ 'data' ] ) )
                if page['title'] == '':
                    if len ( page['data'] ) >100:
                        page['title'] = page['data'][:100] + '...'
                    else:
                        page['title'] = page['data']
            except:
                log.exception(self.log_msg ( 'data not found') )
                page['data'] = ''
            try:
                page['et_author_name'] = stripHtml( review_tag.find('a')\
                                                            .renderContents() )
                if self.DEBUG == 1:
                    log.info ( self.log_msg ('et_author_name :%s'%\
                                                    page[ 'et_author_name' ] ) )
                if not page['et_author_name'] == 'Anonymous':
                    page['et_author_email'] = stripHtml( review_tag.find('a')\
                                                ['href'] ).replace('mailto:','')
                    if self.DEBUG == 1:
                        log.info ( self.log_msg ('et_author_email :%s'%page\
                                                        [ 'et_author_email' ] ) )
            except:
                log.exception(self.log_msg ( 'Author name is not found') )
            try:
                if self.DEBUG == 1:
                    log.info ( page )
                review_hash = get_hash( page )
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                if checkSessionInfo( self.genre, self.session_info_out, \
                            unique_key,self.task.instance_data.get('update'),\
                                                parent_list=[ self.parenturi ]):
                    continue
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo( self.genre, self.session_info_out, \
                                            unique_key, review_hash,'Review', \
                                        self.task.instance_data.get('update'),\
                                                parent_list=[ self.parenturi ])
                if not result['updated']:
                    continue
                parent_list = [ self.parenturi ]
                page['parent_path'] = copy.copy(parent_list)
                parent_list.append( unique_key )
                page['path']=parent_list
                page['task_log_id'] = self.task.id
                page['versioned'] = self.task.instance_data.get('versioned',False)
                page['category'] = self.task.instance_data.get('category','generic')
                page['last_updated_time'] = datetime.strftime(datetime.utcnow()\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
                page['client_name'] = self.task.client_name
                page['entity'] = 'Review'
                page['uri'] = normalize( self.currenturi )
                page['uri_domain'] = urlparse(page['uri'])[1]
                page['priority'] = self.task.priority
                page['level'] = self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
                page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                self.pages.append( page )
                log.info( self.log_msg("Review added") )
            except:
                log.exception( self.log_msg('Error with session info' ) )