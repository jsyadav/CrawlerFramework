
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

# Skumar

import re
import logging
from urllib2 import urlparse
from datetime import datetime
from BeautifulSoup import BeautifulStoneSoup
import copy

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('NikeStoreConnector')
class NikeStoreConnector(BaseConnector):
    '''
    This will fetch the data from www.store.nike.com
    The sample uri is
    get the product id and catalog_id, from uri and use the following format to get data 
    http://niketown.nike.com/services/catalogService.xml?action=loadProduct&productId=199347&catalog=1&view=medium&flattenAttribs=true
    from this xml, get product code
    Static Resource BaseUrl:http://reviews.nike.com/9191/
    http://reviews.nike.com/9191/PRODUCT_ID/reviews.htm    
    '''

    @logit(log , 'fetch')
    def fetch(self):
        self.genre="Review" # Type of Review
        try:
            self.parent_uri = self.currenturi
            product_id = re.search('pid\-([^/]*)',self.currenturi).group(1)
            catalog_id = re.search('cid\-([^/]*)',self.currenturi).group(1)
            xml_uri = 'http://niketown.nike.com/services/catalogService.xml?action=loadProduct&productId=%s&catalog=%s&view=medium&flattenAttribs=true'%(product_id,catalog_id)
            self.soup = BeautifulStoneSoup ( self._getHTML(xml_uri)['result'] )
            #review_id =  stripHtml( xml_soup.find('sn').renderContents() )            
            if not self.__getParentPage():
                log.info(self.log_msg('Parent page not posted'))
            self.__addReviews()
            return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log , '__addReviews')
    def __addReviews(self):
        """
        It will add the reviews,
        for review in soup.findAll('td',{'class':re.compile('BVStandaloneReviewSectionReview.*')}):            
            rating = float(stripHtml(review.find('span','BVratingSummaryFinal').renderContents()))
            posted_date = datetime.strftime(datetime.strptime(stripHtml(review.find('span','BVdateCreated').renderContents()),"%B %d, %Y"),"%Y-%m-%dT%H:%M:%SZ")            
            title = first 50 char of data
            author name =  re.sub('^By','',stripHtml(review.find('span','BVReviewer').renderContents())).strip()
            author_location = re.sub('^from','',stripHtml(review.find('span','BVreviewerLocation').renderContents())).strip()
            ratings = 
            for rating in ratings:
                key = 'ef_rating_'+ stripHtml(rating.find('td','BVSliderDisplayLabelRight').renderContents()).lower().replace(' ','_')
                value = float(re.search('^\d+',rating.find('img')['alt']).group())
                page[key] = value
            data = stripHtml(review.find('td','BVcontent').renderContents())
            ei_data_recommended_count = int(stripHtml(review.find('span','BVrespondedHelpful BVrespondedHelpfulPositive').renderContents()))
            """
         
        for review in self.soup.findAll('td',{'class':re.compile('BVStandaloneReviewSectionReview.*')}):
            page = {}
            try:
                page['et_author_name'] = re.sub('^By','',stripHtml(review.find('span','BVReviewer').renderContents())).strip()
            except:
                log.info(self.log_msg('author name not found'))
            try:
                page['et_author_location'] = re.sub('^from','',stripHtml(review.find('span','BVreviewerLocation').renderContents())).strip()
            except:
                log.info(self.log_msg('author location not found'))
            try:
                page['ef_rating_overall'] = float(stripHtml(review.find('span','BVratingSummaryFinal').renderContents()))
            except:
                log.info(self.log_msg('Rating not found' ) )
            try:
                for rating in review.find('div','BVSecondaryRatings').findAll('tr',{'class':re.compile('^BVRatingRow.*')}):
                    try:
                        key = 'ef_rating_'+ stripHtml(rating.find('td','BVSliderDisplayLabelRight').renderContents()).lower().replace(' ','_')
                        value = float(re.search('^\d+',rating.find('img')['alt']).group())
                        page[ key ] = value
                    except:
                        log.info(self.log_msg('secondary rating not found'))
            except:
                log.info(self.log_msg('Rating not found' ) )
            try:
                page['posted_date'] = datetime.strftime(datetime.strptime(stripHtml(review.find('span','BVdateCreated').renderContents()),"%B %d, %Y"),"%Y-%m-%dT%H:%M:%SZ")
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                ,"%Y-%m-%dT%H:%M:%SZ")
                log.info(self.log_msg('Posted_date not found') )
            try:
                page['data'] = stripHtml(review.find('td','BVcontent').renderContents())
            except:
                log.exception(self.log_msg('Data not found'))
                page['data'] = ''
            try:
                page['title'] = stripHtml(review.find('span','BVreviewTitle').renderContents())
            except:
                log.exception(self.log_msg('Data not found'))
                page['data'] = ''
            try:
                if page['title'] == '':
                    if len(page['data']) > 50:
                        page['title'] = page['data'][:50] + '...'
                    else:
                        page['title'] = page['data']
            except:
                log.exception(self.log_msg('title not found'))
                page['title'] = ''
            try:
                page['ei_data_recommended_count'] = int(stripHtml(review.find('span','BVrespondedHelpful BVrespondedHelpfulPositive').renderContents()))
            except:
                log.info(self.log_msg('data recommended not found'))
            try:
                page['et_product_usage'] = stripHtml(review.find('span','BVReviewValue BVReviewValueIRun').renderContents())
            except:
                log.info(self.log_msg('product usage not found'))
            try:
                page['et_product_used_primarily'] = stripHtml(review.find('span','BVReviewValue BVReviewValueIRunTo').renderContents())
            except:
                log.info(self.log_msg('product usage not found'))
            try:
                page['et_data_title'] = stripHtml(review.find('td','BVfeaturedTitle').renderContents())
            except:
                log.info(self.log_msg('Data title not found'))
            try:
                review_hash = get_hash( page )
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    continue
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            review_hash,'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    continue
                parent_list = [self.parent_uri]
                page['parent_path'] = copy.copy(parent_list)
                parent_list.append( unique_key )
                page['path']=parent_list
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")
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
                page['uri'] = self.currenturi
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append(page)
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))

    @logit(log , 'getParentPage')
    def __getParentPage(self):
            """
            This will get the Parent page
            These parent page information are taken from XML
            """      
            page={}
            try:
                product_price = stripHtml(self.soup.find('flp').renderContents())
                self.updateProductPrice(product_price)
            except:
                log.info(self.log_msg('could not parse product price'))
            try:
                page['title'] = stripHtml(self.soup.find('d').renderContents())
            except:
                log.exception(self.log_msg('could not parse page title'))
                page['title'] = ''
            try:
                # Price
                page['et_product_price'] =  stripHtml(self.soup.find('flp').renderContents()) 
            except:
                log.info(self.log_msg('price not found'))
            try:
                review_id =  stripHtml( self.soup.find('sn').renderContents() )
                self.currenturi = 'http://reviews.nike.com/9191/%s/reviews.htm'%(review_id)
                if not self._setSoup():
                    log.info(self.log_msg('Cannot proceed , mail review page cannot be opened'))
                    return False
            except:
                log.info(self.log_msg('Cannot proceed , mail review page cannot be opened'))
                return False
            try:
                page['et_prodcut_description'] =stripHtml(self.soup.find('p','BVStandaloneHeaderProductDescription').renderContents())
            except:
                log.info(self.log_msg('Product Description cannot be found'))
                
            try:
                page['ef_product_rating'] = float(stripHtml(self.soup.find('span','BVStandaloneRatingSetAverageRatingValue').renderContents()))
            except:
                log.info(self.log_msg('rating not found'))
            try:
                post_hash = get_hash(page)
                if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri \
                                        , self.task.instance_data.get('update')):
                    log.info(self.log_msg('Session infor return True'))
                    return False
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out,\
                        self.parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
                if not result['updated']:
                    return False
                page['path']=[self.parent_uri]
                page['parent_path']=[]
                page['uri'] = normalize(self.parent_uri)
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
                log.info(self.log_msg('Parent Page added'))
                return True
            except:
                log.exception(self.log_msg("parent post couldn't be parsed"))
                return False

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
