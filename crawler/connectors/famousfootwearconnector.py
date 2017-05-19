
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
import traceback
import copy
from urllib2 import urlparse

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('FamousFootWearConnector')

class FamousFootWearConnector (BaseConnector) :
    '''
    sample url:::http://www.famousfootwear.com/product.asp?product_id=1006804&variant_id=68029?CMP=KNC-Adwords&partnerid=Adwords&cpc=Adwords&campaign=FF-Nike&hbx_ou=50&cpckw=[nike%20shoes]&hbx_pk=nike%20shoes#reviews
    ::::http://www.famousfootwear.com/product.asp?product_id=1006804&variant_id=68029
    '''
    @logit(log , 'fetch')
    def fetch(self):
        try:
            self.genre='Review'
            self.parent_url=self.currenturi
            if re.search('http://www\.famousfootwear\.com/product\.asp\?product_id=\d+&variant_id=\d+(.+)?', self.currenturi):
                res=self._getHTML()
                if not res:
                    return False
                self.rawpage=res['result']
                self._setCurrentPage()
                self._getParentPage()
                while True:
                    log.info('SELF.Currenturi: %s' % self.currenturi)
                    self._addReviews()
                    try:
                        if self.soup.find('td',id='BVReviewPaginationNextLinkCell'):
                            self.currenturi =  self.soup.find('td',id='BVReviewPaginationNextLinkCell').a['href']
                            res=self._getHTML()
                            if not res:
                                break
                            self.rawpage=res['result']
                            self._setCurrentPage()
                        else:
                            break
                    except:
                        log.exception(self.log_msg('Exception in finding out Nextpage of no Next page info'))
                        break
            return True
        except Exception, e:
            log.exception(self.log_msg("Exception in Fetch"))
            return False

    @logit (log, "_getParentPage")
    def _getParentPage(self):
        '''
        Actual url for review is hidden. This method also finds out actual url to be processed in addtion to traditional stuff
        '''
        try:
            page={}
            try:
                page['et_product_price']=re.search('\$\d+\.\d+', stripHtml(self.soup.find('span','PD_SalePrice').renderContents())).group(0)
                self.updateProductPrice(page.get('et_product_price'))
                log.info(self.log_msg(page['et_product_price']))
            except:
                log.exception(self.log_msg('No price found for product'))
            try:
                self.currenturi = self.soup.find('div', id='BVdefaultURL').find('a')['href']
                res=self._getHTML()
                if not res:
                    return False
                self.rawpage=res['result']
                self._setCurrentPage()
                log.info(self.log_msg('The curent url is set to %s' % self.currenturi))
            except Exception, e:
                log.info(self.log_msg(traceback.print_exc()))
                log.info(self.log_msg('Actual Review url is not found'))
                return False
            if not checkSessionInfo(self.genre, self.session_info_out, self.parent_url \
                                        , self.task.instance_data.get('update')):
                try:
                    page['title'] = stripHtml(self.soup.find('span','BVproductname').renderContents())
                except:
                    log.info(self.log_msg('Product Title is not found') )
                    page['title'] = ''
                try:
                    page['et_product_desc'] = stripHtml(self.soup.find('p','BVStandaloneHeaderProductDescription').renderContents())
                except:
                    log.info(self.log_msg('Product Review is not found') )
                try:
                    page['ei_product_recommended_count'] = int(stripHtml(self.soup.find('span','BVStandaloneRatingWrapperBuyAgainValue').renderContents()))
                except:
                    log.info(self.log_msg('Product recommended count not found'))
                try:
                    page['ef_product_rating_overall']= float(stripHtml(self.soup.find('span','BVStandaloneRatingSetAverageRatingValue').renderContents()))
                except:
                    log.info(self.log_msg('Overall Rating is not found'))
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
            log.exception(self.log_msg("parent page couldn't be parsed"))
            raise e
    
    @logit (log, "_addReviews")
    def _addReviews(self):
        
        try:
            for review in self.soup.findAll('table','BVReviewDisplay') :
                page={}
                if not checkSessionInfo(self.genre, self.session_info_out, 
                                        self.currenturi,  self.task.instance_data.get('update'), 
                                        parent_list=[self.parent_url]):
                    try:
                        page['ef_overall_rating']=float(review.find('td', 'BVcustomerRatingFirst BVcustomerRating').find('img')['alt'].split('out')[0])
                        log.info(self.log_msg(page['ef_overall_rating']))
                    except:
                        log.info(self.log_msg('Could not parse Overall Rating'))
                    try:
                        quality=review.find('tr', 'BVRatingRow BVRatingRowQuality')
                        page['ef_rating_'+quality.find('td',  'BVcustomerRatingItem').
                             renderContents().strip(':')]=float(quality.findParent('tr').find('span','BVratingSummaryFinal').renderContents())
                    except:
                        log.info(self.log_msg('Could not parse Quality Rating'))
                    try:
                        comfort=review.find('tr', 'BVRatingRow BVRatingRowComfort')
                        page['ef_rating_'+comfort.find('td',  'BVcustomerRatingItem').\
                                 renderContents().strip(':')]=float(comfort.\
                                 findParent('tr').find('span','BVratingSummaryFinal').renderContents())
                    except:
                        log.info(self.log_msg('Could not parse Comfort Rating'))
                    try:
                        support=review.find('tr', 'BVRatingRow BVRatingRowSupport')
                        page['ef_rating_'+support.find('td',  'BVcustomerRatingItem').\
                                 renderContents().strip(':')]=float(support.\
                                 findParent('tr').find('span','BVratingSummaryFinal').renderContents())
                    except:
                        log.info(self.log_msg('Could not parse Support Rating'))
                    try:
                        performance=review.find('tr', 'BVRatingRow BVRatingRowPerformance')
                        page['ef_rating_'+performance.find('td',  'BVcustomerRatingItem').\
                                 renderContents().strip(':')]=float(performance.\
                                                                        findParent('tr').find('span','BVratingSummaryFinal').renderContents())
                    except:
                        log.info(self.log_msg('Could not parse Performance Rating'))
                    try:
                        style=review.find('tr', 'BVRatingRow BVRatingRowStyle')
                        page['ef_rating_'+style.find('td',  'BVcustomerRatingItem').\
                                 renderContents().strip(':')]=float(style.\
                                                                        findParent('tr').find('span','BVratingSummaryFinal').renderContents())
                    except:
                        log.info(self.log_msg('Could not parse style Rating'))
                    author_info_dict = {'et_ athletic_shoe_expertise':'BVReviewLabel BVReviewLabelShoeExpertiseAth',\
                                            'et_author_gender':'BVReviewValue BVReviewValueGender',\
                                            'et_comment_on_size':'BVReviewLabel BVReviewLabelSize',\
                                            'et_comment_on_width':'BVReviewLabel BVReviewLabelWidth'\
                                            }
                    for each in author_info_dict:
                        try:
                            page[each] = stripHtml(review.find('span',author_info_dict[each]).renderContents())            
                        except:
                            log.info(self.log_msg('author %s not found'%each))
                    try:
                        page['et_author_name']=stripHtml(review.find('span','BVReviewerNickname BVReviewerNicknameText').renderContents()).strip()
                    except:
                        log.info(self.log_msg('Author name could not be parsed'))
                    try:
                        page['et_author_location']=stripHtml(review.find('span','BVreviewerLocation').renderContents()).strip('from')
                    except:
                        log.info(self.log_msg('Author Location could not be parsed'))
                    try:
                        page['data']=stripHtml(review.find('span','BVContentReviewText').renderContents()).strip()
                    except:
                        log.info(self.log_msg('Review Data could not be parsed'))
                        page['data']=''
                    try:
                        page['title'] = stripHtml(review.find('span','BVreviewTitle').renderContents())
                    except:
                        log.info(self.log_msg('title could not be found'))
                        page['title'] =''
                    try:
                        page['posted_date']=datetime.strftime(datetime
                           .strptime(review.find('span','BVdateCreated')
                                     .renderContents().strip(), '%B %d, %Y'), '%Y-%m-%dT%H:%M:%SZ')
                    except:
                        page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ") 
                        log.info(self.log_msg('title could not be found'))
                    if page['title'] =='':
                        if len(page['data']) > 50:
                            page['title'] = page['data'][:50] + '...'
                    else:
                        page['title'] = page['data']
                    try:
                        review_hash = get_hash(page)
                    except:
                        log.exception(self.log_msg('could not generate review_hash for '+ self.parent_url))
                    
                    result=updateSessionInfo(self.genre, self.session_info_out, review_hash, \
                                         review_hash,'Review', self.task.instance_data.get('update'),\
                                         parent_list=[self.parent_url])
                    if result['updated']:
                        parent_list = [self.parent_url]
                        page['parent_path'] = copy.copy(parent_list)
                        parent_list.append(review_hash)
                        page['path'] = parent_list
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
                        log.info(self.log_msg('Adding review of %s ' % self.currenturi))
                    else:
                        log.info(self.log_msg('Not adding review of %s ' % self.currenturi))
                        return False
            return True
        except Exception, e:
            log.exception(self.log_msg('Exception in addReviews'))
            raise e
