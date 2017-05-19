
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#prerna
# Skumar

import re
from datetime import datetime
import logging
from urllib2 import urlparse,unquote
import copy
from urlparse import urlparse
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('SamsClubConnector')
class SamsClubConnector(BaseConnector):
    '''
    This will fetch the info for the http://www.sampclub.com
    Test with sample uri
    http://www.samsclub.com/shopping/navigate.do?dest=5&item_nbr=512856&iid=08-29-09_HomePage|Featured|Item01
    http://www.samsclub.com/sams/shop/product.jsp?productId=prod2490323&navAction=#reviews
    '''
    @logit(log , 'fetch')
    def fetch(self):
        try:
            self.genre = 'Review'
            self.__task_elements_dict = {
                    'priority':self.task.priority,
                    'level': self.task.level,
                    'last_updated_time':datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ"),
                    'pickup_date':datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ"),
                    'connector_instance_log_id': self.task.connector_instance_log_id,
                    'connector_instance_id':self.task.connector_instance_id,
                    'workspace_id':self.task.workspace_id,
                    'client_id':self.task.client_id,
                    'client_name':self.task.client_name,
                    'versioned':False,
                    'category':self.task.instance_data.get('category',''),
                    'task_log_id':self.task.id }
            self._setSoup()
            self.parenturi = self.currenturi
            #self.__getParentPage()
            self.currenturi = self.soup.find('iframe')['src']
            self._setSoup()
            main_page_soup = copy.copy(self.soup)
            while self.__addReviews():
                try:
                    self.currenturi = main_page_soup.find('span','BVRRPageLink BVRRNextPage').\
                                        find('a')['href']
                    self._setSoup()
                    main_page_soup = copy.copy(self.soup)
                except:
                    log.exception(self.log_msg('Next Page link  not found for url %s'%self.currenturi))
                    break
            return True
        except:
            log.exception(self.log_msg('Exception while add the theread posts \
                                            for the url %s'%self.currenturi))
        return True

    @logit(log , '__addReviews')
    def __addReviews(self):
        """
        It will add the reviews
        """
        try:
            reviews = self.soup.findAll('div', id = re.compile('BVRRDisplayContentReviewID_\d+'))
            log.info(self.log_msg('no of reviews is %s'%len(reviews)))
            if not reviews:
                return False
        except:
            log.exception(self.log_msg('No Reviews are found'))
            return False
        for review in reviews:
            page = {}
            page['uri'] = self.currenturi
            try:
                page['ef_rating_overall'] = float(review.find('div',id = 'BVRRRatingOverall_Review_Display').\
                                            find('img')['alt'].replace(' out of 5',''))
            except:
                log.exception(self.log_msg('Rating over all not found'))
            try:
                page['ef_rating_quality'] = float(review.find('div',id = 'BVRRRatingQuality_Review_Display').\
                                            find('img')['alt'].replace(' out of 5', ''))
            except:
                log.exception(self.log_msg('Quality Rating not found'))
            try:
                page['ef_rating_value'] = float(review.find('div',id = 'BVRRRatingValue_Review_Display').\
                                            find('img')['alt'].replace(' out of 5', ''))
            except:
                log.exception(self.log_msg('value rating not found'))    
            try:
                page['title'] = stripHtml(review.find('span','BVRRValue BVRRReviewTitle').renderContents())
            except:
                log.exception(self.log_msg('title not found'))
                page['title'] =''
            try:
                date_str = stripHtml(review.find('span','BVRRValue BVRRReviewDate').renderContents())
                page['posted_date'] = datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception(self.log_msg('posted_date not found'))
                page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")
            try:
                page['et_author_name'] = stripHtml(review.find('span','BVRRNickname').renderContents())
            except:
                log.exception(self.log_msg('author_name not found'))
            try:
                page['et_author_location'] = stripHtml(review.find('span','BVRRValue BVRRUserLocation').\
                                                renderContents())
            except:
                log.exception(self.log_msg('author location not found'))
            pros_cons = {'et_data_cons':'BVRRValue BVRRReviewConTags','et_data_pros':'BVRRValue BVRRReviewProTags'}
            for each in pros_cons.keys():
                try:
                    page[each] = stripHtml( review.find('span',pros_cons[each]).renderContents() )
                except:
                    log.exception(self.log_msg('Pros and cons not found' ) )
            try:
                page['data'] = stripHtml(review.find('span','BVRRReviewText').renderContents())
            except:
                log.exception(self.log_msg('data not found'))
                page['data'] =''
            author_info =  {'et_author_age':'Age:','et_author_gender':'Gender:','et_author_owned_product':'Owned product:',\
                            'et_product_usage':'Uses product:','et_author_purchased_location':'Purchase location:',
                            'et_author_reached_type':'How did you hear about Ratings',
                            'et_author_recommended':'Would you recommend this product to '
                         }
            info_tag = review.find('div','BVRRContextDataContainer')
            if info_tag:   
                for each in author_info.keys():
                    try:
                        page[each] = stripHtml(review.find('span',text=re.compile('\s*'+author_info[each]+'\s*')).\
                                        findNext('span').renderContents())
                    except:
                        log.exception(self.log_msg('Author info not found'))
            try:
                if page['title'] =='':
                    if len(page['data']) > 50:
                        page['title'] = page['data'][:50] + '...'
                    else:
                        page['title'] = page['data']
            except:
                log.exception(self.log_msg('title not found'))
                page['title'] = ''
            try:
                review_hash = get_hash(page)
                unique_key =  review['id'].split('_')[-1]
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,self.\
                                task.instance_data.get('update'),parent_list=[ self.parenturi ]):
                    log.info(self.log_msg('Session info returns True for uri %s'% \
                                                                unique_key))
                    return False	
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key \
                                    , review_hash,'Review', self.task.instance_data.get('update'), \
                                                                        parent_list=[ self.parenturi ])
                if result['updated']:
                    parent_list = [ self.parenturi ]
                    page['parent_path'] = copy.copy( parent_list )
                    page['path']=parent_list
                    page['entity'] = 'Review'
                    page['uri'] = self.currenturi + '#' + unique_key
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    page.update(self.__task_elements_dict)
                    self.pages.append(page)
                    log.info('page added for %s'%self.currenturi)
                else:
                    log.exception(self.log_msg('Update session info returns False for \
                                                    url %s'%self.currenturi))
            except:
                log.exception(self.log_msg('Cannot add the post for the uri %s'%self.currenturi))
        return True        
        
##    @logit(log , 'getParentPage')
##    def __getParentPage(self):
##            """It will fetch the Product info
##            """
##            page={}
##            try:
##                page['title'] = page['data'] = stripHtml(self.soup.find('h2','lgDarkBlueHeader').\
##                                                renderContents())
##            except:
##                log.exception(self.log_msg('could not parse page title'))
##                page['title'] = ''
##                page['data'] = ''
##            page['posted_date'] = datetime.strftime(datetime.utcnow()\
##                                                    ,"%Y-%m-%dT%H:%M:%SZ")    
##            try:
##                prod_info = {'et_product_model_no':'Model:','et_product_item_no':'Item #:'}
##                for each in prod_info.keys():
##                    page[each] = stripHtml(self.soup.find('strong',text=re.compile(prod_info[each])).\
##                    findParent('p').renderContents()).split(':')[-1]
##            except:
##                log.exception(self.log_msg('Product model and item no  not found'))
##            try:
##                if checkSessionInfo(self.genre, self.session_info_out, self.parenturi \
##                                                , self.task.instance_data.get('update')):
##                    log.info(self.log_msg('Session infor return True'))
##                    return False
##                post_hash = get_hash(page)
##                id=None
##                if self.session_info_out=={}:
##                    id=self.task.id
##                result=updateSessionInfo(self.genre, self.session_info_out,self.parenturi, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
##                if not result['updated']:
##                    return False
##                page['path'] = [self.parenturi] 
##                page['parent_path'] = []
##                page['uri'] = self.currenturi
##                page['uri_domain'] = unicode(urlparse(page['uri'])[1])                
##                page['entity'] = 'Review'
##                page.update(self.__task_elements_dict)
##                self.pages.append(page)
##                log.info(self.log_msg('Parent Page added'))
##                log.info(page)
##                return True
##            except Exception,e:
##                log.exception(self.log_msg("parent post couldn't be parsed"))
##                return False
##    
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
            log.exception(self.log_msg('Page not for  :%s'%uri))
            raise e
