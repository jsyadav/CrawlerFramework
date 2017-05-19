
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#prerna
import re
import logging
from urlparse import urlparse
from datetime import datetime
import cgi
import copy
from cgi import parse_qsl
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.urlnorm import normalize
from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log=logging.getLogger('ProductWikiConnector')
class ProductWikiConnector(BaseConnector):

    @logit (log, "fetch")
    def fetch(self):
        """
        same fetch method, I need to write something for doc string
        So I m writing this doc string
        sample uri is :http://www.newegg.com/Product/ProductReview.aspx?Item=N82E16830120240
        uri format is :http://www.newegg.com/Product/ProductReview.aspx?Item=.+?
        sample review uri: found in the same page
        fileds captured Parent Page = ei_product_review_count,et_price, et_sale_price,
        ef_product_rating_[excellent/good/average/poor/verypoor]
        Review - et_author_name, et_author_tech_level,et_author_ownership,,ef_rating,
         et_data_pros,et_data_cons,et_data_recommended_yes/no
        comments -NOT FOUND
        """
        try:
            self.genre = 'review'
            self.DEBUG = 0
            self.baseuri = 'http://www.productwiki.com'
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
            self._addReview()
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'%self.currenturi))
        return True        
        
    @logit( log, "_addReview")
    def _addReview(self):
        """
        This will add the Reviews and comments found on this page
        """
        reviews = self.soup.findAll('li', attrs = {'class':re.compile('cra_\d+')})
        log.info(len(reviews))
        for review in reviews:
            page = {}
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            try:
                page['ef_rating_overall'] = float(stripHtml(review.find('span','score ').renderContents()))
            except:
                log.exception(self.log_msg ('ef_rating_overall level not found') )
            try:
                page['data'] = stripHtml(review.find('p',id = re.compile('p_\d+')).
                                renderContents())
                page['title'] = ''                
            except:
                log.exception(self.log_msg('data not found error'))
                return
            try:
                review_hash = get_hash( page )
                unique_key = review['class'].split('_')[-1]
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
    @logit(log, "setSoup")
    def _setSoup(self, data=None, headers={}):
        """It will set soup object for the Current URI
        """
        res = self._getHTML(data=data, headers=headers)
        if res:
            self.rawpage = res['result']
        else:
            raise Exception('Page content not fetched for th url %s'%self.currenturi)
        self._setCurrentPage()

    