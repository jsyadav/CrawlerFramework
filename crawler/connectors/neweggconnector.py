
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#prerna
#Skumar
#Ashish
# removed getTaskUris method, removed unused imports


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

log=logging.getLogger('NewEggConnector')
class NewEggConnector(BaseConnector):

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
            self.baseuri = 'http://www.newegg.com'
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
            data = dict(parse_qsl(self.currenturi.split('?')[-1]))
            headers = {}
            headers['Host'] = 'www.newegg.com'
            headers['Referer'] = self.currenturi
            self._setSoup(data=data, headers=headers) 
            self.parenturi = self.currenturi
            while True:
                self._addReview()
                if not self._setNextPage():
                    break
            return True    
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'%self.currenturi))
        return True        
        
    @logit( log, "_addReview")
    def _addReview(self):
        """
        This will add the Reviews and comments found on this page
        """
        reviews = self.soup.find('table','grpReviews').find('tbody').findAll('tr')
        for review in reviews:
            page = {}
            #if not review.find('td','dark'):
             #   continue
            try:
                page['et_author_name'] = stripHtml( review.find('th','reviewer').\
                                            find('ul').find('em').renderContents() )
            except:
                log.exception(self.log_msg ('author name not found') )
            try:
                page['ei_author_tech_level'] =  int(stripHtml(review.find('li',text = re.compile('^Tech Level:')).\
                                                parent.find('span').renderContents()).split('/')[0])
            except:
                log.exception(self.log_msg ('author tech level not found') )
            try:
                page['ei_author_ownership'] =  int(stripHtml(review.find('li',text = re.compile('^ Ownership:')).\
                                                parent.find('span').renderContents()).split('/')[0])
            except:
                log.exception(self.log_msg ('author owner ship level not found') )
            try:
                date_str = stripHtml(review.find('th','reviewer').find('ul').\
                            find('em').findParent('li').findNext('li').renderContents())
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str,\
                                                        "%m/%d/%Y %I:%M:%S %p"),"%Y-%m-%dT%H:%M:%SZ")
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                log.exception(self.log_msg ('author posted not found') )
            try:
                page['ef_rating_overall'] = float (stripHtml(review.find('span','itmRating').\
                                                renderContents()).split('/')[0].split(':')[-1])
            except:
                log.exception(self.log_msg ('ef_rating_overall level not found') )
            try:
                page['title'] =  re.sub('\d+/\d+' ,'',stripHtml(review.find('span','itmRating').\
                                    findParent('h3').renderContents())).split(':')[-1].strip()
            except:
                log.exception(' Title not found')
                page['title'] = ''
            try:
                review_data = review.find('div','details').__str__()
                pros = review.find('div','details').find('em',text='Pros:')
                cons = review.find('div','details').find('em',text='Cons:')
                other_thoughts = review.find('div','details').find('em',text='Other Thoughts:')
                if pros:
                    pros_str = pros.parent.parent.__str__() # needed for replace tags
                                                
                    review_data = review_data.replace(pros_str,'')
                if cons:
                    cons_str = cons.parent.__str__()# needed for replace tags
                    review_data = review_data.replace(cons_str,'')
                if other_thoughts:
                    other_thoughts_str = other_thoughts.parent.__str__()# needed for replace tags
                    review_data = review_data.replace(other_thoughts_str,'')

                page['data'] = stripHtml(review_data)
            except:
                log.exception(self.log_msg('data not found error'))
                page['data'] = ''
            try:
                page['ei_data_recommended_yes'] = int( re.search('^\d+',stripHtml( review.\
                                                find('p','helpful').renderContents() )).group() )
                if self.DEBUG==1:
                    log.info(self.log_msg ('ei_data_recommended_yes is %s'%page[ 'ei_data_recommended_yes']) )
            except:
                log.exception(self.log_msg ('ei_data_recommended_yes is not found'))
            try:
                review_hash = get_hash( page )
                unique_key = review.find('form')['name'].replace('frmVote','')
                #unique_key = get_hash({'data' : page['data']},{'posted_date' : page['posted_date']})
                #log.info(unique_key)
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
                    page['uri'] = normalize( self.currenturi )
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    page.update(self.__task_elements_dict)
                    self.pages.append(page)
                    log.info('page added for %s'%self.currenturi)
                else:
                    log.exception(self.log_msg('Update session info returns False for \
                                                    url %s'%self.currenturi))
            except:
                log.exception(self.log_msg('Cannot add the post for the uri %s'%self.currenturi))
            #return True                                            
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

    @logit(log, "_setNextPage")
    def _setNextPage(self):
        """
        This will set find the next link and set the soup
        if nextLink not found, it will return a False
        """
        try:
            next_page_found = None
            current_page_no =  int ( self.soup.find('div','pageInput').find('input')['value'] )
            total_page = int( stripHtml( self.soup.find('div','pageTotal').renderContents()).\
                        replace('of ','').strip())            
            if current_page_no == total_page:
                return False
            item = self.parenturi.split('?')[-1].split('&')[0].split('=')[-1][-8:]
            item_str = item[:2]+ '-' +item[2:5] + '-' + item[5:]
            post_url = 'http://www.newegg.com/Product/Product.aspx?Item=' + item_str + '&SortField=0&SummaryType=0&Pagesize=10&SelectedRating=-1&PurchaseMark=&VideoOnlyMark=False&VendorMark=&IsFeedbackTab=true&Keywords=(keywords)&=0&=10&Page=' + str(current_page_no + 1)
            data = dict(parse_qsl(self.parenturi.split('?')[-1]))
            headers = {'Referer':self.currenturi}
            self._setSoup(data=data, headers=headers)
            self.currenturi = post_url
            log.info(self.log_msg('for url : %s'%self.currenturi))
            return True
        except:
            log.exception(self.log_msg('error  with setting next page'))
            return False
        
