
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

# Skumar

import re
from datetime import datetime
import logging
import copy
from urllib2 import urlparse

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('AltrecConnector')
class AltrecConnector(BaseConnector):
    '''
    This will fetch the info for the http://www.altrec.com
    Test with sample uri
    http://www.altrec.com/keen/womens-presidio-shoe#middleTabs
    For testing Start with the following uris
    
    http://www.altrec.com/keen/womens-timberline-slide#middleTabs --33 reviews

    http://www.altrec.com/keen/mens-austin-shoe#middleTabs --50 reviews

    http://www.altrec.com/keen/womens-portola-shoe#middleTabs -- 62 Reviews

    '''

    @logit(log , 'fetch')
    def fetch(self):
        self.genre="Review" # Type of Review
        try:
            self.parent_uri = self.currenturi
            if not self._setSoup():
                log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                return False
            self.__getParentPage()
            self.__addReviews()
            return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log , '__addReviews')
    def __addReviews(self):
        """
        It will add the reviews,
        Reviews are in two divs, readReviewBox
        for div in soup.findAll('div','readReviewBox'):
            for each in div.find('img','stars')
                author name = stripHtml(each.findNext('b').renderContents())
                rating = float(re.search('^\d+',each['alt']).group())
                date_str =datetime.strftime(datetime.strptime(stripHtml(each.findNext('b').nextSibling)[1:].strip() ,"%Y-%m-%d"),"%Y-%m-%dT%H:%M:%SZ")
                title = stripHtml(each.findNext('p').find('b').renderContents())
                 tag_str = each.findNext('p').renderContents()
                 title_str = stripHtml(each.findNext('p').find('b').renderContents())
                 data = stripHtml(tag_str.replace(title_str,''))
                 ei_data_recommended_count = each.findNext('p').findNextSibling('b').findNext('b')
                """
                
        for div in self.soup.findAll('div','readReviewBox'):
            for each in div.findAll('img','stars'):
                page = {}
                try:
                    page['et_author_name'] = stripHtml(each.findNext('b')\
                                                       .renderContents())
                except:
                    log.info(self.log_msg('author name not found'))
                try:
                    page['ef_rating_overall'] = float(re.search('^\d+',\
                                                            each['alt']).group())
                except:
                    log.info(self.log_msg('Rating not found' ) )
                try:
                    page['posted_date'] = datetime.strftime(datetime.strptime\
                                            (stripHtml(each.findNext('b').\
                                           nextSibling)[1:].strip() ,"%Y-%m-%d")\
                                           ,"%Y-%m-%dT%H:%M:%SZ")
                except:
                    page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")
                    log.info(self.log_msg('Posted_date not found') )
                try:
                    page['title'] = stripHtml(each.findNext('p').find('b')\
                                                        .renderContents())
                except:
                    log.exception(self.log_msg('Title not found'))
                    page['title'] =''
                try:
                    tag_str = each.findNext('p').renderContents()
                    title_str = stripHtml(each.findNext('p').find('b')\
                                                    .renderContents())
                    page['data'] = stripHtml(tag_str.replace(title_str,''))
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
                    match_object = re.match('(^\d+) of \d+$',each.findNext('p')\
                                    .findNextSibling('b').findNext('b').renderContents())
                    if match_object:
                        page['ei_data_recommended_count'] = int(match_object.\
                                                                group(1))
                except:
                    log.info(self.log_msg('data recommended not found'))
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
                    parent_list = [ self.parent_uri ]
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
            It will fetch the Product info
            For Testing in Ipython
            Sample Uri : http://www.altrec.com/keen/womens-presidio-shoe#middleTabs
            title =  stripHtml(soup.find('h1','detailH1').renderContents())
            et_product_sale_price = [stripHtml(each.renderContents()) for each in soup.find('div','topPrice').findAll('span','salePrice')]
            ei_product_review_count = int(re.search('^\d+',stripHtml(soup.find('div','topRightBox clearFix').renderContents())).group())            
            product_rating = float(re.search('^\d+',soup.find('div','topRightBox clearFix').findAll('img')[-1]['alt']).group())
            

            """
            page={}
            if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri \
                                        , self.task.instance_data.get('update')):
                log.info(self.log_msg('Session infor return True'))
                return False
            try:
                page['title'] = stripHtml(self.soup.find('h1','detailH1').renderContents())
            except:
                log.exception(self.log_msg('could not parse page title'))
                page['title'] = ''
            try:
                page['et_product_price'] = [stripHtml(each.renderContents()) for each in self.soup.find('div','topPrice').findAll('span')][-1]
                self.updateProductPrice(page.get('et_product_price'))                
            except:
                log.info(self.log_msg('price not found'))
            try:
                page['ef_product_rating'] = float(re.search('^\d+',self.soup.\
                                        find('div','topRightBox clearFix').findAll\
                                                ('img')[-1]['alt']).group())
            except:
                log.info(self.log_msg('rating not found'))
            try:
                page['ei_product_review_count'] = int(re.search('^\d+',\
                                                    stripHtml(self.soup.find\
                                                  ('div','topRightBox clearFix')\
                                                    .renderContents())).group())
            except:
                log.info(self.log_msg('Total review count not found') )
            try:
                post_hash = get_hash(page)
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
