
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#modified by prerna
# Skumar

import re
from datetime import datetime
import logging
import copy
from BeautifulSoup import BeautifulSoup
from urllib2 import urlparse

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('BuyConnector')
class BuyConnector(BaseConnector):
    '''fetch method of Buy Connector
    '''
    @logit(log , 'fetch')
    def fetch(self):
        self.genre="Review"
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
        """This will add the reviews
        """
        try:
            reviews = [ BeautifulSoup(x) for x in self.soup.find('span','blueText').\
                    findParent('td').__str__().split('Was this review helpful?&nbsp;')]
        except:
            log.info(self.log_msg('Cannot find the reviews'))

        for review in reviews:
            page = {}
            try:
                unique_key = [x for x in review.findAll('a') if x.get('name') and \
                                        re.match('a\d+',x.get('name'))][0]['name']
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    continue
            except:
                log.info(self.log_msg('CAnnot find hte uniqeue key'))
                continue
            try:
                #title = review.find('span','blueText').findNextSiblings('b')
                #page['et_author_name'] = stripHtml(title_and_author[1].renderContents())
                page['title'] = stripHtml(review.find('span','blueText').findNextSibling('b').renderContents())
            except:
                log.exception(self.log_msg('author name not found'))
                page['title'] =''
            try:
                page['ef_rating_overall'] = float(stripHtml(review.find('span',\
                                                'blueText').b.renderContents()))
            except:
                log.info(self.log_msg('Rating not found' ) )
            try:
                page['posted_date'] = datetime.strftime( datetime.strptime(stripHtml\
                            (review.find('span','blueText').findNextSibling('b')\
                            .next.next.__str__()),'%m/%d/%Y'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                ,"%Y-%m-%dT%H:%M:%SZ")
                log.info(self.log_msg('Posted_date not found') )
            try:
                author_tag = reviews[4].find('span', id = re.compile('CustomerReviews_customerReviews_ctl\d+_reviewerInfo'))
                author = author_tag.find('b')
                if author:
                    page['et_author_name'] = stripHtml(author.renderContents())
                    author.extract()
                location = stripHtml(author_tag.renderContents())
                if location:
                    page['et_author_location'] =  re.sub('^from ','',stripHtml(\
                                                            location.__str__()))
            except:
                log.exception(self.log_msg('Title not found'))
            try:
                page['data'] = stripHtml(review.find('span', id = re.compile('CustomerReviews_customerReviews_ctl\d+_reviewContent')).\
                                renderContents())
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
                log.info(page)
                review_hash = get_hash( page )
                #unique_key = get_hash( {'data':page['data'],'title':page['title']})
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
            """This will fetch the product_info
            """
            page={}
            if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri \
                                        , self.task.instance_data.get('update')):
                log.info(self.log_msg('Session infor return True'))
                return False
            try:
                page['title'] = stripHtml(self.soup.find('h1').a.renderContents())
            except:
                log.exception(self.log_msg('could not parse page title'))
                page['title'] = ''
            try:
                price_list = [[stripHtml(y.renderContents()) for y in x.findAll\
                        ('td')] for x in self.soup.find('td',id='productPricing')\
                                                    .find('table').findAll('tr')]
                for price in price_list:
                    if not len(price)==2:
                        continue
                    try:
                        if price[0].startswith('List Price'):
                            page['et_product_regular_price'] = price[1]
                        elif price[0].startswith('Our Price') or price[0].startswith('Price'):
                            page['et_product_price'] = price[1]
                        elif price[0].endswith('Save'):
                            page['et_product_saving'] = price[1]
                        elif price[0].endswith('Save:'):
                            page['et_product_saving'] = price[1]
                        elif price[0].startswith('Shipping'):
                            page['et_product_shipping_price'] = price[1]
                        elif price[0].endswith('Total Price:'):
                            page['et_product_total_price'] = price[1]
                    except:
                        log.info(self.log_msg('price not found'))
            except:
                log.info(self.log_msg('price not found'))
            try:
                pass
            except:
                log.info(self.log_msg('price not found'))
                
            try:
                page['ef_product_rating'] = float(re.search('\d+$',self.soup.find\
                        (text=re.compile('based on \d+ reviews?')).parent.parent.\
                                    find('img')['src'].split('.gif')[0]).group())
            except:
                log.info(self.log_msg('rating not found'))
            try:
                page.update(dict([('ef_rating_' + p.lower().replace(' ','_'),float(q)) \
                    for p,q in dict([[stripHtml(y.renderContents()) for  y in \
                    x.findAll('td')] for x in self.soup.find(text='Customer Reviews')\
                    .findParent('table').findAll('tr')[2].find('table').find('table').\
                                                            findAll('tr')]).items()]))
            except:
                log.info(self.log_msg('Total review count not found') )
            try:
                log.info(page)
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
