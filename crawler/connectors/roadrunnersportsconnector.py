
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

# Skumar
# modified on Feb 03, 2009, since the Site has been changed
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

log = logging.getLogger('RoadRunnerSportsConnector')
class RoadRunnerSportsConnector(BaseConnector):
    '''
    This will fetch the info for the http://www.roadrunnersports.com
    Test with sample uri
    http://www.roadrunnersports.com/rrs/products/NIK1206/Mens_Nike_Air_Pegasus+_25_Running_Shoe
    '''

    @logit(log , 'fetch')
    def fetch(self):
        self.genre="Review" # Type of Review
        try:
##            self.currenturi ='http://www.roadrunnersports.com/rrs/products/NIK1206/Mens_Nike_Air_Pegasus+_25_Running_Shoe'            
            
            self.currenturi = self.currenturi.replace(' ','%20')
            self.parent_uri = self.currenturi
            if not self._setSoup():
                log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                return False
            if not self.__getParentPage():
                log.info(self.log_msg('Parent page not set'))
            self.__addReviews()
            return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log , '__addReviews')
    def __addReviews(self):
        """
        It will add the reviews,
        Check for "See all reviews"
        if it is found go to that page and start adding
        if not, add in the current page
        for review in all_reviews:
            author_name = re.search('(.*)rated this product',stripHtml(review.find('b').renderContents())).group(1).strip()
            rating = float(re.search( '^\d+',review.find('b').nextSibling.strip()).group().strip())
            posted_date =datetime.strftime(datetime.strptime(stripHtml( review.find('i').renderContents()) ,"%a, %B %d,%Y"),"%Y-%m-%dT%H:%M:%SZ")
            title = first 50 char of title



        data
        ----
        data_str = review.renderContents()
        posted_str = review.find('i').renderContents()
        starting_index = data_str.find(posted_str)
        if not starting_index == -1:
            data_str = stripHtml( data_str[starting_index+len(posted_str):] )[1:].strip()
        """
        more_reviews_found = False
        try:
            all_reviews_url = self.soup.find('a',text='Read all reviews').parent['href']                    
            all_reviews_url = 'http://www.roadrunnersports.com' + re.search('javascript:openContentWin\(\'(.+?)\'\);',all_reviews_url).group(1).strip().replace(' ','%20')
            headers = { 'Referer':self.parent_uri }
            if not self._setSoup(all_reviews_url,{},headers ):
                return False
        except:
            log.info(self.log_msg('more reviews not found, so fetch avbl reviews'))        
        reviews = None
        try:
            reviews = self.soup.findAll( 'td','prod_detail',valign=None )
            if not reviews:
                log.info(self.log_msg('No Reviews are found'))
                return False
        except:
            log.exception(self.log_msg('No Reviews are found'))
            return False
        for review in reviews:
##            log.info(review.__str__())
            page = {}
            try:
                page['et_author_name'] = re.search('(.*)rated this product',\
                    stripHtml(review.find('b').renderContents())).group(1).strip()
            except:
                log.info(self.log_msg('author name not found'))
            try:
                page['ef_rating_overall'] = float(re.search( '^\d+',review.find('b')\
                                            .nextSibling.strip()).group().strip())
            except:
                log.info(self.log_msg('Rating not found' ) )
            try:
                page['posted_date'] = datetime.strftime(datetime.strptime(stripHtml\
                        ( review.find('i').renderContents()) ,"%a, %B %d,%Y"),\
                                                            "%Y-%m-%dT%H:%M:%SZ")
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                            ,"%Y-%m-%dT%H:%M:%SZ")
                log.info(self.log_msg('Posted_date not found') )
            try:
                data_str = self.__getData( review )
                if data_str.endswith('...') and review.find('a'):
                    prev_soup = copy.copy(self.soup)
                    previous_uri = self.currenturi
                    try:
                        headers = {'Referer':self.parent_uri}
                        self.currenturi = ('http://www.roadrunnersports.com' + re.search('\(\'(.*)\'\)',review.find('a')['href']).group(1)).replace(' ','%20')
                        if self._setSoup():
                            data_str = self.__getData(headers = headers)
                    except:
                        log.info(self.log_msg('cannot get more reviews') )
                    self.soup = copy.copy(prev_soup)
                    self.currenturi = previous_uri
                if data_str:
                    page['data'] = data_str
                else:
                    page['data'] = data_str
                
            except:
                log.exception(self.log_msg('Data not found'))
                page['data'] = ''
            try:
                if len(page['data']) > 50:
                        page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
            except:
                log.exception(self.log_msg('title not found'))
                page['title'] = ''
            try:
                review_hash = get_hash(page)
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                                 self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    log.info(self.log_msg('session info is True'))
                    continue
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            review_hash,'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    log.info(self.log_msg('result of update is True'))
                    continue
                #page['id'] = result['id']
                #page['first_version_id']=result['first_version_id']
                #page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                parent_list = [ self.parent_uri ]
                page['parent_path'] = copy.copy(parent_list)
                parent_list.append( unique_key )
                page['path']=parent_list
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
            Sample Uri : http://www.roadrunnersports.com/rrs/products/NIK1194/Mens_Nike_Air_Zoom_Vomero+_3_Running_Shoe
            title = stripHtml( soup.find('p','prod_detail_name').renderContents() )
            et_product_sale_price = stripHtml(soup.find('span','product_detail_sale_price' ).renderContents()).split(':')[-1].strip()
            et_product_vip_price = stripHtml(soup.find('span','product_detail_vip_price').renderContents()).split(':')[-1].strip()
            rating_str = soup.find('a',text=re.compile('^Average Rating:(.*?)out of 5 stars$',re.S))
            product_rating = float( re.search('^Average Rating:(.*?)out of 5 stars$',rating_str,re.S).group(1).strip() )
            ei_product_review_count = re.search('^\d+',stripHtml( soup.find('div',id='reviewForm').findNext('td').renderContents())).group()
            After Feb 03, 2009
            title = stripHtml( soup.find('span','prod_title').renderContents() )
            rating_str = soup.find('a',text='Read all reviews').findParent('td').findAll('img')[-1].next.strip()            
            product_rating = float(re.search('\((.*?)out of 5 stars\)$',rating_str).group(1).strip())
            
            

            """
            page={}
            try:
                product_price = stripHtml(self.soup.find('span','prod_detail_sale_price').renderContents()).split(':')[-1].strip()
                self.updateProductPrice(product_price)
            except:
                log.info(self.log_msg("couldn't parse product price"))

            if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri \
                                                , self.task.instance_data.get('update')):
                log.info(self.log_msg('Session infor return True'))
                return False
            try:
                page['title'] = stripHtml( self.soup.find('span','prod_title')\
                                                            .renderContents() )
            except:
                log.exception(self.log_msg('could not parse page title'))
                page['title'] = ''
            try:
                # Price
                price_dict = {'et_product_sale_price' : 'prod_detail_sale_price',\
                                    'et_product_vip_price':'prod_detail_vip_price'}
                for each in price_dict.keys():
                    page[ each ] = stripHtml(self.soup.find('span',price_dict[each])\
                                            .renderContents()).split(':')[-1].strip()
            except:
                log.info(self.log_msg('price not found'))

            try:
                rating_str = self.soup.find('a',text='Read all reviews').findParent('td').findAll('img')[-1].next.strip()
                page['ef_product_rating'] = float(re.search('\((.*?)out of 5 stars\)$',rating_str).group(1).strip())
            except:
                log.info(self.log_msg('rating not found'))
#===============================================================================
#            try:
#                page['ei_product_review_count'] = re.search('^\d+',stripHtml( \
#                            self.soup.find('div',id='reviewForm').findNext('td')\
#                                                    .renderContents())).group()
#            except:
#                log.info(self.log_msg('Total review count not found') )
#===============================================================================

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
                #page['first_version_id']=result['first_version_id']
                page['data'] = ''
                #page['id'] = result['id']
                page['task_log_id']=self.task.id
                page['entity'] = 'Post'
                page['category']=self.task.instance_data.get('category','')
                self.pages.append(page)
                log.info(self.log_msg('Parent Page added'))
                return True
            except Exception,e:
                log.exception(self.log_msg("parent post couldn't be parsed"))
                return False
            
    @logit(log, "__getData")
    def __getData( self, review = None ):
        '''It will fetch the data and return it
        '''
        if not review:
            review = copy.copy( self.soup )
        data_str = review.renderContents()
        posted_str = review.find('i').renderContents()
        starting_index = data_str.find(posted_str)
        if not starting_index == -1:
            data_str = stripHtml( data_str[starting_index+len\
                                            (posted_str):] )[1:].strip()
            return stripHtml(data_str)
                

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
