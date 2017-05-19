
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
from urllib2 import urlparse,unquote
import copy
import cgi

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('FinishLineConnector')
class FinishLineConnector(BaseConnector):
    '''
    This will fetch the info for the http://www.finishline.com
    Test with sample uri
    http://www.finishline.com/store/catalog/product.jsp?productId=prod621942
    or
    Sample uris taken are
    1) http://www.finishline.com/store/catalog/product.jsp?productId=prod621942#reviews
    2) http://www.finishline.com/store/catalog/product.jsp?oldRequestedURI=/store/results/search.jsp&Ne=5+3000540+3000559+3000571+2&resetResult=true&productId=prod633567&N=4294966771&categoryId=catNike#reviews
    3) http://www.finishline.com/store/catalog/product.jsp?productId=prod350586&Ntt=Reebok&Ntk=all&isSearch=true&Ntx=mode%2Bmatchallpartial&y=0&N=4294966781&Ns=P_SalePrice|1&x=0#reviews
    4) http://www.finishline.com/store/catalog/product.jsp?productId=prod629948&pageTitle=Men's%20Shoes&N=3000583+3000543&Ns=P_SalePrice|1&categoryId=cat10003
    5) http://www.finishline.com/store/catalog/product.jsp?productId=prod629948&pageTitle=Men's%20Shoes&N=3000583+3000543&Ns=P_SalePrice|1&categoryId=cat10003


    '''

    @logit(log , 'fetch')
    def fetch(self):
        self.genre="Review"
        try:
##            self.currenturi = 'http://www.finishline.com/store/catalog/product.jsp?productId=prod621942#reviews'
            params = self.currenturi.split('?')[-1]
            data = dict(cgi.parse_qsl(params))
            code = data.get('productId')
            if not code:
                return False
            self.currenturi = 'http://www.finishline.com/store/catalog/product.jsp?productId=' + code
            self.review_url = self.parent_uri = self.currenturi
            if not self._setSoup():
                log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                return False
            if not self.__getParentPage():
                log.info(self.log_msg('Parent Page return False'))
            while True:
                self.__addReviews()
                try:
                    self.currenturi =  self.soup.find('td',id='BVReviewPaginationNextLinkCell').find('a')['href']
                    if not self._setSoup():
                        break
                except:
                    log.info(self.log_msg('Next page not found'))
                    break
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
        try:
            reviews =  self.soup.findAll('table','BVReviewDisplay')
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
                page['ef_rating_overall'] = float(stripHtml(review.find('span',\
                                    'BVratingSummaryFinal').renderContents()))
            except:
                log.exception(self.log_msg('Rating over all not found'))

            try:
                page = self.__getRating(review,page)
            except:
                log.info(self.log_msg('Ratings are not found'))

            try:
                page['title'] = stripHtml( review.find('span','BVreviewTitle').renderContents() )
            except:
                log.exception(self.log_msg('title not found'))
                page['title'] =''
            try:
                page['posted_date'] = datetime.strftime( datetime.strptime( \
                            stripHtml( review.find('span','BVdateCreated').\
                            renderContents() ), "%B %d, %Y" ),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception(self.log_msg('title not found'))
                page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")

            try:
                page['et_author_location'] = re.sub('^from','', stripHtml( \
                                        review.find('span','BVreviewerLocation')\
                                                    .renderContents() ) ).strip()
            except:
                log.info(self.log_msg('author location not found'))
            pros_cons = {'et_data_cons':'BVConsTitle','et_data_pros':'BVProsTitle'}
            for each in pros_cons.keys():
                try:
                    page[each] = stripHtml( review.find('span',pros_cons[each]).nextSibling )
                except:
                    log.info(self.log_msg('Pros and cons not found' ) )
            try:
                page['data'] = stripHtml(review.find('span','BVContentReviewText').renderContents())
            except:
                log.info(self.log_msg('data not found'))
                page['data'] =''
            try:
                page['ei_data_recommended_yes'] =int(stripHtml(review.parent.find('span','BVrespondedHelpful BVrespondedHelpfulPositive').renderContents()))
            except:
                log.info(self.log_msg('ei_data_recommended_yes  count not found'))
            try:
                if page['title'] =='':
                    if len(page['data']) > 50:
                        page['title'] = page['data'][:50] + '...'
                    else:
                        page['title'] = page['data']
            except:
                log.exception(self.log_msg('title not found'))
                page['title'] = ''
            previous_soup = copy.copy(self.soup)
            previous_url = self.currenturi
            try:
                page['et_author_name'] = stripHtml(review.find('span','BVReviewer')\
                                                        .find('a').renderContents())
                self.currenturi = review.find('span','BVReviewer').find('a')['href']
                if self._setSoup():
                    self.currenturi = self.soup.find('iframe',id='BVProfileFrame')['src']
                    if self._setSoup():
                        page = self.__getAuthorInfo(page)
            except:
                log.exception(self.log_msg('author name not found'))
            self.soup = copy.copy(previous_soup)
            self.currenturi = previous_url
            try:
                review_hash = get_hash(page)
                unique_key =  re.search('reviewID=([^\&]*)',review.findParent('tr')\
                                .find('a','BVSocialBookmarkingSharingLink')['href']\
                                                .replace('%3D','=')).group(1).strip()
                if checkSessionInfo(self.genre, self.session_info_out, review_hash,\
                                 self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    log.info(self.log_msg('session info return True'))
                    continue
                result=updateSessionInfo(self.genre, self.session_info_out, review_hash, \
                            review_hash,'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                
                
                if not result['updated']:
                    log.info(self.log_msg('result not updated'))
                    continue
                parent_list = [self.parent_uri]
                page['parent_path'] = copy.copy(parent_list)
                parent_list.append(unique_key)
                page['path'] = parent_list
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
                log.info(page) # To do, remove this
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))

    @logit(log , 'getParentPage')
    def __getParentPage(self):
            """
            It will fetch the Product info
            For Testing in Ipython
            Sample Uri : http://www.finishline.com/store/catalog/product.jsp?productId=prod621942#reviews
            title = stripHtml(soup.find('div',id='title').renderContents())
            Search for iframe['src']
            review_url = soup.find('iframe',id='BVFrame')['src']
            set the Soup
            """
            page={}
            try:
                page['title'] = stripHtml(self.soup.find('div',id='title').renderContents())
            except:
                log.exception(self.log_msg('could not parse page title'))
                page['title'] = ''
            try:
                
                page['et_product_price'] = re.search('pricesArr\[0\] = new String\("(\$.*?)"\);',self.rawpage)\
                                                                            .group(1).split(',')[0]
                self.updateProductPrice(page.get('et_product_price'))      

            except:
                log.info(self.log_msg('price not found'))
            try:
                # get the page where the actual reviews are found
                self.currenturi = self.soup.find('iframe',id='BVFrame')['src']
                if not self._setSoup():
                    return False
            except:
                log.exception(self.log_msg('Review Page not found, cannot proceed'))
                return False
            try:
                page = self.__getRating(self.soup.find('td','BVRatingSummaryRatings BVRatingSummarySecondaryRatings'),page)
            except:
                log.info(self.log_msg('Ratings are not found'))
            try:
                recommend_dict = {'ei_product_recommended_yes':'BVratingFinal',\
                'ei_product_review_count':'BVratingTotal'}
                for each in recommend_dict.keys():
                    page[each] = int(self.soup.find('span', recommend_dict[each] )\
                                                                    .renderContents())
            except:
                log.info(self.log_msg('Product recommended yes/product review count not found'))
            try:
                if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri \
                                                , self.task.instance_data.get('update')):
                    log.info(self.log_msg('Session infor return True'))
                    return False
                post_hash = get_hash(page)
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out,self.parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
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
                log.info(page)
                return True
            except Exception,e:
                log.exception(self.log_msg("parent post couldn't be parsed"))
                return False
    @logit(log,'getRating')
    def __getRating(self,rating_block,page):
        """ Returns the page added with ratings
        """
        try:
            ratings = rating_block.findAll('td','BVcustomerRatingItem')
            for each in ratings:
                value = each.findNext('img')['alt']
                key = stripHtml(each.renderContents())
                if key.endswith(':'):
                    key = key[:-1]
                page['ef_product_rating_' + re.sub('[^A-z]','_',key.lower())] =  float(re.search('(.*?)out of 5$',value).group(1).strip())
        except:
            log.info(self.log_msg('Ratings not found'))
        return page

    @logit(log,'getAuthorInfo')
    def __getAuthorInfo(self ,page):
        """ returns the author info
        """
        divs = self.soup.findAll('td','BVReviewerStatisticsSectionTableCell')
        for div in divs:
            item_labels = div.findAll('div','BVReviewerStatisticsItemLabel')
            for item in item_labels:
                key = stripHtml(item.renderContents())
                value = stripHtml(item.findNext('div').renderContents())
                if key =='Community Rank':
                    page['et_author_community_rank'] = value
                if key == 'Reviews' or key =='Featured Reviews' or key=='Helpful Votes':
                    try:
                        key = 'ei_author_' + key.replace(' ','_').lower()+'_count'
                        page[ key ] = int(value)
                    except:
                        log.info(self.log_msg('author info not found'))
                if key =='Average Rating':
                    try:
                        page['ef_author_rating_average'] = float(value)
                    except:
                        log.info(self.log_msg('author info not found'))
                if key =='Recommended':
                    try:
                        page['ef_author_recommended_yes'] = float(value.replace('%',''))
                    except:
                        log.info(self.log_msg('author info not found'))
                if key == 'Active Since' or key =='Last Review':
                    try:
                        key = 'edate_author_' + key.replace(' ','_').lower()
                        page[ key ] =  datetime.strftime( datetime.strptime( value, "%B %d, %Y" ),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg('author info not found'))
        return page

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
