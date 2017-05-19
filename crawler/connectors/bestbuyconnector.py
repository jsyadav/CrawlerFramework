
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

##MOHIT RANKA
#JV
import urllib
import urllib2
from urllib2 import *
from urlparse import urlparse
import simplejson
import re
import logging
from logging import config
from datetime import datetime
import md5
from BeautifulSoup import BeautifulSoup
import pickle
import copy
from baseconnector import BaseConnector
from xml.sax import saxutils
 
from htmlconnector import HTMLConnector
from tgimport import *
from knowledgemate import pysolr
from knowledgemate import model
 
from utils.sessioninfomanager import *
from utils.task import Task
from utils.utils import stripHtml
from utils.urlnorm import normalize
from utils.decorators import *


log = logging.getLogger('BestBuyConnector')

class BestBuyConnector(BaseConnector):  
  
    @logit(log,'fetch')
    def fetch(self):
        """
        Fetched all the reviews for a given self.currenturi and returns Fetched staus depending 
        on the success and faliure of the task
        """
        try:
            self.genre = 'Review'
            self.currenturi = self.currenturi+"?sortOrder=submissionTime"
            self._getParentPage()
            parent_page_url = self.task.instance_data['uri']#self.currenturi #JV 
            log.debug(self.log_msg(self.currenturi))
            self.all_review_count = 0
            self.new_review_count = 0
            self.fetch_next_link = True
            self.fetch_next_review = True
            self.this_crawl_permalinks = []#Just to take account of possible duplicates
            while self.fetch_next_link and self.fetch_next_review:
                self._iterateReviewPages(parent_page_url)
            if len(self.pages) > 0:
                log.debug(self.log_msg("%d new reviews fetched" %self.new_review_count))
            else: 
                log.debug(self.log_msg("No new reviews fetched"))
            return True
        except Exception,e:
            log.exception(self.log_msg("Exception occured in fetch()"))
            return False
        
    @logit(log,'_getParentPage')
    def _getParentPage(self):
        """
        Gets the data for the parent page and appends it to self.pages if the parent page has changed since the last crawl. 
        An empty dictionay is added to self.pages if the parent page is not changed since the last crawl
        """
        try:
            page={}
            if not self.rawpage:
                res=self._getHTML(self.currenturi) # Assuming self.currenturi is at the product page
                self.rawpage=res['result']
            self._setCurrentPage()
            try:
                #page['title']= stripHtml(self.soup.findAll(attrs={'class':re.compile('BVproductname')})[0].renderContents()) #Product Name - Title
                page['title'] = stripHtml(self.soup.find(attrs={'class':re.compile('BVRRSExternalSubjectTitleProductName')}).renderContents()) #Product Name - Title
            except:
                page['title']=''
                log.info(self.log_msg("Error occured while fetching product page title"))
            page['data']=''
            try:
                #page['ef_product_rating_overall']= float(stripHtml(self.soup.findAll(attrs={'class': re.compile('BVStandaloneRatingSetAverageRatingValue')}[0].renderContents())))
                page['ef_product_rating_overall'] = float(stripHtml( self.soup.find(attrs={'class': re.compile('BVRRNumber ')}).renderContents()))
            except:
                log.info(self.log_msg("Error occured while fetching product page overall rating"))
            
            try:
                page['ei_product_recommended_yes'] = int(self.soup.find('span',attrs={'class':'BVRRNumber BVRRBuyAgainTotal'}).renderContents())
            except:
                log.info(self.log_msg("Error occured while fetching number of people who recommended this product"))
            try:
                page['ei_product_recommended_total'] = int(stripHtml(self.soup.find('span',attrs={'class':re.compile('BVRRBuyAgainRecommend')}).renderContents()))
            except:
                log.info(self.log_msg("Error occured while fetching total number of people who rated this product"))
            post_page_soup = copy.copy(self.soup)
            post_page_url = self.currenturi
            self.product_page=page
            try:
                product_details_url= self.soup.find('a', title='Product Details')['href']
            except:
                log.debug(self.log_msg('No product details page found'))
                product_details_url=None

            if product_details_url:
                self.currenturi = product_details_url
                self._fetchProductPrice()
            self.currenturi = post_page_url
            self.soup = copy.copy(post_page_soup)
            log.debug(self.log_msg('got the content of the product main page'))
            log.debug(self.log_msg('checking session info'))
            post_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                                                page.values()))).encode('utf-8','ignore')).hexdigest()
            self.updateParentExtractedEntities(page) #update parent extracted entities
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    self.task.instance_data['uri'], self.task.instance_data.get('update')):
#                                    self.currenturi, self.task.instance_data.get('update')):#JV
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug(id)
                result=updateSessionInfo(self.genre, self.session_info_out, self.task.instance_data['uri'], post_hash, 
                                         'Post', self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    page['path'] = [self.task.instance_data['uri']]
                    page['parent_path'] = []
                    page['task_log_id']=self.task.id
                    page['versioned']=self.task.instance_data.get('versioned',False)
                    page['category']=self.task.instance_data.get('category','generic')
                    page['last_updated_time']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")  #Now
                    page['client_name']=self.task.client_name
                    page['entity']='post'
                    page['uri'] = normalize(self.currenturi)
                    page['uri_domain'] = unicode(urlparse(self.currenturi)[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")  #Now
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ") # As the Product page does not have a posting date, keeping the posting date identical to pickup date
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project
                    self.pages.append(self.product_page)
                    log.debug(self.log_msg("product review main page details stored"))
                    return True
                else:
                    log.debug(self.log_msg("product review main page details NOT stored"))
                    return False
            else:
                log.debug(self.log_msg("product review main page details NOT stored"))
                return False
        except:
            log.exception(self.log_msg("Exception occured in _getParentPage()"))
            return False

    @logit(log,'_fetchProductPrice')
    def _fetchProductPrice(self):
        try:
            tries = 0
            while tries < 3:
                log.debug(self.log_msg("try number %d to fetch the Webpage" %tries))
                tries = tries + 1
                res=self._getHTML(self.currenturi)
                if res:
                    self.rawpage=res['result']
                    self._setCurrentPage()
                    break
            try:
                self.product_page['et_product_price']=stripHtml(self.soup.find('div',attrs={'class':'salenum'}).renderContents())
                return True
            except:
                log.exception(self.log_msg("Product price not picked up for %s" %self.currenturi))
                return False
        except:
            log.exception(self.log_msg("Exception occured in _fetchProductPage()"))
            return False
                                                                           
    @logit(log,'_iterateReviewPages')
    def _iterateReviewPages(self,parent_uri):
        try:
            res=self._getHTML(self.currenturi)
            if res:
                self.rawpage=res['result']
                self._setCurrentPage()
                log.debug(self.log_msg("Fetching the next review page %s" %self.currenturi))
                #page_reviews = self.soup.findAll(attrs={'class':'BVStandaloneReviewSectionReview hreview'})
                page_reviews = self.soup.findAll('div', id=re.compile('BVRRDisplayContentReviewID_'))
                for each_review in page_reviews: # page_reviews = reviews in a page, typically 10,except the last page 
                    try:
                        post_permalink= re.findall("http://reviews.bestbuy.com/.*$",urllib.unquote(each_review.findAll('a',attrs={'class':re.compile('BVRRSocialBookmarkingSharingLink')})[0].get('href')))[0]
                    except:
                        log.info(self.log_msg('Error occured while fetching permalink of the current review'))
                        continue # Skip this review and move on to the next review

                    if post_permalink in self.this_crawl_permalinks:
                        log.debug(self.log_msg("Duplicate Review"))
                        continue
                    else:
                        try:
                            self.all_review_count = self.all_review_count + 1
                            log.debug(self.log_msg("processin review number %d" %(self.all_review_count)))
                            self.current_review = each_review
                            self.this_crawl_permalinks.append(post_permalink)
                            if self.fetch_next_review:
                                self._fetchReview(post_permalink,parent_uri)
                            else:
                                break
                        except:
                            log.exception(self.log_msg("Exception occured while fetching review from url"))
                            continue 
                if self.fetch_next_review and self.fetch_next_link:
                    next_link = self.soup.find('a', title='next', href=True)
                    if next_link:
                        self.currenturi=next_link['href']
                        return True
                    else:
                        log.info(self.log_msg("All reviews fetched"))
                        self.fetch_next_link = False
                        return False
                else:
                    log.info(self.log_msg("All reviews fetched"))
                    self.fetch_next_link = False
                    return False
            else:
                log.info(self.log_msg("Could not set the next page link"))
                self.fetch_next_link = False
                return False
        except:
            log.info(self.log_msg("Next page not found"))
            self.fetch_next_link = False
            return False
    @logit(log,'_fetchReview')
    def _fetchReview(self,review_url,parent_uri):
        """
        Fetches the data from a review page and appends them self.pages
        """
        try:
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    review_url, self.task.instance_data.get('update'),
                                    parent_list=[parent_uri]):
                page={}
                try:
                    page['uri']=normalize(self.currenturi)
                except:
                    page['uri']=''
                    log.info(self.log_msg("Error occured while fetching review url"))
                try:
                    page['title']=stripHtml(self.current_review.findAll(attrs={'class':re.compile('BVRRReviewTitleContainer')})[0].renderContents()) #Review Title
                except:
                    page['title']=''
                    log.info(self.log_msg("Error occured while fetching title"))

                try:
                    #page['et_author_name'] =  self.current_review.findAll(attrs={'class':re.compile('BVReviewerNickname BVReviewerNicknameText')})[0].renderContents().strip()
                    page['et_author_name'] =  stripHtml(self.current_review.findAll(attrs={'class':re.compile('BVRRNickname ')})[0].renderContents())
                except:
                    log.info(self.log_msg("Error occured while fetching author name"))
                try:
                    page['et_author_location'] = stripHtml(self.current_review.find('span', attrs = {'class':re.compile('BVRRValue BVRRUserLocation')}).renderContents())
                except:
                    log.info(self.log_msg("Error occured while fetching author location"))
                try:
                    page['ef_rating_overall'] = float(stripHtml(self.current_review.find(attrs={'class':'BVRRNumber BVRRRatingNumber rating'}).renderContents()))
                except:
                    log.info(self.log_msg("Error occured while fetching overall rating"))
                try:
                    #secondary_ratings = self.current_review.find(attrs={'class':'BVRRSecondaryRatingsContainer'}).findAll(attrs={'class':re.compile('BVRRRatingEntry')})
                    secondary_ratings = self.current_review.find(attrs={'class':re.compile('BodyContentSecondary$')}).findAll(attrs={'class':re.compile('BVRRRatingEntry')})
                    for each_rating in secondary_ratings:
                        try:
                            feature_name =  'ef_rating_%s' %(each_rating.find(attrs={'class':re.compile('BVRRLabel')}).renderContents().replace(' ','_').lower())
                            page[feature_name] = each_rating.find(attrs={'class':re.compile('BVRRRatingNumber')}).renderContents()
                        except:
                            log.info("Secondary rating element not found, continuing to the next element")
                            continue
                except:
                    log.info(self.log_msg("secondary ratings not found for this review"))
                try:   
                    page['et_data_pros'] = stripHtml(self.current_review.find('span', attrs = {'class':re.compile('BVRRValue BVRRReviewPros')}).renderContents())
                except:
                    log.info(self.log_msg("Error occured while fetching pros"))
                try:
                    page['et_data_cons'] = stripHtml(self.current_review.find('span', attrs = {'class':re.compile('BVRRValue BVRRReviewCons')}).renderContents())
                except:
                    log.info(self.log_msg("Error occured while fetching cons"))
                try:
                    page['data'] = stripHtml(self.current_review.find('div', 'BVRRReviewTextContainer').renderContents())
                except:
                    page['data']=''
                    log.info(self.log_msg("Error occured while fetching review data"))
                if not (page['data'] and page['title']):
                    log.info(self.log_msg('Empty title and data found'))
                    return True
                try:
                    page['ei_data_suggested'] = int(stripHtml(self.current_review.find('span', 'BVDIValue BVDINumber').renderContents()))
                except:
                    log.exception(self.log_msg("Error occured while fetching suggestion"))
                review_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                             page.values()))).encode('utf-8','ignore')).hexdigest()
                result=updateSessionInfo(self.genre, self.session_info_out, review_url, review_hash, 
                                         'Review', self.task.instance_data.get('update'), parent_list=[parent_uri])
                if result['updated']:
                    try:
                        page['posted_date'] = datetime.strftime(datetime.strptime(stripHtml(self.current_review.find('span', 'BVRRValue BVRRReviewDate dtreviewed').renderContents()),"%m/%d/%Y"),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ") 
                        log.info(self.log_msg("Error occured while fetching posted date"))
                    parent_list = [parent_uri]
                    page['parent_path'] = copy.copy(parent_list)
                    parent_list.append(review_url)
                    page['path'] = parent_list
                    page['task_log_id']=self.task.id
                    page['versioned']=self.task.instance_data.get('versioned',False)
                    page['category']=self.task.instance_data.get('category','generic')
                    page['last_updated_time']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['client_name']=self.task.client_name
                    page['entity']='review'
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id 
                    log.debug(self.log_msg("Adding %d new review" %self.new_review_count))
                    self.pages.append(page)
                    return True
                else:
                    if not  self.task.instance_data.get('update'):
                        self.fetch_next_review = False
                    log.debug(self.log_msg("Not adding review to pages"))
                    return False
            else:
                if not  self.task.instance_data.get('update'):
                    self.fetch_next_review = False
                log.debug(self.log_msg("Not adding review to pages"))
                return False
        except Exception,e:
            log.exception(self.log_msg("Exception occured in _fetchReview()"))
            raise e
