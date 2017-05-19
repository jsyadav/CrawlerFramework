
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

##MOHIT RANKA
#JV
#Ashish
#fixed by prerna


import urllib2
from urllib2 import *
from urlparse import urlparse
import re
import base64
import logging
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import pickle
import copy
from baseconnector import BaseConnector

from tgimport import *

from utils.sessioninfomanager import *
from utils.utils import stripHtml, get_hash
from utils.urlnorm import normalize
from utils.decorators import *


log = logging.getLogger('BuzzilionsConnector')

class BuzzillionsConnector(BaseConnector):  

    base_url = "http://www.buzzillions.com"

    @logit(log,'fetch')
    def fetch(self):
        """
        Fetched all the reviews for a given self.currenturi and returns Fetched staus depending 
        on the success and faliure of the task
        """
        try:
            self.genre="Review"
            self.currenturi = self.currenturi + "?prRpp=100&reviewSortBy=newestFirst"
            parent_page_url = self.task.instance_data['uri']
            self._getParentPage()
            self.all_review_count = 0
            self.new_review_count = 0
            self.prefetched_next_links = []
            self.fetch_next_link = True
            self.fetch_next_review = True
            self.this_crawl_permaids = []
            while self.fetch_next_link:
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
            self.__headers ={'Referer':self.task.instance_data['uri']}
            if not self.rawpage:
                res=self._getHTML(headers=self.__headers)
                self.rawpage=res['result']
            self._setCurrentPage()
            try:
                page['title'] = stripHtml(self.soup.find(attrs={'id':re.compile('bz-model-name-\d+?$')}).renderContents())
            except:
                page['title'] = ''
                log.info(self.log_msg("Exception in getting product page title"))
            page['data'] = ''
            try:
                page['et_product_price'] = stripHtml(self.soup.find('p',attrs={'class':re.compile('bz-model-priceRange.*')}).renderContents().strip())
            except:
                log.exception(self.log_msg("Error occured while fetching product price from product page"))
            try:
                #page['ef_product_rating_overall']=float(stripHtml(self.soup.find(attrs={'class':'bz-model-prodRating'}).find(attrs={'class':'rating'}).renderContents()))
                page['ef_product_rating_overall']=float(stripHtml(self.soup.find(attrs={'class':'bz-model-prodRating'}).find('span').renderContents()))
            except:
                log.info(self.log_msg("Error occured while fetching overall rating from product page"))

            try:
                page['et_product_pros'] = ', '.join([stripHtml(re.sub("\(\d+?\)","",each.renderContents())) for each in self.soup.find(attrs={'id':'bz-label-pros'}).findNext('div').findAll('li')])
            except:
                log.info(self.log_msg("Product pros not found"))

            try:
                page['et_product_cons'] = ', '.join([stripHtml(re.sub("\(\d+?\)","",each.renderContents())) for each in self.soup.find(attrs={'id':'bz-label-cons'}).findNext('div').findAll('li')])
            except:
                log.info(self.log_msg("Product cons not found"))

            try:
                page['et_product_best_use'] = ', '.join([stripHtml(re.sub("\(\d+?\)","",each.renderContents())) for each in self.soup.find(attrs={'id':'bz-label-bestuses'}).findNext('div').findAll('li')])
            except:
                log.info(self.log_msg("Product best use not found"))
            try:
                post_hash=  get_hash(page)
                
            except:
                log.debug("Error Occured while making parent post hash, Not fetching the parent page data")
                return False
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    self.task.instance_data['uri'], self.task.instance_data.get('update')):
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
                    self.pages.append(page)
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

    @logit(log,'_iterateReviewPages')
    def _iterateReviewPages(self,parent_uri):
        """
        Iterates through the review list pages (typically a review list page contains 10 reviews)
        """
        try:
            log.debug(self.log_msg("Fetching the next review page %s" %self.currenturi))
            for each_review in self.soup.findAll(attrs={'class':re.compile('h[Rr]eview bz\-model\-review')}):
                try:
                    review_permaid=  re.findall('\d+?$',each_review['id'])[0]
                except:
                    log.info(self.log_msg('Error occured while fetching permaid of the current review'))
                    continue # Skip this review and move on to the next review
                if review_permaid in self.this_crawl_permaids or review_permaid==None: 
                    #There is a reason why, not review_permaid (which is pythonic unlike review_permaid==None), is not used, Guess!
                    log.debug(self.log_msg("Duplicate Review or empty review_id"))
                    continue #Continue to next url 
                else:
                    try:
                        self.all_review_count = self.all_review_count + 1
                        log.debug(self.log_msg("processin review number %d" %(self.all_review_count)))
                        self.current_review = each_review
                        self.this_crawl_permaids.append(review_permaid)
                        if self.fetch_next_review:
                            self._fetchReview(review_permaid,parent_uri)
                        else:
                            self.fetch_next_link = False
                            break
                    except:
                        log.exception(self.log_msg("Exception occured while fetching review from url"))
                        continue 
            if self.fetch_next_link:
                next_link= base64.decodestring(self.soup.find('a',attrs={'class':'bz-pagination bz-next bz-base64-encoded'}).get('href'))
                if next_link:
                    self.currenturi=self.base_url+next_link
                    if self.currenturi not in self.prefetched_next_links:
                        self.prefetched_next_links.append(self.currenturi)
                        res=self._getHTML(self.currenturi)
                        if res:
                            self.rawpage=res['result']
                            self._setCurrentPage()
                        else:
                            log.info(self.log_msg("Could not set next link"))
                            self.fetch_next_link = False
                    else:
                        log.info(self.log_msg("Duplicated next link"))
                        self.fetch_next_link = False
            return True
        except:
            self.fetch_next_link = False
            log.exception(self.log_msg("Next page not found, fetched all reviews"))
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
                    page['data'] = ''
                    data_str = stripHtml(self.current_review.find(attrs={'class':re.compile('bz-model-review-comments description')}).renderContents())
                    if not data_str.endswith('Read Full Review') or (not data_str.endswith('Read Full Review')):
                        page['data'] = data_str
                    else:
                        parent_soup = copy.copy(self.soup)
                        parent_url = self.currenturi
                        try:
                            self.currenturi = self.base_url + base64.decodestring(self.current_review.find('a',text=re.compile('Read Full Review &raquo;')).parent['href'])
                            res=self._getHTML(self.currenturi)
                            self.rawpage=res['result']
                            self._setCurrentPage()
                            page['data'] = stripHtml(self.soup.find(attrs={'class':'bz-model-review-comments description'}).renderContents())
                        except:
                            log.exception(self.log_msg('data Cannot be added'))
                        self.soup =copy.copy(parent_soup)
                        self.currenturi = parent_url
                except:
                    log.exception(self.log_msg("Error occured while fetching review data"))
                try:
                    page['title'] = stripHtml(self.current_review.find('h3','summary bz-subheading').renderContents())
                except:
                    page['title']=''
                    log.info(self.log_msg("Error occured while fetching title of current review page"))
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
                    #page['ei_data_recommended_yes']= str(int(re.findall('^\d+',stripHtml(self.current_review.find(attrs={'class':'prReviewHelpfulCount'}).renderContents().strip()))[0]))
                    page['ei_data_recommended_yes'] = int(re.search('\d+',stripHtml(self.current_review.find('a','bz-review-vote-up').renderContents())).group())
                    log.info(page['ei_data_recommended_yes'])
                except:
                    log.info(self.log_msg("Error fetching Number of people who recommended this review"))
                try:
                    #page['ei_data_recommended_total']= str(int(re.findall('\d+$',stripHtml(self.current_review.find(attrs={'class':'prReviewHelpfulCount'}).renderContents().strip()))[0]))
                    page['ei_data_recommended_no'] = int(re.search('\d+',stripHtml(self.current_review.find('a','bz-review-vote-down').renderContents())).group())
                except:
                    log.info(self.log_msg("Error fetching total of people rated this review for helpfulness"))
                try:
                    page['et_data_pros']=', '.join([stripHtml(re.sub("\(\d+?\)","",each.renderContents())) for each in self.current_review.findAll(attrs={'class':'bz-tagGroup'})[0].findAll('li')])
                except:
                    log.info(self.log_msg("Error occured while fetch pros from the review"))
                    
                try:
                    page['et_data_cons']=', '.join([stripHtml(re.sub("\(\d+?\)","",each.renderContents())) for each in self.current_review.findAll(attrs={'class':'bz-tagGroup'})[1].findAll('li')])
                except:
                    log.info(self.log_msg("Error occured while fetch cons from the review"))

                try:
                    page['et_data_best_use']=', '.join([stripHtml(re.sub("\(\d+?\)","",each.renderContents())) for each in self.current_review.findAll(attrs={'class':'bz-tagGroup'})[2].findAll('li')])
                except:
                    log.info(self.log_msg("Error occured while fetch best use from the review"))
                    
                try:
                    page['et_author_name']=stripHtml(self.current_review.find(attrs={'class':'bz-model-review-name fn nickname'}).renderContents())
                except:
                    log.info(self.log_msg("Error occured while fetching review's author name"))
                try:
                    page['et_author_location']=stripHtml(self.current_review.find(attrs={'class':'bz-model-review-location locality'}).renderContents())
                except:
                    log.info(self.log_msg("Error occured while fetching review's author location"))
                try:
                    #page['ef_rating_overall']=  str(float(stripHtml(self.current_review.find(attrs={'class':'bz-rating'}).find(attrs={'class':'rating'}).renderContents())))
                    page['ef_rating_overall']=  stripHtml(self.current_review.find('div','bz-rating').find('span').renderContents())
                except:
                    log.info(self.log_msg("Error occured while fetching overall rating"))
                try:
                    page['et_data_reviewed_at'] = stripHtml(self.current_review.find('div', 'bz-model-review-wtb').img['alt'].__str__())
                except:
                    log.info(self.log_msg('Data reviewed at is not found'))
                try:
                    review_hash =  get_hash(page)
                except:
                    log.debug(self.log_msg("Error in creating review hash, moving to next review"))
                    return False
                result=updateSessionInfo(self.genre, self.session_info_out, review_url, review_hash, 
                                             'Review', self.task.instance_data.get('update'), parent_list=[parent_uri])
                if result['updated']:
                    self.new_review_count = self.new_review_count + 1
                    try:
                        review_post_date_soup =  self.current_review.find(attrs={'class':'bz-model-review-date dtreviewed'})
                        review_post_month = stripHtml(review_post_date_soup.find(attrs={'class':'month'}).renderContents())
                        review_post_day = stripHtml(review_post_date_soup.find(attrs={'class':'day'}).renderContents())
                        review_post_year = stripHtml(review_post_date_soup.find(attrs={'class':'year'}).renderContents())
                        page['posted_date']=datetime.strftime(datetime.strptime(review_post_month+','+review_post_day+','+review_post_year,"%b,%d,%Y"),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ") 
                        log.exception(self.log_msg("Error occured while fetching review's post date"))
                    parent_list = [parent_uri]
                    page['parent_path'] = copy.copy(parent_list)
                    parent_list.append(review_url)
                    page['path'] = parent_list
                    page['uri']=normalize(self.currenturi)
                    page['task_log_id']=self.task.id
                    page['versioned']=self.task.instance_data.get('versioned',False)
                    page['category']=self.task.instance_data.get('category','generic')
                    page['last_updated_time']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")  #Now
                    page['client_name']=self.task.client_name
                    page['entity']='review'
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project
                    log.debug(self.log_msg("Adding %d new review" %self.new_review_count))
                    self.pages.append(page)
                    log.info(self.log_msg('Review added'))
                    return True
                else:
                    log.debug(self.log_msg("Not adding review to pages"))
                    if not self.task.instance_data.get('update'):
                        self.fetch_next_review = False
                    return False
            else:
                log.debug(self.log_msg("Not adding review to pages"))
                if not self.task.instance_data.get('update'):
                    self.fetch_next_review = False
                return False
        except Exception,e:
            log.exception(self.log_msg("Exception occured in _fetchReview()"))
            return False
        
