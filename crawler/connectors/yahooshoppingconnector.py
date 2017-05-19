
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

##MOHIT RANKA
#JV
#Ashish
import urllib
import urllib2
from urllib2 import *
from urlparse import urlparse
import re
import logging
from logging import config
from datetime import datetime
import md5
from BeautifulSoup import BeautifulSoup
import pickle
import copy
from baseconnector import BaseConnector

from htmlconnector import HTMLConnector
from tgimport import *
from knowledgemate import pysolr
from knowledgemate import model

from utils.sessioninfomanager import *
from utils.task import Task
from utils.utils import stripHtml
from utils.urlnorm import normalize
from utils.decorators import *


log = logging.getLogger('YahooShoppingConnector')

class YahooShoppingConnector(BaseConnector):  

    @logit(log,'_createSiteUrl')
    def _createSiteUrl(self,curr_url):
        """
        creates yahoo shopping url of the review page from the given relative url 
        """
        try:
            curr_url = curr_url.replace(' ','%20')
            log.debug(self.log_msg("Current site url: http://shopping.yahoo.com"+curr_url))
            return "http://shopping.yahoo.com"+curr_url
       
        except Exception , e:
            log.info(self.log_msg("Exception occured while creating site URL"))
            raise e


    @logit(log,'fetch')
    def fetch(self):
        """
        Fetched all the reviews for a given self.currenturi and returns Fetched staus depending 
        on the success and faliure of the task
        """
        try:
            self.genre="Review"
            self.currenturi = self._createSiteUrl(re.sub("http://shopping.yahoo.com","",re.sub("\?.+","",self.currenturi))+"?sort=1")
            parent_page_url = self.task.instance_data['uri']#copy.copy(self.currenturi)#JV
            self._getParentPage()
            log.debug(self.log_msg(self.currenturi))
            self.all_review_count = 0
            self.new_review_count = 0
            self.fetch_next_link = True
            self.this_crawl_permalinks = []#Just to take account of possible duplicates
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
            if not self.rawpage:
                res=self._getHTML(self.currenturi) # Assuming self.currenturi is at the product page
                self.rawpage=res['result']
            self._setCurrentPage()
            try:
                page['title'] = stripHtml(self.soup.find('span',{'property':re.compile('^.+?title')}).renderContents())
            except:
                page['title'] = ''
                log.info(self.log_msg("Exception in getting product page title"))

            page['data'] = ''
            try:
                page['et_product_price'] = stripHtml(self.soup.find('div',attrs={'class':'sm-price-div'}).renderContents())
            except:
                log.info(self.log_msg("Error occured while fetching product price from product page"))

            try:
                page['ef_product_rating_overall']=str(float(re.findall("\d\.\d",stripHtml(self.soup.find('div',attrs={'class':'summary'}).find('span',attrs={'property':'review:summary'}).renderContents()).strip())[0]))
            except:
                log.info(self.log_msg("Error occured while fetching overall rating from product page"))

            try:
                page['ei_product_rating_count']= int(re.findall("^\d+",stripHtml(self.soup.find('div',attrs={'class':'summary'}).find('span',{'class':'num-ratings'}).renderContents()).strip())[0])
            except:
                log.info(self.log_msg("Error occured while fetching number of ratings from product page"))

            log.debug(self.log_msg('got the content of the product main page'))
            log.debug(self.log_msg('checking session info'))
            try:
                post_hash=  md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                                                page.values()))).encode('utf-8','ignore')).hexdigest()

            except:
                log.debug("Error Occured while making parent post hash, Not fetching the parent page data")
                return False
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
                    page['path'] = page['parent_path'] = []
                    page['path'].append(self.task.instance_data['uri'])
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
            log.debug(self.log_msg("Fetching the next review page %s" %self.currenturi))
            page_reviews=[each for each in self.soup.find('div',attrs={'id':'shmoduserreviews'}).find('div',attrs={'class':'mod-content'}).findAll('li')]
            for each_review in page_reviews: # page_reviews = reviews in a page, typically 10,except the last page 
                try:
                    post_permalink=  re.sub("_ylt.*\?","",str(each_review.find('div',attrs={'class':'permalink'}).find('a').get('href')))
                except:
                    log.info(self.log_msg('Error occured while fetching permalink of the current review'))
                    continue # Skip this review and move on to the next review
                if post_permalink in self.this_crawl_permalinks:
                    log.debug(self.log_msg("Duplicate Review"))
                    continue #Continue to next url 
                else:
                    try:
                        self.all_review_count = self.all_review_count + 1
                        log.debug(self.log_msg("processin review number %d" %(self.all_review_count)))
                        self.current_review = each_review
                        self.this_crawl_permalinks.append(post_permalink)
                        self._fetchReview(post_permalink,parent_uri)
                    except:
                        log.exception(self.log_msg("Exception occured while fetching review from url"))
                        continue 
        except:
            log.exception(self.log_msg("Exception occured while iterating over reviews"))
            self.fetch_next_link = False
            return False
            
        next_link_tries = 0
        while next_link_tries < 3:
            try:
                next_link= self.soup.find('strong',attrs={'class':'next'}).find('a').get('href')
                next_link_tries = next_link_tries + 1
                log.debug(self.log_msg("next_link_tries=%s"%next_link_tries))
                if next_link:
                    self.currenturi="http://shopping.yahoo.com"+next_link
                    return True
                else:
                    self.fetch_next_link = False
                    log.debug(self.log_msg("Next page not found, fetched all reviews"))
                    return False
            except:
                self.fetch_next_link = False
                log.debug(self.log_msg("Next page not found, fetched all reviews"))
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
                    page['data']=stripHtml(self.current_review.find('div',attrs={'class':'review-details'}).find('p',attrs={'class':re.compile('posting.*')}).renderContents()) 
                except:
                    page['data']=''
                    log.exception(self.log_msg("Error occured while fetching review data"))
                try:
                    review_hash =  md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                                                page.values()))).encode('utf-8','ignore')).hexdigest()
                except:
                    log.debug(self.log_msg("Error in creating review hash, moving to next review"))
                    return False

                result=updateSessionInfo(self.genre, self.session_info_out, review_url, review_hash, 
                                             'Review', self.task.instance_data.get('update'), parent_list=[parent_uri])
                if result['updated']:
                    self.new_review_count = self.new_review_count + 1
                    try:
                        page['et_data_pros']=stripHtml(self.current_review.find('div',attrs={'class':'review-details'}).find('p',attrs={'class':'pros'}).renderContents()).replace('Pros:','') 
                    except:
                        log.exception(self.log_msg("Error occured while fetching review data pros"))
                    try:
                        page['et_data_cons']=stripHtml(self.current_review.find('div',attrs={'class':'review-details'}).find('p',attrs={'class':'cons'}).renderContents()).replace('Cons:','') 
                    except:
                        log.exception(self.log_msg("Error occured while fetching review data cons"))
                    try:
                        page['title']=stripHtml(self.current_review.find('div',attrs={'class':'review-details'}).h3.renderContents())
                    except:
                        page['title']=''
                        log.info(self.log_msg("Error occured while fetching url of current review page"))
                    try:
                        page['et_author_name']=self.current_review.find('div',attrs={'class':'review-details'}).find('p',attrs={'class':'author'}).find('a').renderContents()
                    except:
                        log.info(self.log_msg("Error occured while fetching review's author name"))
                    try:
                        page['et_author_profile']="http://shopping.yahoo.com"+self.current_review.find('div',attrs={'class':'review-details'}).find('p',attrs={'class':'author'}).find('a')['href']
                    except:
                        log.info(self.log_msg("Error occured while fetching author profile link"))           
                    try:
                        rating_div=self.current_review.find('div', {'class':'ratings'})
                        try:
                            for tr in rating_div.findAll('tr'):
                                try:
                                    page['ef_rating_'+stripHtml(tr.find('th').find('p').renderContents()).strip(':').lower().strip().replace(' ','_')]=str(float(re.findall(".*?shrating_(\d)",stripHtml(tr.find('td').find('div')['class']))[0]))
                                except:
                                    log.exception(self.log_msg("Error occured while fetching individual rating"))
                        except:
                            log.info(self.log_msg("Exception occured while fetching individual ratings"))
                    except:
                        log.info(self.log_msg("Exception occured while fetching ratings div"))
                    try:
                        helpful_tuple =  re.findall("(\d+?)\D+(\d+?)\D+$",stripHtml(self.current_review.find('div',attrs={'class':'review-details'}).find('p',attrs={'class':'author'}).renderContents()).split('|')[1])[0]

                        try:
                            page['ei_data_recommended_yes']= str(int(helpful_tuple[0]))
                        except:
                            log.info(self.log_msg("Error occured while fetching number of people who found this review helpful"))
                        try:
                            page['ei_data_recommended_total']=  str(int(helpful_tuple[1]))
                        except:
                            log.info(self.log_msg("Error occured while fetching number of people who found this review helpful"))
                    except:
                        log.debug(self.log_msg("Exception occured while fetching helpfulness of the review"))
                    try:
                        page['posted_date']=datetime.strftime(datetime.strptime(re.sub(".*\\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\b",r"\1",stripHtml(self.current_review.find('div',attrs={'class':'review-details'}).find('p',attrs={'class':'author'}).renderContents()).split('|')[0]).strip(),"%b %d, %Y"),"%Y-%m-%dT%H:%M:%SZ")

                    except:
                        page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ") 
                        log.exception(self.log_msg("Error occured while fetching review's post date"))
                    
                    page['path'] = page['parent_path'] = [parent_uri]
                    page['path'].append(review_url)
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
                    return True
                else:
                    log.debug(self.log_msg("Not adding review to pages"))
                    return False
            else:
                log.debug(self.log_msg("Not adding review to pages"))
                return False
        except Exception,e:
            log.exception(self.log_msg("Exception occured in _fetchReview()"))
            return False
