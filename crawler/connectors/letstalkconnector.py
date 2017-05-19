
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#MOHIT RANKA
#ASHISH YADAV

import re
import md5
import logging

from datetime import datetime
from urlparse import urlparse

from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from utils.utils import stripHtml
from utils.urlnorm import normalize
from utils.decorators import logit

log = logging.getLogger('LetsTalkConnector')

#Note - For letstalk connector reviews, there is no unique identifier, hence hash of the review data is being used as the identifier.

class LetsTalkConnector(BaseConnector):  
    base_url = "http://wsf0.letstalk.com"
    
    @logit(log,'fetch')
    def fetch(self):
        """
        Fetched all the reviews for a given self.currenturi and returns Fetched staus depending 
        on the success and faliure of the task
        """
        try:
            self.genre = 'Review'
            log.debug(self.log_msg("Fetching the prouct page url %s"%self.currenturi))
            res=self._getHTML(self.currenturi) # Assuming self.currenturi is at the product page
            self.rawpage=res['result']
            self._setCurrentPage()
            try:
                self.parent_page_title = stripHtml(self.soup.find('h1',{'id':'pgTitleDetail'}).renderContents())
            except:
                self.parent_page_title =''
            try:
                self.__product_price = self.soup.find('tbody',{'class':'prices'}).td.renderContents().replace('$','')
            except:
                log.exception("Error in fetching product_price")
                self.__product_price = None

            parent_page_url = self.task.instance_data['uri']
            review_first_page_url = self.soup.find('a',text="Show All Customer Reviews &#187; ").parent['href']
            review_url_order = "&sortReviewsBy=DateDescending"
            self.currenturi = self.base_url + review_first_page_url + review_url_order
            log.info(self.log_msg('current_uri :: %s'%(self.currenturi)))
            self._getParentPage()
            self.next_url_links=[]
            self.fetch_next_link = True
            while self.fetch_next_link:
                self._iterateReviewPages(parent_page_url)
            return True
        except Exception,e:
            log.exception(self.log_msg("Exception occured in fetch()"))
            return False
        
    @logit(log,'_getParentPage')
    def _getParentPage(self):
        """
        Gets the data for the parent page and appends it to self.pages if the parent page has changed since the last crawl. 
        """
        try:
            page={}
            res=self._getHTML(self.currenturi) # Assuming self.currenturi is at the product page
            self.rawpage=res['result']
            self._setCurrentPage()
            try:
                page['title']=  self.parent_page_title
            except:
                page['title']=''
                log.exception(self.log_msg("Error occured while fetching product page title"))

            page['data']=''
            try:
                page['ef_product_rating_overall']= float(stripHtml(self.soup.find(text='Overall Rating').parent.parent.parent.
                                                                   parent.renderContents()).splitlines()[-1].strip())
            except:
                log.info(self.log_msg("Error occured while fetching product page overall rating"))

            try:
                page['ei_product_review_count']= int(self.soup.find(text=re.compile('Total Reviews:')).split(':')[-1].strip())
            except:
                log.info(self.log_msg("Error occured while fetching number of reviews for the product"))

            try:
                if self.__product_price:
                    page['et_product_price']= self.__product_price
                    self.updateProductPrice(page.get('et_product_price'))
            except:
                log.info(self.log_msg("Error occured while fetching product price"))
            
            log.debug(self.log_msg('got the content of the product main page'))
            log.debug(self.log_msg('checking session info'))
            try:
                post_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                           page.values()))).encode('utf-8','ignore')).hexdigest()
            except:
                log.exception(self.log_msg("Exception occured while creating post hash for the page %s, returning" %self.currenturi))
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
                    page['parent_path'] = []
                    page['path'] = [self.task.instance_data['uri']]
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
            return False #Not raised deliberately
                                                                           
    @logit(log,'_iterateReviewPages')
    def _iterateReviewPages(self,parent_uri):
        """
        """
        try:
            log.debug(self.log_msg("tring to fetch the Webpage %s" %self.currenturi))
            res=self._getHTML(self.currenturi)
            if res:
                self.rawpage=res['result']
                self._setCurrentPage()
                try:
                    log.debug(self.log_msg("Fetching the next review page %s" %self.currenturi))
                    page_reviews =   self.soup.findAll('div',{'class':'P10'})
                    for each_review in page_reviews:
                        try:
                            self.current_review = each_review.parent.parent
                            self._getReview(parent_uri)
                        except:
                            log.exception(self.log_msg("Exception occured while fetching review from url %s" %self.currenturi))
                            continue 
                except:
                    log.debug(self.log_msg("Next page not found"))
                    self.fetch_next_link = False
                    return False

                try:
                    next_link=self.base_url + [each.get('href') for each in self.soup.findAll('a') if re.match("next",each.renderContents())][0]
                    if next_link not in self.next_url_links: 
                        self.next_url_links.append(next_link)
                        self.currenturi=next_link
                        return True
                    else:
                        log.critical(self.log_msg("PreExisting next_link"))
                        self.fetch_next_link = False
                        return False
                except:
                    log.debug(self.log_msg("Next page not found, fetched all reviews"))
                    self.fetch_next_link = False
                    return False
            else:
                log.debug(self.log_msg("Could not get the HTML Page for the next link %s" %self.currenturi))
                self.fetch_next_link = False
                return False 
        except:
            log.exception(self.log_msg("Exception occured in _iterateReviewPages()"))
            self.fetch_next_link = False
            raise e

    @logit(log,'_getReview')
    def _getReview(self,parent_uri):
        """
        Fetches the data from a review page and appends them self.pages
        """
        try:
            page={}
            review_data = self.current_review.find('div',attrs={'class':'P10'}).findAll('div',attrs={'class':'Std'})
            try:
                page['data']=stripHtml(review_data[1].renderContents())
            except:
                page['data'] = ''
                log.info(self.log_msg("Error occured while fetching review data"))
            
            try:
                review_identity_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                             page.values()))).encode('utf-8','ignore')).hexdigest()
            except:
                log.info(self.log_msg("Error occured while creating the review_identity_hash"))
                return False

            if not checkSessionInfo(self.genre,
                                    self.session_info_out, 
                                    review_identity_hash,
                                    self.task.instance_data.get('update'),
                                    parent_list=[parent_uri]):
                page['title']=''
                try:
                    page['et_author_name'] = stripHtml(re.findall("^by\s(\w+)",stripHtml(review_data[0].renderContents()))[0])
                except:
                    log.info(self.log_msg("Error occured while fetching author name"))

                try:
                    page['et_author_location']= re.match("(.*?--)(.*?--)(.*)$",stripHtml(review_data[0].renderContents())).group(3).strip()

                except:
                    log.info(self.log_msg("Error occured while fetching author location"))   
                try:
                    page['ef_rating_overall'] = float(stripHtml(self.current_review.find('td',attrs={'class':'StdClr2','width':'10%'}).renderContents()))
                except:
                    log.info(self.log_msg("Error occured while fetching overall rating"))
                try:
                    secondary_ratings =  [each for each in self.current_review.find('td',attrs={'class':'rateGFX'}).parent.parent.findAll('tr')
                                          if each.find('td',attrs={'class':'Std'})]

                    for each_rating in secondary_ratings:
                        try:
                            rating_list = each_rating.findAll('td',attrs={'class':'Std'})
                            feature_name =  stripHtml(rating_list[0].renderContents()).lower().replace(' ','_')
                            page['ef_rating_%s'%feature_name] = float(stripHtml(rating_list[1].renderContents()))
                        except:
                            log.info("Secondary rating element not found, continuing to the next element")
                            continue
                except:
                    log.info(self.log_msg("secondary ratings not found for this review"))

                try:
                    review_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                                          page.values()))).encode('utf-8','ignore')).hexdigest()
                except:
                    log.info(self.log_msg("Error occured while creating the review_hash"))
                    return False
                result=updateSessionInfo(self.genre, self.session_info_out, review_identity_hash, review_hash, 
                                         'Review', self.task.instance_data.get('update'), parent_list=[parent_uri])
                if result['updated']:
                    try:
                        review_meta_info = stripHtml(self.current_review.find('div',attrs={'class':'P10'}).findAll('div',attrs={'class':'Std'})[0]\
                                                           .renderContents())
                        posted_date_str = re.findall("--([\w\s]+)--",review_meta_info)[0]
                        page['posted_date']=datetime.strftime(datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",posted_date_str).strip(),"%B %d %Y"),\
                                                                  "%Y-%m-%dT%H:%M:%SZ")
                    except:
                        page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ") 
                        log.info(self.log_msg("Error occured while fetching posted date"))

                    page['uri']=normalize(self.currenturi)
                    page['parent_path'] = [parent_uri]
                    page['path'] = [parent_uri,review_identity_hash]
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
                    log.debug(self.log_msg("Adding review for the url %s" %self.currenturi))
                    self.pages.append(page)
                    return True
                else:
                    log.debug(self.log_msg("Not adding review to pages"))
                    return False
            else:
                log.debug(self.log_msg("Not adding review to pages"))
                return False
        except Exception,e:
            log.exception(self.log_msg("Exception occured in _getReview()"))
            raise e
