
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#MOHIT RANKA
from urllib2 import *
from urlparse import urlparse
import re
import logging
from datetime import datetime
import md5
from BeautifulSoup import BeautifulSoup
import copy
from baseconnector import BaseConnector

from utils.sessioninfomanager import *
from utils.utils import stripHtml
from utils.urlnorm import normalize
from utils.decorators import *

log = logging.getLogger('IbiboOpinionsConnector')

class IbiboOpinionsConnector(BaseConnector):
    
    @logit(log,'fetch')
    def fetch(self):
        """
        Fetched all the reviews for a given self.currenturi and returns Fetched staus depending 
        on the success and faliure of the task
        """
        try:
            self.genre = 'Review'
            log.debug(self.log_msg(self.currenturi))
            self._getParentPage()
            parent_page_url = self.currenturi
            self.url_list = []
            self._getUrlList()
            log.debug(self.log_msg("No of review urls "+str(len(self.url_list))))
            self.new_review_count = 0
            self.all_review_count = 0
            for each_url in self.url_list:
                try:
                    self.all_review_count = self.all_review_count + 1
                    log.debug(self.log_msg("processing review number %d" %(self.all_review_count)))
                    self._fetchReview(parent_page_url,each_url)
                except:
                    log.exception(self.log_msg("Exception occured while fetching review from url"))
                    continue # To the next url
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
                page['title'] = stripHtml(self.soup.find('div',attrs={'id':'singleContentContaier'}).find('span',attrs={'id':re.compile('^.*ProdTitle$')}).renderContents())
            except:
                page['title'] = ''
                log.info(self.log_msg("Exception in getting product page title"))
            page['data']=''
            
            try:
                page['ef_product_rating_overall'] = str(float(stripHtml(self.soup.find('div',attrs={'id':'singleContentContaier'}).find('img')['alt'])))

            except:
                log.info(self.log_msg("Exception in getting overall rating from product page"))
               
            try:
                for each_rating in self.soup.find('div',attrs={'class':'prodRatings'}).findAll('td'):
                    try:
                        rating_name = stripHtml(each_rating.find('span',attrs={'id':re.compile('^.*lblName')}).renderContents().strip().replace(' ','_').lower())
                        rating_value =  stripHtml(re.findall("http://opinions.ibibo.com/images/rating/smlRate(\d)\.gif",each_rating.find('img')['src'])[0])
                        page['ef_product_rating_%s' %rating_name]=str(float(rating_value))
                    except:
                        log.info(self.log_msg("Some error occured while getting the individual rating"))
            except:
                log.info(self.log_msg("Exception occured while fetching individual ratings"))
           
            log.debug(self.log_msg('got the content of the product main page'))
            try:
                post_hash=  md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                                                page.values()))).encode('utf-8','ignore')).hexdigest()

            except:
                log.exception(self.log_msg("Exception occured while creating parent page hash for url %s" %self.currenturi))
                return False

            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    self.currenturi, self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug('got the connector instance first time, sending updatesessioninfo the id : %s' % str(id))
                result=updateSessionInfo(self.genre, self.session_info_out, self.currenturi, post_hash, 
                                             'Post', self.task.instance_data.get('update'), Id=id)

                if result['updated']:
                    page['path']=[self.currenturi]
                    page['parent_path']=[]
                    page['uri'] = normalize(self.currenturi)
                    page['uri_domain'] = unicode(urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project       
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['task_log_id']=self.task.id
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')
                    self.pages.append(page)
                    log.debug(self.log_msg("Parent page %s info added to self.pages" %(self.currenturi)))
                else:
                    log.debug(self.log_msg("Parent page %s info NOT added to self.pages" %(self.currenturi)))
            else:
                log.debug(self.log_msg("Parent page %s info NOT added to self.pages" %(self.currenturi)))
            return True
        except:
            log.exception(self.log_msg("Exception occured in _getParentPage()"))
            return False

    @logit(log,'_getUrlList')
    def _getUrlList(self):
        """
        Iterates through all the review pages by next button and populates self.url_list
        """
        # Assumption - self.soup exists (and set to the First page of the review)
        try:
            while True:
                log.debug(self.log_msg(self.currenturi))
                log.debug(self.log_msg("Before Extending "+str(len(self.url_list))))
                for each in self.soup.find('div',attrs={'id':'leftContentContainer'}).findAll('a',attrs={'id':re.compile("^.*ReadMore$")}):    
                    try:
                        permalink_url = stripHtml(each.get('href'))
                        if permalink_url in self.url_list: # Duplicate post
                            log.debug(self.log_msg("Duplicate url found, continuing to get other review url"))
                            continue
                        else:
                            self.url_list.append(permalink_url)
                    except:
                        log.exception(self.log_msg("Exception while fetching permalink/titleurl, not appending the review"))
                log.debug(self.log_msg("After Extending "+str(len(self.url_list))))
                try:
                    try:
                        next_link =  stripHtml([each.get('href') for each in self.soup.find('div',attrs={'id':'leftContentContainer'}).findAll('a') if each and each.renderContents()=="Next" ][0]) 
                        log.debug(self.log_msg("Next Link is: "+next_link))
                    except:
                        log.info(self.log_msg("Next link not found"))
                        break
                    if next_link:
                        self.currenturi = next_link
                        res=self._getHTML(self.currenturi)
                        self.rawpage=res['result']
                        self._setCurrentPage()
                    else:
                        log.debug(self.log_msg("All Urls are captured, Exiting the While loop"))
                        break
                except:
                    log.exception(self.log_msg("Exception occured while fetching next link from the page"))
                    break
            return True
        except:
            log.exception(self.log_msg("Exception occured in _getUrlList()"))
            return False

    @logit(log,'_fetchReview')
    def _fetchReview(self,parent_uri,review_url):
        """
        Fetches the data from a review page and appends them self.pages
        """
        try:
            self.currenturi = review_url+"?flag=read#read" #So that we avoid an extra fetch will would be required if we dont append the flag
            res=self._getHTML(self.currenturi)
            self.rawpage=res['result']
            self._setCurrentPage()
            review_page_url = self.currenturi
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    review_url, self.task.instance_data.get('update'),
                                    parent_list=[parent_uri]):
                page={}
                try:
                    page['et_author_name'] =  stripHtml(self.soup.find('div',attrs={'class':'userCredit'}).find('a').renderContents())
                except:
                    log.info(self.log_msg("Exception occured while fetching author name from the review"))
                try:
                    page['title'] = stripHtml(self.soup.find('span',attrs={'id':re.compile('^.*ReviewTitle$')}).renderContents().strip())
                except:
                    page['title'] = ''
                    log.info(self.log_msg("Exception occured while fetching review title"))

                try:
                    page['et_author_profile'] = stripHtml(self.soup.find('div',attrs={'class':'userCredit'}).find('a')['href'])
                except:
                    log.info(self.log_msg("Exception occured while fetching author's profile link"))

                try:
                    page['ef_rating_overall'] = str(float(re.findall("http://opinions.ibibo.com/images/rating/smlRate(\d)\.gif",stripHtml(self.soup.find('div',attrs={'class':'prodRatings'}).find('td').find('img')['src']))[0]))

                except:
                    log.info(self.log_msg("Exception occured while fetching overall rating"))

                try:
                    page['ei_data_recommended_yes']= str(int(stripHtml(self.soup.find('a',attrs={'id':re.compile('^.*thumbsUpBtn$')}).renderContents().strip())))
                except:
                    log.info(self.log_msg("Exception occured while fetching number of people who found this review helpful"))

                try:
                    page['ei_data_recommended_no']= str(int(stripHtml(self.soup.find('a',attrs={'id':re.compile('^.*thumbsDownBtn$')}).renderContents().strip())))
                except:
                    log.info(self.log_msg("Exception occured while fetching total number of people who found this review not helpful"))

                try:
                    page['et_data_pros'] = stripHtml([each.findNext('span').renderContents() for each in self.soup.findAll('span') if each.renderContents()=="Positives"][0])
                except:
                    log.info(self.log_msg("Exception occured while fetching pros from the review"))

                try:
                    page['et_data_cons'] = stripHtml([each.findNext('span').renderContents() for each in self.soup.findAll('span') if each.renderContents()=="Negatives"][0])

                except:
                    log.info(self.log_msg("Exception occured while fetching cons from the review"))

                try:
                    page['et_data_suggested'] = stripHtml([each.findNext('span').renderContents() for each in self.soup.findAll('span') if each.renderContents()=="Worth the price?"][0])

                except:
                    log.info(self.log_msg("Exception occured while fetching recommedation from the review"))

                try:
                    page['data']= stripHtml(self.soup.find('div',attrs={'id':'leftContentContainer'}).find('span',attrs={'id':re.compile("^.*lblVal1$")}).renderContents())
                except:
                    page['data']=''
                    log.info(self.log_msg("Exception occured while fetching review data"))

                try:
                    for each_rating in self.soup.find('div',attrs={'class':'prodRatings'}).findAll('td')[1:]: #As overall rating has been picked before
                        try:
                            rating_name = stripHtml(each_rating.find('span',attrs={'id':re.compile('^.*lblName')}).renderContents().strip().replace(' ','_').lower())
                            rating_value =  stripHtml(re.findall("http://opinions.ibibo.com/images/rating/smlRate(\d)\.gif",each_rating.find('img')['src'])[0])
                            page['ef_rating_%s' %rating_name]=str(float(rating_value))
                        except:
                            log.info(self.log_msg("Some error occured while getting the individual rating"))
                except:
                    log.info(self.log_msg("Exception occured while fetching individual ratings"))
                try:
                    review_hash =  md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                                                page.values()))).encode('utf-8','ignore')).hexdigest()

                except:
                    log.debug(self.log_msg("Error occured while making hash of  %s review" %review_url))
                    return False
                review_soup = copy.copy(self.soup)
                result=updateSessionInfo(self.genre, self.session_info_out, self.currenturi, review_hash, 
                                         'Review', self.task.instance_data.get('update'), parent_list=[parent_uri])
                if result['updated']:
                    try:
                        page['posted_date'] = datetime.strftime(datetime.strptime(stripHtml(re.findall("\d{1,2}:\d{2}\s(AM|PM)\son\s(.*)$",self.soup.find('span',attrs={'id':re.compile('^.*CreationDate$')}).renderContents())[0][1]),"%B %d %Y"),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        log.info(self.log_msg("Exception occured while fetching post date from review"))
                    
                    parent_list = [parent_uri]
                    page['parent_path']=copy.copy(parent_list)
                    parent_list.append(self.currenturi)
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
                    page['uri']=normalize(self.currenturi)
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    self.current_page = page
                    if page.get('et_author_profile') and self.task.instance_data.get('pick_user_info'):
                        self._getUserInfo(page['et_author_profile'])
                    self.new_review_count = self.new_review_count + 1
                    self.pages.append(self.current_page)
                    log.debug(self.log_msg("Review %s added to self.pages" %(review_url)))
                else:
                    log.debug(self.log_msg("Review %s NOT added to self.pages" %(review_url)))
            else:
                 log.debug(self.log_msg("Review %s NOT added to self.pages" %(review_url)))
           
            if self.task.instance_data.get('pick_comments'):
                self.soup = review_soup
                self.comment_list=[]
                self.new_comment_count = 0
                self.all_comment_count = 0
                self.comment_list = self.soup.find('div',attrs={'id':re.compile('^.*ReadComment$')}).findAll('div',attrs={'class':'flLt'})
                for each_comment in self.comment_list:
                    self.current_comment = each_comment
                    self._getReviewComment(review_url,[parent_uri,review_page_url])
        except Exception,e:
            log.exception(self.log_msg("Exception occured in _fetchReview()"))
            raise e
        
    @logit(log,'_getReviewComments')
    def _getReviewComment(self,review_identity,parent_list):
        """
        Gets new/updated comments from a particular review and appends to self.pages
        """
        try:
            page={}
            try:
                page['et_author_name']=stripHtml(self.current_comment.find('a',attrs={'id':re.compile('^.*UserProfile$')}).renderContents())
            except:
                log.info(self.log_msg("Could not fetch comment author name"))
            try:
                page['data']= stripHtml(self.current_comment.find('span',attrs={'id':re.compile('^.*CommentDescription$')}).renderContents())
                page['title']=str(page['data'])[:50]
            except:
                page['data']=''
                page['title']=''
                log.info(self.log_msg("Review data not found"))
            try:
                comment_hash =  md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                                                page.values()))).encode('utf-8','ignore')).hexdigest()

            except:
                log.exception(self.log_msg("Exception occured while creating hash of the comment of the review:%s" %review_identity))
            if not checkSessionInfo(self.genre, self.session_info_out,
                                    comment_hash, self.task.instance_data.get('update'),
                                    parent_list=parent_list):
                result=updateSessionInfo(self.genre, self.session_info_out, comment_hash, comment_hash,
                                         'Comment', self.task.instance_data.get('update'),
                                         parent_list=parent_list)
                if result['updated']:
                     try:
                         page['posted_date'] = datetime.strftime(datetime.strptime( re.findall("\d{1,2}:\d{2}\s(AM|PM)\son\s(.*)$",self.current_comment.find('span',attrs={'id':re.compile('^.*PostedDate$')}).renderContents())[0][1],"%B %d %Y"),"%Y-%m-%dT%H:%M:%SZ")
                     except:
                         page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                         log.info(self.log_msg("Exception occured while fetching post date from review"))
                     page['parent_path']=copy.copy(parent_list)
                     parent_list.append(comment_hash)
                     page['path']=parent_list
                     page['versioned']=self.task.instance_data.get('versioned',False)
                     page['category']=self.task.instance_data.get('category','generic')
                     page['client_name']=self.task.client_name
                     page['last_updated_time']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ") 
                     page['task_log_id']=self.task.id
                     page['entity']='comment'
                     page['uri'] = normalize(self.currenturi)
                     page['uri_domain'] = urlparse(page['uri'])[1]
                     page['priority']=self.task.priority
                     page['level']=self.task.level
                     page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                     page['connector_instance_log_id'] = self.task.connector_instance_log_id
                     page['connector_instance_id'] = self.task.connector_instance_id
                     page['workspace_id'] = self.task.workspace_id
                     page['client_id'] = self.task.client_id 
                     self.current_page = page
                     if page.get('et_author_profile') and self.task.instance_data.get('pick_user_info'):
                         self._getUserInfo(page['et_author_profile'])
                     log.debug(self.log_msg("Appending a new comment %s" %comment_hash))
                     self.new_comment_count = self.new_comment_count + 1
                     self.pages.append(page)
                     return True
                else:
                    log.info(self.log_msg("Comment has been fetched before"))
                    return False
            else:
                log.info(self.log_msg("Comment has been fetched before"))
                return False
        except:
            log.exception(self.log_msg("Error occured while fetching comments"))
            return False

    @logit(log,'_getUserInfo')
    def _getUserInfo(self,author_profile_link):
        try:
            user_profile_id = re.findall("http://myaccount\.ibibo\.com/MyIbibo\.aspx\?uId=(.*)$",author_profile_link)[0]
            self.currenturi = "http://my.ibibo.com/Profile/view/" + user_profile_id
            res=self._getHTML(self.currenturi)
            self.rawpage=res['result']
            self._setCurrentPage()
            try:
                self.current_page['ei_author_age']= str(int(stripHtml(self.soup.find('span',attrs={'id':'UserAgeSexLocationInfo'}).previous)))
            except:
                log.info(self.log_msg("Error occured while fetching author's age"))
           
            try:
                self.current_page['et_author_gender']= stripHtml(self.soup.find('span',attrs={'id':'UserAgeSexLocationInfo'}).findNext('span').renderContents().replace(',','').replace('\n',' ').strip().split()[0])

            except:
                log.info(self.log_msg("Error occured while fetching author's gender"))

            try:
                self.current_page['et_author_location']= ' '.join(stripHtml(self.soup.find('span',attrs={'id':'UserAgeSexLocationInfo'}).findNext('span').renderContents().replace(',','').replace('\n',' ').strip().split()[1:]))
            except:
                log.info(self.log_msg("Error occured while fetching author's location"))

            log.debug("Fetched user info from the url %s" %author_profile_link)
            return True
        except:
            log.exception(self.log_msg("Exception occured while fetching user profile"))
            return False
