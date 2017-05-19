
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#MOHIT RANKA
#prerna

from urllib2 import *
from urlparse import urlparse
import re
import logging
from datetime import datetime
import copy
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.urlnorm import normalize
from utils.sessioninfomanager import *
from utils.decorators import *

log = logging.getLogger('CnetReviewsConnector')

class CnetReviewsConnector(BaseConnector):
    
    @logit(log,'_createSiteUrl')
    def _createSiteUrl(self,curr_url):
        """
        Returns a url address of the HTML page 
        """
        try:
            self.curr_url = curr_url.replace(' ','+')
            log.debug(self.log_msg("creating new cnet reviews' page url: %s" %(self.curr_url.replace(' ','+'))))
            return self.curr_url 
        except Exception,e:
            log.exception(self.log_msg("Exception occured while creating URL"))
            raise e

    @logit(log,'fetch')
    def fetch(self):
        """
        Fetches all the reviews for a given self.currenturi and returns Fetched staus depending 
        on the success and faliure of the task
        """
        try:
            self.genre = 'review'
            #the url given in self.currenturi is unordered
            self.currenturi = self._createSiteUrl("%s?ord=creationDate desc" %(self.currenturi.split('?')[0]))
            if not self.__getParentPage():
                log.info(self.log_msg('Parent page already added'))
            log.debug(self.log_msg('fetch starting with currenturi %s: ' % self.currenturi))
            self.next_url_links = []
            self.fetch_next_link = True
            self.fetch_next_review = True
            self.__iterateReviews()
            return True
        except Exception,e:
            log.exception(self.log_msg("Exception occured in fetch()"))
            return False

    @logit(log,'__getParentPage')
    def __getParentPage(self):
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
                page['title'] = stripHtml(self.soup.find('h1').renderContents())
            except:
                #page['title'] = ''
                log.info(self.log_msg("Exception in getting product page title"))
                return False
            page['data']=''
            log.debug(self.log_msg('got the content of the product main page'))
            try:
                post_hash=  get_hash(page)
                #log.info(page)
            except:
                log.debug(self.log_msg("Error occured while creating the parent page hash"))
                return False
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    self.task.instance_data['uri'], self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug('got the connector instance first time, sending updatesessioninfo the id : %s' % str(id))
                result=updateSessionInfo(self.genre, self.session_info_out, self.task.instance_data['uri'], post_hash, 
                                             'Post', self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    try:
                        page['ef_product_rating_overall'] = float(re.findall("\d{1,2}\.\d",stripHtml(self.soup.find('li', 'userRate').find('span').renderContents()))[0])
                    except:
                        log.info(self.log_msg("Exception in getting overall rating from product page"))
                    page['path'] = [self.task.instance_data['uri']]
                    page['parent_path'] = []
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
                    page['entity'] = 'post'
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

    @logit(log,'__iterateReviews')
    def __iterateReviews(self):
        """
        Iterates through all the review pages by next button and populates self.url_list
        """
        try:
            while self.fetch_next_link:
                log.debug(self.log_msg('trying to get review urls, with current uri: %s' % self.currenturi))
                search_page_soup = self.soup
                for each in self.soup.find('div', section='summaryList').ul.findAll('li', recursive=False): 
                    permalink_url = unicode(each.find('div',
                                                      attrs={'class':'postTools'}).find('a',attrs={'class':'permalink toolTipElement'}).get('href'))
                    #print permalink_url
                    if permalink_url.startswith('http://reviews.cnet.com'):
                        permalink_url = permalink_url.replace('http://reviews.cnet.com','')
                    if self.fetch_next_review:
                        self.__getReview(self.task.instance_data['uri'],permalink_url)
                    else:
                        self.fetch_next_link = False
                        break
                if self.fetch_next_link:
                    self.soup= search_page_soup
                    #next_link =  [stripHtml(each.find('a').get('href')) for each in self.soup.find('ul',attrs={'class':'pagination'}).findAll('li') if each.find('a') and each.find('a').renderContents()=="next" ][0]
                    next_link =  [x.get('href') for x in self.soup.findAll('a', 'nextButton')][0]
                    if next_link.startswith('http://reviews.cnet.com'):
                        next_link = next_link.replace('http://reviews.cnet.com','')
                    self.currenturi = self._createSiteUrl("http://reviews.cnet.com%s"%next_link)
                    if self.currenturi not in self.next_url_links:
                        self.next_url_links.append(self.currenturi)
                        res=self._getHTML(self.currenturi)
                        if res: 
                            self.rawpage=res['result']
                            self._setCurrentPage()
                        else:
                            log.debug(self.log_msg("could not set the next url link"))
                            self.fetch_next_link = False
                            self.fetch_next_review = False
                            break
                    else:
                        log.critical(self.log_msg("Duplicate next url link"))
                        self.fetch_next_link = False
                        self.fetch_next_review = False
                        break
            return True
        except:
            log.exception(self.log_msg("Exception occured in __iterateReviews"))
            self.fetch_next_link = False
            self.fetch_next_review = False
            return False

    @logit(log,'__getReview')
    def __getReview(self,parent_uri,review_url):
        """
        Fetches the data from a review page and appends them self.pages
        """
        try:
            self.currenturi = "http://reviews.cnet.com%s"%review_url
            log.info(self.log_msg(self.currenturi))
            temp_review_uri=review_url
            res=self._getHTML(self.currenturi)
            self.rawpage=res['result']
            self._setCurrentPage()
            page={'data':''}
            # As data is using extraction on soup, we need to make the copy of soup as soon as the soup is created
            # This copied soup (review_soup) is used while fetching comments
            review_soup = copy.copy(self.soup)
            review_url = self.currenturi
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    temp_review_uri, self.task.instance_data.get('update'),
                                    parent_list=[parent_uri]):
                try:
                    author = self.soup.find('span', 'reviewer')
                    page['et_author_name'] = stripHtml(author.renderContents())
                except:
                    log.exception(self.log_msg("Exception occured while fetching author name from the review"))
                try:
                    page['title'] = stripHtml(self.soup.find('span', 'summary').renderContents())
                except:
                    page['title'] = ''
                    log.exception(self.log_msg("Exception occured while fetching review title"))
                try:
                    author_link = author.findParent('a')['href']
                    if author_link.startswith('http://reviews.cnet.com'):
                        author_link = author_link.replace('http://reviews.cnet.com','')
                    page['et_author_profile'] = "http://reviews.cnet.com%s"%author_link
                except:
                    log.exception(self.log_msg("Exception occured while fetching author's profile link"))
                try:
                    page['ef_rating_overall'] = float(re.findall("^\d\.\d",stripHtml(self.soup.find('p', 'stars').find('span', 'rating').renderContents()))[0])
                except:
                    log.exception(self.log_msg("Exception occured while fetching overall rating"))
                try:
                    page['ei_data_recommended_yes']= int(stripHtml(self.soup.find('span', 'thumbsUp').renderContents())[1:-1])
                except:
                    log.exception(self.log_msg("Exception occured while fetching number of people who found this review helpful"))
                try:
                    page['ei_data_recommended_no']= int(stripHtml(self.soup.find('span', 'thumbsDown').renderContents())[1:-1])
                except:
                    log.exception(self.log_msg("Exception occured while fetching number of people who found this review helpful"))
                
                info_tag = self.soup.find('div', 'description')
                try:
                    pros_cons = ['pros','cons'] #,'summary']
                    for each in pros_cons:
                        key = each
                        data_tag = info_tag.find('p', each).extract()
                        title_tag = data_tag.find('strong')
                        if title_tag:
                            title_tag.extract()
                        page['et_data_' + each]= stripHtml(data_tag.renderContents())
                except:
                    log.exception(self.log_msg("Exception occured while fetching cons from the review"))
##                try:
##                    info_tag.find('strong').extract()
##                    notify_tags = info_tag.findAll('span', 'notifyMsg')
##                    for each in notify_tags:
##                        each.next.next.extract()
##                        each.extract()
##                except:
##                    log.exception(self.log_msg('No update messsage is found'))
                try:
                    data_tag = info_tag.find('p','summary')
                    if data_tag:
                        page['data'] = stripHtml(data_tag.renderContents())
                    else:
                        page['data'] = page['et_data_pros'] + page['et_data_cons']    
                except:
                    log.exception(self.log_msg('data not found'))
                    page['data'] = ''
                try:
                    date_str = stripHtml(self.soup.find('time', 'dtreviewed').renderContents())
                    page['posted_date'] = datetime.strftime(datetime.strptime(date_str,"%B %d, %Y"),"%Y-%m-%dT%H:%M:%SZ")
                except:
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    log.exception(self.log_msg("Exception occured while fetching post date from review %s" %review_url))
                try:
                    review_hash =  get_hash(page)
                    #log.info(page)
                except:
                    log.debug(self.log_msg("Error occured while creating the review hash %s" %self.currenturi))
                    return False
                result=updateSessionInfo(self.genre, self.session_info_out, temp_review_uri, review_hash, 
                                         'review', self.task.instance_data.get('update'), parent_list=[parent_uri])
                if result['updated'] and page['data']:
                    parent_list = [parent_uri]
                    page['parent_path'] = copy.copy(parent_list)
                    parent_list.append(temp_review_uri)
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
                    page['entity'] = 'review'
                    page['category'] = self.task.instance_data.get('category','')
                    page['task_log_id']=self.task.id
                    page['uri']=normalize(self.currenturi)
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    self.current_page = page
                    if page.get('et_author_profile'):# and self.task.instance_data.get('pick_user_info'):
                        self._getUserInfo(page['et_author_profile'])
                    self.pages.append(self.current_page)
                    log.debug(self.log_msg("Review %s added to self.pages" %(review_url)))
                else:
                    if not self.task.instance_data.get('update'):
                        self.fetch_next_review = False
                    log.debug(self.log_msg("Review %s NOT added to self.pages" %(review_url)))
            else:
                if not self.task.instance_data.get('update'):
                    self.fetch_next_review = False
                log.debug(self.log_msg("Review %s NOT added to self.pages" %(review_url)))
            try:
                if self.task.instance_data.get('pick_comments'):
                    self.soup = review_soup
                    self.currenturi = review_url
                    self.comment_list = self.soup.find('ul',attrs={'id':'commentList'}).findAll('li',attrs={'commentid':True}) 
                    self.new_comment_count = 0
                    for comment_idx, each_comment in enumerate(self.comment_list):
                        log.debug(self.log_msg("processing %d comment" %(comment_idx+1)))
                        self.current_comment = each_comment
                        self._getReviewComment(review_url,[parent_uri,temp_review_uri])
                        log.debug(self.log_msg("fetched %d new comments for this review" %self.new_comment_count))
            except:
                log.debug(self.log_msg("No comments found for review %s" %review_url))
        except Exception,e:
            log.exception(self.log_msg("Exception occured in _getReview() for review %s" %review_url))
            raise e
           
    @logit(log,'_getReviewComments')
    def _getReviewComment(self,review_identity,parent_list):
        """
        Gets new/updated comments from a particular review and appends to self.pages
        """
        try:
            page={}
            comment_iden = self.current_comment.get('commentid')
            if not checkSessionInfo(self.genre, self.session_info_out,
                                    comment_iden, self.task.instance_data.get('update'),
                                    parent_list=parent_list):
                try:
                    page['et_author_name']=stripHtml(self.current_comment.find('p',attrs={'class':'author'}).find('a').renderContents())
                except:
                    log.info(self.log_msg("Could not fetch comment author name"))
                try:
                    author_link = stripHtml(self.current_comment.find('p',attrs={'class':'author'}).find('a').get('href'))
                    if author_link.startswith('http://reviews.cnet.com'):
                        author_link = author_link.replace('http://reviews.cnet.com','')
                    page['et_author_profile']="http://reviews.cnet.com%s"%(author_link)
                except:
                    log.info(self.log_msg("Author profile link not found"))
                try:
                    page['data'] = stripHtml(self.current_comment.find('p',attrs={'class':'author'}).findNext('p').renderContents())
                    page['title']=page['data'][:50]
                except:
                    log.info(self.log_msg("Review data not found"))
                    return False
                try:
                    comment_hash =  get_hash(page)
                    #log.info(page)
                except:
                    log.debug(self.log_msg("Error occured while creating the comment hash %s" %comment_iden))
                    return False
                result=updateSessionInfo(self.genre, self.session_info_out, comment_iden, comment_hash,
                                         'comment', self.task.instance_data.get('update'),
                                         parent_list=parent_list)
                if result['updated']:
                    try:
                        page['posted_date']= datetime.strftime(datetime.strptime(str(re.findall("\w+\s\d{1,2},\s\d{4}",self.current_comment.find('p').renderContents())[0]),"%B %d, %Y"),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        log.info(self.log_msg("Exception occured while fetching post date from review"))
                    page['parent_path'] = copy.copy(parent_list)
                    parent_list.append(comment_iden)
                    page['path'] = parent_list
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
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project
                    self.new_comment_count = self.new_comment_count + 1
                    self.current_page = page
                    if page.get('et_author_profile') and self.task.instance_data.get('pick_user_info'):
                        self._getUserInfo(page['et_author_profile'])
                    self.pages.append(self.current_page)
                    log.debug(self.log_msg("Comment %s added for review %s" %(comment_iden,review_identity)))
                    return True
                else:
                    log.debug(self.log_msg("Comment %s NOT added for review %s" %(comment_iden,review_identity)))
            else:
                log.debug(self.log_msg("Comment %s NOT added for review %s" %(comment_iden,review_identity)))
        except:
            log.exception(self.log_msg("Error occured while fetching comment %s for review %s" %(comment_iden,review_identity)))
            return False

    @logit(log,'_getUserInfo')
    def _getUserInfo(self,author_profile_link):
        try:
            self.currenturi = author_profile_link
            res=self._getHTML(self.currenturi)
            self.rawpage=res['result']
            self._setCurrentPage()
            try:
                self.current_page['edate_author_member_since']= datetime.strftime(datetime.strptime(stripHtml(self.soup.find('ul',attrs={'class':'profileInfo'}).span.renderContents()),"%B %d, %Y"),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.info(self.log_msg("Error occured while fetching member since info"))
            try:
                self.current_page['ei_author_reviews_count']= int(stripHtml(self.soup.findAll('ul',attrs={'class':'profileInfo'})[1].findAll('span')[0].find('a').renderContents()))
            except:
                log.info(self.log_msg("Error occured while fetching the number of reviews user has written"))

            try:
                self.current_page['ei_author_comments_count']= int(stripHtml(self.soup.findAll('ul',attrs={'class':'profileInfo'})[1].findAll('span')[1].find('a').renderContents()))
            except:
                log.info(self.log_msg("Error occured while fetching the number of reviews user has written"))

            log.debug("Fetched user info from the url %s" %author_profile_link)
            return True
        except:
            log.exception(self.log_msg("Exception occured while fetching user profile"))
            return False
