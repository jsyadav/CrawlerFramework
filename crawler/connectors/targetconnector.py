
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#prerna
#modified for new url on 20dec
import re
import copy
from datetime import datetime
import logging
from urllib2 import urlparse,unquote
from cgi import parse_qsl
from BeautifulSoup import BeautifulSoup
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.httpconnection import HTTPConnection
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('TargetConnector')
class TargetConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        """Create the url, fetch the page and extract reviews
        """
        try:
            self.genre = 'review'
            self.__task_elements_dict = {
                'priority':self.task.priority,
                'level': self.task.level,
                'last_updated_time':datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ"),
                'pickup_date':datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ"),
                'connector_instance_log_id': self.task.connector_instance_log_id,
                'connector_instance_id':self.task.connector_instance_id,
                'workspace_id':self.task.workspace_id,
                'client_id':self.task.client_id,
                'client_name':self.task.client_name,
                'versioned':False,
                'category':self.task.instance_data.get('category',''),
                'task_log_id':self.task.id }
            self.__setSoupForCurrentUri()
            self.__setParentPage()
            conn = HTTPConnection()
            next_page = 1
            noOfReviews = 10
            total_reviews = int(re.search('^\d+',stripHtml(self.soup.find('div', id='review-summary').\
                        renderContents()).splitlines()[0]).group())
            if not total_reviews%10 == 0: 
                total_page = total_reviews/10+1
            else:
                total_page =total_reviews/10    
            catEntryId =  self.soup.find('input',attrs ={'name':'catEntryId'})['value']
            partNumber =  self.soup.find('input',attrs ={'name':'partNumber'})['value']
            while True:
                try:
                    self.__addReviews()
                    if  total_reviews >10:
                        next_page +=1
                        if  next_page>total_page:
                            log.info('reached last page')
                            return False
                        data = {'currentPage':next_page, 'noOfReviews':noOfReviews, 'catEntryId':catEntryId, 'partNumber':partNumber,'communitySortBy':'mostrecent','invokeDB':'true'}
                        headers = {'Host':'www.target.com','Referer':'http://www.target.com/p/Danby-Countertop-Dishwasher-White/-/A-10826013'}
                        uri='http://www.target.com/webapp/wcs/stores/servlet/SortedReviews'
                        conn.createrequest(uri,data=data,headers=headers)
                        self.rawpage = conn.fetch().read()
                        self._setCurrentPage()
                        noOfReviews +=10
                    else:
                        log.info('reviews in one page only')
                        return False        
                except:
                    log.exception(self.log_msg('next page not found %s'%self.currenturi)) 
                    break   
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'%self.currenturi))
        return True

    @logit(log , '_setParentPage')
    def __setParentPage(self):
        """Get the average rating from the parent page
        """
        page={}
        try:
            if checkSessionInfo(self.genre, self.session_info_out, self.currenturi,\
                                         self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info return True, Already exists'))
                return False
            #page['title'] = stripHtml(self.soup.find('h1',id='productTitle').renderContents())
            page['title'] = stripHtml(self.soup.find('h2','product-name item').renderContents())
        except:
            log.exception(self.log_msg('could not parse page title'))
        try:
            page['ef_product_rating'] = float(stripHtml(self.soup.find('div',id='ProductDetails').\
                                        find('span','rating').renderContents()))                                            
        except:
            log.exception(self.log_msg('could not parse overall_rating'))
        try:
            page['ei_product_reviews_count'] =  int(re.search('^\d+',stripHtml(self.soup.find('div', id='review-summary').\
                        renderContents()).splitlines()[0]).group())
        except:
            log.exception(self.log_msg('could not parse total_reviews count'))
        try:
            unwanted_tag= self.soup.find('div',id='giftCard').find('p','price').\
                            find('strong')
            if unwanted_tag:
                unwanted_tag.extract()
            page['et_product_price'] = stripHtml(self.soup.find('div',id='giftCard').\
                                        find('p','price').renderContents())
        except:
            log.exception(self.log_msg('could not parse product price'))
        try:
            post_hash = get_hash( page )
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo( self.genre, self.session_info_out, self.\
                   currenturi, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                return False
            page['path']=[self.currenturi]
            page['parent_path']=[]
            page['uri'] = self.currenturi
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
            log.info(page)
            log.info(self.log_msg('Parent Page added'))
            return True
        except:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False    
            
    
    @logit(log , '__addReviews')
    def __addReviews(self):
        try:
           # reviews = self.soup.find('div', id='reviews-container').findAll('div','review-content')
            reviews = self.soup.findAll('div','review-content',id  = re.compile('\s*'))
            log.info(self.log_msg('no of review :%s'%len(reviews)))
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        for review in reviews:
            try:
                unique_key = review['id']
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                =[self.task.instance_data['uri']]):
                    continue
                page = self.__getData(review)
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            get_hash( page ),'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.task.instance_data['uri']])
                if not result['updated']:
                    continue
                parent_list = [self.task.instance_data['uri']]
                page['parent_path'] = parent_list[:]
                parent_list.append(unique_key)
                page['path'] = parent_list
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
                self.pages.append( page )
                #log.info(page)
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))
        return True
            
    def __getData(self, review):
        """ This will return the page dictionry
        """
        page = {'title':''}
        try:
            data_helpful = stripHtml(review.find('a',title='Helpful:Yes').\
                            findNext('span','count').renderContents())
            page['ei_data_helpful_count'] = int(re.sub('\(|\)|\,','',data_helpful))
        except:
            log.exception(self.log_msg('data helpful count not found'))
        try:
            page['ef_rating_overall'] = float(re.split(' of',stripHtml(review.\
                                        find('p','ratings-current screen-reader-only').\
                                        find('strong').renderContents()))[0])
        except:
            log.exception(self.log_msg('Rating not found '))
        try:
            try:
                date_str = stripHtml(review.find('div','review-date').renderContents())
            except:
                date_str = stripHtml(review.find('span','review-date').renderContents())
            page['posted_date'] =  datetime.strftime(datetime.strptime(date_str,"%b %d, %Y"),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('posted date may be todays date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            author_info =  review.find('p','reviewer-info')
            date_tag  = author_info.find('span','review-date')
            if date_tag:
                date_tag.extract()
            author_loc = author_info.find('span','reviewer-location')
            if author_loc:
                author_loc.extract()
            author_name = review.find('p','reviewer-info')    
            page['et_author_name'] = stripHtml(author_name.renderContents())
            page['et_author_location'] = re.split(re.compile('^from',re.IGNORECASE),stripHtml(author_loc.renderContents())
                                        )[-1].strip()
                    
        except:
            log.exception(self.log_msg('author name not found'))
        try:
            page['title']= stripHtml(review.find('h3','review-title').renderContents())
        except:
            log.exception(self.log_msg('Cannot find the title'))
            page['title'] = ''
        try:
            try:
                page['data'] = re.sub('\t|\n|\r','', stripHtml(review.find('p','review-text').renderContents()))
            except:
                review_str = review.__str__()
                review_str = review_str.replace(review.find('span','reviewer').\
                                parent.__str__(),review.find('span','reviewer').\
                                parent.__str__() + '<serendio_tag>')
                review_str = review_str.replace(review.find('div','helpfulVote').__str__(), '</serendio_tag>' + review.find('div','helpfulVote').__str__())
                page['data'] =stripHtml(re.search('<serendio_tag>.*?</serendio_tag>',review_str,re.S).group())
        except:
            log.exception(self.log_msg('data not found'))
            page['data']=''
        try:
            if page['title']=='':
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
            
        return page

    @logit(log,'__setSoupForCurrentUri')
    def __setSoupForCurrentUri(self, data=None, headers={}):
        """It will set soup object for the Current URI
        """
        res = self._getHTML(data=data, headers=headers)
        if res:
            self.rawpage = res['result']
        else:
            raise Exception('Page content not fetched for th url %s'%self.currenturi)
        self._setCurrentPage()
    

