'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

'''for homedepot the url given is 
(http://www.homedepot.com/p/Admiral-3-4-cu-ft-Top-Load-Washer-in-White-ATW4675YQ/203284243#customer_reviews)
In this url there is totally 579 reviews but since it is written by javascript, 
so from the associated url (http://reviews.homedepot.com/1999/203284243/reviews.htm?format=brandvoice) 
only 57 reviews are being fetched...!!!! '''


#modified by riya
import re
from datetime import datetime
import logging
from urllib2 import urlparse,unquote,urlopen
import copy
from utils.httpconnection import HTTPConnection
from BeautifulSoup import BeautifulSoup
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('HomeDepotConnector')

class HomeDepotConnector(BaseConnector):
    @logit(log , 'fetch')
    def fetch(self):
        self.genre='Review'
        try:
            self.parent_uri=self.currenturi
            #if not self._setSoup():
             #   log.info(self.log_msg('Soup not set , Returning False from Fetch'))
             #   return False
              
            #------Modified by riya-------

            conn = HTTPConnection()
            conn.createrequest(self.currenturi)
            res = conn.fetch().read()
            temp_soup = BeautifulSoup(res)
            #temp_soup.find('a',id ='RatingsPromoTabSectionImageLink')
            uri1 = temp_soup.find('a',id ='RatingsPromoTabSectionImageLink')['href']
            #print uri1
            current_uri =uri1.split('writereview.htm')[0]+'reviews.htm?format=brandvoice'
            #print current_uri
            
            while True:
                #print 'inside while loop....'
                self.soup = BeautifulSoup(urlopen(current_uri).read())
                self.__addReviews()
                next_page_uri = self.soup.find('span','BVRRPageLink BVRRNextPage')
                if(next_page_uri == None):
                    log.info(self.log_msg('Next page not found...!!!'))
                    break
                current_uri = next_page_uri.a['href']
            return True
            
                       
            #if not self.__getParentPage():
             #   log.info(self.log_msg('Parent Page return False'))
            
        except:
            log.exception(self.log_msg('Exception in fetch'))
        return True
        
    @logit(log, 'getparentPage')
    def __getParentPage(self):
        page={}
        page['uri'] = self.currenturi
        try:
            parent_details=self.soup.find('div','details')
        except:
            log.exception(self.log_msg("Parent Page not found!!"))    
        try:
            specifications=self.soup.find('div',id='tab-specs').findAll('li')
        except:
            log.exception(self.log_msg("Specifications not mentioned"))       
        try:
            page['title']=re.sub('\s+',' ',stripHtml(str(parent_details.find('p','product-name'))))
        except:
            log.exception(self.log_msg("Could not get the title"))
        try:
            page['et_product_model_no']=re.sub('\s+',' ',stripHtml(str(parent_details.find('p','model-number'))))
        except:
            log.exception(self.log_msg("Model no not specified "))
        try:
            page['et_product_price']=stripHtml(str(parent_details.find('div','prices').find('p'))).split('/')[0]
        except:
            log.exception(self.log_msg('price not mentioned'))
        try:
            specifications=soup.find('div',id='productSpecs').findAll('li')
        except:
            log.exception(self.log_msg("Specifications not found!!"))    
        try:
            for each in specifications:
               page['et_product_specifications_data_'+ re.sub('\s+','_',re.sub('[\(\)\/]+','',stripHtml(str(each.find('strong'))).strip()).lower())] = stripHtml(str(each.renderContents()).split(':')[1])    
        except:
            log.exception(self.log_msg("No individual specifications mentioned"))    
        main_page_soup = copy.copy(self.soup)
        main_page_uri = self.currenturi
        self.currenturi=self.soup.find('div',id='tab-reviews').find('iframe')['src']
        self._setSoup()
        try:
             page['ef_overall_rating'] = self.soup.find('td','BVcustomerRatingFirst BVcustomerRating').findParent("table").find("img")['alt'].replace("out of 5","")
        except:
            log.exception(self.log_msg('Rating over all not found'))
        try:
                if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri \
                                                , self.task.instance_data.get('update')):
                    log.info(self.log_msg('Session info return True'))
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
                #page['posted_date']=datetime.strftime
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
            
    @logit(log,'addReviews')    
    def __addReviews(self):
            #-----Modified by riya------
            try:
                #print('inside addReviews....')
                reviews = self.soup.findAll('div',id=re.compile('BVRRDisplayContentReviewID_\d+'))
                #print 'no. of reviews...'
                #print len(reviews)
                log.info(self.log_msg('no. of reviews is %s:'%len(reviews)))
                if not reviews:
                    return False
            except:
                log.exception(self.log_msg('No Reviews are found'))
                return False
            for review in reviews:
                page = {}
                page['uri'] = self.currenturi
                
                try:
                    title_str = review.find('span','BVRRValue BVRRReviewTitle').renderContents().strip()
                    page['title'] = stripHtml(title_str)
                except:
                    log.exception(self.log_msg('title not found.!!'))    
                
                try:      
                    data_str = review.find('span','BVRRReviewText').renderContents().strip()      
                    page['data']=stripHtml(data_str)  
                except:
                    log.exception(self.log_msg('No data found.!!'))    
                
                try:
                    date_str = review.find('span','BVRRValue BVRRReviewDate').renderContents().strip()
                    page['posted_date'] = datetime.strftime(datetime.strptime(stripHtml(date_str),'%B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
                    #log.info(self.log_msg("Date:")%page['posted_date']);
                    #page['posted_date'] = datetime.strftime(datetime.strptime(stripHtml(self.soup.find('span','BVdateCreated').renderContents()),'%B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.exception(self.log_msg('title not found.!!'))
                    page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")
                
                try:    
                    author_str = review.find('span','BVRRNickname').renderContents().strip()
                    page['et_author_name'] = stripHtml(author_str)
                    #page['et_author_name']=stripHtml(str(review.find('span','BVReviewerNickname BVReviewerNicknameText')))
                except:
                    log.exception(self.log_msg('Author not mentioned.!!'))    
                
                try:
                    rating_str = review.find('span','BVRRNumber BVRRRatingNumber').renderContents().strip()
                    page['ef_rating_overall'] = rating_str
                    #page['ef_rating_overall']=review.find('td','BVcustomerRatingFirst BVcustomerRating').findParent("table").find("img")['alt'].replace("out of 5","")
                except:
                    log.exception(self.log_msg("No individual review rating found.!!"))     
                
                try:
                    review_hash = get_hash(page)
                    #log.info(page)
                    unique_key = get_hash( {'data':page['data'],'title':page['title']})
                    if checkSessionInfo(self.genre, self.session_info_out, unique_key,self.task.instance_data.get('update'),parent_list = [self.parent_uri]):
                        log.info(self.log_msg('session info return True'))
                        return False
                    result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                                    review_hash,'Review', self.task.instance_data.get('update'),\
                                                                                           parent_list=[self.parent_uri])

                    if not result['updated']:   
                        log.info(self.log_msg('result not updated'))
                        
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
                    print page        
            return True    
    
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

    
