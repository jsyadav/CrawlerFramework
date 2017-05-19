'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Pratik
#Modified by Riya....

import re
from datetime import datetime
import logging
import urllib2
from urllib2 import urlparse,urlopen
from BeautifulSoup import BeautifulSoup 
from utils.httpconnection import HTTPConnection
import copy
from cgi import parse_qsl
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
import simplejson
import math
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('SearsConnector')
class SearsConnector (BaseConnector) :
    '''
    SAMPLE url: 'http://www.sears.com/shc/s/p_10153_12605_05710005000P?vName=Computers+%26+Electronics&cName=GPS+Systems&sName=View+All#reviewsWrap'
    '''

    @logit(log , 'fetch')
    def fetch(self):
        self.genre="Review"
        try:
           
            for i in range(2):
              print 'Inside i for loop...'
              self.parent_uri = self.currenturi
              conn = HTTPConnection()
              data1 = dict(parse_qsl(self.currenturi.split('?')[-1]))
              conn.createrequest(self.currenturi,data=data1)
              res = conn.fetch().read()
              temp_soup = BeautifulSoup(res)
              if i==1:
               print i
               break
            
            #This is to find out the no. of pages for the reviews.....
            #nop=no. of pages...
            count = float(temp_soup.find('span',id='revCountTot').renderContents().strip())
            temp_nop = count/25
            nop=int(math.ceil(temp_nop))
            print 'No. of pages....:::'
            print nop
                                  
            temp_data = str(temp_soup.find('div', 'revSort').find('option',attrs={'title':'newest'})['value'])
            add_last = '&storeId=10153&callType=AJAX&methodType=GET&shcAJAX=1&pagination=true&offset='
            
            for j in range(1,(nop+1)): 
                print 'inside j for loop....'
                current_uri = 'http://www.sears.com/shc/s/RatingsAndReviewsCmd?'+temp_data+add_last+str(j)
                print '*************************************'
                print current_uri
                #self.soup = BeautifulSoup(urlopen(current_uri).read())
                
                for k in range(2):
                   print 'inside k for loop....'
                   new_conn = HTTPConnection()
                   data2 = dict(parse_qsl(current_uri.split('?')[-1]))
                   new_conn.createrequest(current_uri,data=data2)
                   res = new_conn.fetch().read()
                   self.soup = BeautifulSoup(res)
                   if k==1:
                    print k
                    break
                   
               
                self.__addReviews()
                if j==nop:
                 print j   
                 break
            
        
        except:
            #self.task.status['fetch_status']=False
            log.exception(self.log_msg('Exception in fetch'))
        return True

    @logit(log,'addReviews')    
    def __addReviews(self):
            #-----Modified by riya------
            try:
                print('inside addReviews....')
                reviews = self.soup.findAll('div','previewContents')
                print 'no. of reviews...'
                print len(reviews)
                log.info(self.log_msg('no. of reviews is %s:'%len(reviews)))
                if not reviews:
                    return False
            except:
                log.exception(self.log_msg('No Reviews are found...!!!'))
                return False
            
            for review in reviews:
                page = {}
                page['uri'] = self.currenturi
                
                try:
                    title_str = review.find('span','pReviewHeadlineText').renderContents().strip()
                    page['title'] = stripHtml(title_str)
                except:
                    log.exception(self.log_msg('title not found.!!'))    
                
                try:      
                    data_str = review.find('div','pReviewThoughts').renderContents().strip()      
                    page['data']=stripHtml(data_str)  
                except:
                    log.exception(self.log_msg('No data found.!!'))    
                
                try:
                    #date_str = review.find('div','pReviewDate').renderContents().strip()
                    temp_date_str = review.find('div','pReviewDate')
                    
                    if temp_date_str.find('span','pReviewViaStore'):
                        temp_date_str.find('span','pReviewViaStore').extract()
                        
                    date_str = temp_date_str.renderContents().strip()
                    page['posted_date'] = datetime.strftime(datetime.strptime(stripHtml(date_str),'%b %d ,  %Y'),"%Y-%m-%dT%H:%M:%SZ")
                    print page['posted_date']                   
                    #log.info(self.log_msg("Date:")%page['posted_date']);
                    #page['posted_date'] = datetime.strftime(datetime.strptime(stripHtml(self.soup.find('span','BVdateCreated').renderContents()),'%B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.exception(self.log_msg('title not found.!!'))
                    page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")
                
                try:
                    if review.find('span','pVal'):
                        author_str = review.find('span','pVal').renderContents().strip()
                        page['et_author_name'] = stripHtml(author_str)
                    
                    #if not(review.find('span','pVal').renderContents().strip()):
                    else:
                        author_str = review.find('p','pVal').renderContents().strip()
                        page['et_author_name'] = stripHtml(author_str)
                        
                    #page['et_author_name']=stripHtml(str(review.find('span','BVReviewerNickname BVReviewerNicknameText')))
                except:
                    log.exception(self.log_msg('Author not mentioned.!!'))    
                
                '''try:
                    rating_str = review.find('span','BVRRNumber BVRRRatingNumber').renderContents().strip()
                    page['ef_rating_overall'] = rating_str
                    #page['ef_rating_overall']=review.find('td','BVcustomerRatingFirst BVcustomerRating').findParent("table").find("img")['alt'].replace("out of 5","")
                except:
                    log.exception(self.log_msg("No individual review rating found.!!"))'''     
                
                try:
                    review_hash = get_hash(page)
                    #log.info(page)
                    unique_key = get_hash( {'data':page['data'],'title':page['title']})
                    log.info(self.log_msg('unique_key: %s' %unique_key))
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

    @logit(log,'getParentPage')
    def __getParentPage(self):
        page={}
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri \
                                    , self.task.instance_data.get('update')):
            log.info(self.log_msg('Session infor return True'))
            return False
        try:
            page['title'] = self.soup.find('div',{'class':'productName clearfix'}).next.replace('\n','').replace('\r','').replace('\t','').strip()
        except:
            log.exception(self.log_msg('could not parse page title'))
            page['title'] = ''
        try:
            rc = re.compile(r'\d\.\d',re.U)
            va_temp = self.soup.find('span',{'class':'starrating'}).next.next
            value =''
            if rc.search(va_temp):
                value = rc.search(va_temp).group()
            else:
                rc = re.compile(r'\d',re.U)
                value = rc.search(va_temp).group()
            page['ef_product_rating_overall'] = value
        except:
            log.exception(self.log_msg('rating not found'))
        try:
            page['ei_product_reviews_count'] = int(stripHtml(self.soup.find('span',{'class':'starrating'}).next.next.next.renderContents()).split('Customer')[0].strip())
        except:
            log.info(self.log_msg('Total review count not found') )
            
        try:
            product_info = stripHtml(self.soup.find('span',{'class':'vendor'}).nextSibling.next.renderContents())
            product_info = product_info.replace('\n','').replace('\t','').replace('#','')
            produ_lis = product_info.split('\r')
            page['et_product_item'] = produ_lis[1]
            page['et_product_model']=produ_lis[3]
        except:
            log.info(self.log_msg('product info can not be parsed'))
        try:
            tempP =self.soup.find('div',{'class':'youPay bl'})
            tempP1 =self.soup.find('div',{'class':'youPay'})
            val=''
            if tempP:
                val = stripHtml(tempP.renderContents())
            elif tempP1:
                val = stripHtml(tempP1.renderContents())
            page['et_product_price'] = val
        except:
            log.exception(self.log_msg('price not found'))
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
