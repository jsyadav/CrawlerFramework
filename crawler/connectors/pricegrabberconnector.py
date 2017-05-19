
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar
#Removed extract method and unused import, Dec 5, 2008 and remove _getTaskUris()

#There is no pagination for this url because,all the reviews are available in same page....!!!!

#modified by Riya


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


log = logging.getLogger('PriceGrabberConnector')

class PriceGrabberConnector(BaseConnector):
    
    ''' 
    Sample url = 'http://reviews.pricegrabber.com/water-purifiers-filters/m/10163553/form_keyword=Brita/st=product/sv=review/'
    '''
    
    @logit(log , 'fetch')
    def fetch(self):
        self.genre='Review'
        try:
            self.parent_uri=self.currenturi
            conn = HTTPConnection()
            conn.createrequest(self.currenturi)
            res = conn.fetch().read()
            self.soup = BeautifulSoup(res)
            #self.soup = BeautifulSoup(urlopen(self.currenturi).read())
            self.__addReviews()          
                        
        except:
            log.exception(self.log_msg('Exception in fetch'))
        return True
  
      
    @logit(log,'addReviews')    
    def __addReviews(self):
            
            try:
                print('Inside addReviews....')
                reviews = self.soup.findAll('div','review')
                print '******************************************************'
                print 'no. of reviews...'
                print len(reviews)
                log.info(self.log_msg('no. of reviews is %s:'%len(reviews)))
                if not reviews:
                    return False
            except:
                log.exception(self.log_msg('No Reviews are found'))
                return True
            
            
            for review in reviews:
                page = {}
                page['uri'] = self.currenturi
                
                
                #title/Heading
                try:
                    #title_temp = review.find('div','reviewDetails').find('h5').extract()
                    #title_str = title_temp.renderContents().strip()
                    title_str = review.find('h3').renderContents().strip()                   
                    page['title'] = stripHtml(title_str)
                except:
                    log.exception(self.log_msg('title not found.!!'))    
                
                
                #data/reviews contents
                try:     
                    #data_str = review.find('div','reviewDetails').renderContents().strip()
                    if (review.find('p','margT5 fullComment')):
                        data_str = review.find('p','margT5 fullComment').renderContents().strip()
                        page['data']=stripHtml(data_str)  
                        
                    else:
                        break;
                        
                
                except:
                    log.exception(self.log_msg('No data found.!!'))    
                
                
                #posted date
                try:
                    #date_str = review.find('div','reviewedByWrapper').findChild('div').renderContents().strip()
                    date_temp = review.find('p','rev_credits').renderContents().strip()
                    date_str = date_temp.split('- ')[1]
                    page['posted_date'] = datetime.strftime(datetime.strptime(stripHtml(date_str),'%b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
                    
                except:
                    log.exception(self.log_msg('date not found.!!'))
                    page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")
                   
                
                #author of the review             
                try:    
                    #author_str = review.find('div','reviewedByWrapper').findChild('a').renderContents().strip()
                    author_temp = review.find('p','rev_credits').find('span').extract()
                    #author_str = author_temp.renderContents().strip()
                    page['et_author_name'] = stripHtml(author_temp.renderContents().strip())
                    
                except:
                    log.exception(self.log_msg('Author not mentioned.!!'))    
                
                try:
                    #rating_str = review.find('span','BVRRNumber BVRRRatingNumber').renderContents().strip()
                    #rating_str = float(review.find('div','stars')['title'].split('out')[0].strip())
                    page['ef_rating_overall'] = float(review.find('div','stars')['title'].split('out')[0].strip())
                    
                except:
                    log.exception(self.log_msg("No individual review rating found.!!"))     
                
                try:
                    review_hash = get_hash(page)
                    #log.info(page)
                    unique_key = get_hash( {'data':page['data'],'title':page['title']})
                    if checkSessionInfo(self.genre, self.session_info_out, unique_key,self.task.instance_data.get('update'),parent_list = [self.parent_uri]):
                        log.info(self.log_msg('session info return True'))
                        continue
                        #return False
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
    
          

    