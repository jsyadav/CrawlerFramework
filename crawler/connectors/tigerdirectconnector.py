
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''


#Riya

import re
from datetime import datetime
import logging
from urllib2 import urlparse,unquote,urlopen
import copy
from cgi import parse_qsl
from utils.httpconnection import HTTPConnection
from BeautifulSoup import BeautifulSoup
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger("TigerDirectConnector")
class TigerDirectConnector(BaseConnector):
        
    ''' 
    Sample url = 'http://www.tigerdirect.com/applications/searchtools/item-details.asp?EdpNo=6744581&body=REVIEWS#CustomerReviewsBlock'
    '''
    
    @logit(log , 'fetch')
    def fetch(self):
        self.genre='Review'
        try:
              self.parent_uri = self.currenturi
              print'************************************************'
              print 'main uri::::::::::::::'
              print self.currenturi
              self.currenturi = self.currenturi.split('&')[0]+'&body=REVIEWS#CustomerReviewsBlock'
              print 'current uri::::::::::::::'
              print self.currenturi
              
                      
              while True:
                print 'inside while loop....'
                #self.soup = BeautifulSoup(urlopen(cur_uri).read())
                self.__setSoupForCurrentUri()
                self.__addReviews()
                next_page_temp = self.soup.find('a', title='Next')
                if(next_page_temp == None):
                    log.info(self.log_msg('Next page not found...!!!'))
                    break
                self.currenturi = 'http://www.tigerdirect.com' + next_page_temp['href']
                print '*************************** Next Page url *************************'
                print self.currenturi
              return True
            
              #self.__addReviews()          
                        
        except:
            log.exception(self.log_msg('Exception in fetch'))
        return True
  
      
    @logit(log,'addReviews')    
    def __addReviews(self):
            
            try:
                print('Inside addReviews....')
                #reviews = self.soup.findAll('div','marginTopBottomTwenty paddingBottomTwenty borderBottomColorOneExtraLight')
                reviews = [x.findParent('div') for x in self.soup.findAll('div','review')]
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
                    
                    #title_str = review.find('span',id='TitleLbl').renderContents().strip()
                    title_str = review.find('h6').renderContents().strip()
                    page['title'] = stripHtml(title_str)
                except:
                    log.exception(self.log_msg('title not found.!!'))    
                
                
                #data/reviews contents
                try:     
                   
                    #data_str = review.find('div','fontSizeLarge').renderContents().strip()
                    data_str = review.find('p').renderContents().strip()
                    page['data']=stripHtml(data_str)
                    
                except:
                    log.exception(self.log_msg('No data found.!!'))    
                
                
                #posted date
                try:
                    
                    #date_str = review.find('span',id=re.compile('AuthorLocationTimeLbl')).renderContents().split(' on ')[-1]
                    dt_list = review.find('dl','reviewer').findAll('dt')
                    for each in dt_list:
                        if each.renderContents().strip()=='Date:':
                            date_str = each.findNext('dd').renderContents().strip()
                    #print date_str
                    page['posted_date'] = datetime.strftime(datetime.strptime(stripHtml(date_str),'%b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
                    
                except:
                    log.exception(self.log_msg('date not found.!!'))
                    page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")
                   
                
                #author of the review             
                try:    
                    
                    #author_str = str(review.find('meta',itemprop='author')['content'])
                    a_list = review.find('dl','reviewer').findAll('dt')
                    for each in a_list:
                        if each.renderContents().strip()=='Reviewer:':
                            author_str = each.findNext('dd').renderContents().strip()
                    page['et_author_name'] = stripHtml(author_str) # for text values
                    
                except:
                    log.exception(self.log_msg('Author not mentioned.!!'))    
                
                
                try:
                    #rating_str = review.find('span','BVRRNumber BVRRRatingNumber').renderContents().strip()
                    rating_str = review.find('span','stars srating\d+').findNext('strong').renderContents().strip()
                    page['ef_rating_overall'] = rating_str
                    
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
    
    @logit(log,'__setSoupForCurrentUri')
    def __setSoupForCurrentUri(self, data=None, headers={}):
        """It will set soup object for the Current URI
        """
        res = self._getHTML(data=data, headers=headers)
        if res:
            self.rawpage = res['result']
        else:
            log.info(self.log_msg('Page Content cannot be fetched for the url: \
                                                            %s'%self.currenturi))
            raise Exception('Page content not fetched for th url %s'%self.currenturi)
        self._setCurrentPage()      

    

