
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#modified by prerna 20july
#modified by prerna
#ASHISH YADAV

import re
from utils.utils import get_hash
import md5
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import logging
import pickle
from urllib2 import urlparse,unquote
from urlparse import urlparse
import copy
from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('EbayReviewConnector')


class EbayReviewConnector(BaseConnector):

    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        sample url: http://catalog.ebay.com/Apple-iPad-Wi-Fi-16-GB-Tablet-Computer-/80701747/r.html?_fcls=1
                    http://www.ebay.com/ctg/Apple-iPad-Wi-Fi-16-GB-Tablet-Computer-/80701747/r.html?_fcls=1
        '''
        try:
            self.genre='Review'
            self.baseuri = 'http://www.ebay.com'
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
            self.__setSoup()
            self.parenturi = self.currenturi
            #self.__getParentPage()
            #count = 0
            main_page_soup = copy.copy(self.soup)
            while self.__addReviews():
                try:
                    self.currenturi = self.baseuri + main_page_soup.find('a',title ='Next')['href']
                    log.info(self.currenturi)
                    self.__setSoup()
                    main_page_soup = copy.copy(self.soup)
##                    count +=1
##                    if count>=4:
##                        break
                        
                except:
                    log.exception(self.log_msg('Next Page link  not found for url %s'%self.currenturi))
                    break
            return True    
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'%self.currenturi))
        return True        
        

    @logit(log , 'addreviews')
    def __addReviews(self):

        '''
        next_page is used to determine cases in which there is single page of reviews , so it will be null for first function call
        
        ebay - the section [top reviews] , if it's present for a product
        case 1        if number of pages = 1 , top review won't be repeated 
        case 2        if number of pages > 1 , apart from coming in top reviews , it will come as a part of reviews in later pages ( so 2 time    s , top reviews(page 1 , page x later)
        problem :
        for no updates case , we continue till we reach the review we already crawled , evaluating using unique review_id
        which fails for case 2
        solution:
        for review pages more than 1 , don't pick top reviews or case 2
        '''
        #reviews  = self.soup.findAll('div',{'class':re.compile('\s*singlereview')}) #that are top reviews and general reviews
        reviews  = self.soup.findAll('div','rvw')
        log.info(len(reviews))
        for review in reviews:
            page ={}           
            try:
                page['ef_rating_overall'] = float(review.find('div','RatingBar').\
                                            find('span',attrs ={'class':re.compile('rating-lf avs\d+')})['class'].\
                                            strip().split('avs')[-1])
                
##                                       float(review.find('div','cll rrbr').\
##                                            find('img')['src'].split('/')[-1].\
##                                            split('_')[-1].split('.gif')[0])
            except:
                log.exception(self.log_msg('could not parse overall rating'))

            try:
                page['data'] =  stripHtml(review.find('div','cll con').\
                                renderContents()).replace('\n>','')
            except:
                page['data'] = ''
                log.exception(self.log_msg('could not parse review data'))

            try:
                num_helpful = review.find('div',{'class':'cll hlp'})
                if num_helpful:
                    page['ei_data_recommended_yes'] = int(num_helpful.strong.renderContents())
                    page['ei_data_recommended_total'] = int(num_helpful.findAll('strong')[1].\
                                                        renderContents())
            except:
                log.exception(self.log_msg('could not parse review helpfulness'))
            try:
                page['title'] = stripHtml(review.find('h4','g-b').renderContents())
            except:
                page['title'] = ''
                log.exception(self.log_msg('could not parse review title'))
            try:
                date_str = stripHtml(review.find('div','dte cllr').renderContents()).split(':')[-1].strip()
                page['posted_date'] =  datetime.strptime(date_str,"%m/%d/%y").strftime("%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception(self.log_msg('could not parse posted_date'))
                page['posted_date'] =  datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")    
                
            try:
                page['et_author_name'] = stripHtml(review.find('span','mbg-nw').renderContents())
            except:
                log.exception(self.log_msg('could not parse author name'))
            copyuri = self.currenturi 
            try:
                page['et_author_profile']=review.find('span','mbg-nw').findParent('a')['href']
                self.currenturi = page['et_author_profile']
                self.__setSoup()
                try:
                    member_info = stripHtml(self.soup.find('div' , {'id':'MemberInfo'}).renderContents())
                    info_dict = dict([line.split(':') for line in re.split(re.compile(r'\n+'),member_info) if line.strip()])
                    info_dict = dict( [ [k.strip(),v.strip()] for k,v in info_dict.items()])
                except:
                    info_dict = {}
                    log.info(self.log_msg('could not parse member information'))
                if info_dict.get('Location'):
                    page['et_author_location'] = info_dict['Location'].strip()
                if info_dict.get('Member since'):
                    member_since = info_dict['Member since'].strip()
                    page['edate_author_member_since'] = datetime.strftime(datetime.strptime(member_since,'%b-%d-%y'),"%Y-%m-%dT%H:%M:%SZ")
                self.currenturi = copyuri
            except:
                log.exception(self.log_msg('could not parse author profile link'))    
            try:    
                review_hash = get_hash( page )    
                unique_key = get_hash({'data' : page['data'], 'posted_date' : page['posted_date']})
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,self.\
                                task.instance_data.get('update'),parent_list=[ self.parenturi ]):
                    log.info(self.log_msg('Session info returns True for uri %s'% \
                                                                unique_key))
                    return False	
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key \
                                    , review_hash,'Review', self.task.instance_data.get('update'), \
                                                                        parent_list=[ self.parenturi ])
                if result['updated']:
                    parent_list = [ self.parenturi ]
                    page['parent_path'] = copy.copy( parent_list )
                    page['path']=parent_list
                    page['entity'] = 'Review'
                    page['uri'] = normalize( self.currenturi )
                    page['uri_domain'] = urlparse(page['uri'])[1]
##                    parent_soup = copy.copy(self.soup)
##                    try:
##                        if self.task.instance_data.get('pick_user_info') and page.get('et_author_profile'):
##                            self.__getUserInfo(page , page['et_author_profile'])
##                        else:
##                            log.info(self.log_msg('could not get user profile link or pick_user_info option is not enabled'))
##                    except:
##                            log.exception(self.log_msg('could not parse user information'))
##                    self.soup = parent_soup
                    page.update(self.__task_elements_dict)
                    self.pages.append(page)
                    log.info('page added for %s'%self.currenturi)
                else:
                    log.exception(self.log_msg('Update session info returns False for \
                                                    url %s'%self.currenturi))
            except:
                log.exception(self.log_msg('Cannot add the post for the uri %s'%self.currenturi))
        return True    

##    @logit(log , 'getuserinfo')
##    def __getUserInfo(self,page,profile_url):
##        try:
##            self.currenturi = profile_url
##            self.__setSoup()
##            try:
##                member_info = stripHtml(self.soup.find('div' , {'id':'MemberInfo'}).renderContents())
##                info_dict = dict([line.split(':') for line in re.split(re.compile(r'\n+'),member_info) if line.strip()])
##                info_dict = dict( [ [k.strip(),v.strip()] for k,v in info_dict.items()])
###                page['et_author_location'] = info_dict[' Location'].strip()
##            except:
##                info_dict = {}
##                log.info(self.log_msg('could not parse member information'))
##                
##            if info_dict.get('Location'):
##                page['et_author_location'] = info_dict['Location'].strip()
##            if info_dict.get('Member since'):
##                member_since = info_dict['Member since'].strip()
##                page['edate_author_member_since'] = datetime.strftime(datetime.strptime(member_since,'%b-%d-%y'),"%Y-%m-%dT%H:%M:%SZ")
##        except:
##            log.exception(self.log_msg('could not parse user information'))
    
    
    @logit(log, "setSoup")
    def __setSoup(self, data=None, headers={}):
        """It will set soup object for the Current URI
        """
        res = self._getHTML(data=data, headers=headers)
        if res:
            self.rawpage = res['result']
        else:
            raise Exception('Page content not fetched for th url %s'%self.currenturi)
        self._setCurrentPage()