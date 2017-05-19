'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#prerna

import re
import logging
from urllib2 import urlparse
from datetime import datetime
from cgi import parse_qsl
from BeautifulSoup import BeautifulSoup
from urllib2 import urlopen
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging. getLogger("PhilipsConnector")

class PhilipsConnector(BaseConnector):    
    """Connector for philips.com 
    """
    @logit(log,'fetch')
    def fetch(self):
        """
        It fetches the data from the url mentioned
        sample url : http://www.usa.philips.com/c/home-theater-systems/1000w-rms-power-dvd-playback-hts3371d_f7/prd/en/;jsessionid=FBDDBE0415040FDCBC20A9F115B12657.app102-drp1?st=user&t=awards"""
        self.__genre = "Review"
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
        try:
            self.__setSoupForCurrentUri()
            self.parenturi = self.currenturi
            product_id = self.currenturi.split('/prd/en')[0].split('-')[-1]
            if product_id:
                self.currenturi = 'http://philips.ugc.bazaarvoice.com/7543b-en_us/' +product_id+ '/reviews.djs?format=embeddedhtml'
                #http://philips.ugc.bazaarvoice.com/7543b-en_us/55PFL7705D_F7/reviews.djs?format=embeddedhtml
                s = urlopen(self.currenturi).read()
                ss1 = [x for x in s.splitlines() if x.startswith('var materials=')][0].strip()
                ss2 = ss1.replace('var materials={"BVRRSecondaryRatingSummarySourceID":" ', '')[:-2].\
                        replace("\\n",'').replace("\\r",'').replace("\\",'')
                self.soup = BeautifulSoup(ss2)
            while True:
                if not self.__addReviews():
                    log.info(self.log_msg('fetched all posts')) 
                    break 
                try:
                    self.currenturi = self.soup.find('a', title ='next')['data-bvjsref']
                    s = urlopen(self.currenturi).read()
                    ss1 = [x for x in s.splitlines() if x.startswith('var materials=')][0].strip()
                    ss2 = ss1.replace('var materials={"BVRRSecondaryRatingSummarySourceID":" ', '')[:-2].\
                            replace("\\n",'').replace("\\r",'').replace("\\",'')
                    self.soup = BeautifulSoup(ss2)
                except:
                    log.info(self.log_msg('Next page not found for the url %s'\
                                                            %self.currenturi)) 
                    break 
            return True
        except:
            log.exception(self.log_msg("Exception in fetch %s"%self.currenturi)) 
        return True
        
    @logit(log, '__addReviews')
    def __addReviews(self):
        '''
            It will fetch the the reviews from the given review uri
            and append it  to self.pages
        '''
        reviews = self.soup.findAll('div',id = re.compile('BVRRDisplayContentReviewID_\d+'))
        log.info(self.log_msg('no of reviews is %s' % len(reviews)))
        if not reviews:
            return False
        for review in reviews:
            page = {'uri':self.currenturi}
            try:
                unique_key = review['id'].split('_')[-1]
                if checkSessionInfo(self.__genre, self.session_info_out, unique_key, \
                             self.task.instance_data.get('update'), parent_list\
                                                        =[self.task.instance_data['uri']]):
                    log.info(self.log_msg('session info return True'))
                    return False
            except:
                log.exception(self.log_msg('Unique key cannot be fetched for url %s'\
                                                                    %self.currenturi))
            try:
                data_str = review.find('span','BVRRReviewText').renderContents()
                if data_str:
                    page['data'] = stripHtml(data_str.replace('<br />>', ''))
                    #log.info(self.log_msg('data: %s'%page['data']))
                #title_tag = review.find('span', 'BVRRValue BVRRReviewTitle')
                title_tag = review.find('span', 'BVRRValue BVRRReviewTitle')
                if title_tag:
                    page['title'] =  stripHtml(title_tag.renderContents())
                    #log.info(self.log_msg('title: %s'%page['title']))
                else:
                    page['title'] = ''    
            except:
                log.exception(self.log_msg('data cannot be fetched for url %s'\
                                                                    %self.currenturi))
                page['data'] = ''                   
                page['title'] =''
                            
            #Check that both title and data are not absent, so baseconnector does not throw exception    
            if not page['title'] and not page['data']:
                log.info(self.log_msg("Data and title not found for %s,"\
                    " discarding this review"%(self.currenturi)))
                continue
            try:
                date_str = stripHtml(review.find('span', 'BVRRValue BVRRReviewDate').\
                            renderContents()).strip()
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str\
                                                ,'%d %B %Y'),"%Y-%m-%dT%H:%M:%SZ")
                #log.info(self.log_msg('posted: %s'%page['posted_date']))                                
                
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                log.exception(self.log_msg('Posted date cannot be fetched for url\
                                                                %s'%page['uri']))
            rating = {'ef_rating_overall':'BVRRRatingOverall_Review_Display',\
                        'ef_rating_feature':'BVRRRatingPerformance_Review_Display',\
                        'ef_rating_quality':'BVRRRatingDesign_Review_Display',\
                        'ef_rating_ease_of_use':'BVRRRatingEaseOfUse_Review_Display'}
            for each in rating.keys():
                try:
                    rating_tag = review.find('div', id= rating[each])
                    if rating_tag:
                        page[each] = float(stripHtml(rating_tag.find('img')['title'].split('/')[0]))
            #            log.info(self.log_msg('%s:%s'%(each,page[each])))
                except:
                    log.exception(self.log_msg('rating not found' ) )                                                            
                                                                                                                                                                                                
            try:
                page['et_author_name'] = stripHtml(review.find('span', 'BVRRNickname').renderContents())
                
            except:
                log.exception(self.log_msg("Author name cannot be fetched for url %s\
                                                                "%page['uri']))
            author_info = {'et_author_gender':'BVRRValue BVRRContextDataValue BVRRContextDataValueGender', 'et_author_location':'BVRRValue BVRRUserLocation', 'et_author_age':'BVRRValue BVRRContextDataValue BVRRContextDataValueAge'}
            for each in author_info.keys():
                try:
                    info_tag = review.find('span', author_info[each])
                    if info_tag:
                        page[each] = stripHtml( info_tag.renderContents())
             #           log.info(self.log_msg('%s:%s'%(each,page[each])))    
                except:
                    log.exception(self.log_msg('Pros and cons not found' ) )                                                    
            try:
                helpful_tag = review.find('span','BVDI_FVVote BVDI_FVPositive BVDI_FVLevel1')
                if helpful_tag:
                    page['ei_data_helpful_count'] = int(stripHtml(helpful_tag.find('span','BVDINumber').\
                        renderContents()))
            except:
                log.exception(self.log_msg("data helful count not fetched for the uri %s"%page['uri']))    
                
            pros_cons = {'et_data_cons':'BVRRValue BVRRReviewConTags', 'et_data_pros':'BVRRValue BVRRReviewProTags'}
            for each in pros_cons.keys():
                try:
                    html_tag = review.find('span', pros_cons[each])
                    if html_tag:
                        page[each] = stripHtml( html_tag.renderContents())
                except:
                    log.exception(self.log_msg('Pros and cons not found' ) )
            try:
                result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                    get_hash(page), 'Review', self.task.instance_data.get('update'), \
                                                parent_list=[self.task.instance_data['uri']])
                if not result['updated']:
                    log.info(self.log_msg('result not updated'))
                    continue
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [self.task.instance_data['uri'], unique_key]
                page['uri'] = self.currenturi + '#' + unique_key
                page['entity'] = 'Review'
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                page.update(self.__task_elements_dict)
                self.pages.append(page)
                #log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Exception while adding page for the\
                                                            url %s'%page['uri']))
        return True
    
    @logit(log,'setparentpage')    
    def __setParentPage(self):
        """It fetches the product information
        """
        page = {'uri': self.task.instance_data['uri']}
        try:
            page['data'] = page['title'] = stripHtml(self.soup.find('span', id='ProductMainInfo_ProductTitle').\
                            renderContents())
        except:
            log.exception(self.log_msg("Exception Occurred while fetching the title!!"))
            return False
        try:
            page['et_product_secondary_title'] = stripHtml(self.soup.find('span', id='ProductMainInfo_ProductSecondaryTitle').\
                    renderContents())
        except:
            log.exception(self.log_msg("Exception Occurred while fetching the title!!"))
        try:
            page['et_product_sku_id'] = stripHtml(self.soup.find('span', id='ProductMainInfo_ItemNumber').\
                renderContents()).split('#')[-1].strip()
        except:
            log.info(self.log_msg("Exception while getting the Product Id in url :%s"\
                                                            %self.currenturi ))
        try:
            page['et_product_price'] = stripHtml(self.soup.find('span', id='ProductMainInfo_Price').renderContents())
        except:
            log.info(self.log_msg("Exception while getting the Product PRice  in url :%s"\
                                                            %self.currenturi ))
        self.__goToReviewsPage()
        try:
            page['ef_product_rating_overall'] = float(re.search('\d+', self.soup.find('td', ' rating').img['alt']).group())
        except:
            log.info(self.log_msg("Exception while getting the Product Rating in url :%s"\
                                                                    %self.currenturi ))
        try:
            page['ei_product_recommended_count'] = int(stripHtml(self.soup.find('span', 'BVStandaloneRatingWrapperBuyAgainValue').renderContents()))
        except:
            log.info(self.log_msg("Exception while getting the Product Rating in url :%s"\
                                                                    %self.currenturi ))
        try: 
            self.updateParentExtractedEntities(page) 
            if checkSessionInfo(self.__genre, self.session_info_out, self.task.instance_data['uri'],  \
                                self.task.instance_data.get('update')):
                log.info( self.log_msg('Session info return True for the url %s\
                                                            '%self.currenturi) )
                return False
            result = updateSessionInfo(self.__genre, self.session_info_out, self.task.instance_data['uri'], \
                    get_hash(page) ,'Post', self.task.instance_data.get('update'))
            if not result['updated']:
                return False
            page['path'] = [self.task.instance_data['uri']]
            page['parent_path'] = []
            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])            
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['entity'] = 'Post'
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            log.info(self.log_msg('Parent Page added'))
        except:
            log.exception(self.log_msg("parent post couldn't be parsed for url \
                                                        %s"%self.currenturi))
    
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
    