'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#KRITHIKA

import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging. getLogger("DellConnector")

class DellConnector(BaseConnector):
    @logit(log,'fetch')
    def fetch(self):
        """It fetches the data from the url mentioned
        """
        self.genre = "Review"
        try:
            self.parent_uri = self.currenturi
            if not self._setSoup():
                log.info(self.log_msg("Soup not set,returning false"))
                return False
            if not self._getParentPage():
                log.info(self.log_msg("Parent page not found"))
            while True:
                if self.__review_soup_set:
                    self.__addReviews()
                    try:
                        self.currenturi =  self.soup.find('td',id='BVReviewPaginationNextLinkCell').find('a')['href']
                        if not self._setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next page not found'))
                        break
            return True
        except:
            log.exception(self.log_msg("Exception in fetch"))
            return False
        
    @logit(log,'getparentpage')    
    def _getParentPage(self):
        """It fetches the product information
        """
        page = {'uri': self.currenturi,'data':'' }
        try:
            page['title'] = stripHtml(self.soup.find('span','emphasizedtitle').renderContents())+ stripHtml(self.soup.find('span','emphasizedtitle_suffix').renderContents())
        except:
            log.exception(self.log_msg("No title found!!"))
            return False 
        main_page_soup = copy.copy(self.soup)
        try:
            # get the specification
            self.currenturi = 'http://'+ urlparse.urlparse(self.currenturi)[1] + eval(re.findall ("'.*?'",self.soup.find('font',text='Tech Specs').findParent('a')['href'])[-1])         
            self._setSoup()
            page.update(dict([('et_product_' + re.sub('[^\w]+','_',stripHtml(y.renderContents()).lower()),stripHtml(x.renderContents())) for x,y in zip([each.findParent('tr') for each in self.soup.find('div',id='cntTabsCnt').findAll('td',colspan='2')],[each.findParent('tr') for each in self.soup.find('div',id='cntTabsCnt').findAll('td','titlestylelight')])]))
        except:
            log.exception(self.log_msg("Specifications not found!!"))
        try:
            self.currenturi = main_page_soup.find('iframe')['src'].replace('?format=noscript',' ')
            if self._setSoup():
                self.__review_soup_set = True
                try:
                    page['ef_product_rating_overall'] = stripHtml(self.soup.find('span','BVStandaloneRatingSetAverageRatingValue average').renderContents())
                except:
                    log.info(self.log_msg("Overall rating not found!"))
                try:
                    page['ei_data_recommended_count'] = stripHtml(self.soup.find('span','BVStandaloneRatingWrapperBuyAgainValue').renderContents())     
                except:
                    log.info(self.log_msg("Number of people recommended not found!!"))                        
            else:
                self.__review_soup_set = False
        except:
            log.info(self.log_msg('review soup not set'))
            self.__review_soup_set = False
        try:
            if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri, \
                                self.task.instance_data.get('update')):
                log.info(self.log_msg('Session infor return True'))
                return False
            result = updateSessionInfo(self.genre, self.session_info_out,self.parent_uri, get_hash(page) ,'Post',self.task.instance_data.get('update')) #, Id=id)
            if not result['updated']:
                return False
            page['path'] = [self.parent_uri]
            page['parent_path'] = []
            page['uri'] = normalize(self.parent_uri)
            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
            page['priority'] = self.task.priority
            page['level'] = self.task.level
            page['posted_date'] = page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            page['client_name'] = self.task.client_name
            page['last_updated_time'] = page['pickup_date']
            page['versioned'] = False
            page['task_log_id'] = self.task.id
            page['entity'] = 'Post'
            page['category'] = self.task.instance_data.get('category','')
            self.updateParentExtractedEntities(page)
            self.pages.append(page)
            log.info(self.log_msg('Parent Page added'))
            return True
        except:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False
        
    @logit(log, '_addreviews')
    def __addReviews(self):
        
        try:
            reviews = self.soup.findAll('td',attrs={'class':re.compile('BVStandaloneReviewSectionReview')})
            log.info(self.log_msg('no of reviews is %s'%len(reviews)))
            if not reviews:
                return False
        except:
            log.exception(self.log_msg('No Reviews are found'))
            return False
        for review in reviews:
            page = {}
            page['uri'] = self.currenturi
            try:  
                page['title'] = stripHtml(review.find('span','BVreviewTitle').renderContents()) 
            except:
                log.info(self.log_msg("No title found!!"))
                page['title'] = ''
            try:
                page['et_author_name'] = stripHtml(review.find('span','BVReviewerNickname BVReviewerNicknameText reviewer').renderContents())
            except:
                log.info(self.log_msg("Author name not specified!!"))    
            try:
                page['ef_rating_overall'] = review.find('td','BVcustomerRating').find('img')['alt'].replace('out of 5',' ').strip()
            except:
                log.info(self.log_msg("Individual rating not given"))
            try:
                page['et_product_used_period'] = stripHtml(review.find('span', 'BVReviewValue BVReviewValueUsedProduct').renderContents())
            except:
                log.info(self.log_msg("Duration of product uised not given"))
            try:        
                page['et_author_expert_level'] = stripHtml(review.find('span','BVReviewValue BVReviewValueTechExpertise').renderContents())
            except:
                log.info(self.log_msg("Level of expertise is not mentioned"))
            try:
                page.update(dict([('ef_rating_' + stripHtml(x.find('td','BVcustomerRatingItem').renderContents()).lower().replace(' ','_')[:-1],float(stripHtml(x.find('span','BVratingSummaryFinal').renderContents()))) for x in review.find('div','BVSecondaryRatings').findAll('tr')]))
            except:
                log.info(self.log_msg("Extra features not found!!"))
            review_str = review.find('div','BVcontent').renderContents()    
            try:
                page['et_data_pros'] = stripHtml(re.search(re.escape(review.find('span',text='Pros:&#160;').parent.__str__())+'(.*?)<span.*?>',review_str,re.DOTALL).group(1)).replace('>','')
            except:
                log.info(self.log_msg("Pros for the data is not there"))
            try:
                page['et_data_cons'] = stripHtml(re.search(re.escape(review.find('span',text='Cons:&#160;').parent.__str__())+'(.*?)<span.*?>',review_str,re.DOTALL).group(1)).replace('>','')
            except:
                log.info(self.log_msg("No data cons found!!"))
            try:
                page['data'] = stripHtml(review.find('span','description').renderContents())
            except:
                data = ''
                try:
                    if page.get('et_data_pros'):
                        data = data + page['et_data_pros']
                    if page.get('et_data_cons'):
                        data = data + page['et_data_cons']
                    if data.strip()=='':
                        log.info(self.log_msg('No data found'))
                        continue
                    page['data'] = data.strip()
                except:
                    log.info(self.log_msg('No data found'))
                    continue
            try:
                page['posted_date'] = datetime.strftime(datetime.strptime(review.find('span','BVdateCreated').span.next.next.strip(),'%d %B, %Y'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                log.info(self.log_msg("Posted date not mentioned!!"))
            try:
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                        =[self.parent_uri]):
                    log.info(self.log_msg('session info return True'))
                    return False
                    
                result = updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                    get_hash(page),'Review', self.task.instance_data.get('update'),\
                                                parent_list=[self.parent_uri])

                if not result['updated']:
                    log.info(self.log_msg('result not updated'))
                    continue
                page['path'] = page['parent_path'] = [self.parent_uri]
                page['path'].append(unique_key)
                page['priority'] = self.task.priority
                page['level'] = self.task.level
                page['last_updated_time'] = page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                page['client_name'] = self.task.client_name
                page['versioned'] = False
                page['entity'] = 'Review'
                page['category'] = self.task.instance_data.get('category','')
                page['task_log_id'] = self.task.id
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append(page)
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))
                
    
        
    
                

    @logit(log,'setSoup')
    def _setSoup(self, url=None, data=None, headers={}):
        """
            It will set the uri to current page, written in seperate
            method so as to avoid code redundancy
        """
        if url:
            self.currenturi = url
        try:
            log.info(self.log_msg( 'for uri %s' %(self.currenturi) ))
            res = self._getHTML(data=data, headers=headers)
            if res:
                self.rawpage = res['result']
            else:
                log.info(self.log_msg('self.rawpage not set.... so Sorry..'))
                return False
            self._setCurrentPage()
            return True
        except Exception, e:
            log.exception(self.log_msg('Page not for  :%s' %uri))
            raise e

    
        
                                                
                    
                
                
    
    
        