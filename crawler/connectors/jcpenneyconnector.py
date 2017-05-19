
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

# Skumar

import re
from datetime import datetime
import logging
from urllib2 import urlparse
import copy
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('JcPenneyConnector')
class JcPenneyConnector(BaseConnector):
    '''
    This will fetch the info for the http://www4.jcpenney.com
    Test with sample uri
    
    uri = http://www4.jcpenney.com/jcp/X6.aspx?DeptID=61482&CatID=61733&GrpTyp=PRD&ItemID=14771a1&attrtype=&attrvalue=&CMID=61482|61731&Fltr=&Srt=&QL=F&IND=3&CmCatId=61482|61731|61733#reviews
    soup = BeautifulSoup ( urlopen(uri) )
    review_url = soup.find('div',id='BVdefaultURL').find('a')['href']
    take all from review url
    
    Actual Reviews are found in 


    '''

    @logit(log , 'fetch')
    def fetch(self):
        self.genre="Review"
        try:
##            self.currenturi = 'http://www.finishline.com/store/catalog/product.jsp?productId=prod621942#reviews'
            self.parent_uri = self.currenturi 
            if not self._setSoup():
                return False
            if not self.__getParentPage():
                log.info(self.log_msg('Parent Page return False'))
            while True:
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
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log , '__addReviews')
    def __addReviews(self):
        """
        It will add the reviews,
        reviews =soup.findAll('table','BVReviewDisplay')
        for review in all_reviews:
            
            rating_dict = {'ef_rating_overall':'overall rating:',\
                            'ef_rating_value' :'Value:',\
                            'ef_rating_fit':'Fit:',\
                            'ef_rating_styling':'Styling:'\
                            }
            for each in rating_dict:
                page[each] =float(review.find('td',text=rating_dict[each]).findParent('tr').find('span','BVratingSummaryFinal').renderContents())
            author_info_dict = {'et_author_name':'BVReviewerNickname BVReviewerNicknameText',\
                                'et_author_age_group':'BVReviewValue BVReviewValueAge',\
                                'et_author_gender':'BVReviewValue BVReviewValueGender',\
                                'et_author_shopping_frequency':'BVReviewValue BVReviewValueShopFrequency'\
                                }
            for each in author_info_dict:
                page[each] = stripHtml(review.find('span',author_info_dict[each]).renderContents())
            author location = review.find('span','BVreviewerLocation').find('span','BVReviewLabel').nextSibling.strip()            
            author_contributional_title = stripHtml(review.find('td','BVtop1000ContributorTitle').renderContents())
            title = stripHtml(review.find('span','BVreviewTitle').renderContents())
            data = stripHtml(review.find('span','BVContentReviewText').renderContents())
            pros_cons = {'et_data_cons':'BVConsTitle','et_data_pros':'BVProsTitle'}
            for each in pros_cons.keys():
                try:
                    page[each] = review.find('span',pros_cons[each]).nextSibling
                except:
                    pass
            posted_date = datetime.strftime( datetime.strptime(review.find('span','BVdateCreated').find('span','BVReviewLabel').nextSibling.strip(),"%B %d, %Y" ),"%Y-%m-%dT%H:%M:%SZ")
            data_helpful_cout = int(stripHtml(soup.find('span','BVrespondedHelpful BVrespondedHelpfulPositive').renderContents()))        
        """
        try:
            reviews =  self.soup.findAll('table','BVReviewDisplay')
            log.info(self.log_msg('no of reviews is %s'%len(reviews)))
            if not reviews:
                return False
        except:
            log.exception(self.log_msg('No Reviews are found'))
            return False
        for review in reviews:
            page = {}
            page['uri'] = self.currenturi
            rating_dict = {'ef_rating_overall':'overall rating:',\
                            'ef_rating_value' :'Value:',\
                            'ef_rating_fit':'Fit:',\
                            'ef_rating_styling':'Styling:'\
                            }
            for each in rating_dict:
                try:
                    page[each] =float(review.find('td',text=rating_dict[each]).findParent('tr').find('span','BVratingSummaryFinal').renderContents())            
                except:
                    log.info(self.log_msg('Rating not found'))
            try:
                page['et_author_name'] =  re.sub('^Written By:','',stripHtml(review.find('span','BVReviewer').renderContents())).strip()
            except:
                log.info(self.log_msg('Author name not found'))
                    
            author_info_dict = {'et_author_age_group':'BVReviewValue BVReviewValueAge',\
                                'et_author_gender':'BVReviewValue BVReviewValueGender',\
                                'et_author_shopping_frequency':'BVReviewValue BVReviewValueShopFrequency'\
                                }
            for each in author_info_dict:
                try:
                    page[each] = stripHtml(review.find('span',author_info_dict[each]).renderContents())            
                except:
                    log.info(self.log_msg('author %s not found'%each) )
            try:
                page['et_author_location'] = stripHtml(review.find('span','BVreviewerLocation').find('span','BVReviewLabel').nextSibling)
            except:
                log.info(self.log_msg('author Location not found'))
            try:
                page['et_author_title'] = stripHtml(review.find('td','BVtop1000ContributorTitle').renderContents())
            except:
                log.info(self.log_msg('author title not found'))
            try:
                page['title'] = stripHtml(review.find('span','BVreviewTitle').renderContents())
            except:
                log.exception(self.log_msg('title not found'))
                page['title'] =''
            try:
                page['posted_date'] = datetime.strftime( datetime.strptime(review.find('span','BVdateCreated').find('span','BVReviewLabel').nextSibling.strip(),"%B %d, %Y" ),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception(self.log_msg('posted date  not found'))
                page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")            
            pros_cons = {'et_data_cons':'BVConsTitle','et_data_pros':'BVProsTitle'}
            for each in pros_cons.keys():
                try:
                    page[each] = stripHtml( review.find('span',pros_cons[each]).nextSibling )
                except:
                    log.info(self.log_msg('Pros and cons not found' ) )
            try:
                page['data'] = stripHtml(review.find('span','BVContentReviewText').renderContents())
            except:
                log.info(self.log_msg('data not found'))
                page['data'] =''
            try:
                page['ei_data_recommended_yes'] = int(stripHtml(review.find('span','BVrespondedHelpful BVrespondedHelpfulPositive').renderContents()))
            except:
                log.info(self.log_msg('ei_data_recommended_yes  count not found'))
            try:
                if page['title'] =='':
                    if len(page['data']) > 50:
                        page['title'] = page['data'][:50] + '...'
                    else:
                        page['title'] = page['data']
            except:
                log.exception(self.log_msg('title not found'))
                page['title'] = ''
            try:
                review_hash = get_hash(page)
                unique_key =  re.search('reviewID=([^\&]*)', review.find('a',"BVSocialBookmarkingSharingLink")['href'].replace('%3D','=')).group(1)
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                                 self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    log.info(self.log_msg('Session info return True'))
                    continue
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            review_hash,'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    log.info(self.log_msg('result not updated'))
                    continue
                parent_list = [self.parent_uri]
                page['parent_path'] = copy.copy(parent_list)
                parent_list.append(review_hash)
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
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))

    @logit(log , 'getParentPage')
    def __getParentPage(self):
            """
            It will fetch the Product info
            For Testing in Ipython            
            price = re.sub('^Now','',stripHtml(psoup.find('span','X6PromotionNowText').renderContents())).strip()
            change the current uri to Sample Uri : http://reviews.jcpenney.com/1573/83824/reviews.htm
            currenturi = soup.find('div',id='BVdefaultURL').find('a')['href']        
            rating = float(re.search('^(\d) out of 5',soup.find('td','BVStandaloneRatingSetAverageRatingLabel').findNext('td').find('img')['alt']).group(1))
            title = stripHtml(soup.find('span','BVproductname').renderContents())
             product Recommendation = int(stripHtml(soup.find('span','BVStandaloneRatingWrapperBuyAgainValue').renderContents()))
            product Desc = stripHtml(soup.find('p','BVStandaloneHeaderProductDescription').renderContents())                       
            
            
            """
            page={}
            try:
                product_price = re.sub('^Now','',stripHtml(self.soup.find('span','X6PromotionNowText').renderContents())).strip()
                self.updateProductPrice(product_price)
            except:
                log.info(self.log_msg("couldn't parse product price"))
            try:
                page['et_product_price'] = re.sub('^Now','',stripHtml(self.soup.find('span','X6PromotionNowText').renderContents())).strip()
            except:
                log.exception(self.log_msg('could not parse page title'))

            try:
                self.currenturi = self.soup.find('div',id='BVdefaultURL').find('a')['href']
            except:
                log.info(self.log_msg('Actual Review page not found'))
                return False
            if not self._setSoup():
                log.info(self.log_msg('review url page not set , Returning False from Fetch'))
                return False
            try:
                page['ef_product_rating_overall'] = float(re.search('^(\d) out of 5',self.soup.find('td','BVStandaloneRatingSetAverageRatingLabel').findNext('td').find('img')['alt']).group(1))
            except:
                log.info(self.log_msg('Ratings are not found'))
            try:
                page['ei_product_recommended_count'] = int(stripHtml(self.soup.find('span','BVStandaloneRatingWrapperBuyAgainValue').renderContents()))
            except:
                log.info(self.log_msg('Product recommended count not found'))
            try:
                page['et_product_desc'] = stripHtml(self.soup.find('p','BVStandaloneHeaderProductDescription').renderContents())
            except:
                log.info(self.log_msg('Product desc is not found') )
            try:
                page['title'] = stripHtml(self.soup.find('span','BVproductname').renderContents())
            except:
                log.info(self.log_msg('Product desc is not found') )
                page['title'] = ''
            try:
                if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri \
                                                , self.task.instance_data.get('update')):
                    log.info(self.log_msg('Session infor return True'))
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
                page['data'] = ''
                page['task_log_id']=self.task.id
                page['entity'] = 'Post'
                page['category']=self.task.instance_data.get('category','')
                self.pages.append(page)
                log.info(self.log_msg('Parent Page added'))
                return True
            except Exception,e:
                log.exception(self.log_msg("parent post couldn't be parsed"))
                return False
    
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
            log.exception(self.log_msg('Page not for  :%s'%url))
            raise e
