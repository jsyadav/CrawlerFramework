
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Pratik
from baseconnector import BaseConnector
from BeautifulSoup import BeautifulSoup
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from urllib2 import *
import re
import copy
from datetime import datetime
import traceback
import md5
from tgimport import *
import pickle
import logging
from utils.sessioninfomanager import checkSessionInfo,updateSessionInfo

log = logging.getLogger('CircuitCityConnector')

class CircuitcityConnector(BaseConnector):

    @logit(log,'_createSiteUrl')
    def _createSiteUrl(self,code):
        return 'http://www.circuitcity.com/ccd/Search.do?keyword=%s&searchSection=All'%(code)

    @logit(log,'fetch')
    def fetch(self):
        try:
            # TESTING SENTIMENT EXTRACTION
            #rcheck = re.compile(r'body=REVIEWS',re.U)
##            if not rcheck.search(urlparse.urlparse(self.currenturi)[4]):
##                self.task.status['fetch_status']=False
##                log.info(self.log_msg('page other that this could not be parsed , so returning'))
##                return False
            self.genre='Review'
            self.parent_url=self.currenturi
            self.review_count=0
            if not self._setSoup():
                return False
            if not self._getParentPage():
                log.info(self.log_msg('Parent page not posted'))
            count = 1
            counts = 1
            while True:
                parent_soup=self.soup
                if not self._addReviews():
                    break
                try:
                    if len(parent_soup.findAll('a',{'class':'PageNav'})) < 2 and (count > 1):
                        break
                    self.currenturi='http://www.circuitcity.com'+parent_soup.findAll('a',{'class':'PageNav'})[-1]['href'].replace('\r','').replace('\t','').replace('\n','')
                    log.info(self.log_msg('Next page is set as %s' % self.currenturi))
                    #if counts > 7:
                        #break
                    res=self._getHTML()
                    if not res:
                        break
                    self.rawpage=res['result']
                    self._setCurrentPage()
                    count = count+1
                    counts =counts+1
                except:
                    log.exception(self.log_msg('Exception in finding out Next_page'))
                    break

            return True
        except Exception,e:
            log.exception(self.log_msg('Exception in Fetch()'))
            return False

    @logit(log,'addreviews')
    def _addReviews(self):
        exreviews = [each.parent.parent for each in self.soup.findAll('table',{'class':'font_size6'})]
        log.info(self.log_msg('no of reviews found on (%s) = %d' %(self.currenturi , len(exreviews))))
        for review in exreviews:
            self.review_count=self.review_count+1
            page={}
            try:
                tvalue = stripHtml(review.find('td',{'class':'CustomerReviewHeading'}).find('span').renderContents())
                rc = re.compile(r'Reviewer:',re.U)
                if rc.search(tvalue):
                    page['title']=''
                else:
                    page['title']=tvalue
            except:
                log.exception(self.log_msg('exception in getting Title for review'))
                page['title'] = ''
            author_tmp =stripHtml(review.find('td',{'class':'CustomerReviewHeading'}).find('span',{'class':'CustomerReviewer'}).renderContents())
            author_tmp = author_tmp.encode('ascii','replace')
            author_tmp = author_tmp.replace('\r','').replace('\t','').replace('?','')
            author_ = author_tmp.split('on\n')
            try:
                page['et_author_name'] = author_[0].replace('Reviewer:\n','')
            except:
                log.info(self.log_msg('exception in getting Author name'))
            try:
                date_str = author_[1]
                log.info(date_str)
                page['posted_date']=datetime.strftime(datetime.strptime(date_str,'%b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception(self.log_msg('exception in getting posted_date'))
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            try:
                rating_str =stripHtml(review.find('td',{'class':'CustomerReviewHeading'}).find('span',{'class':'ReviewCustomerRating'}).renderContents()).split('\n')[-1].replace('\t','')
                #rating_str = rating_str.encode('ascii','replace')
                #rating_str = rating_str.replace('\n','').replace('\r','').replace('\t','').replace('?','')

                page['et_customer_rating']= float(rating_str.strip())
            except:
                log.exception(self.log_msg('exception in Product rating'))
            try:
                page['data'] = stripHtml(review.find('td',{'class':'ReviewCustomerText'}).renderContents())
            except:
                page['data']=''
                log.info(self.log_msg('exception in fetching data'))
            try:
                ratings = review.find('table',{'class':'ReviewCustomerChartInside'}).findAll('tr')
                for rating in ratings[:-1]:
                    feature  = stripHtml(rating.renderContents()).split('\n\n')[0].strip()
                    value = stripHtml(rating.renderContents()).split('\n\n')[1].strip()
                    page['ef_rating_'+feature] = float(value)
            except:
              log.info(self.log_msg('could not parse review ratings'))
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
                review_hash = get_hash( page )
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
            
                if checkSessionInfo(self.genre, self.session_info_out,
                                unique_key,  self.task.instance_data.get('update'),
                                parent_list=[self.parent_url]):
                    log.info(self.log_msg('reached last crawled page returning'))
                    continue
                
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            review_hash,'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_url])
                if not result['updated']:
                    continue
                parent_list = [ self.parent_url ]
                page['parent_path'] = copy.copy(parent_list)
                parent_list.append( unique_key )
                page['path']=parent_list
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
                self.pages.append(page)
                log.info(page)
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))
        return True

    @logit(log , '_getParentPage')
    def _getParentPage(self):
            ##J- I think these needs to be in a try except- if th title fails or rating fails - coz the html changed---what crash?
            ## a try-except-raise
            try:
                page={}
                try:
                    ratings = self.soup.find('table',{'class':'ReviewMainChartInside'}).findAll('tr')
                    #page['ef_rating_overall'] = float(stripHtml(ratings[-1].findAll('td')[-1].renderContents()))
                    for rating in ratings[:-1]:
                        feature = stripHtml(rating.td.renderContents()).strip()
                        value = stripHtml(rating.find('td',{'align':'center'}).renderContents())
                        page['ef_product_rating_overall_'+feature] = float(value.strip())
                except:
                    log.info(self.log_msg('could not parse post avg feature ratings'))
                try:
                    overall_str = stripHtml(self.soup.find('span',{'class':'ReviewMainHeadingBottom'}).renderContents())
                    overall_str = overall_str.encode('ascii','replace')
                    overall_str = overall_str.replace('\r','').replace('\t','').replace('?','')
                    page['ef_product_rating_overall'] = float(overall_str.split('\n\n')[0].replace('\n','').replace('Customer Rating:','').strip())
                    page['ei_reviews_count']=int(overall_str.split('\n\n')[1].replace('Customer Reviews:','').strip())
                except:
                    log.info(self.log_msg('could not parse post avg. ratings'))
                try:
                    page['title'] = stripHtml(self.soup.find('td',{'class':'itemDetailsHeader'}).renderContents())
                except Exception,e:
                    log.exception(self.log_msg('could not parse post title'))
                    raise e
                try:
                    pro_info = self.soup.findAll('td',{'class':'font_right_prod','valign':'top'})
                    for info in pro_info:
                        feature2 = stripHtml(info.renderContents()).replace(':','').strip()
                        value2 = stripHtml(info.nextSibling.next.renderContents()).strip()
                        page['et_product_'+ feature2]= value2
                        
                except:
                    log.info(self.log_msg("product extra info cant be parsed"))
                try:
                    prices = self.soup.find('table',{'id':'myPrice'}).findAll('tr')
                    for price in prices:
                        if len(stripHtml(price.renderContents())) > 1:
                            feature1 = stripHtml(price.renderContents()).split('\n')[0].replace(':','').replace('\r','').strip()
                            value1 = stripHtml(price.renderContents()).split('\n')[-1].replace('\t','').replace('\r','').strip()
                            page['et_product_'+feature1] = value1
                except:
                    log.exception(self.log_msg('could not parse post price info'))
                try:
                    post_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x ,\
                                                               page.values()))).encode('utf-8','ignore')).hexdigest()
                except Exception,e:
                    log.info(self.log_msg('could not build post hash , so returning'))
                    raise e

                if not checkSessionInfo(self.genre, self.session_info_out,
                                        self.parent_url, self.task.instance_data.get('update')):
                    id=None
                    if self.session_info_out == {}:
                        id=self.task.id
                        log.debug('got the connector instance first time, sending updatesessioninfo the id : %s' % str(id))
                    result=updateSessionInfo(self.genre, self.session_info_out,self.parent_url, post_hash,
                                             'Post', self.task.instance_data.get('update'), Id=id)
                    if result['updated']:
                        page['path']=[self.parent_url]
                        page['parent_path']=[]
                        page['uri'] = normalize(self.currenturi)
                        page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
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
                        #page['first_version_id']=result['first_version_id']
                        page['data'] = ''
                        #page['id'] = result['id']
                        page['task_log_id']=self.task.id
                        page['entity'] = 'Post'
                        page['category']=self.task.instance_data.get('category','')
                        self.pages.append(page)
                        log.info(page)
                        return True
                    else:
                        log.info(self.log_msg('parent page is not updated'))
                else:
                    log.info(self.log_msg('parent page is not added to self.pages'))
                    return False
            except Exception,e:
                log.exception(self.log_msg("parent post couldn't be parsed"))
                return False
                raise e
            
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

