
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Latha

import re
from datetime import datetime
import logging
from urllib2 import urlparse
from BeautifulSoup import BeautifulSoup

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('ZapposConnector')

class ZapposConnector (BaseConnector) :
    uri='http://www.zappos.com'
    ''' sample url ::::http://www.zappos.com/n/p/dp/42123788/c/178276.html '''
    @logit(log, 'fetch')
    def fetch(self):
        self.genre="Review"
        self.parent_url=self.currenturi
        try:
            if  re.match("^http://www\.zappos\.com/n/p/d?p/\d+(.html)?(/c/\d+.html)?(\#reviews)?",self.currenturi):
                res=self._getHTML()
                if not res:
                    return False
                self.rawpage=res['result']
                self._setCurrentPage()
                self._getParentPage()
                self.count=0
                while True:
                    next= self.soup.find('a',{'href':True}, text=re.compile('Next|Last'))
                    if self._addReviews() and next:
                        self.currenturi=self.uri+next.parent['href']
                        res=self._getHTML()
                        if res:
                            self.rawpage=res['result']
                            self._setCurrentPage()
                        else:
                            break
                    else:
                        break
                                               
                return True
        except Exception, e:
            log.exception(self.log_msg('Exception occured in Fetch()'))
            return False
            
    
    @logit(log, '_getParentPage')
    def _getParentPage(self):
        try:
            page={}
            try:
                product_price= self.soup.find('font', face="VERDANA, GENEVA, ARIAL", size="2").\
                    findNext(text=re.compile('\$')).findNext().renderContents()
                product_price = '$' + str(product_price)
                self.updateProductPrice(product_price)
            except:
                log.exception(self.log_msg('Exception in finding product price'))

            if not checkSessionInfo(self.genre, self.session_info_out, self.parent_url \
                                        , self.task.instance_data.get('update')):
                try:
                    parent_soup=[each for each in self.soup.findAll('td') if each.find('font', {'face':'verdana, geneva, arial'})]
                    page['title']=''.join([stripHtml(r.find('font', color="#333399").renderContents()).strip()\
                                               for r in parent_soup if r.find('font', color="#333399")])
                except:
                    log.exception(self.log_msg('Title is not found'))
                    page['title'] =''
                try:    
                    post_hash = get_hash(page)
                except:
                    log.exception(self.log_msg("Exception in Building posthash in _getParentpage"))

                try:
                    page['ef_overall_rating_average']= float([re.sub('stars', r'', r.find(text=re.compile('Overall:')).next['alt'])\
                                                                  for r in parent_soup if r.find(text=re.compile('Overall:'))][0])
                except:
                    log.exception(self.log_msg(' ef_overall_rating_average is not found'))
                try:
                    page['ef_comfort_rating_average']=float([re.sub('stars', r'', r.find(text=re.compile('Comfort:')).next['alt'])\
                                                                 for r in parent_soup if r.find(text=re.compile('Comfort:'))][0])
                except:
                    log.exception(self.log_msg(' ef_comfort_rating_average is not found'))
                try:
                    page['ef_look_rating_average']=float([re.sub('stars', r'', r.find(text=re.compile('Look:')).next['alt'])\
                                                              for r in parent_soup if r.find(text=re.compile('Look:'))][0])
                except:
                    log.exception(self.log_msg(' ef_Look_rating_average is not found'))
                try:
                    page['et_product_price']= self.soup.find('font', face="VERDANA, GENEVA, ARIAL", size="2").\
                        findNext(text=re.compile('\$')).findNext().renderContents()
                    self.updateProductPrice(page.get('et_product_price'))
                except:
                    log.exception(self.log_msg('Exception in finding product price'))
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out,self.parent_url, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
                if result['updated']:
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
                    page['first_version_id']=result['first_version_id']
                    page['data'] = ''
                    page['id'] = result['id']
                    page['task_log_id']=self.task.id
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')
                    self.pages.append(page)
                    log.info(self.log_msg('Parent Page title is added to the page dictionary'))
            return True
                                                        
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            raise e


                
    @logit(log, '_addReviews')
    def _addReviews(self):
        try:
            pat=re.compile('<i>\d+-\d+-\d+ \d+:\d+:\d+</i>')
            review_soup=[each for each in self.soup.findAll('td') if each.find('font', {'face':'verdana, geneva, arial'})]
            date=re.findall(pat, str(review_soup))
            date.insert(0,0)# dummy value to be stored becos 0th element of review_info is not been considered
            review_info=re.split(pat, str(review_soup))
            if not checkSessionInfo(self.genre, self.session_info_out,
                                    self.currenturi,  self.task.instance_data.get('update'),
                                    parent_list=[self.parent_url]):
                for i in xrange(1, len(review_info)):
                    page={}
                    review=BeautifulSoup(review_info[i])
                    try:
                        if review.find(text=re.compile('Reviewer:' , re.DOTALL)):
                            try:
                                page['et_author_name']=review.find(text=re.compile('Reviewer:' , re.DOTALL)).findNext().renderContents().strip()
                            except:
                                page['et_author_name']=''
                                log.exception(self.log_msg("exception in fetching the author name"))
                    except:
                        log.exception(self.log_msg("exception in fetching the author name"))
                    try:
                        loc= review.find(text=re.compile('Reviewer:')).findNext().next.next
                        if  loc.__contains__('from'):
                            page['et_author_location']=review.find(text=re.compile('Reviewer:')).\
                                findNext().next.next.strip().strip('from')
                        else:
                            page['et_author_location']=''
                    except:
                        page['et_author_location']=''
                        log.exception(self.log_msg("exception in fetching the author Location"))
                    try:
                        page['ef_overall_rating']=float(review.find(text=re.compile('Overall:')).\
                                                                 findNext()['alt'].strip('stars'))
                    except:
                        log.exception(self.log_msg("exception in fetching the overall_ratings"))
                    try:
                        page['ef_comfort_rating']= float(review.find(text=re.compile('Comfort:')).\
                                                             findNext()['alt'].strip('stars'))
                    except:
                        log.exception(self.log_msg("exception in fetching the Comfort_ratings"))
                    try:
                        page['ef_look_rating']= float(review.find(text=re.compile('Look:')).\
                                                          findNext()['alt'].strip('stars'))
                    except:
                        log.exception(self.log_msg("exception in fetching the Look_ratings"))
                    try:
                        feature=review.findAll('font', color="#333399" )
                        for each in feature[:-1]:
                            page['et_feature_'+each.renderContents()]=each.next.next
                    except:
                        log.exception(self.log_msg("exception in fetching the et_features like"\
                                                       "shoe arch, shoe width etc.. "))
                    try:
                        if review.find(text=re.compile('Shoe Arch:')):
                            page['data']=stripHtml(review.find(text=re.compile('Shoe Arch:')).\
                                                       findNext().next.next).strip()
                        elif review.find(text=re.compile('Shoe Width:')):
                            page['data']=stripHtml(review.find(text=re.compile('Shoe Width:')).\
                                                        findNext().next.next).strip()
                        elif review.find(text=re.compile('Shoe Size:')):
                            page['data']=stripHtml(review.find(text=re.compile('Shoe Size:')).\
                                                       findNext().next.next).strip()
                        elif  review.find(text=re.compile('Look:')):
                            page['data']=stripHtml(review.find(text=re.compile('Look:')).findNext().next.next.next).strip()
                                              
                    except:
                        page['data']=''
                        log.exception(self.log_msg("exception in fetching Data"))
                    try:
                        review=str(review)
                        page['title']= review[0:review.index('<br />')]
                    except:
                        page['title']=''
                        log.exception(self.log_msg("exception in fetching Title"))
                    try:
                        review_hash = get_hash(page)
                    except:
                        log.exception(self.log_msg('could not generate review_hash for '+ 
                                                   self.currenturi))
                    try:
                        page['posted_date']= datetime.strftime(datetime.strptime(date[i].\
                               strip('</?i>'),'%Y-%m-%d %H:%M:%S'),'%Y-%m-%dT%H:%M:%SZ')
                    except:
                        log.exception(self.log_msg("exception in fetching Posted_date"))
                        page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    result=updateSessionInfo(self.genre, self.session_info_out, review_hash, \
                                                 review_hash,'Review', self.task.instance_data.get('update'),\
                                                  parent_list=[self.parent_url])
                    if result['updated']:
                        page['uri'] = self.currenturi
                        page['id'] = result['id']
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
                        page['first_version_id']=result['first_version_id']
                        page['entity'] = 'Review'
                        page['category'] = self.task.instance_data.get('category','')
                        page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                        page['task_log_id']=self.task.id
                        page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                        self.pages.append(page)
                        self.count=self.count+1
                        log.info(self.log_msg('Adding %dth review of  %s ' % (self.count, self.currenturi)))
            return True
                    
        except Exception, e:
            log.exception(self.log_msg('Exception in addReviews'))
            raise e

                    
                    
