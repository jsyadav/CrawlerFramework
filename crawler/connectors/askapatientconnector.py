
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#ASHISH YADAV

import re
import md5
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import logging
import copy
from urllib2 import urlparse,unquote

from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('AskapatientConnector')
class AskapatientConnector(BaseConnector):

    @logit(log , 'fetch')
    def fetch(self):                                                                                         
        self.genre="Review"
        try:
            self._baseUrl = 'http://www.askapatient.com/'
            self.currenturi = self.currenturi + '&sort=dateAdded&order=0' #order acc. to date descending
            parenturi = self.currenturi
            res=self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            self._getParentPage(parenturi)
            links = set([self._baseUrl + link['href'] for 
                              link in self.soup.findAll('a',href=re.compile('.*&page=[2-9]+$'))])
            if not self.addreviews(parenturi):#called for the first page , and then call sunsequently for next set of pages
                return True
            for next_link in list(links):
                try:
                    self.currenturi = next_link.replace(' ','%20')
                    res=self._getHTML()
                    self.rawpage=res['result']
                    self._setCurrentPage()
                    log.debug(self.log_msg("fetching the page no. %s" %(self.currenturi)))
                    if not self.addreviews(parenturi):
                        break
                except Exception, e:
                    log.exception(self.log_msg('exception in iterating pages in fetch'))
            return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False


    @logit(log , '_getParentPage')
    def _getParentPage(self,parent_uri):
            try:
                page={}
                try:
                    page['title'] = stripHtml(self.soup.title.renderContents().split(': ')[0])
                except Exception, e:
                    page['title'] = ''
                    log.exception(self.log_msg('could not parse page title'))
                try:
                    product_info = re.findall(re.compile(r'The average rating for <b>(.*?)</b> is <b>(.*?)</b>.'),str(self.soup))
                    page['ef_product_rating_overall'] = float(product_info[0][-1])
                except Exception, e:
                    log.exception(self.log_msg('could not parse product rating'))

                try:
                    post_hash = get_hash(page)
                except Exception,e:
                    log.exception(self.log_msg('could not build post_hash'))
                    raise e

                #continue if returned true
                if not checkSessionInfo(self.genre, self.session_info_out, 
                                        parent_uri, self.task.instance_data.get('update')):
                    id=None
                    if self.session_info_out=={}:
                        id=self.task.id
                    result=updateSessionInfo(self.genre, self.session_info_out,parent_uri, post_hash, 
                                             'Post', self.task.instance_data.get('update'), Id=id)
                    if result['updated']:
                        page['path']=[parent_uri]
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
                        page['data'] = ''
                        page['task_log_id']=self.task.id
                        page['entity'] = 'Post'
                        page['category']=self.task.instance_data.get('category','')                        
                        self.pages.append(page)
            except Exception,e:
                log.exception(self.log_msg("parent post couldn't be parsed"))
                raise e

    @logit(log , 'addreviews')
    def addreviews(self,parenturi):
        all_reviews = [s.small.font.renderContents() for s in self.soup.findAll('td',{'bgcolor':True,'valign':'top','height':'19'})]
        log.info(self.log_msg('no. of reviews found on page %d'%(len(all_reviews)/9)))
        num = 0
        reviews=[]
        while num < len(all_reviews):
            reviews.append(all_reviews[num:num+9])
            num+=9
        for index,review in enumerate(reviews):
            try:
                log.info(self.log_msg("processing review %s"%index))
                page ={}
                page['uri'] = normalize(self.currenturi)
                page['title'] = stripHtml(review[1])
                page['data'] = stripHtml(review[3])
                if not page['data'] and not page['title']:
                    log.info(self.log_msg('Empty data and title found in url %s'%self.currenturi))
                    continue
                page['et_data_side_effects'] = stripHtml(review[2])
                try:
                    page['ef_data_rating'] = float(stripHtml(review[0]))
                except:
                    log.info(self.log_msg("couldn't parse review rating"))
                page['et_author_sex'] = stripHtml(review[4])
                try:
                    page['ei_author_age'] = int(stripHtml(review[5]))
                except:
                    log.info(self.log_msg("couldn't parse author age"))
                page['et_author_time_taken'] =  stripHtml(review[6])
                try:
                    posted_date= re.findall(r'[0-9]+/[0-9]+/[0-9]+', stripHtml(review[8]))[0]
                    posted_date = datetime.strptime(posted_date , '%m/%d/%Y')
                    page['posted_date'] = datetime.strftime(posted_date,"%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.exception(log.info("couldn't parse posted_date : %s"%review[8]))
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                id_hash = get_hash({'data':page['title'][:50] + page['data'][:50] + page['et_data_side_effects'][:50]})
                log.info(self.log_msg(id_hash))
                if not checkSessionInfo(self.genre, self.session_info_out, 
                                        id_hash, self.task.instance_data.get('update'),
                                        parent_list=[parenturi]):
                    result=updateSessionInfo(self.genre, self.session_info_out, id_hash, id_hash, 
                                             'Review', self.task.instance_data.get('update'), parent_list=[parenturi])
                    if result['updated']:
                        parent_list = [parenturi]
                        page['parent_path']= copy.copy(parent_list)
                        parent_list.append(id_hash)
                        page['path']=parent_list
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
                        page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                        self.pages.append(page)
            except:
                log.exception(self.log_msg("exception in addreviews"))
                continue
        return True
