
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

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('SoccerConnector')

class SoccerConnector (BaseConnector) :
    '''Given a url, SAMPLE URL http://www.soccer.com/review/index2.php?SID&item_id=235251&site=01 it checks out for 'Veiwall links' if True, It considers this url as self.currenturi
    '''
    uri_domain='http://www.soccer.com'
    @logit(log , 'fetch')
    def fetch(self): 
        try:
            self.genre='Review'
            self.parent_url=self.currenturi
            if re.search('http://www\.soccer\.com/review/index2.php\?(SID&)?item_id=\d+&site=01', self.currenturi):
                res=self._getHTML()
                if not res:
                    return False
                self.rawpage=res['result']
                self._setCurrentPage()
                if self.soup.find('div', id='reviewSort').find('a', text='View All'):
                    self.currenturi=self.uri_domain+ self.soup.find('div', id='reviewSort').find('a', text='View All').parent['href'] ## This url has all the reivews in a single page. No need to navigate through page by page
                    log.info(self.log_msg('The curent url is modified to %s to get all review in one page' % self.currenturi))
                    res=self._getHTML()
                    if not res:
                        return False
                    self.rawpage=res['result']
                    self._setCurrentPage()
                if self._getParentPage():
                    log.info(self.log_msg('parent_page is updated'))
                if self._addReviews():
                    log.info(self.log_msg('Review_information is updated'))
                return True
        except Exception, e:
            log.exception(self.log_msg("Exception in Fetch"))
            return False


    @logit (log, "_getParentPage")
    def _getParentPage(self):
        try:
            page={}
            if not checkSessionInfo(self.genre, self.session_info_out, self.parent_url \
                                        , self.task.instance_data.get('update')):
                try:
                    page['title'] = stripHtml(self.soup.find('div',  id="reviewHeadLeft").h2.renderContents()).strip()
                except:
                    log.exception(self.log_msg('Title is not found'))
                    page['title'] =''
                try:
                    page['ef_avg_customer_reviews']=float(self.soup.find('div',  id="reviewHeadLeft")\
                                                           .div.b.nextSibling.strip().strip('(')[0:self.soup.\
                                                           find('div',  id="reviewHeadLeft").div.b.nextSibling.strip().strip('(').index('Stars')])
                except:
                    log.exception(self.log_msg("Exception in finding  Average Customer Reviews"))
                try:    
                    post_hash = get_hash(page)
                except:
                    log.exception(self.log_msg("Exception in Building posthash in _getParentpage"))
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out,self.parent_url, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    page['uri'] = self.parent_url
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
                else:
                    log.info(self.log_msg('Parent Page title is not  added to the page dictionary'))
                    return False
                                                        
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            raise e
    
    @logit (log, "_addReviews")
    def _addReviews(self):
        '''page['posted_date'] is get from self.task.pagedata['posted_date'] ....same for review title '''
        try:
            reviews=self.soup.findAll('table',  cellspacing="0", cellpadding="0",
                                      border="0", align="center", width="100%")[1].findAll('tr')
            for review in reviews:
                page={}
                if not checkSessionInfo(self.genre, self.session_info_out, 
                                        self.currenturi,  self.task.instance_data.get('update'), 
                                        parent_list=[self.parent_url]):
                    if review.find('img'):
                        try:
                            page['ef_customer_rating']=float(review.find('img')['src'][review.find('img')['src'].rindex('stars-')+len('stars-'):review.
                                                                                       find('img')['src'].rindex('-.png')].replace('-', '.'))
                        except:
                            log.exception(self.log_msg('could not parse customer Rating for '+ self.currenturi))
                        try:
                            page['title']=stripHtml(review.find('img').findNext('b').renderContents()).strip()
                        except:
                            page['title']=''
                            log.exception(self.log_msg('could not find Title for '+ self.currenturi))
                        try:
                            page['posted_date']=datetime.strftime(datetime.strptime(stripHtml\
                                                (review.find('img').findNext('b').nextSibling).strip('-').strip(), '%b %d, %Y'), '%Y-%m-%dT%H:%M:%SZ')
                        except:
                            page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                            log.exception(self.log_msg('could not parse posted_date for the comment'))
                        try:
                            page['et_author_category']=stripHtml(review.find('i').renderContents()).strip(':').strip()
                        except:
                            page['et_author_category']=''
                            log.exception(self.log_msg('could not parse player/reviewer category'))
                        try:
                            page['et_author_name']=stripHtml(review.find('i').nextSibling.next.renderContents()).strip()
                        except:
                            page['et_author_name']=''
                            log.exception(self.log_msg('could not parse Author Name'))
                        try:
                            page['et_author_location']=review.find('i').nextSibling.next.next.next.strip().strip('(').strip(')')
                        except:
                            page['et_author_location']=''
                            log.exception(self.log_msg('could not parse Author Location'))
                        try:
                            page['data']=stripHtml(review.find('i').findNextSibling().findNextSibling('br').findNextSibling('br').next).strip()
                        except:
                            page['data']=''
                            log.exception(self.log_msg('could not parse DATA'))
                        if not page['title']:
                            if len(page['data']) > 50:
                                page['title'] = page['data'][:50] + '...'
                            else:
                                page['title'] = page['data']
                        try:
                            review_hash = get_hash(page)
                        except:
                            log.exception(self.log_msg('could not generate review_hash for '+ self.parent_url))
                            continue
                        result=updateSessionInfo(self.genre, self.session_info_out, review_hash, \
                                                     review_hash,'Review', self.task.instance_data.get('update'),\
                                                     parent_list=[self.parent_url])
                        if result['updated']:
                            page['uri'] =self.parent_url
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
                            log.info(self.log_msg('Adding review of %s ' % self.currenturi))
                        else:
                            log.info(self.log_msg('Not adding review of %s ' % self.currenturi))
                    
            return True
        except Exception, e:
            log.exception(self.log_msg('Exception in addReviews'))
            raise e
            

            
            
