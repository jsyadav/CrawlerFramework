
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#latha
#Ashish
import re
import md5
import logging
import urlparse
from datetime import datetime
from BeautifulSoup import BeautifulSoup

from tgimport import *
from baseconnector import BaseConnector
from utils.task import Task
from utils.utils import stripHtml
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from utils.utils import get_hash

log = logging.getLogger('LaptopCommunityConnector')

class LaptopCommunityConnector(BaseConnector):
    base_url='http://laptopcommunity.com/'
    
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetches Reviews from http://www.laptopcommunity.com/.. for a given product
        """
        self.genre="Review"
        try:
            if re.match("^http://(www.)?laptopcommunity.com/(.+)-Laptop.html$", self.currenturi):
                log.debug(self.currenturi)
                res=self._getHTML()
                self.rawpage=res['result']
                self._setCurrentPage()
                self.parent_url=self.currenturi
                if self._getParentPage():
                    self.parent_page_url = self.currenturi
                    self._getReviewLinks()
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False
        return True
    
    @logit(log , '_getParentPage')
    def _getParentPage(self ):
        try:
            page={}
            try:            
                if self.soup.find('div', {'id':'brandNameArea'}).h2:
                    page['title'] = self.soup.find('div', {'id':'brandNameArea'}).h2.renderContents().split('Reviews')[0].strip()
            except:
                page['title'] =''
                log.exception(self.log_msg('Could not parse title from the parent page'))
            try:
                post_hash = md5.md5(''.join(sorted(page.values())).encode('utf-8', 'ignore')).hexdigest()
            except:
                log.exception(self.log_msg('exception in building post_hash  moving onto next review'))
            log.debug('checking session info')
            if not checkSessionInfo(self.genre, self.session_info_out,
                                    self.parent_url, self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug(id)
                result=updateSessionInfo(self.genre, self.session_info_out, self.parent_url, post_hash, 'Post', self.task.instance_data.get('update'), Id=id)
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
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['first_version_id']=result['first_version_id']
                    page['data'] = ''
                    page['id'] = result['id']
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')
                    page['task_log_id']=self.task.id
                    page['client_name'] = self.task.client_name
                    page['versioned']=self.task.instance_data.get('versioned',False)
                    page['first_version_id']=result['first_version_id']
                    page['data'] = ''
                    page['id'] = result['id']
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')
                    page['task_log_id']=self.task.id
                    page['client_name'] = self.task.client_name
                    page['versioned']=self.task.instance_data.get('versioned',False)
                    self.pages.append(page)
        except Exception, e:
            log.exception(self.log_msg('could not parse post'))
            return False
        return True

    @logit(log, '_getReviewLinks')
    def _getReviewLinks(self):
        """
        fetches review links for further processes
        """
        try:
            while True:
                next = self.soup.find('td', {'align':'left'}).find(text='Next &raquo;')
                for each in self.soup.findAll('div', {'class':'contentBlock'}):
                    log.info(self.log_msg('Review link to be processed is : '+   self.base_url+str(each.h3.a['href'])))
                    if self.addReviews(self.base_url+each.h3.a['href']) :
                        log.info(self.log_msg("DATA processed for the the reviewlink" + self.base_url+each.h3.a['href']))
                if next:
                    self.currenturi=self.parent_url + next.parent['href']
                    log.info(self.log_msg('NEXT pAge URL ' + self.currenturi))
                    res=self._getHTML()
                    self.rawpage=res['result']
                    self._setCurrentPage()
                else:
                    log.info(self.log_msg('Reached last page of review'))
                    break
                
        except Exception, e:
            log.exception(self.log_msg('could not fetch review links'))
            return False
        return True
    
    @logit(log, 'addReviews')
    def addReviews (self, review_link):
        """
        fetches reviews and various kinds of ratings...
        """
        try:
            self.currenturi=review_link
            res=self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            page={}
            if not checkSessionInfo(self.genre, self.session_info_out,
                                    self.currenturi, self.task.instance_data.get('update'),
                                    parent_list=[self.parent_url]):

                try:
                    page_soup=self.soup.find('div', {'id':'viewDetailsData'}).findAll('table', {'width':'100%', 'border':'0'})
                    if page_soup:
                        data =[each.findAll('tr') for each in page_soup if each.find('tr' )]
                        try:
                            page['title'] = ''.join([each.find('td', text=re.compile('Laptop Brand')).next.next.renderContents() for each in data[0] if each.find('td', text=re.compile('Laptop Brand'))])
                            
                        except:
                            page['title'] =''
                            log.exception(self.log_msg('Could not parse Brand name for a :' + review_link))
                        try:
                            if [each.find('td', text=re.compile('Name')) for each in data[0]] :
                                page['et_author_name'] =''.join([each.find('td', text=re.compile('Name')).next.next.renderContents() for each in data[0] if each.find('td', text=re.compile('Name:'))])
                        except:
                            page['et_author_name'] =''
                            log.exception(self.log_msg('Could not parse author name for a :' + review_link))
                        try:
                            page['et_model_name'] = ''.join([each.find('td', text=re.compile('Laptop Model:')).next.next.renderContents() for each in data[0] if each.find('td', text=re.compile('Laptop Model:'))])
                        except:
                            page['et_model_name'] =''
                            log.exception(self.log_msg('Could not parse Model name for a :' + review_link))
                        try:
                            if [each.find('td', text=re.compile('^Overall Rating')) for each in data[0] if each.find('td', text=re.compile('^Overall Rating.+'))]:
                                temp=''.join([each.find('td', text=re.compile('^Overall Rating')).next.next.renderContents() for each in data[0] if each.find('td', text=re.compile('^Overall Rating.+'))])
                                page['ef_rating_overall'] =float(temp.split('(')[0])
                        except:
                            page['ef_rating_overall']=0.0
                            log.exception(self.log_msg('Could not parse Overall rating for a :' + review_link))
                        try:
                            if [each.find('td', text=re.compile('^Rate.+Customer Support.+')) for each in data[0] if each.find('td', text=re.compile('^Rate.+Customer Support.+'))]:
                                temp= ''.join([each.find('td', text=re.compile('^Rate.+Customer Support.+')).next.next.renderContents() for each in data[0] if each.find('td', text=re.compile('^Rate.+Customer Support.+'))])
                                page['ef_rating_consumerSupport'] =float(temp.split('(')[0])
                        except:
                            page['ef_rating_consumerSupport'] =0.0
                            log.exception(self.log_msg('Could not parse customer support rating for a :' + review_link))
                        try:
                            if  [each.find('td', text=re.compile('^Rate.+Performance:')) for each in data[0]if each.find('td', text=re.compile('^Rate.+Performance:'))]:
                                temp=''.join([each.find('td', text=re.compile('^Rate.+Performance:')).next.next.renderContents() for each in data[0] if each.find('td', text=re.compile('^Rate.+Performance:'))])
                                page['ef_rating_performance'] =float(temp.split('(')[0])
                        except:
                            page['ef_rating_performance'] =0.0
                            log.exception(self.log_msg('Could not parse Performance rating for a :' + review_link))
                        try:
                            if [each.find('td', text=re.compile('^Rate.+Build.+'))for each in data[0]  if each.find('td', text=re.compile('^Rate.+Build.+'))]:
                                temp= ''.join([each.find('td', text=re.compile('^Rate.+Build.+')).next.next.renderContents() for each in data[0] if each.find('td', text=re.compile('^Rate.+Build.+'))])
                                page['ef_rating_build'] =float(temp.split('(')[0])
                        except:
                            page['ef_rating_build']=0.0
                            log.exception(self.log_msg('Could not parse Build rating for a :' + review_link))
                        try:
                              if [each.find('td', text=re.compile('^Rate.+ease of use.+')) for each in data[0] if each.find('td', text=re.compile('^Rate.+ease of use.+'))]:      
                                  temp=''.join([each.find('td', text=re.compile('^Rate.+ease of use.+')).next.next.renderContents() for each in data[0] if each.find('td', text=re.compile('^Rate.+ease of use.+'))])
                                  page['ef_rating_easeofuse'] =float(temp.split('(')[0])
                        except:
                            page['ef_rating_easeofuse']=0.0
                            log.exception(self.log_msg('Could not parse ease of use rating for a :' + review_link))
                        try:
                            if [each.find('td', text=re.compile('^Rate.+features.+')) for each in data[0]if each.find('td', text=re.compile('^Rate.+features.+'))] :
                                temp=''.join([each.find('td', text=re.compile('^Rate.+features.+')).next.next.renderContents() for each in data[0] if each.find('td', text=re.compile('^Rate.+features.+'))])
                                page['ef_rating_features'] =float(temp.split('(')[0])
                        except:
                            page['ef_rating_features'] =0.0
                            log.exception(self.log_msg('Could not parse Features rating for ' + review_link))
                        try:
                            if [each.find('td', text=re.compile('^Rate.+portability.+')) for each in data[0] if each.find('td', text=re.compile('^Rate.+portability.+'))]:
                                temp=''.join([each.find('td', text=re.compile('^Rate.+portability.+')).next.next.renderContents() for each in data[0] if each.find('td', text=re.compile('^Rate.+portability.+'))])
                                page['ef_rating_portability'] =float(temp.split('(')[0])
                        except:
                            page['ef_rating_portability'] = 0.0
                            log.exception(self.log_msg('Could not  parse Portability rating for ' + review_link))
                        try:
                            temp= ''.join([stripHtml(each.find('td', text=re.compile('^Review.+')).next.next.renderContents()) for each in data[0] if each.find('td', text=re.compile('^Review.+'))])
                            temp_=re.sub('\x19s', '', temp)
                            page['data']=re.sub('\x1d', '', temp_)
                            
                             
                        except:
                            page['data']=''
                            log.exception(self.log_msg('Could not parse DATA for ' + review_link))
                        try:
                            review_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , page.values()))).encode('utf-8','ignore')).hexdigest()
                        except:
                            log.exception(self.log_msg("exception in buidling post_hash , moving onto next post"))
                            return False

                        result=updateSessionInfo(self.genre, self.session_info_out,self.currenturi, review_hash, 
                                                 'Review', self.task.instance_data.get('update'), 
                                                 parent_list=[self.parent_url])
                        if result['updated']:
                            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                            page['id']=result['id']
                            page['priority']=self.task.priority
                            page['level']=self.task.level
                            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                            page['last_updated_time'] = page['pickup_date']
                            page['connector_instance_log_id'] = self.task.connector_instance_log_id
                            page['connector_instance_id'] = self.task.connector_instance_id
                            page['workspace_id'] = self.task.workspace_id
                            page['client_id'] = self.task.client_id  # TODO: Get the client from the project 
                            page['client_name'] = self.task.client_name
                            page['versioned'] = False
                            page['uri'] = self.currenturi
                            page['uri_domain'] = urlparse.urlparse(self.currenturi)[1]
                            page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                            page['first_version_id'] = result['first_version_id']
                            page['task_log_id']=self.task.id
                            page['entity'] = 'Review'
                            page['category'] = self.task.instance_data.get('category' ,'')
                            self.pages.append(page)
                        else:
                            log.info(self.log_msg("Page is not appended for "+ self.curenturi))
                except:
                    log.exception(self.log_msg('Exception in addReviews'))
                    return False
                                                    
        except:
            log.exception(self.log_msg('Exception in addReviews'))
            return False
        return True
        



    


            
                                
                           
