'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Pratik

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
from TaskMaster.utils.httpconnection import HTTPConnection
import urllib2
from BeautifulSoup import BeautifulSoup
import md5

log = logging.getLogger('studentsreviewconnector')

class StudentsReviewConnector (BaseConnector) :
    '''
    SAMPLE url
    '''

    @logit(log , 'fetch')
    def fetch(self):
        try:
            self.genre = 'Review'
            self.parent_url=self.currenturi
            self.currenturi = self.currenturi #+ '/SortOrder/2' # Sort by Date
            self.review_count=1
            self.positive = '#009704'
            self.negative='#970016'
            self.neutral = '#977500'
            self.advice = '#001397'
            if not self._setSoup():
                return False
            if not self._getParentPage():
                log.info(self.log_msg('Parent page not posted'))
            count = 0;
            while True:
                parent_soup=self.soup
                try:
                    tempL = self.soup.find(text = re.compile('^Next')).parent.parent.parent.previousSibling.parent.findAll('a')
                    check = len(tempL)-3
                    last_page = int(stripHtml(tempL[check].renderContents()))
                except:
                    log.info(self.log_msg('last_page not found'))
                    last_page=2
                log.info(last_page)
                try:
                    link_add = self.currenturi.split('/')[3].strip()

                except:
                    log.info(self.log_msg('link_add not found'))
                log.info(link_add)
                if not self._addReviews():
                    break
                try:
                    #count = count+1
                    #if count>last_page:
                        #break
                    self.currenturi= 'http://www.studentsreview.com/'+link_add+'/'+parent_soup.find(text = re.compile('^Next')).next.find('a')['href']
                    self.currenturi = self.currenturi.replace(' ','%20')
                    log.info(self.log_msg('Next page is set as %s' % self.currenturi))
                    self._setSoup()
                except:
                    log.exception(self.log_msg('Exception in finding out Next_page'))
                    break
                #count = count+1
            return True
        except Exception,e:
            log.exception(self.log_msg('Exception in Fetch()'))
            return False


    @logit(log, '_addReviews')
    def _addReviews(self):
        reviews = [each.parent.parent.parent for each in self.soup.findAll('font',attrs={'face':'arial'})]
        log.info(len(reviews))
        if self.review_count > 1 and (len(reviews) == 0):
            return False
        for review in reviews:
            self.review_count=self.review_count+1
            log.info("Fetching review no.", self.review_count)
            page={}
            try:
                page['title'] = stripHtml(review.find('font',attrs={'face':'georgia'}).renderContents())
            except:
                log.exception(self.log_msg('exception in getting Title for review'))
                page['title'] = ''
            try:
                date_str = stripHtml(review.find('td',attrs ={'width':'25%'}).renderContents())
                page['posted_date']=datetime.strftime(datetime.strptime(date_str,'%b %d %Y'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception(self.log_msg('exception in getting posted_date'))
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            try:
                data_tag = stripHtml(review.find('font',attrs = {'face':'arial'}).renderContents())
                page['data'] = data_tag
            except:
                page['data'] = ''
                log.info(self.log_msg('Data not found for this post'))
            try:
                rating_str = stripHtml(review.find('td',attrs={'valign':'bottom'}).renderContents()).split(',')
                log.info(rating_str)
                for each in rating_str:
                    try:
                        var = 'et_data_'+ each.split('   ')[0].replace(':','').strip()
                        page[var.lower()] = each.split('   ')[1].strip()
                    except IndexError:
                        continue
                    
            except:
                log.exception(self.log_msg('extra info cannot be fetched'))
            try:
                #profile_link = review.find(text = re.compile('^More')).parent.parent['href']
                profile_link = review.find('div',id= re.compile('^response')).find('a')['href']
                log.info(self.log_msg("profile link is:"))
                log.info(profile_link)
            except:
                log.info(self.log_msg('profile link not found'))
            if profile_link:
                ret_page = self.__getAuthorInfo(profile_link)
                page.update(ret_page)
            try:
                color_code =review.find('img',attrs={'width':'15'}).parent.parent.parent['bgcolor']
                if color_code==self.positive:
                    page['et_review_type'] = 'positive'
                if color_code == self.negative:
                    page['et_review_type'] = 'negative'
                if color_code == self.neutral:
                    page['et_review_type'] = 'neutral'
                if color_code == self.advice:
                    page['et_review_type'] = 'advice'
            except:
                log.info(self.log_msg('color key is not found'))
            try:
                author_details = stripHtml(review.find('td',attrs ={'width':'50%'}).renderContents()).replace('\n','').replace('\t','').split('  ')
                if len(author_details)==4:
                    page['et_author_year'] = author_details[0].replace('Year','')
                    page['et_author_gender'] = author_details[1]
                if len(author_details)==3:
                    page['et_author_gender'] = author_details[0]
                page['ei_author_class'] = author_details[-1].replace('Class','')
            except:
                log.info(self.log_msg('author details cannot be fetched'))
            try:
                if page['title'] =='':
                    if len(page['data']) > 50:
                        page['title'] = page['data'][:50] + '...'
                    else:
                        page['title'] = page['data']
            except:
                log.exception(self.log_msg('title not found'))
                page['title'] = ''

            page_digest = md5.md5(repr(page['title'] + page['data'] + page['posted_date'])).hexdigest()
             #unique_key = get_hash( {'data':page['data'],'title':page['title']})
            if checkSessionInfo(self.genre, self.session_info_out,
                                page_digest,  self.task.instance_data.get('update'),
                                parent_list=[self.parent_url]):
                log.info(self.log_msg('reached last crawled page returning'))
                continue

            try:
                review_hash = get_hash(page)
                result=updateSessionInfo(self.genre, self.session_info_out, page_digest, \
                                             review_hash,'Review', self.task.instance_data.get('update'),\
                                             parent_list=[self.parent_url])
                if not result['updated']:
                    continue
                parent_list = [self.parent_url]
                page['parent_path'] = copy.copy(parent_list)
                parent_list.append(page_digest)
                page['path'] = parent_list
                page['task_log_id'] = self.task.id
                page['versioned'] = self.task.instance_data.get('versioned',False)
                page['category'] = self.task.instance_data.get('category','generic')
                page['last_updated_time'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['client_name'] = self.task.client_name
                page['entity'] = 'Review'
                page['uri'] = self.currenturi
                page['priority'] = self.task.priority
                page['level'] = self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                            ,"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                page['uri_domain'] =  unicode(urlparse.urlparse(page['uri'])[1]) #urlparse.urlparse(page['uri'])[1]
                self.pages.append(page)
                log.info(page)
                log.info(self.log_msg('Adding review %d of %s ' % (self.review_count ,self.currenturi)))
            except:
                log.exception(self.log_msg('Error in adding session info'))
        return True

    
    @logit (log, "_getParentPage")
    def _getParentPage(self):
        page={}
        if not checkSessionInfo(self.genre, self.session_info_out, self.parent_url \
                                    , self.task.instance_data.get('update')):
            try:
                page['title'] =  stripHtml(self.soup.find('span',{'class':'title'}).renderContents())
            except:
                log.exception(self.log_msg('Title is not found'))

                page['title'] =''
            try:
                student_rates= self.soup.find('div',style = re.compile('^border:')).findAll('font',attrs={'size':'3'})
                student_values = self.soup.find('div',style = re.compile('^border:')).findAll('font',attrs={'size':'4'})
                snap_1 = stripHtml(student_rates[0].renderContents()) + '  '+ stripHtml(student_values[0].renderContents())
                snap_2 = stripHtml(student_rates[1].renderContents()) + '  '+ stripHtml(student_values[1].renderContents())
                log.info(snap_1)
                log.info(snap_2)
                page['et_rate_tag1'] = snap_1
                page['et_rate_tag2'] = snap_2
            except:
                log.exception(self.log_msg('info can not be extracted'))
            try:
                review_str = stripHtml(self.soup.find('td',attrs={'width':'50%'}).find('font').renderContents())
                rc = re.compile(r'\d+',re.U)
                review_count= rc.search(review_str).group()
                page['ei_comments_count'] = int(review_count)
            except:
                log.info(self.log_msg('Comments count is not found'))
            try:
                post_hash = get_hash(page)
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out,self.parent_url, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
                if not result['updated']:
                    log.info(self.log_msg('session info return True'))
                    return False
                page['parent_path'] = []
                page['path'] = [self.parent_url]
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
                page['data'] = ''
                page['task_log_id']=self.task.id
                page['entity'] = 'Post'
                page['category']=self.task.instance_data.get('category','')
                self.pages.append(page)
                log.info(page)
                log.info(self.log_msg('Parent Page is added to the page dictionary'))
                return True
            except:
                log.exception(self.log_msg('exception while adding the parent page'))
                return False
                
    @logit(log, "_getAuthorInfo")
    def __getAuthorInfo(self,link):
        try:
            temp_page = urllib2.urlopen(link).read()
            temp_soup = BeautifulSoup(temp_page)
            temp_page={}
            try:
                feat_0 = temp_soup.findAll('table',attrs={'width':'98%'})
                feat_1 = feat_0[0].find('table',attrs={'width':'100%'})
                feat_2 = feat_1.findAll('td')
                if len(feat_2)>1:
                    count =0
                    for each in range(len(feat_2)):
                        try:
                            var = 'et_data_'+ stripHtml(feat_2[count].renderContents()).strip().replace(' ','_').replace('/','_')
                            temp_page[var.lower()] = stripHtml(feat_2[count+1].renderContents()).split('  ')[0]
                            if len(temp_page[var]) > 3:
                                temp_page[var]=''
                            count = count+2
                        except:
                            continue
            except:
                log.exception(self.log_msg('features can not be found'))
        except:
            log.exception(self.log_msg("exception in fetching data"))
        log.info(temp_page)
        return temp_page
    
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
