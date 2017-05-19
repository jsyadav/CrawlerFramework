'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

# SKumar

import re
import logging
import copy
import cgi

from datetime import datetime
from urlparse import urlparse
from BeautifulSoup import BeautifulSoup

from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit

log = logging.getLogger('OpiniujConnector')

class OpiniujConnector( BaseConnector ):
    '''
    A Connector to find the data in opiniuj.pl
    test with the sample uris
    #http://opiniuj.pl/37703,mbank-ekonto.html
    '''
    @logit(log,"fetch")
    def fetch(self):
        """
        Fetch in GameStop
        """
        self.genre = 'Review'
        try:
            #self.currenturi = 'http://opiniuj.pl/37703,mbank-ekonto.html'
            self.parent_uri = self.currenturi
            self.baseurl = 'http://opiniuj.pl'
            self.preview_type = False
            if not self.__setSoup():
                log.info(self.log_msg('Soup not set, cannot not proceed'))
                return False
            if not self.__getParentPage():
                log.info(self.log_msg('Parent page not added'))
            next_page_no = 2
            while True:
                if not self.__addReviews():
                    log.info(self.log_msg('Reviews not found'))
                try:
                    self.currenturi = self.parent_uri + self.soup.find('a',href='?page='+str(next_page_no))['href']
                    if not self.__setSoup():
                        break
                    next_page_no = next_page_no + 1
                except:
                    log.info(self.log_msg('Next Page not found'))
                    break
            return True
        except:
            log.exception(self.log_msg('Error in Fetch methor') )
            return False

    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will fetch the parent info
        This has nothing but title
        """
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        page = {}
        try:
            page['title'] = stripHtml(self.soup.find('div',id='product').find('h1').renderContents())
            #page['title'] = stripHtml(self.soup.find('span',id=re.compile('.*ProductTitle$')).renderContents())
        except:
            log.info(self.log_msg(' Title cannot be found'))
        try:
            page['et_product_category'] = stripHtml(self.soup.find('div',id='product').find('span').findNext().renderContents())
        except:
            log.info(self.log_msg(' Category cannot be found'))
        try:
            ratings = [ x.findParent('tr') for x in self.soup.find('div',id='rating').findAll('td','product_data_l')]
            for rating in ratings:
                try:
                    key = stripHtml(rating.find('td','product_data_l').renderContents()).lower().replace(' ','_')
                    page ['ef_product_'+key + '_rating'] = float(stripHtml(rating.find('td','product_data_p').renderContents()))
                except:
                    log.info(self.log_msg('rating not found'))
        except:
            log.info(self.log_msg('Ratings not found'))
        try:
            post_hash = get_hash( page )
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo( self.genre, self.session_info_out, self.\
                   parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                log.info(self.log_msg('result [update] return false'))
                return False
            page['parent_path'] = []
            page['path'] = [self.parent_uri]
            page['uri'] = normalize( self.currenturi )
            page['uri_domain'] = unicode(urlparse(page['uri'])[1])
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
            #page['first_version_id']=result['first_version_id']
            page['data'] = ''
            #page['id'] = result['id']
            page['task_log_id']=self.task.id
            page['entity'] = 'Post'
            page['category']=self.task.instance_data.get('category','')
            self.pages.append(page)
            log.info(self.log_msg('Parent Page added'))
            return True
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False

    @logit(log,'__addReviews')
    def __addReviews(self):
        """This will get all the reviews found for the product
        """
        try:
            reviews = [ x.findParent('tr') for x in self.soup.findAll('td','recenzje_list') ]
            review_urls = list(set([ self.baseurl +review.find('a',text=re.compile('wi.cej')).parent['href'].replace(' ','+') for review in reviews]))
        except:
            log.info(self.log_msg('Cannot proceed , No reviews are found'))
            return False
        for review_url in review_urls:
            try:
                self.currenturi =  review_url
                if checkSessionInfo(self.genre, self.session_info_out, self.currenturi,\
                                 self.task.instance_data.get('update'),parent_list\
                                                            =[ self.parent_uri ]):
                    log.info(self.log_msg('Review cannot be added, session info return True'))
                    continue
                if not self.__setSoup():
                    continue
                page = self.__getData()
                page['uri'] = self.currenturi
                review_hash = get_hash(page)
                log.info(page)
                result=updateSessionInfo(self.genre, self.session_info_out, self.currenturi, \
                            review_hash,'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    continue
                parent_list =[self.parent_uri]
                page['parent_path'] = copy.copy(parent_list)
                parent_list.append(self.currenturi)
                page['path'] = parent_list
                #page['id'] = result['id']
                #page['first_version_id']=result['first_version_id']
                #page['parent_id']= '-'.join(result['id'].split('-')[:-1])
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
                page['uri_domain'] = urlparse(page['uri'])[1]
                self.pages.append(page)
                log.info(self.log_msg('Review Added'))
                
            except:
                log.exception(self.log_msg('Error while adding session info'))
        return True
                
    @logit(log, '__getData')
    def __getData(self):
        ''' It will get the review info and send the page data
        '''
        page = {'title':'','posted_date':datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")}
        try:
            review = self.soup.find('div',id='recenzja')
            if not review:
                log.info(self.log_msg('No REview is found'))
                return False
        except:
            log.info(self.log_msg('review block not found'))
        try:
            span_tag = review.find('font','recenzje_name')
            date_aut_str = stripHtml(span_tag.renderContents()).split(',')
            aut_str = date_aut_str[1].strip()
            if aut_str.startswith('~'):
                aut_str = aut_str[1:]
            page['et_author_name'] = aut_str
            date_str = date_aut_str[0]
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%Y-%m-%d %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
            page['title'] = stripHtml(span_tag.findNext('span').renderContents())
        except:
            log.exception(self.log_msg('posted date not found'))
        try:
            ratings = [ x.findParent('tr') for x in review.findAll('td','product_data_l')]
            for rating in ratings:
                try:
                    key = stripHtml(rating.find('td','product_data_l').renderContents()).lower().replace(' ','_')
                    page ['ef_rating_'+key] = float(stripHtml(rating.find('td','product_data_p').renderContents()))
                except:
                    log.info(self.log_msg('rating not found'))
        except:
            log.info(self.log_msg('user Raing not found'))
        pros_cons = {'pros':'et_data_pros','cons':'et_data_cons','howLong':'et_data_usage_time'}
        for each in pros_cons.keys():
            try:
                pros_tag = copy.copy(review.find('div',each))
                [x.extract() for x in  pros_tag.findAll(True)]
                page[pros_cons[each]] = stripHtml(pros_tag.renderContents())
            except:
                log.info(self.log_msg('data pros not found'))
        try:
            page['ei_data_feedback_score'] = int(stripHtml(review.find('div','help').find('b').renderContents()))
        except:
            log.info(self.log_msg('Feed back scrore not found'))
        try:
            copy_review = copy.copy(review)
            [x.extract() for x in copy_review.findAll(True) if not x.name=='a']
            page['data'] = stripHtml(copy_review.renderContents())
        except:
            page['data']=''
            log.info(self.log_msg('data not found'))
        try:
            if page['title'] =='':
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
        except:
            log.exception(self.log_msg('title not found'))
        return page
            
    @logit(log, "_setSoup")
    def __setSoup( self, url = None, data = None, headers = {} ):
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

