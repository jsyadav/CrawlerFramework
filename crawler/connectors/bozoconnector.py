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
from BeautifulSoup import BeautifulStoneSoup , BeautifulSoup

from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit

log = logging.getLogger('BozoConnector')

class BozoConnector( BaseConnector ):
    '''
    A Connector to find the data in www.gamestop.com
    test with the sample uris
    #http://www.bozo.pl/Finanse/Kredyty-hipoteczne/Polbank/
    '''
    @logit(log,"fetch")
    def fetch(self):
        """
        Fetch in GameStop
        """
        self.genre = 'Review'
        try:
            #self.currenturi = 'http://www.bozo.pl/Finanse/Kredyty-hipoteczne/Polbank/'
            self.parent_uri = self.currenturi
            if not self._setSoup():
                log.info(self.log_msg('Soup not set, cannot not proceed'))
                return False
            if not self.__getParentPage():
                log.info(self.log_msg('Parent page not added'))
            while True:
                self.__addReviews()
                try:
                    self.currenturi =  'http://www.bozo.pl' + self.soup.find('a',text='>>').parent['href']
                    if not self._setSoup():
                        break
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
            page['title'] = stripHtml(self.soup.find('h1').renderContents())
            #page['title'] = stripHtml(self.soup.find('span',id=re.compile('.*ProductTitle$')).renderContents())
        except:
            log.info(self.log_msg(' Title cannot be found'))
        try:
            page['et_product_rating'] =  float(stripHtml(self.soup.find('span','mark').renderContents()).split('/')[0].strip())
        except:
            log.info(self.log_msg('Price not found'))
        try:
            log.info(page)
            post_hash = get_hash( page )
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo( self.genre, self.session_info_out, self.\
                   parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                log.info(self.log_msg('result [update] return false'))
                return False
            #page['first_version_id']=result['first_version_id']
            #page['id'] = result['id']
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
            page['data'] = ''
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
        """
        This will get all the reviews found for the product

        """
        try:
            reviews = self.soup.findAll('div','review')
            if not reviews:
                log.info(self.log_msg('No Reviews found for this page'))
                return False
        except:
            log.info(self.log_msg('Cannot proceed , No reviews are found'))
            return False
        for review in reviews:
            page = {}
            page['uri'] = self.currenturi
            review_id = review.find('div',id= re.compile('review\d+')).extract()
            unique_key = review_id['id']
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                                 self.task.instance_data.get('update'),parent_list\
                                                            =[ self.parent_uri ]):
                    log.info(self.log_msg('Review cannot be added'))
                    continue
            try:
                page['ef_data_rating'] = float(len([x for x in  review.find('small').find('strong').findAll('img') if x['src'].endswith('img/red-small-star.png')]))
            except:
                log.info(self.log_msg('User Raing not found cannot be found'))
            try:
                date_str = date_str = review.find('font',text='&nbsp;data:').next.strip()
                page['posted_date'] = datetime.strftime( datetime.strptime(date_str,'%Y-%m-%d %H:%M:%S'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.info(self.log_msg('posted date not found'))
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            try:
                data_str = review.find('small').__str__()
                match_object = re.search('<font class="error">zalety:</font>(.*?)<font class="error">wady:</font>(.*?)<font class="error">opinia:</font>(.*)',data_str, re.DOTALL)
                page['et_data_pros'] = stripHtml(match_object.group(1))
                page['et_data_cons'] = stripHtml(match_object.group(2))
                page['data'] = stripHtml(match_object.group(3))
            except:
                log.info(self.log_msg('DAta not found'))
                page['data'] = ''
            try:
                page['ei_data_helpful'] = int(re.search('^(\d+) osoby z \d+',review.find('small').next.strip()).group(1))
            except:
                log.info(self.log_msg('data help ful not found'))
            try:
                user_info = [stripHtml(x.renderContents()) for x in review.find('table','userimage').findAll('tr') if not stripHtml(x.renderContents())=='']
                if len(user_info)==2:
                    page['et_author_name'] =user_info[0]
                    page['ei_author_visitor_points'] = int(re.search('^\d+',user_info[1].split('-')[-1].strip()).group())
            except:
                log.info(self.log_msg('Author info not found'))
            try:
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
            except:
                log.exception(self.log_msg('title not found'))
                page['title'] = ''
            try:
                log.info(page)
                review_hash = get_hash(page)
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            review_hash,'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    continue
                parent_list =[self.parent_uri]
                page['parent_path'] = copy.copy(parent_list)
                parent_list.append(unique_key)
                page['path'] = parent_list
                #page['id'] = result['id']
                #page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                #page['first_version_id']=result['first_version_id']
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

