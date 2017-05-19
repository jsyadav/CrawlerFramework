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

log = logging.getLogger('UwagiConnector')

class UwagiConnector( BaseConnector ):
    '''
    A Connector to find the data in 
    test with the sample uris
    #http://www.uwagi.pl/articles/index/5/subcategory_id/Uslugi/Bankowe
    '''
    @logit(log,"fetch")
    def fetch(self):
        """
        Fetch in 
        http://www.uwagi.pl/articles/index/6/subcategory_id/Uslugi/Budowlane
        """
        self.genre = 'Review'
        try:
            #self.currenturi = 'http://www.uwagi.pl/articles/index/6/subcategory_id/Uslugi/Budowlane'
            self.base_url = 'http://www.uwagi.pl'
            self.parent_uri = self.currenturi
            if self.currenturi.startswith('http://www.uwagi.pl/articles/view/'):
                if not self._setSoup():
                    log.info(self.log_msg('Soup not set, cannot not proceed'))
                    return False
                if not self.__getParentPage():
                    log.info(self.log_msg('Parent page not added'))
                self.__addReviews()
                return True
            elif self.currenturi.startswith('http://www.uwagi.pl/articles/index'):
                while True:
                    try:
                        if not self._setSoup():
                            return False
                        reviews = self.soup.find('ul','lista').findAll('li')
                        hrefs = []
                        for review in reviews:
                            comments_count = int(re.search('\((\d+)\)',review.find('a','k2').string).group(1))
                            if not comments_count>0:
                                continue
                            else:
                                hrefs.append(self.base_url + review.find('div','dpr').findNext('a')['href'])
                        for href in hrefs:
                            temp_task=self.task.clone()
                            temp_task.instance_data[ 'uri' ] = normalize( href )
                            self.linksOut.append( temp_task )
                        self.currenturi = self.base_url + self.soup.find('div',id='pagination').find('a',text= re.compile('nast.pna >>')).parent['href']
                    except:
                        log.info(self.log_msg('error while adding task'))
                        break
            else:
                log.info(self.log_msg('url is wrong'))
                

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
##        try:
##            page['data'] = '\n'.join([stripHtml(x.renderContents()) for x in data_tag.findAll('p')])
##        except:
##            log.info(self.log_msg('data not found'))
##            page['data'] = ''
        try:
            page['title'] = stripHtml(self.soup.find('div','opis').find('h2').renderContents())
        except:
            log.info(self.log_msg('title not found'))
            page['title'] =''
        try:
            rating = self.soup.find('div','opis').find('div','ocena').extract()
        except:
            log.info(self.log_msg('rating not found'))
        try:
            pos_neg = {'ef_data_rating_postive':'zi','ef_data_rating_negative':'cz'}
            for each in pos_neg.keys():
                page[each] = float(re.search('^\d+',stripHtml(rating.find('span',pos_neg[each]).renderContents())).group())
            page['ei_data_votes_count'] = int(re.search('<span class="t11">.*?(\d+)<span>',rating.find('p').__str__()).group(1))
        except:
            log.info(self.log_msg('User Raing not found cannot be found'))
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
            reviews = self.soup.find('ul','lista').findAll('li')
        except:
            log.info(self.log_msg('Cannot proceed , No reviews are found'))
            return False
        for review in reviews:
            page = {}
            page['uri'] = self.currenturi
            try:
                page['et_author_name'] = stripHtml(review.find('strong').renderContents())
            except:
                log.exception(self.log_msg('author name not found'))
            try:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                span_tags = review.findAll('span')
                date_str = stripHtml(span_tags[-1].renderContents())
                page['posted_date'] = datetime.strftime( datetime.strptime(date_str,'(%Y-%m-%d %H:%M:%S)'),"%Y-%m-%dT%H:%M:%SZ")
                if len(span_tags)==2:
                    page['et_author_ipaddress'] = stripHtml(span_tags[0].renderContents())[1:-1]
            except:
                log.info(self.log_msg('post info not found'))
                
            try:
                data_tag = copy.copy(review)
                [x.extract() for x in data_tag.findAll('span') ]
                data_tag.find('strong').extract()
                page['data'] = re.sub('^Autor:\s*?>','',stripHtml(data_tag.renderContents()),re.DOTALL).strip().strip()
            except:
                log.info(self.log_msg('rating not found'))
                page['data'] =''
            try:
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
            except:
                log.exception(self.log_msg('title not found'))
                page['title'] = ''
            try:
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                                 self.task.instance_data.get('update'),parent_list\
                                                            =[ self.parent_uri ]):
                    log.info(self.log_msg('Review cannot be added'))
                    continue
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

