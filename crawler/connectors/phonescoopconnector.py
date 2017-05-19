'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#SKumar

import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime
from BeautifulSoup import BeautifulSoup

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging. getLogger("PhoneScoopConnector")

class PhoneScoopConnector(BaseConnector):    
    
    @logit(log,'fetch')
    def fetch(self):
        """
        It fetches the data from the url mentioned
        sample url : http://www.phonescoop.com/phones/user_reviews.php?phone=2320
        """
        self.genre = "Review"
        try:
            self.currenturi = self.parent_uri = self.currenturi + '&s=d'
            if not self.__setSoupForCurrentUri():
                log.info( self.log_msg("Soup not set for the uri:%s"\
                                                            %self.currenturi))
                return False
            if not self.__getParentPage():
                log.info(self.log_msg("Parent page not found for the uri %s"\
                                                            %self.currenturi))
            while True:
                main_page_soup = copy.copy(self.soup)
                if not self.__addReviews():
                    log.info(self.log_msg('fetched all posts for the url %s'%\
                                                                self.parent_uri))
                    break 
                try:
                    self.currenturi = self.task.instance_data['uri'].rsplit\
                        ('/', 1)[0] + '/' + main_page_soup.find('div', 'pgb')\
                                        .find('a', text='Next').parent['href']
                    if not self.__setSoupForCurrentUri():
                        log.info(self.log_msg('Soup not set for uri %s'%self.currenturi))
                        break 
                except:
                    log.info(self.log_msg('Next page not found for the url %s'\
                                                            %self.currenturi)) 
                    break 
            return True
        except:
            log.exception(self.log_msg("Exception in fetch %s"%self.parent_uri)) 
            return False
        
    @logit(log,'getparentpage')    
    def __getParentPage(self):
        """It fetches the product information
        """
        page = {'uri': self.currenturi, 'data':''}
        try:
            page['title'] = stripHtml(self.soup.find('h1').renderContents())
        except:
            log.exception(self.log_msg("Exception Occurred while fetching the title!!"))
            return False
        try:
            rating_info = stripHtml( self.soup.find('a', attrs={'name':'avgRating'})\
                .findParent('p').renderContents()).replace('Average User Rating:', '')\
                                                                .strip().split('\n')
        except:
            log.info(self.log_msg("Exception while getting the Rating Tag in url :%s"\
                                                                    %self.currenturi ))
        try:
            page['ef_product_rating_overall'] = float(rating_info[0])
        except:
            log.info(self.log_msg("Exception while getting the Product Rating in url :%s"\
                                                                    %self.currenturi ))
        try:
            page['ei_product_reviews_count'] = int(re.search('based on (\d+)',\
                                                        rating_info[1]).group(1))
        except:
            log.info(self.log_msg("Exception while getting the Product Reviews \
                                        count in the url:%s"%self.currenturi))
        try: 
            self.updateParentExtractedEntities( page ) 
            if checkSessionInfo(self.genre, self.session_info_out, self.currenturi, \
                                self.task.instance_data.get('update')):
                log.info( self.log_msg('Session info return True for the url %s\
                                                            '%self.currenturi) )
                return False
            result = updateSessionInfo(self.genre, self.session_info_out, self.currenturi,\
                    get_hash(page) ,'Post', self.task.instance_data.get('update'))
            if not result['updated']:
                return False
            page['path'] = [self.currenturi]
            page['parent_path'] = []
            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
            page['priority'] = self.task.priority
            page['level'] = self.task.level
            page['last_updated_time'] = page['posted_date'] = page['pickup_date'] \
                    = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            page['client_name'] = self.task.client_name
            page['versioned'] = False
            page['task_log_id'] = self.task.id
            page['entity'] = 'Post'
            page['category'] = self.task.instance_data.get('category','')
            self.pages.append(page)
            log.info(self.log_msg('Parent Page added'))
            return True
        except:
            log.exception(self.log_msg("parent post couldn't be parsed for url \
                                                        %s"%self.currenturi))
            return False
        
    @logit(log, '_addreviews')
    def __addReviews(self):
        '''
            It will fetch the the reviews from the given review uri
            and append it  to self.pages
        '''
        try:
            review_uri = self.currenturi
            review_seperator = str(self.soup.find('div','pshr'))
            reviews = [ BeautifulSoup(x) for x in self.soup.find('div',\
                        id='content480').renderContents().split(review_seperator)]
            log.info(self.log_msg('no of reviews is %s' % len(reviews)))
            if not reviews:
                return False
        except:
            log.exception(self.log_msg('Cannot get Reviews in url:%s'%review_uri))
            return False
        for review in reviews:
            page = {}
            try: 
                title_tag = review.find('h2')
                page['title'] =  stripHtml(title_tag.renderContents())
            except:
                log.exception(self.log_msg('Title cannot be fetched for url %s'\
                                                                    %review_uri))
                page['title'] = ''
            try:
                page['data'] = stripHtml(review.find('p','semism').findPrevious\
                                                        ('p').renderContents())
            except:
                log.exception(self.log_msg('data cannot be fetched for url %s'\
                                                                    %review_uri))
                page['data'] = ''                                   
            #Check that both title and data are not absent, so baseconnector does not throw exception    
            if not page['title'] and not page['data']:
                log.info(self.log_msg("Data and title not found for %s,"\
                    " discarding this review"%(review_uri)))
                continue
            try:
                page['uri'] = review_uri + '#' + review.find('h2').findNext('a')\
                                                                        ['name']
            except:
                log.exception(self.log_msg('uri cannot be fetched for url %s'\
                                                                    %review_uri))
                continue
            try:
                rating_tag = title_tag.findPrevious('nobr')                
                date_str = stripHtml(rating_tag.findParent('p').renderContents())
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str\
                                                ,'%b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception(self.log_msg('Posted date cannot be fetched for url\
                                                                %s'%review_uri))
                continue
            try:
                page['ef_rating_overall'] = float(len(rating_tag.findAll('img',\
                                                    src=re.compile('r_b\.gif'))))
            except:
                log.info(self.log_msg("Individual rating cannnot be fetched for \
                                                        the uri%"%spage['uri']))
            try:
                author_tag = title_tag.findNext('p').a
                page['et_author_name'] = stripHtml(author_tag.renderContents())
            except:
                log.info(self.log_msg("Author name cannot be fetched for url %s\
                                                                "%page['uri']))
            try:
                page['et_author_profile'] = 'http://www.phonescoop.com/'  + \
                                            author_tag['href'].split('/',1)[-1]
            except:
                log.info(self.log_msg("Author Profile cannot be fetched for url\
                                                                %s"%page['uri']))
            try:
                self.currenturi =  page['et_author_profile']
                if self.__setSoupForCurrentUri():
                    div_tag = self.soup.find('div', id='content480')
                    try:
                        author_desc = '\n'.join([stripHtml(x.renderContents()) for\
                                                    x in div_tag.findAll('ul')])
                        if author_desc:
                            page['et_author_description'] = author_desc
                    except:
                        log.info(self.log_msg("Author description not fetched \
                                for the author %s"%page['et_author_profile']))
                    try:
                        date_str = div_tag.find('p', text=re.compile('^Member since'))
                        page['edate_author_member_since'] = datetime.strftime(\
                            datetime.strptime(date_str, 'Member since %B %d, %Y'),\
                                                            "%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg("Author member since not fetched \
                                    for the author %s"%page['et_author_profile']))
            except:
                log.info(self.log_msg("Author info not fetched for the uri %s"%page['uri']))
            try:
                if checkSessionInfo(self.genre, self.session_info_out, page['uri'],\
                             self.task.instance_data.get('update'), parent_list\
                                                        =[self.parent_uri]):
                    log.info(self.log_msg('session info return True'))
                    return False
                result = updateSessionInfo(self.genre, self.session_info_out, page['uri'], \
                    get_hash(page), 'Review', self.task.instance_data.get('update'), \
                                                parent_list=[self.parent_uri])
                if not result['updated']:
                    log.info(self.log_msg('result not updated'))
                    continue
                page['path'] = page['parent_path'] = [self.parent_uri]
                page['path'].append(page['uri'])
                page['priority'] = self.task.priority
                page['level'] = self.task.level
                page['last_updated_time'] = page['pickup_date'] = datetime.\
                            strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                page['client_name'] = self.task.client_name
                page['versioned'] = False
                page['entity'] = 'Review'
                page['category'] = self.task.instance_data.get('category','')
                page['task_log_id'] = self.task.id
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append(page)
                log.debug(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Exception while adding page for the\
                                                            url %s'%page['uri']))
        return True
                

    @logit(log,'__setSoupForCurrentUri')
    def __setSoupForCurrentUri(self, data=None, headers={}):
        """It will set soup object for the Current URI
        """
        res = self._getHTML(data=data, headers=headers)
        if res:
            self.rawpage = res['result']
        else:
            log.info(self.log_msg('HTML Content cannot be fetched for the url: \
                                                            %s'%self.currenturi))
            return False
        self._setCurrentPage()
        return True