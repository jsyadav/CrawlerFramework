
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

import urllib
import urllib2
import re
from datetime import datetime
import md5
import pickle
import logging
from logging import config
import simplejson
import cgi
from urlparse import urlparse
from BeautifulSoup import BeautifulSoup
import copy

from baseconnector import BaseConnector
from htmlconnector import HTMLConnector
from tgimport import *
from kmlogging import *
from knowledgemate import pysolr
from knowledgemate import model

from utils.sessioninfomanager import *
from utils.task import Task
from utils.utils import stripHtml
from utils.urlnorm import normalize
from utils.decorators import *

logging.config.fileConfig('logging.cfg')
log = logging.getLogger('DiggConnector')

class DiggConnector(BaseConnector):

    @logit(log,'_createSiteUrl')
    def _createSiteUrl(self,digg_url):
        """
        Returns a digg url for the given keyword and other attributes
        """
        try:
            page_url = digg_url+"&type=all&area=all&sort=new"
            log.debug(self.log_msg(page_url))
            return page_url
        except Exception , e:
            log.exception(self.log_msg("Exception occured while creating URL"))
            raise e

    @logit(log,'fetch')
    def fetch(self):
        """
        Fetches the first RESULTS_ITERATIONS results as specified by the attributes, and populate the result links to self.linksOut
        """
        try:
            self.genre = 'Search'
            self.entity = 'search_result_page'
            self.last_timestamp = datetime(1,1,1) #initialize it to least value , to be updated as timestamp of the recent post
            self.currenturi = self._createSiteUrl(self.task.instance_data['uri'])
            self.RESULTS_ITERATIONS = tg.config.get(path='Connector',key='digg_search_numresults') #This variable is used as a constant, hence its in all Caps
            self.iterator_count = 0
            self.done = False
            while self.iterator_count < self.RESULTS_ITERATIONS and not self.done:
                res = self._getHTML(self.currenturi)
                self.rawpage = res['result']
                self._setCurrentPage()
                self._searchPageResults()
                try:
                    next_link  = [cgi.unescape(each.get('href')) for each in self.soup.findAll('a',attrs={'class':'nextprev'}) if re.match("Next.*",each.renderContents())][0]
                    self.currenturi = "http://digg.com"+next_link
                    log.debug(self.log_msg(self.currenturi))
                except:
                    self.done = True
                    log.info(self.log_msg("Next link not found while iterating over"))
                    break
                       
            log.debug(self.log_msg(str(self.iterator_count)+" results iterated"))
            if self.linksOut:
                updateSessionInfo(self.genre, self.session_info_out,self.last_timestamp , None,self.entity,self.task.instance_data.get('update'))
            return True
        except:
            log.exception(self.log_msg("Exception occured in fetch()"))
            return False
        
    @logit(log,'_searchPageResults')
    def _searchPageResults(self):
        """
        Reads the entire page for the links and calls the _getLinksData() Function to stores them in self.linksOut 
        """
        try:
            if self.soup:
                search_page_soup =  self.soup.findAll('div',attrs={'class':'news-summary'})
                for each_link in search_page_soup:
                    try:
                        if self.iterator_count <= self.RESULTS_ITERATIONS and not self.done:
                            self.each_search_link=each_link 
                            self._getLinkData()
                        else:
                            self.done = True
                            return True
                    except:
                        log.exception(self.log_msg("Exception occured while calling _getLinkData()"))
                        continue 
            else: #This should not happen ever - 
                log.info(self.log_msg("No HTML Page is set"))
        except:
            log.exception(self.log_msg("Exception occured in _searchPageResults()"))
            return False
               
    @logit(log,'_getLinkData')
    def _getLinkData(self):
        """
        Stores the data related to a particular digg search result link to self.linksOut
        """
        try:
            self.iterator_count = self.iterator_count + 1    
            log.debug(self.log_msg("Picking link number %d" %(self.iterator_count)))
            if self.iterator_count < self.RESULTS_ITERATIONS:
                try:
                    review_date = datetime.strptime(self.each_search_link.findAll('span',attrs={'property':'dc:date'})[0].get('content'),"%Y-%m-%d %H:%M:%S")
                except:
                    review_date = datetime.utcnow()
                    log.info(self.log_msg("Error occured while fetching review date"))
                    
                if  not checkSessionInfo(self.genre,
                                         self.session_info_out, review_date, 
                                         self.task.instance_data.get('update')):

                    self.last_timestamp = max(review_date,self.last_timestamp)

                    try:
                        review_link =  self.each_search_link.findAll('h3',attrs={'id':'title0'})[0].find('a').get('href')
                    except:
                        log.info(self.log_msg("Error occured while fetching review link"))
                        return False
                    try:
                        review_title =  stripHtml(self.each_search_link.findAll('h3',attrs={'id':'title0'})[0].find('a').renderContents())
                        log.debug(self.log_msg(review_title))
                    except:
                        review_title = ''
                        log.info(self.log_msg("Error occured while fetching review title"))

                    try:
                        author_name = stripHtml(self.each_search_link.find('span',attrs={'class':'tool user-info'}).find('a').renderContents())
                    except:
                        log.info(self.log_msg("Error occured while fetching author name"))

                    try:
                        author_profile_link = self.each_search_link.find('span',attrs={'class':'tool user-info'}).find('a').get('href')
                    except:
                        log.info(self.log_msg("Error occured while fetching author profile link"))

                    temp_task = self.task.clone()
                    temp_task.instance_data['uri']=review_link #We are certain this exists (else we would have returned from the function)
                    temp_task.pagedata['posted_date'] = datetime.strftime(review_date,"%Y-%m-%dT%H:%M:%SZ")
                    temp_task.pagedata['title']= review_title #This DOES exists
                    if author_name:
                        temp_task.pagedata['et_author_name']=author_name
                    if author_profile_link:
                        temp_task.pagedata['et_author_profile']="http://digg.com%s" %(author_profile_link)
                    self.linksOut.append(temp_task)
                    return True
                else:
                    #Not logging the link name as it would require an unnecessary retrival of link
                    log.debug(self.log_msg("Not appending digg link to temp_task")) 
                    return True
            else:
                log.debug(self.log_msg("Read all the links for the url%s" %(self.task.instance_data['uri'])))
                self.done = True
                return True
        except:
            log.exception(self.log_msg("Exception occured in _getLinkData()"))
            return False

    @logit(log,'saveToSolr')                                          
    def saveToSolr(self):
        """
        Saves the data to Solr
        """
        # As nothing to save from THIS connector, simply return True
        return True
