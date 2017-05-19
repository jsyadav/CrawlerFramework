
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

import time
import random
import cgi
import traceback
from BeautifulSoup import BeautifulSoup
from tgimport import *
from utils.urlnorm import normalize
from utils.utils import stripHtml
import urllib2
import urlparse
from utils.task import Task
from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo,updateSessionInfo

import logging
log = logging.getLogger('microsoftlivesearchconnector')
from utils.decorators import *

class MicrosoftLiveSearchConnector(BaseConnector):

    @logit(log, '_createUrl')
    def _createUrl(self,code):
        if not code:
            code=(self.task.instance_data.get('queryterm') or '')

        url_template = "http://search.live.com/results.aspx?q=%s&go=&form=QBLH"

        query_terms = []
        if self.task.keywords and self.task.instance_data.get('apply_keywords'):
            query_terms = ['%s %s'%(q.decode('utf-8','ignore'),code.decode('utf-8','ignore'))  for q in self.task.keywords]
        else:
            query_terms = [code]

        return [url_template %(urllib2.quote(urllib2.unquote(query_term.strip().encode('utf-8')))) for query_term in query_terms]

    @logit(log, 'fetch')
    def fetch(self):

        #A MATTER OF POLICY - NOWHERE RELATED TO TECHNICAL
        #google connector and google news connector should not increase the level
        #since clone in task.py increases the level of the linksOut this ovveride shall handle it
        #        self.task.level=self.task.level-1
        try:
            # Do not use the connector as a linkout from another URL
            _baseUri = 'http://search.live.com'
            parentUrl = self.currenturi
            numresults = tg.config.get(path='Connector', key='live_search_numresults')
            self.genre = 'generic'
            if self.task.level > 1:
                log.debug(self.log_msg('recursion level greater then MAX, returning'))
                return True
            # Check if keywords should be added to the query term
            if not self.task.instance_data.get('already_parsed'):
                url_params = urlparse.urlparse(self.currenturi)[4]
                query_term = cgi.parse_qs(url_params).get('q')
                if query_term:
                    code = query_term[0]
                else:
                    code = None
                urls = self._createUrl(code)

                if len(urls) == 1:
                    self.currenturi = urls[0]
                else:
                    # Set the task level back by 1 since it is a search connector
                    self.task.level = self.task.level - 1
                    for url in urls:
                        temp_task=self.task.clone()
                        temp_task.instance_data['uri']= url
                        temp_task.instance_data['already_parsed'] = True
                        self.linksOut.append(temp_task)
                        log.info(self.log_msg('creating task : %s'%url))
                    log.info(self.log_msg('.no of Tasks added %d'%(len(self.linksOut))))
                    return True
                if not urls:
                    log.info(self.log_msg('url could not be created , by parsing url for code , from queryterm or from keyword'))
                    return False

            log.info('Fetching uri :: %s'%(self.currenturi))
            fetch_result = self._getHTML()
            self.rawpage=fetch_result['result']
            self.task.status['fetch_message']=fetch_result['fetch_message']
            self._setCurrentPage()
#             links = [[urllib2.unquote(each.a['href'].split('**')[-1]),stripHtml(each.a.renderContents())] for each in \
#                          self.soup.findAll('div',{'class':re.compile('res')}) \
#                          if not each.find('address')]
            #log.debug("Adding %s links>>%s" %(len(self.links), str(self.links)))
            unq_links = []
            #using genre generic , and updating session info like usual , and sending post_hash as ''
            #creating top level url   , as google site connector
            if not checkSessionInfo(self.genre, self.session_info_out,
                                        parentUrl, self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    result=updateSessionInfo(self.genre, self.session_info_out,parentUrl, '',
                                             'Search', self.task.instance_data.get('update'), Id=id)

            #then creating nodes for  all the linkouts from search page.
            start = 0 
            next = True
            count = 0
            while next:
                results = self.soup.findAll('ul',{'class':'sb_meta'});                
                next = self.soup.find('a',{'class':'sb_pagN'}) 
                for res in results:
                    if count > numresults:
                        break
                    else:
                        count+=1
                    try:
                        link = res.parent.a['href']
                        title = stripHtml(res.parent.a.renderContents())
                        if link not in unq_links and  not checkSessionInfo(self.genre,
                                                                             self.session_info_out, link,
                                                                             self.task.instance_data.get('update'),
                                                                             parent_list=[parentUrl]):
                            id=None
                            if self.session_info_out=={}:
                                id=self.task.id
                            updateSessionInfo(self.genre, self.session_info_out, link, '',
                                              'Search', self.task.instance_data.get('update'), Id=id,parent_list=[parentUrl])
                            unq_links.append(link)
                            temp_task=self.task.clone()
                            temp_task.instance_data['uri']=normalize(link)
                            temp_task.pagedata['title'] = title
                            temp_task.instance_data['already_parsed'] = True
                            self.linksOut.append(temp_task)
                    except:
                        log.exception(self.log_msg('Error while iterating through results'))
                if count > numresults:
                    break
                if next:
                    self.currenturi = _baseUri  + next['href']
                    fetch_result = self._getHTML()
                    self.rawpage=fetch_result['result']
                    self._setCurrentPage()
                    log.info('Fetching uri :: %s'%(self.currenturi))
            log.info("no. of linkouts = " +  str(len(self.linksOut)))
            return True
        except Exception,e:
            log.exception(self.log_msg('exception in fetch '))
            return False

    @logit(log, 'saveToSolr')
    def saveToSolr(self):
        return True
