
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#JV
#ASHISH YADAV
#Mohit

import re
import time
import random
import traceback
import copy
import md5
from BeautifulSoup import *
from datetime import datetime
from tgimport import *
import urllib2
from urllib2 import *
import urlparse
from baseconnector import BaseConnector
#from customdataconnector import CustomDataConnector
from utils.httpconnection import HTTPConnection
from keywordfilter import KeywordFilter
from extractors.extractor import Extractor
#from rssconnector import RSSConnector
from utils import utils
from utils.urlnorm import normalize
from utils.sessioninfomanager import *

import logging
log = logging.getLogger('HTMLConnector')
from utils.decorators import *

class HTMLConnector(BaseConnector):

    @logit(log, 'HTMLConnector')
    def fetch(self):
        """
        """
         #title assignment heuristics
         #1. if the title set from a rss feed or metapage is equal to the title found - no problem
         #else
         #OR
         #2. do a title and count(title), set all the titles, where count(title)>2 to none - use this
         #3. when at page - chk len(page.title)>len(link) ==> replace
         #print self.task.pagedata['title'], pagetitle
        try:
            self.genre = 'Generic'
            self.task.instance_data['source_type'] = self.task.instance_data.get('source_type','others')
            self.task.instance_data['source'] = self.task.instance_data.get('source',unicode(urlparse.urlparse(self.task.instance_data['uri'])[1]))
            page={}
            if not self.task.instance_data.get('metapage'):
                if not checkSessionInfo(self.genre, self.session_info_out,
                                        self.task.instance_data['uri'], self.task.instance_data.get('update')):
                    id=None
                    if self.session_info_out=={}:
                        id=self.task.id
                        log.debug(id)
                    if not self.rawpage:
                        log.debug('TaskID:%s::Client:%s::calling httpC_getHTML' % (self.task.id, self.task.client_name))
                        fetch_result=self._getHTML()
                        if not fetch_result:
                            log.error('TaskID:%s::Client:%s::httpC_getHTML failed' % (self.task.id, self.task.client_name))
                            return True
                        self.rawpage=fetch_result['result']
                        self.task.status['fetch_message']=fetch_result['fetch_message']
                        # if self.mimeType != 'text/html':
                        #     ## log.exception('TaskID:%s::Client:%s::MIME type not in text/html - returning' % (self.task.id,
                        #     ##                                                                                 self.task.client_name))
                        #     log.debug("Going for file connector as mime type is %s" %(self.mimeType))
                        #     # Get the file contents
                        #     try:
                        #         log.debug('TaskID:%s::Client:%s::taking up customdataconnector' % (self.task.id, self.task.client_name))
                        #         cInstance = CustomDataConnector(self.task, self.rawpage)
                        #         cInstance.processTask(self.token)
                        #         return True
                        #     except:
                        #         log.exception('TaskID:%s::Client:%s::no suitable connector found - returning' % (self.task.id, self.task.client_name))
                        #         log.critical('TaskID:%s::Client:%s::no suitable connector found - returning' % (self.task.id, self.task.client_name))
                        #         return False

                    self._setCurrentPage() # prepare the soup object
                    if not self.task.instance_data.get('disable_pagination'):
                        self._findFirstPage()
                    self._addLinksToCrawler()

                    page['data'] = utils.removeJunkData(self.rawpage)
                                ## page limit must be taken from config
                    page_num=0
                    if not self.task.instance_data.get('disable_pagination'):
                        while self._nextPageFound() and page_num < 5:
                            self.related_uris.append(self.currenturi)
                            page_num = page_num + 1
                            self._addLinksToCrawler()
                            if not self.task.instance_data.get('metapage'):
                                page['data'] = page['data'] + utils.removeJunkData(self.rawpage)
                    log.debug(self.log_msg('checking session info'))
                    try:
                        post_hash =  md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                                                    page.values()))).encode('utf-8','ignore')).hexdigest()
                    except:
                        print traceback.format_exc()
                        log.debug(self.log_msg("Error occured while creating hash in html connector"))
                        return False

                    result=updateSessionInfo(self.genre, self.session_info_out, self.task.instance_data['uri'], post_hash,
                                             'Post', self.task.instance_data.get('update'), Id=id)
                    if result['updated']:
                        try:
                            pagetitle = str(self.soup.find('title').find(text=True))
                            pagetitle = unicode(BeautifulStoneSoup(pagetitle, convertEntities=BeautifulStoneSoup.ALL_ENTITIES))
                        except Exception, e:
                            print traceback.format_exc()
                            pagetitle_match = re.search(r'<title>(.*?)</title>',str(self.soup))
                            if pagetitle_match:
                                pagetitle = unicode(BeautifulStoneSoup(pagetitle_match.group(1), convertEntities=BeautifulStoneSoup.ALL_ENTITIES))
                            else:
                                pagetitle = 'No Title'
                        page['title'] = pagetitle
                        if self.task.pagedata['title'] and len(pagetitle) < len(self.task.pagedata['title']):
                            log.debug('TaskID:%s::Client:%s::changing pagetitle' % (self.task.id, self.task.client_name))
                            page['title'] = self.task.pagedata['title']
                        log.debug('TaskID:%s::Client:%s::page title: %s ' % (self.task.id, self.task.client_name, page['title']))
                        page['uri'] = self.task.instance_data['uri']
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
                        page['parent_path'] = []
                        page['path'] = [page['uri']]
                        page['task_log_id']=self.task.id
                        page['entity'] = 'Html Page'
                        page['category']=self.task.instance_data.get('category','')
                        for key,value in self.task.pagedata.iteritems():
                            if key != 'title' and value:
                                page[key]=value
                        self.pages.append(page)
                        self.related_uris=list(set(self.related_uris))
                    else:
                        print "inner most else"
                        log.debug(self.log_msg("HTML page is not fetched %s" %self.currenturi))
                else:
                    print "inner2 else"
                    log.debug(self.log_msg("HTML page is not fetched %s" %self.currenturi))
            else:
                if not self.rawpage:
                    log.debug('TaskID:%s::Client:%s::calling httpC_getHTML' % (self.task.id, self.task.client_name))
                    fetch_result=self._getHTML()
                    if not fetch_result:
                        log.error('TaskID:%s::Client:%s::httpC_getHTML failed' % (self.task.id, self.task.client_name))
                        return True
                    self.rawpage=fetch_result['result']
                    self.task.status['fetch_message']=fetch_result['fetch_message']
                    if self.mimeType != 'text/html':
                        log.exception('TaskID:%s::Client:%s::MIME type not in text/html - returning' % (self.task.id,
                                                                                                        self.task.client_name))
                        return False
                self._setCurrentPage()
                self._addLinksToCrawler()
                log.debug(self.log_msg("Metapage found for url %s, returning" %self.currenturi))
            return True
        except Exception, e:
            print traceback.format_exc()
            log.exception('TaskID:%s::Client:%s::fetch failed in html connector' % (self.task.id, self.task.client_name))
            return False

    @logit(log, '_addLinksToCrawler')
    def _addLinksToCrawler(self):
        """
        """
        try:
            log.info(self.log_msg('levels : %s , %s:%s:%s'%(self.currenturi,self.task.level,self.level,self.max_recursion_level)))
            if self.task.level > self.max_recursion_level and not self.task.instance_data.get('metapage'):
                log.debug('TaskID:%s::Client:%s::recursion level greater then MAX, returning for %s' % (self.task.id, self.task.client_name,self.currenturi))
                return

            #increment=1
            #if self.task.instance_data['metapage']:
                #increment=0

            for anchor in self.soup.findAll('a',href=True):
                try:
                    url = normalize(unicode(anchor['href']), self.currenturi, self.base)
                    #apply regex patters to urls :
                    if self.task.instance_data.get('url_filter'):
                        url_pattern = re.compile(self.task.instance_data['url_filter'],
                                                 re.IGNORECASE|re.DOTALL)
                        if not url_pattern.search(url):
                            continue
                    log.info(self.log_msg("clone uri :: %s"%normalize(unicode(anchor['href']), self.currenturi, self.base)))
                    temp_task=self.task.clone()
                    temp_task.instance_data['uri']=normalize(unicode(anchor['href']), self.currenturi, self.base)
                    #temp_task.level=int(self.task.level)+increment
                    temp_task.pagedata['title']=getTitleFromLink(anchor)
                    temp_task.priority=self.task.priority
                    self.linksOut.append(temp_task)
                except:
                    log.exception('TaskID:%s::Client:%s::failed to create one of the clone tasks' % (self.task.id, self.task.client_name))
                    continue
            return True #intentional indentation
        except:
            log.exception('TaskID:%s::Client:%s::addLinksToCrawler failed' % (self.task.id, self.task.client_name))


@logit(log, 'getTitleFromLink')
def getTitleFromLink(anchor):
    try:
        if utils.stripHtml(str(anchor)) == '':
            return None
        title = utils.stripHtml(unicode(anchor.renderContents(),'utf-8'))
        #TODO - what is this and what does it do?
        title = unicode(BeautifulStoneSoup(title, convertEntities=BeautifulStoneSoup.ALL_ENTITIES))
        return title
    except:
        log.exception('TaskID:%s::Client:%s:: getTitleFromLink failed' % (self.task.id, self.task.client_name))
        return ''
