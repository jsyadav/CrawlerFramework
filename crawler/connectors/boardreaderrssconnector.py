
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#JV

from baseconnector import BaseConnector
import urllib
import urllib2
import urlparse
import re
import copy
import feedparser
from datetime import datetime, timedelta
import time
from tgimport import *
from utils.task import Task
from utils.urlnorm import normalize
from utils.utils import stripHtml
import logging
from logging import config
from utils.decorators import *
from utils.sessioninfomanager import *

logging.config.fileConfig('logging.cfg')
log = logging.getLogger('BoardReaderRSSConnector')

class BoardReaderRSSConnector(BaseConnector):
    
    @logit(log,'_createSiteUrl')
    def _createSiteUrl(self,code):
        """
        returns Boardreader RSS Url for the given product code 
        """
        try:
            url='http://boardreader.com/index.php?a=rss&q=%s&p=%s&format=RSS2.0' %\
                (code,tg.config.get(path='Connector', key='boardreader_numresults'))
            log.debug(self.log_msg("seed url : %s" %(url)))
            return url
        except:
            log.exception(self.log_msg("Exception occured while creating url"))

    @logit(log,'fetch')    
    def fetch(self):
        """
        specific needs: feedparser
        
        SAMPLE URL
        http://boardreader.com/index.php?a=s&amp;q=%22Nokia+5300+XpressMusic%22
        http://boardreader.com/s/Nokia+5610+Express+Music.html

        SAMPLE Keyword
        "iPhone 3G"

        """
        try:
            code = None
           # if self.task.instance_data['queryterm'] and not self.task.instance_data['queryterm'].strip()=='':
               #  log.debug(self.log_msg("Creating url from keyword"))
#                 code = urllib.quote_plus(self.task.instance_data['queryterm'])
#             else:
            php_pattern =  "^http://boardreader\.com/index\.php\?a=s&q=(.+)$"
            html_pattern = "^http://boardreader\.com/s/(.+?)\.html"
            self.genre = 'Search'
            self.entity = 'search_result_page'
            self.last_timestamp = datetime(1,1,1) #initialize it to least value , to be updated as timestamp of the recent post
            if re.match(php_pattern,self.currenturi) and len(re.match(php_pattern,self.currenturi).groups())==1: #It is a PHP page
                log.debug(self.log_msg("page url is .php"))
                code=re.match(php_pattern,self.currenturi).group(1)
            elif re.match(html_pattern,self.currenturi) and len(re.match(html_pattern,self.currenturi).groups())==1: #It is a HTML page
                log.debug(self.log_msg("page url is .html"))
                code = re.match(html_pattern,self.currenturi).group(1)
            if code:
                self.uri = self._createSiteUrl(code)
            else:
                log.debug(self.log_msg("Not a boardreader url and No keyterm provided, Quitting"))
                self.task.status['fetch_status']=False
                return False
            parser = feedparser.parse(self.uri)
            if len(parser.version) == 0:
                log.debug(self.log_msg("%s is not a Feed url, Quitting" %(str(self.uri))))
                self.task.status['fetch_status']=False
                return False
            if parser is not None:
                feeds=[{'title':stripHtml(entity.title), 'link':entity.link,
                        'posted_date':datetime.fromtimestamp(time.mktime(entity.updated_parsed))} for entity in parser.entries]

                log.debug(self.log_msg("number of feeds %d" % len(feeds)))
                for entity in feeds:
                    try:
                        if  not checkSessionInfo(self.genre,
                                                 self.session_info_out, entity['posted_date'], 
                                                 self.task.instance_data.get('update')):
                            self.last_timestamp = max(entity['posted_date'],self.last_timestamp)
                            entity['link'] = normalize(entity['link']).encode('utf-8','replace')
                            log.debug(self.log_msg("Fetching %s" %(entity['link'])))
                            temp_task=self.task.clone()
                            temp_task.instance_data['uri']= normalize(entity['link'])
                            temp_task.pagedata['title']=entity['title']
                            temp_task.pagedata['posted_date']=datetime.strftime(entity['posted_date'],"%Y-%m-%dT%H:%M:%SZ")
                            self.linksOut.append(temp_task)
                        else:
                            '''
                            Important note - We are using RSS FEED for links, but the links are not necessarily in
                            order of date (and I have not find a way to do that yet), hence the loop run for all the links expected for the 
                            feed, and add those which should be added. It does not quit fetching the links at the occurance of the first 
                            link which has the timestamp less than prior crawl timestamp.
                            '''
                            log.debug(self.log_msg("Not appending boardreader link to temp_task")) 
                            continue
                    except:
                        log.exception(self.log_msg("Exception occured getting data from feed entity"))
                        continue
            log.debug(self.log_msg("number of new links added %d" %(len(self.linksOut))))
            if self.linksOut:
                updateSessionInfo(self.genre, self.session_info_out,self.last_timestamp , None,self.entity,self.task.instance_data.get('update'))
            return True
        except Exception, e:
            log.exception(self.log_msg("Exception occured in fetch()"))
            return False
    @logit(log,'saveToSolr')        
    def saveToSolr(self):
        return True
