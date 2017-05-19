
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#JV
#Ashish
from baseconnector import BaseConnector
import urllib2
import urlparse
import feedparser
from datetime import datetime, timedelta
import time
from tgimport import *
from utils.task import Task
from utils.urlnorm import normalize
import copy
import cgi
from utils.sessioninfomanager import checkSessionInfo,updateSessionInfo

import logging
log = logging.getLogger('GoogleNewsConnector')
from utils.decorators import *


class GoogleNewsConnector(BaseConnector):
    @logit(log, '_createSiteUrl')
    def _createSiteUrl(self,code,lang):
        if not code:
            code=(self.task.instance_data.get('queryterm') or '')
        if not lang:
            lang = 'en'
        url_template = 'http://news.google.com/news?q=%s&num=%s&output=rss&hl=%s&scoring=n'
        numresults = tg.config.get(path='Connector', key='google_news_search_numresults')
        query_terms = []
#         if self.task.keywords and self.task.instance_data.get('apply_keywords'):
#             query_terms = self.task.keywords
#         if code:
#             query_terms = ['%s+%s'%(q,code)  for q in query_terms]
#         if not query_terms and code:
        if self.task.keywords and self.task.instance_data.get('apply_keywords'):
            query_terms = ['%s %s'%(q.keyword.decode('utf-8','ignore'),code.decode('utf-8','ignore'))  for q in \
                                                                                self.task.keywords if q.filter]
        else:
            query_terms = [code]
        return [url_template %(urllib2.quote(urllib2.unquote(query_term.strip().encode('utf-8'))),numresults,lang) for query_term in query_terms]


    @logit(log, 'fetch')
    def fetch(self):
        """
        specific needs: feedparser
        """
        try:
            '''
            a flag to check if this task has already come to this connector and parsed as a url , so as to avoid infinite loops ,
            as i don't a have a way to distinguish between task created by me and the one which comes the first time , 
            as both may have the same url
            '''
            self.genre = 'Search'
            self.entity = 'search_result_page'
            if not self.task.instance_data.get('already_parsed'):
                #url_params = urlparse.urlparse(self.currenturi)[4]
                url_params = cgi.parse_qs(urlparse.urlparse(self.currenturi)[4])
                query_term = url_params.get('q')
                language_code = url_params.get('hl')
                if not language_code:
                    lang = self.task.instance_data.get('language','en')
                else:
                    lang = language_code[0]
                if query_term:
                    code = query_term[0]
                else:
                    code = None
                urls = self._createSiteUrl(code,lang)
                if len(urls) == 1:
                    self.currenturi = urls[0]
                else:
                    for url in urls:
                        temp_task=self.task.clone()
                        temp_task.instance_data['already_parsed'] = True
                        temp_task.instance_data['uri']= url
                        self.linksOut.append(temp_task)
                        log.info(self.log_msg('creating task : %s'%(url)))
                    log.info(self.log_msg('.no of Tasks added %d'%(len(self.linksOut))))
                    return True
                if not urls:
                    log.info(self.log_msg('url could not be created , by parsing url for code , from queryterm or from keyword'))
                    return False
            url = self.currenturi.replace('&scoring=n','') + '&ie=UTF-8' #not changeing url_template in this case because url is used to 
                                                              #get session_info , so i am changing the feedurl before getting the feed 
            
            log.info(self.log_msg('parsing %s for feed'%url))
            parser = feedparser.parse(url)
            unq_links = []
            if len(parser.version) == 0:# or (self.site.level > 1 and self.search_engine is False):
                return False
            if parser is not None:
                feeds=[{'title':entity.title, 'link':entity.link,
                        'posted_date':datetime.fromtimestamp(time.mktime(entity.updated_parsed))} for entity in parser.entries]

                if not feeds:
                    return True
                last_timestamp= datetime(1,1,1) #set it to least possible  value .
                for entity in feeds:
                    link_url_params = urlparse.urlparse(entity['link'])[4]
                    if not  cgi.parse_qs(link_url_params).get('url'):
                        continue
                    url = normalize(cgi.parse_qs(link_url_params).get('url')[0])
                    if not url in unq_links and not checkSessionInfo(self.genre,
                                                                     self.session_info_out, entity['posted_date'],
                                                                     self.task.instance_data.get('update')):
                        last_timestamp = max(entity['posted_date'],last_timestamp)
                        unq_links.append(url)
                        temp_task=self.task.clone()
                        temp_task.instance_data['already_parsed'] = True
                        temp_task.instance_data['uri']= url
                        temp_task.pagedata['title']=entity['title']
                        temp_task.pagedata['posted_date']=datetime.strftime(entity['posted_date'] , "%Y-%m-%dT%H:%M:%SZ")
                        self.linksOut.append(temp_task)
                log.info(self.log_msg('.no of links added %d'%(len(self.linksOut))))
#            self.session_info_out['timestamp']=feeds[0]['posted_date']
                if self.linksOut:
                    updateSessionInfo(self.genre, self.session_info_out,last_timestamp , None,self.entity,self.task.instance_data.get('update'))
            return True
        except Exception, e:
            log.exception(self.log_msg('exception in fetch'))
            return False


    @logit(log, 'saveToSolr')
    def saveToSolr(self):
        return True

