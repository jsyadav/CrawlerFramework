
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Ashish
from baseconnector import BaseConnector
import urllib2
import urlparse
import feedparser
from datetime import datetime, timedelta
import time
import copy
from tgimport import *
from utils.task import Task
from utils.urlnorm import normalize
from utils.utils import stripHtml
from utils.sessioninfomanager import *
import logging
import traceback
import cgi
#from logging import config
#logging.config.fileConfig('logging.cfg')

log = logging.getLogger('GoogleBlogSearchConnector')
class GoogleBlogSearchConnector(BaseConnector):
    def _createUrl(self,code):
        #creating url with self.task.instanceData['queryTerm']
        if not code:
            code=(self.task.instance_data.get('queryterm') or '')
        url_template = 'http://blogsearch.google.com/blogsearch_feeds?hl=en&scoring=d&ie=utf-8&output=rss&q=%s&num=%s'
        numresults = tg.config.get(path='Connector', key='google_blog_search_numresults')
        query_terms = []
#         if self.task.instance_data.get('apply_keywords'):
#             query_terms = self.task.keywords
#         if code:
#             query_terms = ['%s+%s'%(q,code)  for q in query_terms]
#         if not query_terms and code:
#             query_terms = [code]
        if self.task.keywords and self.task.instance_data.get('apply_keywords'):
            query_terms = ['%s %s'%(q.keyword.decode('utf-8','ignore'),code.decode('utf-8','ignore'))  for q in \
                               self.task.keywords if q.filter]
        else:
            query_terms = [code]
        return [url_template %(urllib2.quote(urllib2.unquote(query_term.strip().encode('utf-8'))),numresults) for query_term in query_terms]

#        url = 'http://blogsearch.google.com/blogsearch_feeds?hl=en&scoring=d&ie=utf-8&output=rss&q=%s&num=%s'%\
#            (code.replace(' ','+'),tg.config.get(path='Connector', key='google_blog_search_numresults'))
    
    def fetch(self):
        """
        specific needs: feedparser
        """
        try:
            self.unq_feedurls = []
            self.genre = 'Search'
            self.entity = 'search_result_page'
            if not self.task.instance_data.get('already_parsed'): #a flag to distinguish between tasks created by me , and original tasks
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
                    for url in urls:
                        temp_task=self.task.clone()
                        temp_task.instance_data['uri']= url
                        temp_task.instance_data['already_parsed'] = True
                        self.linksOut.append(temp_task)
                        log.info(self.log_msg('.no of Tasks added %d'%(len(self.linksOut))))
                    return True
                if not urls:
                    log.info(self.log_msg('url could not be created , by parsing url for code , from queryterm or from keyword'))
                    return False
            parser = feedparser.parse(self.currenturi)
            if len(parser.version) == 0:
                log.info(self.log_msg('parser version not found , returning'))
                return False
            if parser is not None:
                feeds=[{'title':stripHtml(entity.get('title','')), 'link':entity.link,
                        'posted_date':datetime.fromtimestamp(time.mktime(entity.updated_parsed)),
                        'author':stripHtml(entity.get('author','')) ,
                        'publisher':stripHtml(entity.get('publisher',''))} for entity in parser.entries]

                if not feeds:
                    self.task.status['fetch_status'] = True
                    log.info(self.log_msg('no results found for the above feed'))
                    return True
                log.info(self.log_msg('for blogsearch %s , %s entries found'%(self.currenturi,len(feeds))))
                last_timestamp= datetime(1,1,1) # initialize it to least possible value 
                for entity in feeds:
                    try:
                        if normalize(entity['link']) not in self.unq_feedurls and not \
                                                             checkSessionInfo(self.genre,
                                                                             self.session_info_out, entity['posted_date'],
                                                                             self.task.instance_data.get('update')):
                            last_timestamp = max(entity['posted_date'],last_timestamp)
                            self.unq_feedurls.append(normalize(entity['link']))
                            temp_task=self.task.clone()
                            temp_task.instance_data['already_parsed'] = True
                            temp_task.instance_data['uri']= normalize(entity['link'])
                            temp_task.pagedata['title']=entity['title']
                            temp_task.pagedata['posted_date']=datetime.strftime(entity['posted_date'] , "%Y-%m-%dT%H:%M:%SZ")
                            temp_task.pagedata['et_author_name'] = entity['author']
                            temp_task.pagedata['et_publisher_name'] = entity['publisher'] 
                            self.linksOut.append(temp_task)
                    except:
                        log.exception(self.log_msg("exception in adding temptask to linksout while parsing feed"))
                        continue
            log.info(self.log_msg("no. of unique new links added %d" %(len(self.linksOut))))
            if self.linksOut:
                updateSessionInfo(self.genre, self.session_info_out,last_timestamp , None,
                                  self.entity,self.task.instance_data.get('update'))
            return True
        except Exception, e:
            log.exception(log.info('exception in fetch'))
            self.task.status['fetch_status']=False
            return False
        
    def saveToSolr(self):
        return True
