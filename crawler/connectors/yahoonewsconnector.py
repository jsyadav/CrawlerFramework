
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
import simplejson
#from logging import config
#logging.config.fileConfig('logging.cfg')

log = logging.getLogger('YahooNewsConnector')
class YahooNewsConnector(BaseConnector):
    def _createUrl(self,code):
        #creating url with self.task.instanceData['queryTerm']
        if not code:
            code=(self.task.instance_data.get('queryterm') or '')
#        url_template = 'http://boss.yahooapis.com/ysearch/news/v1/%s?appid=%s&format=json&orderby=date&start=##start##&count=##count##'
        url_template = 'http://news.search.yahoo.com/ysearch/news/v1/%s?appid=%s&format=json&orderby=date&start=##start##&count=##count##'
        appid = tg.config.get(path='Connector', key='yahoo_appid')
        query_terms = []
        if self.task.keywords and self.task.instance_data.get('apply_keywords'):
            query_terms = ['%s %s'%(q.decode('utf-8','ignore'),code.decode('utf-8','ignore'))  for q in self.task.keywords]
        else:
            query_terms = [code]
        return [url_template %(urllib2.quote(urllib2.unquote(query_term.strip().encode('utf-8'))),appid) for query_term in query_terms]
    
    def fetch(self):
        """
        specific needs: feedparser
        """
        try:
            numresults = tg.config.get(path='Connector', key='yahoo_news_search_numresults')
            appid = tg.config.get(path='Connector', key='yahoo_appid')
            self.unq_feedurls = []
            self.genre = 'Search'
            self.entity = 'search_result_page'
            if not self.task.instance_data.get('already_parsed'): #a flag to distinguish between tasks created by me , and original tasks
                url_params = urlparse.urlparse(self.currenturi)[4]
                query_term = cgi.parse_qs(url_params).get('p')
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
            log.info('TASkURI ::: %s'%self.currenturi)
            last_timestamp= datetime(1,1,1) # initialize it to least possible value 
            self.currenturi = self.currenturi.replace('http://news.search.yahoo.com','http://boss.yahooapis.com')
            start  = 0
            count = numresults if numresults <= 50 else 50
            self.currenturi = self.currenturi.replace('##count##',str(count)).replace('##start##',str(start))
            while True:
                log.info(self.log_msg('app_request_uri :: %s'%(self.currenturi)))
                results = simplejson.loads(urllib2.urlopen(self.currenturi).read())
                if results['ysearchresponse']['responsecode'] != '200':
                    return False
                if results['ysearchresponse']['totalhits'] == '0':
                    log.info(self.log_msg('No results were found for the query'))
                    return True
                for each in results['ysearchresponse']['resultset_news']:
                    try:
                        url = each['url']
                        posted_date = datetime.strptime(each['date']+each['time'] , '%Y/%m/%d%H:%M:%S')
                        if normalize(url) not in self.unq_feedurls and not \
                                                                             checkSessionInfo(self.genre,
                                                                             self.session_info_out, posted_date,
                                                                             self.task.instance_data.get('update')):
                            log.info(self.log_msg('appending url:: %s'%url))
                            last_timestamp = max(posted_date,last_timestamp)
                            self.unq_feedurls.append(normalize(url))
                            temp_task=self.task.clone()
                            temp_task.instance_data['already_parsed'] = True
                            temp_task.instance_data['uri']= normalize(url)
                            try:
                                temp_task.pagedata['title']=each['title']
                            except:
                                log.exception(self.log_msg('error in extracting title of the search entry'))
                            temp_task.pagedata['posted_date']=datetime.strftime(posted_date , "%Y-%m-%dT%H:%M:%SZ")
                            try:
                                if each['source']:
                                    temp_task.pagedata['et_publisher_name'] = stripHtml(each['source'])
                            except:
                                log.info(self.log_msg('could not extract publisher name'))
                            self.linksOut.append(temp_task)
                    except:
                        log.exception(self.log_msg("exception in adding temptask to linksout"))
                        continue
                start += count
                if start < numresults and start <= results['ysearchresponse']['totalhits']:
                    self.currenturi = 'http://boss.yahooapis.com' + results['ysearchresponse']['nextpage']
                else:
                    break
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

#     def get_date(self,date):
#         try:
#             posted_date = None
#             date_exp = re.search(re.compile(r'([0-9]*) (hour|hours|minute|minutes|day|days|month|months) ago'),date)
#             if date_exp:
#                 if date_exp.group(2) in ['day','days']:
#                     posted_date = datetime.utcnow()-timedelta(days=int(date_exp.group(1)))
#                 elif date_exp.group(2) in ['hour','hours']:
#                     posted_date =  datetime.utcnow()-timedelta(seconds=3600*int(date_exp.group(1)))
#                 elif date_exp.group(2) in '[minute,minutes]':
#                     posted_date =  datetime.utcnow()-timedelta(seconds=60*int(date_exp.group(1)))
#                 elif date_exp.group(2) in '[month,months]':
#                     posted_date =  datetime.utcnow()-timedelta(days=30*int(date_exp.group(1)))
#         except:
#             log.debug(traceback.format_exc())
#         if not posted_date:
#             posted_date = datetime.utcnow()
#         return posted_date
