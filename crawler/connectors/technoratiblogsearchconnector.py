
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#ashish

from baseconnector import TimeoutException
from baseconnector import BaseConnector
from BeautifulSoup import BeautifulSoup
from utils.task import Task
from utils.utils import stripHtml
from utils.urlnorm import normalize
from utils.httpconnection import HTTPConnection
from utils.decorators import logit
import urllib2
import urllib
import re
import datetime
import traceback
import logging
from tgimport import *
import pickle
import copy
from urllib import urlencode
from urllib2 import unquote,quote,urlparse
import md5
import cgi
from utils.sessioninfomanager import checkSessionInfo,updateSessionInfo

log = logging.getLogger('TechnoratiBlogSearchConnector')

class TechnoratiBlogSearchConnector(BaseConnector):

#     @logit(log,'_createSiteUrl')
#     def _createSiteUrl(self,code,params):
#         #testing 
# #        if not code:
# #            code =  '"Iphone 3G"'
#         #testing 
#         self.__referrer = 'http://search.technorati.com/' + quote(code) +'?'+urllib.urlencode(params)
#         log.info('seedurl: %s'%(self.__referrer))
#         self.__headers ={'Referer':self.__referrer}
#         return self.__referrer

    @logit(log, '_createUrl')
    def _createUrl(self,code,params):
        if not code:
            code=self.task.instance_data.get('queryterm')
#        url_template = 'http://search.technorati.com/' + '%s' +'?'+urllib.urlencode(params)
        query_terms = []
        if self.task.keywords and self.task.instance_data.get('apply_keywords'):
            query_terms = [q.keyword  for q in self.task.keywords if q.filter]
        if code:
            query_terms = ['%s %s'%(q,code)  for q in query_terms]
        if not query_terms and code:
            query_terms = [code]
        return ['http://search.technorati.com/'+urllib2.quote(urllib2.unquote(query_term.encode('utf-8'))) +'?'+ \
                    urllib.urlencode(params) for query_term in query_terms]


    @logit(log,'fetch')
    def fetch(self):
        try:
            self.genre = 'Search'
            self.entity = 'search_result_page'
            self.__num_posts_crawled = 0 #keeps track of posts crawled in current crawl..
            self.__num_search_results=tg.config.get(path='Connector', key='technorati_numresults')
            self.urls_fetched = []
#             code = None
#             if self.currenturi:
#                parsed_url = urlparse.urlparse(self.currenturi)
#                code = parsed_url[2]
#                params = {}
#                try:
#                    params = dict([part.split('=') for part in parsed_url[4].split('&')])
#                except:
#                    log.info(self.log_msg('could not find seed url any url params'))
#                params['authority'] = params.get('authority','a4') #default parameters for authority
#                params['language'] = params.get('language','en') #default parameters for language
#                log.info(self.log_msg(code))
#                if not code:
#                    self.task.status['fetch_status']=False
#                    return False
#             self.currenturi = self._createSiteUrl(unquote(code.replace('/','')),params)

            if not self.task.instance_data.get('already_parsed'): #a flag to distinguish between tasks created by me , and original tasks
                parsed_url = urlparse.urlparse(self.currenturi)
                query_term = parsed_url[2]
                params = dict(cgi.parse_qsl(parsed_url[4]))
                params['authority'] = params.get('authority','a4') #default parameters for authority
                params['language'] = params.get('language','en') #default parameters for language
                log.info(self.log_msg(query_term))
                urls = self._createUrl(query_term.replace('/',''),params)
                if len(urls) == 1:
                    self.currenturi = urls[0]
                else:
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

            self.__headers ={'Referer':self.currenturi}
            self.__last_timestamp = datetime.datetime(1,1,1) #initialize it to least value , to be updated as timestamp of the recent post
            res=self._getHTML(headers=self.__headers)
            self.rawpage=res['result']
            self._setCurrentPage()
            times_fetched = 0 #no of times 0 posts fetched for a url , if > 10 then it's search with no results and not a wrong response.
            while True:
                try:
                    if not self.addreviews():
                        break
                    next_page = self.soup.find('li',{'class':'next'})
                    if next_page: #checks if we have reached previously crawled page , reached page limit , 
                                                        #or there is no next page
                        log.info(self.log_msg('fetching :: %s'%('http://search.technorati.com' + next_page.a['href'])))
                        times_fetched = 0
                        self.currenturi = 'http://search.technorati.com' + next_page.a['href']
                        res=self._getHTML(headers=self.__headers)
                        self.rawpage=res['result']
                        self._setCurrentPage()
                    elif not self.soup.find('li',{'class':'hentry'}):
                        #problem with technorati search , sometimes gives back 0 posts result page for a normal request , so refetching until we get the required page
                        if times_fetched <= 5:
                            times_fetched+=1
                            log.info("got 0 posts page from so (%s) so fetching same page again"%(self.currenturi))
                            res=self._getHTML(headers=self.__headers)
                            self.rawpage=res['result']
                            self._setCurrentPage()
                        else:
                            break # got no results or less then  self.__num_search_results for this search else reached last page
                    else:
                        break
                except:
                    log.exception(self.log_msg('exception in fetch'))
                    continue

            log.info(self.log_msg("unique outlinks from technorati search page = %d"%(len(self.linksOut))))
            if self.linksOut:
                updateSessionInfo(self.genre, self.session_info_out,self.__last_timestamp , None,self.entity,self.task.instance_data.get('update'))
            self.task.status['fetch_status']=True
            return True
        except:
            log.exception(self.log_msg('exception in fetch'))
            self.task.status['fetch_status']=False
            return False

    @logit(log,'addreviews')
    def addreviews(self):
        not_last_page = False
        posts = self.soup.findAll('li',{'class':'hentry'})
        for post in posts:
            try:
                self.__num_posts_crawled +=1
                try:
                    date = post.find('abbr',{'class':'published'})['title']
                    posted_date = datetime.datetime.strptime(date.split('-')[0].strip(),'%a, %d %b %Y %H:%M:%S')
                except:
                    log.info(self.log_msg('could not parse post date'))
                    continue

                if self.__num_posts_crawled <= self.__num_search_results and not checkSessionInfo(self.genre, 
                                                                                        self.session_info_out, posted_date, 
                                                                                        self.task.instance_data.get('update')):
                    self.__last_timestamp = max(posted_date,self.__last_timestamp)
                    try:
                        title = stripHtml(post.find('h2',{'class':'entry-title'}).a.renderContents())
                    except:
                        title=''
                        log.info(self.log_msg('link title could not be parsed'))
                    try:
                        author = post.find('address' ,{'class':'author hcard'}).a
                        author_name = author.renderContents()
                        author_profile = author['href']
                    except:
                        author=None
                        log.info(self.log_msg('could not parse author info'))

                    try:
                        publisher = stripHtml(post.find('div',{'class':'meta'}).cite.renderContents())
                    except:
                        publisher=None

                    try:
                        authority = post.find('a',{'class':'links'}, text=re.compile('Authority:')).replace('Authority:','').strip()
                        authority = int(authority)
                    except:
                        authority=None
                        log.info(self.log_msg('could not parse link authority'))
                    try:
                        url = normalize(post.find('h2',{'class':'entry-title'}).a['href'])
                    except:
                        log.info(self.log_msg('url could not be normalized'))

                    not_last_page = True
                    if url in self.urls_fetched: #url already fetched in this current crawl
                        log.info(self.log_msg('got previously crawled url , so continue with the next url'))
                        continue
                    else:
                        self.urls_fetched.append(url)
                    temp_task=self.task.clone()
                    temp_task.instance_data['uri'] = url
                    temp_task.pagedata['title'] = title                                             
                    temp_task.pagedata['posted_date'] = datetime.datetime.strftime(posted_date, "%Y-%m-%dT%H:%M:%SZ")
                    if author:
                        temp_task.pagedata['et_author_name'] = author_name
                        temp_task.pagedata['et_author_profile'] = author_profile
                    if authority:
                        temp_task.pagedata['ei_url_authority'] = authority
                    if publisher:
                        temp_task.pagedata['et_publisher_name'] = publisher
                    self.linksOut.append(temp_task)
                else:
                    return False #limit of posts to crawl reached
                    
            except:
                log.exception(self.log_msg('exception in addreviews'))
                continue
        return not_last_page

    @logit(log,'saveToSolr')
    def saveToSolr(self,):
        return True
