
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
import logging
from urlparse import urlparse
from urllib2 import quote,unquote
from datetime import datetime
import cgi

from tgimport import tg
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.urlnorm import normalize
from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log=logging.getLogger('TechTargetSearchStorageConnector')
class TechTargetSearchStorageConnector(BaseConnector):

    @logit (log, "fetch")
    def fetch(self):
        """
        Sample input Uri
        http://searchstorage.techtarget.com/search/1,293876,sid5,00.html?query=Inmage&x=4&y=8
        http://searchstorage.techtarget.com/search/1,293876,sid5,00.html?query=Inmage&x=0&y=0
        http://searchstorage.techtarget.com/search/1,293876,sid5,00.html?query=Inmage+store&x=0&y=0
        This will give search results of 3 Kinds
        1) SEARCHSTORAGE.COM RESULTS FOR: ANY_KEYWORD
        2) VENDOR RESULTS FOR: ANY_KEYWORD
        3) WEB RESULTS
        --- It is instructed to take only the SEARCHSTORAGE.COM RESULTS
        --- and the results are to be taken after 01, Jan 2007
        --- Not to take comments
        --- take Data, Posted data, Author name
        --- Eleminate the data which are from diffent sites or which requires login
        Things to Do
        -----------
            1) Check for "VIEW MORE SEARCHSTORAGE.COM RESULTS", Set the soup
            2) Pick up the Search Results Which had been posted after 2007
            3) set the session info with the maximum time stamp
            4) Create temp task and send back the links
            5) These will come to the same connector againg from the taskmaste So
                Check the kind of uri, and extract the data
                for article kind of uri genre is "Review"
                for search based, the genre is "Search"
        ----> Here are following types of data
            1) News
            2) Magazine with multiple pages
            3) Magazine with single article

        """

        try:
            self.genre = 'Search'
            self.baseurl = 'http://searchstorage.techtarget.com'
##            self.currenturi = 'http://searchstorage.techtarget.com/magazineFeature/0,296894,sid5_gci1341907,00.html'
##            self.currenturi = 'http://searchstorage.techtarget.com/magazineFeature/0,296894,sid5_gci1334530,00.html'
##            self.currenturi = 'http://searchstorage.techtarget.com/newsItem/0,289139,sid5_gci1005918,00.html'
##            self.currenturi ='http://searchstorage.techtarget.com/originalContent/0,289142,sid5_gci1255552,00.html'
##            self.currenturi ='http://searchstorage.techtarget.com/news/article/0,289142,sid5_gci1320726,00.html'
##            self.currenturi ='http://searchstorage.techtarget.com/search/1,293876,sid5,00.html?query=Inmage&x=4&y=4'
##            self.currenturi ='http://searchstorage.techtarget.com/columnItem/0,294698,sid5_gci1157571,00.html'
            if re.match('http://searchstorage\.techtarget\.com/search/.+\.html\?query=.+&x=\d+&y=\d+'
                                                                            ,self.currenturi):
                try:
                    if not self.task.instance_data.get('already_parsed'): #a flag to distinguish between tasks created by me , and original tasks
                        url_params = urlparse(self.currenturi)[4]
                        query_term = cgi.parse_qs(url_params).get('query')
                        if query_term:
                            code = query_term[0]
                        else:
                            code = None
                        urls = self.__createUrl( code )
                        if len(urls) == 1:
                            self.currenturi = urls[0]
                        else:
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
                    else:
                        log.info('already Parsed')
                except:
                    log.exception(self.log_msg('Erorr while checking forming the urls using key words and Query  term'))
                self.entity = 'search_result_page'
                self.parenturi = self.currenturi
                if not self._setSoup():
                    return False
                more_page = False
                try:
##                    try:
##                        params = self.currenturi.split('?')[-1]
##                    except:
##                        log.info(self.log_msg('no params'))
##                        params= None
##                    try:
##                        data = dict(cgi.parse_qsl(params))
##                    except:
##                        log.info(self.log_msg('no data'))
##                        data =None
##                    headers = {'Referer':self.currenturi}
                    self.currenturi = 'http://searchstorage.techtarget.com' + self.soup.find(True,text=re.compile('\s+SEARCHSTORAGE.COM RESULTS FOR:.+')).findParent('table').findNext('table').find('span','a2').findParent('a')['href']
                    if self._setSoup():
                        more_page= True
                    else:
                        log.info(self.log_msg('Soup is not set'))
                except:
                    log.exception(self.log_msg ('More Results Page not found'))
                    more_page = False
                recent_time_stamp = datetime(1,1,1)

                while True:
                    try:
                        for each in self.soup.find(True,text=re.compile('\s*SEARCHSTORAGE.COM RESULTS FOR:.+')).findParent('table').findNext('table',width='100%',cellspacing='2').findAll('td','body'):
                            try:
                                posted_date = datetime.strptime( stripHtml( each.find('span').renderContents().replace('.','') ), "(%b %d, %Y)" )
                            except:
                                log.info(self.log_msg('Not an entity'))
                                log.info(each.__str__())
                                break
                            recent_time_stamp = max( recent_time_stamp , posted_date )
                            if not checkSessionInfo(self.genre,self.session_info_out, posted_date , self.task.instance_data.get('update')):
                                temp_task=self.task.clone()
                                temp_task.instance_data[ 'uri' ] = normalize( each.find('a')['href'] )
                                self.linksOut.append( temp_task )
                        self.currenturi = 'http://searchstorage.techtarget.com' + self.soup.find('a',text='Next').parent['href']
                        if not self._setSoup():
                            log.info(self.log_msg('Soup is not set'))
                            break
                    except:
                        log.exception(self.log_msg('next page not found'))
                        break
                if self.linksOut:
                    updateSessionInfo(self.genre, self.session_info_out, recent_time_stamp, None,self.entity,self.task.instance_data.get('update'))
                return True
            elif re.match(r'http://searchstorage.techtarget.com/.+/.+\.html$',self.currenturi ):
                log.info(self.log_msg ('information need to be Extracted ') )
                if not self._setSoup():
                    log.info(self.log_msg('soup is not set, so exit from fetch'))
                    return False
                if self._addArticlePage():
                    log.info(self.log_msg('article added'))
                    return True
                else:
                    return False
        except:
            log.exception(self.log_msg ('Error in Fetch '))
            return True


    @logit(log, "_setSoup")
    def _setSoup( self, url = None, data = None, headers = {} ):
        """
            It will set the uri to current page, written in seperate
            method so as to avoid code redundancy
        """
        if not url:
            url = self.currenturi
        else:
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

    @logit(log,"_addArticlePage")
    def _addArticlePage( self ):
        """
        get the article info,
        It will get title, posted_date,uri,source
        """
        page = {}
        self.genre = 'Review'
        head_str =''
        title = self.soup.find('h1','a4')
        if title:
            page = self.__getArticlePage( page )
        elif self.soup.find('span','homeSplashTitle'):
            page = self.__getMagazinePage(page)
        else:
            log.info(self.log_msg('It is a article page from other site'))
            return False
        try:

            article_hash = get_hash( page )
            if checkSessionInfo(self.genre,self.session_info_out, self.currenturi\
                                         , self.task.instance_data.get('update')):
                log.info(self.log_msg ('check Session Info return True'))
                return False
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo( self.genre, self.session_info_out, \
                                        self.currenturi, article_hash,'Post', \
                                    self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                log.info(self.log_msg ('result of update is false'))
                return False
            page[ 'id' ] = result[ 'id' ]
            page['first_version_id'] = result[ 'first_version_id' ]
            page['task_log_id'] = self.task.id
            page['versioned'] = self.task.instance_data.get('versioned',False)
            page['category'] = self.task.instance_data.get('category','generic')
            page['last_updated_time'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")
            page['client_name'] = self.task.client_name
            page['entity'] = 'Review'
            page['uri'] = normalize( self.currenturi )
            page['uri_domain'] = urlparse(page['uri'])[1]
            page['priority'] = self.task.priority
            page['level'] = self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            self.pages.append( page )
            log.info( self.log_msg("Review added") )
            return True
        except:
            log.exception( self.log_msg('Error with session info' ) )
            return False

    @logit(log,'__getArticlePage')
    def __getArticlePage(self, page):
        """Return the page dict extracted from news article
        """
        title = self.soup.find('h1','a4')
        try:
            page['title'] = stripHtml( title.renderContents() )
        except:
            log.exception( self.log_msg('title is not found') )
            page['title'] =''
        try:
            head_str = stripHtml( title.findParent('table').renderContents() ).split('|')[0].strip().replace(page['title'],'').strip()
            info_match = re.search('By(.+)(\d{2} \w+ \d{4})',head_str,re.S)
            page['et_author_name'] = info_match.group(1).strip()
            page['posted_date'] = datetime.strftime( datetime.strptime(info_match.group(2).strip(),"%d %b %Y") ,"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg ('posted data not found') )
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            data_tag = self.soup.find('td','a4').findParent('td')
            data_str = data_tag.renderContents()
            data_str = data_str.replace(title.findParent('table').renderContents() ,'') # Title, Digg This
            data_str = data_str.replace( data_tag.findAll(True,'hideToPrint')[-1].renderContents(),'') # Hide to Print tag
            try:
                data_str = data_str.replace(data_tag.find('td','body').findParent('table').renderContents(),'')
            except:
                log.info(self.log_msg('Meta data inside the data not found'))
            data_str = re.sub('ttWrite.*?\(\'searchStorage.*?\'\);','',data_str)
##            data_str = data_str.replace("ttWriteMboxDiv('searchStorage_News_Article_Body');",'')
##            data_str = data_str.replace("ttWriteMboxContent('searchStorage_News_Article_Body');",'')
##            ttWriteMboxDiv('searchStorage_News_Column_Body');
##			ttWriteMboxContent('searchStorage_News_Column_Body');
            page['data'] = stripHtml(data_str)
            if page['title'] =='':
                if len(page['data']) > 100: #title is set to first 100 characters or the post whichever is less
                    page['title'] = page['data'][:100] + '...'
                else:
                    page['title'] = page['data']
        except:
            log.exception( self.log_msg( 'data is not found' ) )
            page['data'] =''
        return page

    @logit(log,'__getMagazinePage')
    def __getMagazinePage(self, page):
        """Return the page dict extracted from news article
        """
        try:
            page['title'] = stripHtml( self.soup.find('span','homeSplashTitle').renderContents() )
        except:
            page['title'] =''
            log.exception(self.log_msg('title not found'))
        try:
            author_str = stripHtml(self.soup.find('span','homeSplashTitle').findParent('tr').findNext('tr').renderContents())
            if author_str.startswith('by'):
                page['et_author_name'] = re.sub ('^by','',author_str).strip()
        except:
            log.exception(self.log_msg('author not found'))
        try:
            posted_date =  re.sub('^Issue:','',stripHtml( self.soup.find('span',text='Issue: ').findParent('td').renderContents() )).strip()
            page['posted_date'] = datetime.strftime( datetime.strptime('01 ' + posted_date,"%d %b %Y") ,"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg ('posted_date not found '))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            table_tag =  self.soup.find('span','homeSplashTitle').findParent('table').findParent('table').findNextSibling('table').renderContents()
            try:
                tag_unwanted = self.soup.find('span',text=' PREV PAGE').findParent('td').renderContents()
                table_tag = table_tag.replace(tag_unwanted,'')
            except:
                log.info(self.log_msg('un wanted data not found'))
            page['data'] = stripHtml( table_tag )
        except:
            log.info(self.log_msg( 'data not found' ) )
            page['data'] = ''
        return page
    
    
    @logit(log,"__createUrl")
    def __createUrl(self,code):
        if not code:
            ccode=(self.task.instance_data.get('queryterm') or '')
        url_template = 'http://searchstorage.techtarget.com/search/1,293876,sid5,00.html?query=%s&x=0&y=0'
        query_terms = []
#         if self.task.instance_data.get('apply_keywords'):
#             query_terms = self.task.keywords
#         if code:
#             query_terms = ['%s+%s'%(q,code)  for q in query_terms]
#         if not query_terms and code:
#             query_terms = [code]
        if self.task.keywords and self.task.instance_data.get('apply_keywords'):
            query_terms = ['%s %s'%(q.keyword,code)  for q in self.task.keywords if q.filter]
        else:
            query_terms = [code]
        return [url_template %query_term.replace(' ','+') for query_term in query_terms]
