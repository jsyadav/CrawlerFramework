
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
from datetime import datetime
import cgi
import copy
from urllib2 import quote,unquote

#from tgimport import tg
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.urlnorm import normalize
from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log=logging.getLogger('InsideHpcConnector')
class InsideHpcConnector(BaseConnector):

    @logit (log, "fetch")
    def fetch(self):
        """
        Sample input Uri
        http://insidehpc.com/index.php?s=HPC
        A Connector for "http://www.insidehpc.com"
        """
        try:
            self.genre = 'Search'
            if re.match(r'http://insidehpc.com/index.php\?s=.*?',self.currenturi):
                try:
                    if not self.task.instance_data.get('already_parsed'): #a flag to distinguish between tasks created by me , and original tasks
                        url_params = urlparse(self.currenturi)[4]
                        query_term = cgi.parse_qs(url_params).get('s')
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
                except:
                    log.exception(self.log_msg('Erorr while checking forming the urls using key words and Query  term'))                
                self.entity = 'search_result_page'
                search_result_from_year =2007
                no_posts_to_be_fetched = 100
##                search_result_from_year = tg.config.get( path='Connector', key='inside_hpc_search_results_from_year')
##                no_posts_to_be_fetched = tg.config.get( path='Connector', key='inside_hpc_search_numresults')
                min_date = datetime.strptime( '01.01.' + str( search_result_from_year ) , "%m.%d.%Y" )
                if not self._setSoup():
                    return False
                recent_time_stamp = datetime(1,1,1)
                break_loop = False
                no_of_posts_fetched = 0
                while True:
                    for each in self.soup.findAll('h2','posttitle'):
                        try:
                            posted_date = datetime.strptime( each.find('span','postdate').renderContents(), "%m.%d.%Y" )
                            no_of_posts_fetched = no_of_posts_fetched + 1
                            if not posted_date >= min_date or no_of_posts_fetched > no_posts_to_be_fetched:
                                break_loop = True

                            recent_time_stamp = max (recent_time_stamp,posted_date)
                            if not checkSessionInfo(self.genre,self.session_info_out, posted_date , self.task.instance_data.get('update')):                                
                                temp_task=self.task.clone()
                                temp_task.instance_data[ 'uri' ] = normalize( each.find('a',rel='bookmark')['href'] )
                                self.linksOut.append( temp_task )
                            else:
                                log.info(self.log_msg('Session info return True, Cannot be added'))
                        except:
                            log.exception( self.log_msg('error with getting uri info' ) )
                    try:
                        self.currenturi = self.soup.find('p','nav').find('a',text\
                                        =re.compile('Next Page.+')).parent['href']
                        if not self._setSoup():
                            break_loop = True
                    except:
                        log.exception(self.log_msg ('next page not found'))
                        break_loop = True
                    if break_loop:
                        break
                log.info(self.log_msg('Total no of urls fetched is %d'%no_of_posts_fetched))
                if self.linksOut:
                    updateSessionInfo(self.genre, self.session_info_out, recent_time_stamp\
                                 , None,self.entity,self.task.instance_data.get('update'))
                return True
            elif re.match (r'http://insidehpc.com/\d+/\d+/\d+/.+/',self.currenturi):
                if self._addArticlePage():
                    return True
                else:
                    return False
        except:
            log.exception( self.log_msg ( 'Error with fetch' ) )
            return False

    @logit(log, "_setSoup")
    def _setSoup( self, url = None, data = None, headers = {} ):
        """
            It will set the uri to current page, written in seperate
            method so as to avoid code redundancy
        """
        if not url:
            url = self.currenturi
        try:
            log.info(self.log_msg( 'for uri %s'%(self.currenturi) ))
            res = self._getHTML( url, data = data, headers=headers  )
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
        It will get the Article page info
        fetch data, title, posted_date and data source
        """
        page = {}
        self.genre = 'Review'
        self.entity = 'Review'
        if checkSessionInfo(self.genre,self.session_info_out, self.currenturi
                                    , self.task.instance_data.get('update')):
                log.info(self.log_msg ('check Session Info return True'))
                return False
        if not self._setSoup():
            return False
        try:
            page['posted_date'] =  datetime.strftime( datetime.strptime( self.\
                    soup.find('span','postdate').renderContents(), "%m.%d.%Y" )\
                                                         ,"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('Posted_date is not found') )
            page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['title'] = stripHtml( self.soup.find('h2','posttitle').find\
                                        ('a',rel='bookmark').renderContents() )
        except:
            log.exception(self.log_msg ('The title is not found') )
            page['title'] = ''
        try:
            content_str =  stripHtml( self.soup.find( 'div','the_content' ).renderContents() )
            # After striping Html Tags, It is found that SHARETHIS.addEntry({ title:
            #"Pana....", url: "http://insidehpc.com/20..."
            # to strip this Script, It is found till the begining of the Script
            share_tag_index = content_str.find('SHARETHIS.addEntry')
            if not share_tag_index == -1:
                content_str = content_str[ :share_tag_index ]
            page['data'] = content_str.strip()
        except:
            log.exception( self.log_msg( 'data is not found' ) )
            page['data'] = ''
        if page['title']=='':
            if len(page['data']) > 100:
                    page['title'] = page['data'][:100] + '...'
            else:
                page['title'] = page['data']
        try:
            page['et_author_name'] = re.search('by(.+)\|', stripHtml( self.soup.find('p','postmeta').renderContents() ) ).group(1).strip()
        except:
            log.exception(self.log_msg('Author name not found') )
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
            page['path']=[self.currenturi]
            page['parent_path']=[]
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
    @logit(log,"__createUrl")
    def __createUrl(self,code):
        if not code:
            code=(self.task.instance_data.get('queryterm') or '')
        url_template = 'http://insidehpc.com/index.php?s=%s'
        query_terms = []
#         if self.task.instance_data.get('apply_keywords'):
#             query_terms = self.task.keywords
#         if code:
#             query_terms = ['%s+%s'%(q,code)  for q in query_terms]
#         if not query_terms and code:
#             query_terms = [code]
        if self.task.keywords and self.task.instance_data.get('apply_keywords'):
            query_terms = ['%s %s'%(q,code)  for q in self.task.keywords]
        else:
            query_terms = [code]
        return [url_template %(quote(unquote(query_term.strip().encode('utf-8')))) for query_term in query_terms]

#        url = 'http://blogsearch.google.com/blogsearch_feeds?hl=en&scoring=d&ie=utf-8&output=rss&q=%s&num=%s'%\
#            (code.replace(' ','+'),tg.config.get(path='Connector', key='google_blog_search_numresults'))

    
