
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

log=logging.getLogger('HpcWireConnector')
class HpcWireConnector(BaseConnector):

    @logit (log, "fetch")
    def fetch(self):
        """
        Sample input Uri
        http://www.hpcwire.com/search/?sort=date&keywords=Panasas+HPC&submitArchives=Search
        A Connector for "http://www.hpcwire.com"
        Parent uri = http://www.hpcwire.com/offthewire/17887339.html
        The Input may be given as
        http://www.hpcwire.com/search/?keywords=Panasas+HPC&submitArchives=Search
        All the search results to be picked ,

        Things to Do
        ------------
        if searched uri is given

            1) Sort Everything By Relavancy.
            2) pick up the resuls which has the relavancy > N ( N from config file )
            2) Check the session info as with recent time stamp (self.genre ='Search')
            3) send links out
            4) update session info
        if article uri is given
            1) Add the article page
            2) Self.genre = "Review"
            3) comments are not required
        """
        try:
            self.genre = 'Search'
            if re.match(r'http://www.hpcwire.com/search/\?keywords=.*?&submitArchives=Search'\
                                                                            ,self.currenturi):
                try:
                    if not self.task.instance_data.get('already_parsed'): #a flag to distinguish between tasks created by me , and original tasks
                        url_params = urlparse(self.currenturi)[4]
                        query_term = cgi.parse_qs(url_params).get('keywords')
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
                # To do: Read from config file
##                search_relavancy_limit = tg.config.get( path='Connector', \
##                                    key='hpcwire_search_relavancy_percentage')
                search_relavancy_limit = 50
                log.info(search_relavancy_limit)
                uri_info_list = self.currenturi.split('?')
                self.currenturi = uri_info_list[ 0 ] + '?sort=score&' + uri_info_list [ 1 ]
                if not self._setSoup():
                    return False
                # fetch urls which has the search relavancy >= 50
                urls_date_dic = { }
                recent_time_stamp = datetime(1,1,1)
                exceed_relavancy = False
                while True:
                    for post_info_div_tag  in [ each.parent.findParent( 'div' ) for each in \
                                            self.soup.findAll( 'div', text = re.compile\
                                                            ( '^Date: \d+/\d+/\d+' ) ) ]:
                        try:
                            search_relavancy = stripHtml( post_info_div_tag.findPrevious\
                                                                ('div').renderContents() )
                            if int(re.match('Search Relevance: (\d+)%$',search_relavancy)\
                                                    .group(1)) >= search_relavancy_limit:
                                posted_date_info = re.sub('Date:','', post_info_div_tag.find\
                                                    ('span','bold').renderContents()).strip()
                                posted_date = datetime.strptime( posted_date_info, "%m/%d/%y" )
                                recent_time_stamp = max( recent_time_stamp , posted_date )
                                urls_date_dic[ post_info_div_tag.find( 'a' )['href'] ] = posted_date
                            else:
                                log.info(self.log_msg('search relavancy is going down the limit') )
                                exceed_relavancy = True
                                break
                        except:
                            log.exception(self.log_msg ('error with getting relavant uris' ) )
                    if exceed_relavancy:
                        break
                    try:
                        self.currenturi = 'http://www.hpcwire.com' + self.soup.\
                                    find('a',text=' Next 20&#62; ').parent['href']
                        if not self._setSoup():
                            break
                    except:
                        log.exception(self.log_msg ('next page not found'))
                        break
                for url in urls_date_dic.keys():
                    if not checkSessionInfo(self.genre,self.session_info_out, \
                                            urls_date_dic[ url ] , self.task.\
                                                    instance_data.get('update')):
                        log.info('Task need to be added')
#===============================================================================
#                        temp_task=self.task.clone()
#                        temp_task.instance_data[ 'uri' ] = normalize( url )
#                        self.linksOut.append( temp_task )
#===============================================================================
                if self.linksOut:
                    updateSessionInfo(self.genre, self.session_info_out, recent_time_stamp\
                                 , None,self.entity,self.task.instance_data.get('update'))
                return True
            elif re.match (r'http://www.hpcwire.com/.+/.+html',self.currenturi):
                if not self._setSoup():
                    return False
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
        get the article info,
        It will get title, posted_date,uri,source
        """
        page = {}
        self.genre = 'Review'
        self.entity = 'Review'
        try:
            page['posted_date'] =  datetime.strftime( datetime.strptime(stripHtml( self.soup.find('p','dateline').renderContents() ),"%B %d, %Y") ,"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('Posted_date is not found') )
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['title'] = stripHtml( self.soup.find('h2','black noBorder')\
                                                            .renderContents() )
        except:
            log.exception(self.log_msg ('The title is not found') )
            page['title'] = ''
        try:
            data_hierachy = [ each.strip() for each in stripHtml\
                                        ( self.soup.find('div','whiteLiner')\
                                        .findPrevious('div').renderContents() )\
                                                                .split('>>')]
            if data_hierachy[0]=='HPCwire':
                page[ 'et_data_hierachy' ] = data_hierachy
        except:
            log.info( self.log_msg('data hierachy cannot be found ') )
        try:
            body_str = self.soup.find('div',id='bodytext').renderContents()
            tag_src_str = self.soup.find('div',id='bodytext').find(True,text=\
                                                        re.compile('^Source:'))
            if tag_src_str:
                body_str = stripHtml( body_str.replace(tag_src_str.parent.__str__(),'')\
                                                             ).strip('-').strip()
                page['et_data_source'] = re.sub('^Source:','',stripHtml\
                                                    ( tag_src_str ) ).strip()
            page['data'] = stripHtml ( body_str ).strip('-').strip()
            if page['title'] =='':
                if len(page['data']) > 100: #title is set to first 100 characters or the post whichever is less
                    page['title'] = page['data'][:100] + '...'
                else:
                    page['title'] = page['data']
        except:
            log.exception( self.log_msg( 'data is not found' ) )
            page['data'] =''
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
            page['parent_path'] = []
            page['path'] = [self.currenturi]
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
    def __createUrl( self, code ):
        if not code:
            code=(self.task.instance_data.get('queryterm') or '')
        url_template = 'http://www.hpcwire.com/search/?keywords=%s&submitArchives=Search'
        query_terms = []
#         if self.task.instance_data.get('apply_keywords'):
#             query_terms = self.task.keywords
#         if code:
#             query_terms = ['%s+%s'%(q,code)  for q in query_terms]
#         if not query_terms and code:
#             query_terms = [code]
        if self.task.keywords and self.task.instance_data.get('apply_keywords'):
            query_terms = ['%s %s'%(q.keyword,code)  for q in \
                               self.task.keywords if q.filter]
        else:
            query_terms = [code]
        return [url_template %(quote(unquote(query_term.strip().encode('utf-8')))) for query_term in query_terms]
