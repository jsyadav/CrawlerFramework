
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''


import re
import logging
from urlparse import urlparse
from datetime import datetime

from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.urlnorm import normalize
from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log=logging.getLogger('SysconConnector')
class SysconConnector(BaseConnector):

    @logit (log, "fetch")
    def fetch(self):
        """
        A connector for www.sys-con.com
        
        [ 'http://www.sys-con.com' + each.findParent('a') for each in soup.find('div','gems-search-results').findAll('span','storytitle')]
        Next Page ---> soup.find('a','active')['href']
        
        
        
        """
        try:
#            self.currenturi = 'http://www.sys-con.com/node/607955'
            self.genre = 'Search'
            if re.match('http://www\.sys\-con\.com/search\?s=.*&submit=Search',self.currenturi):
                self.entity = 'search_result_page'
                if not self._setSoup():
                    return False
    ##          search_result_from_year = tg.config.get( path='Connector', key='inside_hpc_search_results_from_year')
                search_result_from_year =2007
                min_date = datetime.strptime( '01.01.' + str( search_result_from_year ) , "%m.%d.%Y" )
                recent_time_stamp = datetime(1,1,1)
                while True:
                    for each in [ a_tag.findParent('a') for a_tag in self.soup\
                                            .find('div','gems-search-results').\
                                                findAll('span','storytitle')]:
                        try:  
                            posted_date = datetime.strptime( re.sub('Reads\s*:\s*\d*','',\
                                        stripHtml(each.findNextSibling('div','storytagline').\
                                          find('span').nextSibling)).strip() , "%b. %d, %Y" )                             
                            if posted_date < min_date:
                                continue
                            recent_time_stamp = max (recent_time_stamp,posted_date)
                            if not checkSessionInfo(self.genre,self.session_info_out, posted_date , self.task.instance_data.get('update')):
                                temp_task=self.task.clone()
                                temp_task.instance_data[ 'uri' ] = normalize( 'http://www.sys-con.com' + each['href'] )
                                self.linksOut.append( temp_task )
                                
                            else:
                                log.info(self.log_msg('Session info return True, Cannot be added'))
                        except:
                            log.exception( self.log_msg('error with getting uri info' ) )
                    try:
                        self.currenturi = 'http://www.sys-con.com' + self.soup.find('a',text=re.compile('^next.*')).parent['href']                                        
                        if not self._setSoup():
                            break
                    except:
                        log.exception(self.log_msg ('next page not found'))
                        break
                if self.linksOut:
                    log.info(self.log_msg('Hello'))
                    updateSessionInfo(self.genre, self.session_info_out, recent_time_stamp,\
                                       None,self.entity,self.task.instance_data.get('update'))
                return True
            elif re.match('http://www\.sys\-con\.com/node/.*',self.currenturi):                
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
        if checkSessionInfo(self.genre,self.session_info_out, self.currenturi
                                    , self.task.instance_data.get('update')):
                log.info(self.log_msg ('check Session Info return True'))
                return False
        if not self._setSoup():
            return False
        try:
            date_str = date_str = stripHtml(self.soup.find('div','storydatetime').renderContents())
            page['posted_date'] =  datetime.strftime( datetime.strptime(date_str,"%b. %d, %Y %I:%M %p" ),"%Y-%m-%dT%H:%M:%SZ")                    
        except:
            log.exception(self.log_msg('Posted_date is not found') )
            page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['title'] = stripHtml(self.soup.find('div','storytitle').renderContents())
            
        except:
            log.exception(self.log_msg ('The title is not found') )
            page['title'] = ''
        try:
            page['et_data_subtitle'] = stripHtml(self.soup.find('div','storyminortitle').renderContents())
        except:
            log.exception(self.log_msg ('The sub title is not found') )
        try:
            page['et_author_name'] = re.sub('^By:','',stripHtml( self.soup.find('div','writtenby').renderContents())).strip()
        except:
            log.info(self.log_msg('Author name not found'))
        try:
            data_str =''
            review_started = False
            for each in self.soup.find('div','storybody'):
                tag_str = each.__str__()
                if tag_str.startswith('<div class="item-list">'):
                    continue
                if tag_str.rfind('<div class="toolbar">') > 0:
                        review_started = True
                        continue
                if tag_str.startswith('<div class="storyfooter">'):
                        break
                if review_started:
                    data_str = data_str + tag_str
            #page['data'] = stripHtml(''.join([each.renderContents() for each in self.soup.findAll('p') ]))
            page['data'] = stripHtml(data_str)
        except:
            log.exception( self.log_msg( 'data is not found' ) )
            page['data'] = ''
        try:
            if page['title']=='':
                if len(page['data']) > 100:
                        page['title'] = page['data'][:100] + '...'
                else:
                    page['title'] = page['data']
        except:
            log.info(self.log_msg('title not found'))
            page['title'] = ''
        try:
            log.info(page)
            article_hash = get_hash( page )
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