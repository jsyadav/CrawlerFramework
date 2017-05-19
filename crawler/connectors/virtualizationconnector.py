
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Latha

import re
import logging
import urllib2
import urlparse
from urllib2 import urlparse
from datetime import datetime
import cgi

from tgimport import tg
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.urlnorm import normalize
from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log=logging.getLogger('VirtualizationConnector')
class VirtualizationConnector(BaseConnector):
    '''
    Fetches data from http://virtualization.com/. Uses app.cfg file for no. of results to be crawled and lowest limit of year.....Given a search term it ext    racts urls and enques it
    '''
    @logit (log, "_createUrl")
    def _createUrl(self, code):
        if not code:
            code=(self.task.instance_data.get('queryterm') or '')
        url_template='http://virtualization.com/?s=%s&submit=search'
        query_terms = []
        if self.task.keywords and self.task.instance_data.get('apply_keywords'):
            query_terms = ['%s %s'%(q.keyword.decode('utf-8','ignore'),code.decode('utf-8','ignore'))  for q in \
                               self.task.keywords if q.filter]
            log.info(self.log_msg('query terms:%s'% str(query_terms)))
        else:
            query_terms = [code]
        log.info(self.log_msg('The urls formed from keywords %s' %  str([url_template %(urllib2.quote(urllib2.unquote(query_term.strip().encode('utf-8')))) for query_term in query_terms])))
        return  [url_template %(urllib2.quote(urllib2.unquote(query_term.strip().encode('utf-8')))) for query_term in query_terms]
    
                

    @logit (log, "fetch")
    def fetch(self):
        '''http://virtualization.com/?s=vmware&submit=search ---sample url'''
        try:
            self.genre = 'Search'
            self.entity = 'search_result_page'
            self.parent_url=self.currenturi
            if not self.task.instance_data.get('already_parsed'):
                url_params = urlparse.urlparse(self.currenturi)[4]
                query_term = cgi.parse_qs(url_params).get('s')
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
            last_timestamp = datetime(1,1,1)
            #search_iteration, year_limit = tg.config.get(path='Connector',key='virtuallization_numresults'), \
            #    datetime.strptime(str(tg.config.get(path='Connector',key='virtuallization_year_limit'))+'/01/01', '%Y/%m/%d')
            year_limit = datetime.strptime(str(tg.config.get(path='Connector',key='virtuallization_year_limit'))+'/01/01', '%Y/%m/%d')         
            #count=0
            self.review_count=0
            finish=False
            res=self._getHTML(self.currenturi)
            log.info(self.log_msg('The current parent uri is %s' % self.currenturi))
            if not res:
                return False
            self.rawpage=res['result']
            self._setCurrentPage()
            task_flag=True
            if re.search('http://virtualization.com/\?s=.+&submit=search', self.currenturi):
                while task_flag and not finish :
            #while count <= search_iteration and task_flag and not finish :
                    for each in self.soup.findAll('div', {'id':re.compile('post\-\d+')}):
                        try:
                            #count=count+1
                            posted_date=datetime.strptime(re.sub(r'\b(\d+)(rd|th|st|nd\b)', r'\1',\
                                                  each.find('div','entry').find('a').previousSibling.strip().strip('|').strip()), '%B %d, %Y')
                            last_timestamp=max(last_timestamp, posted_date)
                            log.info(self.log_msg('last_time_update %s ' % last_timestamp))
                        except:
                            posted_date=datetime.utcnow()
                            log.exception(self.log_msg('Exception in fetching posted_date'))
                        if not checkSessionInfo(self.genre,self.session_info_out, posted_date , self.task.instance_data.get('update')):
                            temp_task=self.task.clone()
                            temp_task.instance_data[ 'uri' ] = normalize( each.find('a',rel='bookmark')['href'] )
                            temp_task.instance_data['already_parsed'] = True
                            temp_task.pagedata['posted_date']=posted_date
                            log.info(self.log_msg('adding uri %s to temp_task'% temp_task.instance_data[ 'uri' ]))
                            self.linksOut.append( temp_task )
                        else:
                            log.debug(self.log_msg("Not appending to temp_task")) 
                            if posted_date <= year_limit :
                                finish = True
                    try:
                        if self.soup.find('div', 'navigation clearfix').a:
                            self.currenturi=self.soup.find('div', 'navigation clearfix').a['href']
                            res=self._getHTML()
                            if not res:
                                break
                            self.rawpage=res['result']
                            self._setCurrentPage()
                        else:
                            break
                    except Exception, e:
                        log.exception(self.log_msg('No next page found for %s' %self.currenturi))
                        finish=True
                        break
                if self.linksOut:
                    updateSessionInfo(self.genre, self.session_info_out, last_timestamp\
                                          , None,self.entity,self.task.instance_data.get('update'))
                    task_flag=False
                    return False
            else:    
                self._addReviews()
            return True
            
        except Exception, e:
            log.exception(self.log_msg('Exception in fetch, return false'))
            return False

        
    @logit (log, "_addReviews")
    def _addReviews (self):
        try:
            self.genre = 'Review'
            self.entity = 'Review'
            self.review_count=self.review_count+1
            page={}
            if not checkSessionInfo(self.genre,self.session_info_out, self.currenturi\
                                        , self.task.instance_data.get('update')):
                try:
                    scripts=self.soup.findAll(['script', 'style', 'noscript', 'iframe'])
                    [each.extract() for each in scripts]
                    data=self.soup.findAll('div', 'entry')
                    [each.find('p').extract() for each in data]
                    page['data']=''.join([stripHtml(each.renderContents()).strip() for each in data])
                    #page['data']=stripHtml(self.soup.find('div', 'entry').renderContents()).strip()
                    
                except:
                    page['data']=''
                try:
                    page['title']=  stripHtml(self.soup.find('div', 'post').h1.a.renderContents()).strip()
                    
                except:
                    if len(page['data']) > 50:
                        page['title'] = page['data'][:50] + '...'
                    else:
                        page['title'] = page['data']
                try:
                    review_hash = get_hash(page)
                except:
                    log.exception(self.log_msg('could not generate review_hash for '+ self.parent_url))     
                try:
                    try:
                        pos=self.soup.find('div', 'postinfo').next.rfind('&bull')
                        page['posted_date']= datetime.strftime(datetime.strptime(re.sub(r'\b(\d+)(rd|th|st|nd\b)', r'\1', self.soup.find('div', 'postinfo').next[0:pos-1].strip()), '%B %d, %Y'),'%Y-%m-%dT%H:%M:%SZ')
                    except:
                        page['posted_date']=self.task.pagedata['posted_date']

                except:
                    page['posted_date']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    log.exception(self.log_msg('could not parse date for '+ self.currenturi))
                            
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo( self.genre, self.session_info_out, \
                                              self.currenturi, review_hash,'Post', \
                                              self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    page[ 'id' ] = result[ 'id' ]
                    page['first_version_id'] = result[ 'first_version_id' ]
                    page['task_log_id'] = self.task.id
                    page['versioned'] = self.task.instance_data.get('versioned',False)
                    page['category'] = self.task.instance_data.get('category','generic')
                    page['last_updated_time'] = datetime.strftime(datetime.utcnow()\
                                                                      ,"%Y-%m-%dT%H:%M:%SZ")
                    page['client_name'] = self.task.client_name
                    page['entity'] = 'Review'
                    page['uri'] = self.currenturi 
                    page['priority'] = self.task.priority
                    page['level'] = self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                                ,"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    page['uri_domain'] =  unicode(urlparse.urlparse(page['uri'])[1]) #urlparse.urlparse(page['uri'])[1]
                    self.pages.append(page)
                    log.info(self.log_msg('Adding review %d of %s ' % (self.review_count ,self.currenturi)))
                    return True
                else:
                    log.info(self.log_msg ('Update is false and not appending review of %s' % self.currenturi))
                    return False
            
        except Exception, e:
            log.exception(self.log_msg('Exception in addReviews'))
            raise e

                            

