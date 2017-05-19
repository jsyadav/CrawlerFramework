
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
from datetime import datetime
import logging
from urllib2 import urlparse
import copy

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('TeaViewsConnector')
class TeaViewsConnector( BaseConnector ):
    '''
    A Connector for www.teaviews.com
    The sample uri is :
    1) http://www.teaviews.com/2009/01/04/review-mighty-leaf-acai-pomegranate-black-decaf/
    2) http://www.teaviews.com/2008/07/09/review-tea-guys-chocolate-delight/
    3) http://www.teaviews.com/2008/09/08/review-tea-guys-chocolate-delight-2/
    4) http://www.teaviews.com/2008/12/18/review-tea-guys-chocolate-delight-5/
    5) http://www.teaviews.com/2008/10/13/review-drink-the-leaf-red-peony/
    6) http://www.teaviews.com/2008/05/20/review-primula-flowering-teas-2/
    7) http://www.teaviews.com/2008/05/15/review-primula-flowering-teas/
    8) http://www.teaviews.com/2008/10/20/review-the-necessiteas-chocolate-banana-heart-blooming-tea/
    9) http://www.teaviews.com/2008/04/24/review-just4tea-jasmine-bloom/
    
    
    
    Solr Fields to capture are other than, title,posted_data,uri,data :
    et_author_name
    et_author_info
    et_product_rating
    Other's Ratings are to be ignored    
    '''
     
    @logit(log , 'fetch')
    def fetch(self):                                                                                         
        self.genre ="Review"
        self.parent_uri = self.currenturi
        try:
            if not self._setSoup():
                log.info(self.log_msg('Task uri not set, cannot proceed'))
                return False
            if self.__getParentPage():
                log.info(self.log_msg('Parent page return false'))
            self.__addReview()
            return True
        except:
            log.exception(self.log_msg('Error in fetch'))
            return False
    
    @logit(log,"getParentPage")
    def __getParentPage(self):
        """
        This will add the Parent Page info 
        Only Title and uri can be found,
        No data are found
        """
        page = {}
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri \
                                                , self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info return True'))
                return False        
        try:
            page['title'] = re.sub ( '^Review:','',stripHtml(self.soup.find('div','post-title').find('h2').renderContents())).strip()                       
            
        except:
            log.exception(self.log_msg('Title Could not be found'))
            page['title'] =''
        try:    
                post_hash = get_hash( page )
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out,self.parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
                if not result['updated']:
                    return False
                page['path']=[self.parent_uri]
                page['parent_path']=[]
                page['uri'] = normalize(self.parent_uri)
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                page['client_name'] = self.task.client_name
                page['last_updated_time'] = page['pickup_date']
                page['versioned'] = False
                page['data'] = ''
                page['task_log_id']=self.task.id
                page['entity'] = 'Post'
                page['category']=self.task.instance_data.get('category','')
                self.pages.append(page)
                log.info(self.log_msg('Parent Page added'))
                return True
        except:
            log.exception(self.log_msg("parent post couldn't be posted"))
            return False
    @logit(log, "addReview")
    def __addReview(self):
        """
        It will add the review which has
        et_data_snapshot, et_author_name, et_author_info
        """
        page = {}
        try:
            url_part =urlparse.urlparse(self.parent_uri)[2]
            date_str =  re.search('^/(\d{4}/\d{1,2}/\d{1,2})',url_part).group(1)
            page['posted_date'] = datetime.strftime( datetime.strptime( date_str,"%Y/%m/%d" ),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('posted_date not found '))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['data'] = ''
            for each in self.soup.find('div','entry').findAll('p'):
                para_tag = stripHtml( each.renderContents() )
                if not para_tag.startswith('To purchase',2):
                    page['data'] =page['data'] + para_tag
        except:
            log.exception(self.log_msg('data cannot be retrieved'))
        try:
             page['ef_product_rating'] = float(len(filter(lambda x: x['src']=='/images/bar1.gif' , self.soup.find('div','reviewbox').find('td',valign='top').findAll('img'))))             
        except:
            log.info(self.log_msg('Rating not found'))
        try:
            if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
            else:
                page['title'] = page['data']
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
            
        try:
            page['et_author_name'] = stripHtml(self.soup.find('div','reviewbox').findNextSibling('table').find('b').renderContents())
            self.currenturi = 'http://www.teaviews.com'  + self.soup.find('div','reviewbox').findNextSibling('table').find('a',text=re.compile('.*s profile page')).parent['href']
            if self._setSoup():
                page['et_author_info'] = stripHtml(self.soup.find('div',id='content').\
                                                    find('p').renderContents())
        except:
            log.info(self.log_msg('author info not found'))
        try:
            review_hash = get_hash(page)
            unique_key = get_hash( {'data':page['data'],'title':page['title']})
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                        =[self.parent_uri]):
                return False
            result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                        review_hash,'Review', self.task.instance_data.get('update'),\
                                                    parent_list=[self.parent_uri])
            if not result['updated']:
                return False
            page['uri'] = normalize(self.parent_uri)
            parent_list = [ self.parent_uri ]
            page['parent_path'] = copy.copy(parent_list)
            parent_list.append( unique_key )
            page['path']=parent_list
            page['priority']=self.task.priority
            page['level']=self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            page['client_name'] = self.task.client_name
            page['last_updated_time'] = page['pickup_date']
            page['versioned'] = False
            page['entity'] = 'Review'
            page['category'] = self.task.instance_data.get('category','')
            page['task_log_id']=self.task.id
            page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
            self.pages.append(page)
            log.info(self.log_msg('Review Added'))
        except:
            log.exception(self.log_msg('Error while adding session info'))                       

            
    @logit(log, "_setSoup")
    def _setSoup( self, url = None, data = None, headers = {} ):
        """
            It will set the uri to current page, written in seperate
            method so as to avoid code redundancy
        """
        if url:
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

