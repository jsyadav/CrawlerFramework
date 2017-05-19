
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#modified by prerna
#SKumar
# Removed redundant function., getDateTime(), checkUri(),getScoreValue
# Rating and features done dynamically,and spacing modified on 4th Dec

import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime

from utils.utils import stripHtml,get_hash
from utils.decorators import logit
from utils.urlnorm import normalize
from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log=logging.getLogger('TrustedReviewsConnector')
class TrustedReviewsConnector(BaseConnector):
    """
    For www.trustedreviews.com, which has the format of 'http://www.trustedreviews
    .com/notebooks/review/2008/11/11/Apple-MacBook-13in---Aluminium-2008-Edition/
    comments' It will be picked if, the uri is, of the above format
    """
    
    @logit( log, "fetch" )
    def fetch(self):
        log.info( self.log_msg( 'I m inside the Trusted Reviews Connector' ) )
        self.genre = "Review"
        self.parenturi = self.currenturi
        try:
            #if self.currenturi == 'http://www.trustedreviews.com/':
            self._setSoup()
                #c =  self.soup.findAll('a',text = re.compile('Read \d+ comment(s)?') )
            #self._getParentPage()
            self._addReviews()   
             
##            main_page_soup = copy.copy(self.soup)
##            while self._addReviews():
##                log.info('i m in addReviews')
##                try:
##                    self.currenturi = main_page_soup.find('a','pluck-comm-pagination-next pluck-png')['href']
##                    log.info(self.log_msg('current uri:%s'%self.currenturi))
##                    if not self.currenturi:
##                        break
##                    self._setSoup()
##                    main_page_soup = copy.copy(self.soup)
##                except:
##                    log.exception('problem in getting uri')
##                    break
##            return True        
##            
##            #self._getParentPage()
##            #self._addReviews()
##            #self.pages=[]
##            return True
        except:
            log.exception('exception in fetch method')
        return True
        
    @logit( log,"_addReviews")
    def _addReviews(self):
        """
        This will add the reviews
        """
        #comments = self.soup.findAll('h3',{'class': re.compile('commenthead( staff)?')} )
        comments = self.soup.findAll('div','pluck-comm-single-comment-main pluck-png')
        for comment in comments:
            page ={}
            try:
                date_str = stripHtml(comment.find('p','pluck-comm-timestamp').\
                            renderContents()).strip()
                page['posted_date'] = datetime.strptime(date_str,'%I:%M %p on %d %B, %Y').\
                                        strftime("%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception('posted date not found %s'%self.currenturi)                            
                page['posted_date'] = datetime.strftime(datetime.utcnow(),\
                                                            "%Y-%m-%dT%H:%M:%SZ")
            try:
                page['data'] = stripHtml(comment.find('p','pluck-comm-body').\
                                renderContents()).replace('>','')
                page['title'] =''                
            except:
                log.exception(self.log_msg('data not found'))
                page['data'] = page['title']
            try:
                page['et_author_name'] = stripHtml(comment.find('h3','pluck-comm-username-url pluck-comm-username-display').\
                                        find('a').renderContents())                        
            except:
                log.exception(self.log_msg('author_name not found %s',self.currenturi))                            
            page[ 'uri' ] = self.currenturi
            try:
                
                review_hash = get_hash(page)
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                if checkSessionInfo( self.genre, self.session_info_out, \
                            unique_key,self.task.instance_data.get('update'),\
                                                parent_list=[ self.parenturi ]):
##                    continue
                    log.info(self.log_msg('session info return True'))
                    return False
                result = updateSessionInfo( self.genre, self.session_info_out,\
                    unique_key,review_hash,'Review', self.task.instance_data.\
                                get('update'), parent_list=[ self.parenturi ] )
                if result['updated']:
                    parent_list = [ self.parenturi ]
                    page['parent_path'] = copy.copy(parent_list)
                    parent_list.append( unique_key )
                    page['path']=parent_list
                    page['uri']=normalize(self.currenturi)
                    page['task_log_id']=self.task.id
                    page['versioned']=self.task.instance_data.get('versioned',False)
                    page['category']=self.task.instance_data.get('category','generic')
                    page['last_updated_time']= datetime.strftime(datetime.utcnow()\
                                                            ,"%Y-%m-%dT%H:%M:%SZ")
                    page['client_name']=self.task.client_name
                    page['entity']='review'
                    page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  
                    self.pages.append(page)
                    log.info('review page added')
                else:
                    log.debug(self.log_msg("review page not added"))
            except:
                log.exception('Problem  in adding session info')
        return True        

    @logit( log, "_getParentPage")
    def _getParentPage(self):
        """
        This is for setting up the parent page
        """
        page = {}
        page [ 'uri' ] = self.currenturi
        try:
            page['data'] = page['title'] = stripHtml(self.soup.find('span', attrs = {'class':re.compile('compare-item-title$')}).renderContents())
        except:
            log.exception( self.log_msg( 'page [title] is not found' ) )
            page['title'] = ''
        try:
            date_str = stripHtml(self.soup.find('p','tr-info-author-reviewed-at').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,"%d %B %Y"),\
                                            "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception( self.log_msg( 'page[posted_date] is not found' ) )
            page[ 'posted_date' ] = datetime.strftime(datetime.utcnow(),\
                                                            "%Y-%m-%dT%H:%M:%SZ")
##        try:
##            info_dic = { 'et_author_name':'Author',
##                        'et_product_manufacturer':'Manufacturer',
##                        'et_product_supplier':'Supplier' }
##            for each in info_dic.keys():
##                page[ each ] = stripHtml ( self.soup.find('td',text = info_dic[each])\
##                                                        .findNext().renderContents() )
##        except:
##            log.exception( self.log_msg( 'info not found for %s'%each ) )
        try:
            page[ 'et_product_price' ] = stripHtml(self.soup.find('p','tr-reviews-price-as-reviewed').\
                                            renderContents())
            log.info( page[ 'et_product_price' ] )
        except:
            log.info( self.log_msg( 'Price  is not found' ) )
        try:
            page[ 'et_author_name' ] = stripHtml(self.soup.find('span', itemprop='author').renderContents())
        except:
            log.info( self.log_msg( 'author name is not found' ) )    

        try:
            log.info(page)
            post_hash = get_hash(page)
            self.updateParentExtractedEntities(page)
            if not checkSessionInfo(self.genre, self.session_info_out,self.task.\
                    instance_data['uri'],self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out, \
                              self.task.instance_data['uri'], post_hash,'Post',\
                                 self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    page['path']=[self.parenturi]
                    page['parent_path']=[]
                    page['task_log_id'] = self.task.id
                    page['versioned'] = self.task.instance_data.get('versioned',False)
                    page['category'] = self.task.instance_data.get('category','generic')
                    page['last_updated_time'] = datetime.strftime(datetime.utcnow()\
                                                            ,"%Y-%m-%dT%H:%M:%SZ")
                    page['client_name'] = self.task.client_name
                    page['entity'] = 'post'
                    page['uri'] = normalize(self.currenturi)
                    page[ 'data' ] = ''
                    page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                    page['priority'] = self.task.priority
                    page['level'] = self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                            ,"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    log.info( page )
                    self.pages.append( page )
                    log.debug(self.log_msg("Parent page added"))
                    return True
                else:
                    log.debug(self.log_msg("Parent page not added"))
                    return False
        except:
            log.exception('There is some problem with Session info')
    @logit(log, '_setSoup')
    def _setSoup( self ):
        """
            It will set the uri to current page, written in seperate
            method so as to avoid code redundancy
        """
        try:
            log.info( 'for uri %s'%(self.currenturi) )
            res = self._getHTML()
            if res:
                self.rawpage = res[ 'result' ]
            else:
                log.info('self.rawpage not set.... so Sorry..')
                return False
            self._setCurrentPage()
            return True
        except Exception, e:
            log.exception(self.log_msg('Page not set as current page :%s'%uri))
            raise e
        
        
        
