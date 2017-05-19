
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#SKumar
#modified , removed unused imports and redundant variables, on Dec 5, 2008


import re
import logging
from urllib2 import urlparse
from datetime import datetime
import copy

from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.urlnorm import normalize
from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log=logging.getLogger('MaximumPcConnector')
class MaximumPcConnector(BaseConnector):
    """
    For www.maximumpc.com, which has the format of 'http://www.maximumpc.com/article/reviews/
    product_name It will be picked if, the uri is, of the above format
    ex : http://www.maximumpc.com/article/reviews/lenovo_thinkpad_w700
    """

    @logit( log, "fetch" )
    def fetch(self):
        """
        same fetch() , fetch the reviews and parent page
        this site to be captured comments as reviews
        """
        try:
            self.baseuri = 'http://www.maximumpc.com'
##            self.currenturi = 'http://www.maximumpc.com/articles/reviews/hardware/notebooks'
##            self.currenturi = 'http://www.maximumpc.com/article/reviews/gateway_p7811_fx'
            self.genre = "Review"
            self.parenturi = self.currenturi
            if re.compile(r'http://www.maximumpc.com/articles/reviews/hardware/.+?$').\
                                                                match(self.currenturi):
                if not self._setSoup():
                    return False
                uri_list = [ ]
                while True:
                    uri_list = uri_list + [ span_tag.find('a')['href'] for \
                            span_tag in self.soup.findAll('span','general-topic') ]
                    try:
                        next_link = self.soup.find('a',{'class':re.compile\
                                        ('pager\-(next|last) active') })['href']
                        self.currenturi = self.baseuri + next_link
                        if not self._setSoup():
                            break
                    except:
                        log.exception('error with fetching uris')
                        break
                
                for uri in uri_list:
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] = normalize( uri )
                    self.linksOut.append( temp_task )
                log.info('total uri is found is %d'%(len(uri_list)))
            elif re.compile(r'http://www.maximumpc.com/article/(reviews/)?.+?$')\
                                                        .match(self.currenturi):
                log.info('Info need to be captured')
                if not self._setSoup():
                    return False
                self._getParentPage()
                self._addReviews()
                log.info('Reviews Added')
                return True
            else:
                log.info(self.log_msg ('Uri is not in the correct format'))
                return False
        except:
            log.exception(self.log_msg ( 'Error with fetch') )
            return False

    @logit( log,"_addReviews")
    def _addReviews(self):
        """This will add the reviews
        """
        comments = self.soup.findAll('div','comment')
        log.info('Total Review found is %d'%(len(comments)))
        for comment in comments:
            page ={}
            try:
                page['title'] =   stripHtml(comment.find('span','general-topic')\
                                                                .renderContents())
            except:
                page['title'] =''
                log.exception('title not found')
            try:
                page['uri'] =  comment.find('span','general-topic').find('a')['href']
            except:
                log.exception('uri not found')
                page['uri'] = ''
            post_str=''
            try:
                post_str =  stripHtml( comment.find('p','post-info').renderContents() )
                date_str = post_str[post_str.find(',')+1:].strip()
                page['posted_date'] =  datetime.strftime(datetime.strptime\
                                (date_str,'%Y-%m-%d %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                log.exception('no posted _date found')
            try:
                post_str = comment.__str__() #.__str__() needed to replace the tags
                tag_list = ['post-avatar' , 'general-topic']
                for tag in tag_list:
                    post_str = post_str.replace(comment.find('span',tag).__str__(),'')
                post_info_str = comment.find('p','post-info').__str__()
                post_str = post_str.replace(post_info_str,'')
                inline_text = comment.find('div','header-inline-text').__str__()
                post_str = post_str.replace (inline_text,'')
                page['data'] = stripHtml( post_str )
            except:
                page['data'] = ''
                log.exception('data not found')
            previous_uri = self.currenturi
            try:
                 page['et_author_name'] = stripHtml( comment.find('p','post-info').find('a').renderContents() )
                 author_link =  self.baseuri + comment.find('p','post-info').find('a')['href']
                 self.currenturi = author_link
                 if self._setSoup():
                     try:
                        date_str = self.soup.find('span','user-usage').find\
                                    ('strong',text='Maximum PC Member Since:')\
                                                                .next.strip()
                        page['edate_author_member_since'] = datetime.strftime\
                                      (datetime.strptime(date_str,'%B %d, %Y'),\
                                                            "%Y-%m-%dT%H:%M:%SZ")
                     except:
                        log.exception('author member since is not found')

                     try:
                         count_str = self.soup.find('span','user-usage').find\
                                ('strong',text='Total Posted Comments:').next.strip()
                         page['ei_author_comments_count'] = int( re.search\
                                                        ('^\d+',count_str).group() )
                     except:
                        log.exception('author comments count is not found')
            except:
                log.exception('author name is not found')
            self.currenturi = previous_uri
            try:
                log.info(page)
                review_hash = get_hash( page )
                unique_key = review_hash
                if not page['uri']=='':
                    unique_key = page['uri']
                else:
                    page['uri']== self.parenturi
                result = updateSessionInfo( self.genre, self.session_info_out,\
                    unique_key ,review_hash,'Review', self.task.instance_data.\
                                get('update'), parent_list=[ self.parenturi ] )
                if result['updated']:
                    parent_list = [self.parenturi]
                    page['parent_path'] = copy.copy(parent_list)
                    parent_list.append( unique_key )
                    page['path'] = parent_list
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
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),\
                                                            "%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    self.pages.append(page)
                    log.info('review page added')
                else:
                    log.debug(self.log_msg("review page not added"))
            except:
                log.exception(self.log_msg ('Problem  in adding session info') )

    @logit( log, "_getParentPage")
    def _getParentPage(self):
        """This is for setting up the parent page
        """
        page = {}
        try:
            page ['title'] =  stripHtml( self.soup.find('h1').renderContents() )
        except:
            log.exception('title is not found')
            page['title'] = ''
        post_str =''
        try:
            post_str = stripHtml( self.soup.find('p','post-info').renderContents() )
            date_str = post_str[:post_str.find('|')].strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,\
                        'Posted %m/%d/%y at %I:%M:%S %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception('error with posted str')
            page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                            ,"%Y-%m-%dT%H:%M:%SZ")
        try:
            if not post_str=='':
                page['et_author_name'] = re.sub('^by','',post_str\
                                        [post_str.find('|')+1:].strip()).strip()
        except:
            log.exception('Author name not found')
        try:
            post_hash = get_hash(page)
            if not checkSessionInfo(self.genre, self.session_info_out,\
                            self.currenturi,self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug(id)
                result=updateSessionInfo(self.genre, self.session_info_out,
                                    self.currenturi ,post_hash,'Post',self.task.\
                                            instance_data.get('update'), Id=id)
                                    
                if result['updated']:
                    page['path']=[self.parenturi]
                    page['parent_path']=[]
                    page[ 'task_log_id' ] = self.task.id
                    page[ 'versioned' ] = self.task.instance_data.get('versioned'\
                                                                        ,False)
                    page[ 'category' ] = self.task.instance_data.get('category',\
                                                                        'generic')
                    page[ 'last_updated_time' ] = datetime.strftime(datetime.utcnow()\
                                                            ,"%Y-%m-%dT%H:%M:%SZ")
                                                            
                    page[ 'client_name' ] = self.task.client_name
                    page[ 'entity' ] = 'post'
                    page[ 'uri' ] = normalize(self.currenturi)
                    page[ 'data' ] = ''
                    page[ 'uri_domain' ] = urlparse.urlparse(page['uri'])[1]
                    page[ 'priority' ] = self.task.priority
                    page[ 'level' ] = self.task.level
                    page[ 'pickup_date' ] = datetime.strftime(datetime.utcnow()\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
                    page[ 'connector_instance_log_id' ] = self.task.connector_instance_log_id
                    page[ 'connector_instance_id' ] = self.task.connector_instance_id
                    page[ 'workspace_id' ] = self.task.workspace_id
                    page[ 'client_id' ] = self.task.client_id
                    log.info( page )
                    self.pages.append( page )
                    log.debug(self.log_msg("Parent page added"))
                    return True
                else:
                    log.debug(self.log_msg("Parent Page not added"))
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
            log.exception(self.log_msg('could not set the page as current page :%s'%uri))
            raise e



