
'''
Copyright (c)2008-2010 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

# Packiaraj

import re
from datetime import datetime
import logging
import copy
from urllib2 import urlparse

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('EngadgetConnector')
class EngadgetConnector(BaseConnector):
    '''
    This will fetch the info for the http://www.engadget.co
    Test with sample uri
    For testing Start with the following uris
    
    http://www.engadget.com/2009/10/30/motorola-droid-review/
    '''

    @logit(log , 'fetch')
    def fetch(self):
        self.genre = "Review" 
        try:
             if not self._setSoup():
                log.info(self.log_msg("Soup not set,returning false"))
                return False
             if not self._getParentPage():
                log.info(self.log_msg("Parent page not found"))
             while True:
                self.__addComments()
                try:
                    self.currenturi = self.task.instance_data['uri'] + self.soup.find('div',id='cmt_paging').find('a', text='Next 50 Comments').parent['href']
                    if not self._setSoup():
                        break
                except:
                    log.info(self.log_msg('Next page not found'))
                    break
             return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log , '__addComments')
    def __addComments(self):
        """
        It will add the comments """
        try:
            comments = self.comments = self.soup.findAll('div','container_contents_holder')
            log.info(self.log_msg('no of comments is %s'%len(comments)))
            if not comments:
                return False
        except:
            log.exception(self.log_msg('No comments are found'))
            return False
        for comment in comments:
            page = {}
            try:
                comment_info = comment.find('div','comment_data').findAll('a')
                page['et_author_name'] = stripHtml(comment_info[0].renderContents())
            except:
                log.info(self.log_msg('Cannot find the author name'))
            try:
                page['et_comment_rank'] = stripHtml(comment.find('span').renderContents())
            except:
                log.info(self.log_msg('Cannot find the comment ranking' ))
            try:
                posted_date_str = stripHtml(comment_info[1].renderContents())
                page['posted_date'] = datetime.strftime(datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",posted_date_str).strip(),"%b %d %Y %I:%M%p"),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.info(self.log_msg('Cannot find the posted_date'))
            try:
                page['data'] = stripHtml(comment.find('div','comment_text').find('p').renderContents())
                page['title'] = page['data'][:50]
            except:
                log.exception(self.log_msg('Cannot find the Data and also title'))
                continue
            try:
                comment_hash = get_hash( page )
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.task.instance_data['uri']]):
                    continue
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            comment_hash,'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.task.instance_data['uri']])
                if not result['updated']:
                    continue
                parent_list = [ self.task.instance_data['uri'] ]
                page['parent_path'] = copy.copy(parent_list)
                parent_list.append( unique_key )
                page['path']=parent_list
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")
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
                page['uri'] = self.currenturi
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append(page)
                log.info(self.log_msg('Comments Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))

    @logit(log , 'getParentPage')
    def _getParentPage(self):
            """
            It will fetch the Product info
            """
            page = {}
            if checkSessionInfo(self.genre, self.session_info_out, self.task.instance_data['uri'] \
                                        , self.task.instance_data.get('update')):
                log.info(self.log_msg('Session infor return True'))
                return False
            try:
                page['title'] = stripHtml(self.soup.find('div','post_content permalink').find('h4').renderContents())
            except:
                log.exception(self.log_msg('could not parse page title'))
                page['title'] = ''
            try:
                page['et_author_name'] = self.soup.find('div','post_content permalink').find('div','post_byline').find('a').renderContents()
            except:
                log.info(self.log_msg('product author name found'))
            try:
                product_post_dt = self.soup.find('div','post_content permalink').find('div','post_byline')
                posted_date_str = stripHtml(product_post_dt.find('span','caption').find('span','post_time').renderContents())
                page['posted_date'] =  datetime.strftime(datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",posted_date_str).strip(),"%b %d %Y %I:%M%p"),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.info(self.log_msg('posted_date not found'))
            try:
                page['data'] = stripHtml(self.soup.find('div','post_body').renderContents())
            except:
                log.info(self.log_msg('product data not found')).renderContents()
            try:
                post_hash = get_hash(page)
                id = None
                if self.session_info_out == {}:
                    id = self.task.id
                result = updateSessionInfo(self.genre, self.session_info_out,\
                        self.task.instance_data['uri'], post_hash,'Post',self.task.instance_data.get('update'), Id=id)
                if not result['updated']:
                    return False
                page['path']=[self.task.instance_data['uri']]
                page['parent_path']=[]
                page['uri'] = normalize(self.task.instance_data['uri'])
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
                page['task_log_id']=self.task.id
                page['entity'] = 'Post'
                page['category']=self.task.instance_data.get('category','')
                self.pages.append(page)
                log.info(self.log_msg('Parent Page added'))
                return True
            except:
                log.exception(self.log_msg("parent post couldn't be parsed"))
                return False

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
