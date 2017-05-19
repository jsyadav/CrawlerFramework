'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

import re
import logging

from datetime import datetime
from urllib2 import urlparse

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from utils.urlnorm import normalize

log =logging.getLogger('NoteBookForumConnector')

class NoteBookForumConnector(BaseConnector):
    """A connector for www.notebookforum.com
    """
    @logit(log, 'fetch')
    def fetch (self) :
        """Initial fetch method of this connector,Entry Point
        """
        try:
            self.genre="Review"
            self.parenturi=self.currenturi
            if not self.__setSoup():
                return False
            self.__getParentPage()
            comment_ulrs = list(set([each['href'] for each in self.soup.findAll('a', href =re.compile('post[\d]+\.html'))]))
            for each_url in comment_ulrs:
                self.currenturi = 'http://www.notebookforums.com' + each_url
                if not self.__setSoup():
                    continue
                while True:
                    self.__addReviews()
                    try:
                        self.currenturi = 'http://www.notebookforums.com/' + self.soup.find('a', {'href':True, 'title':re.compile('^Next.+')}, text ='&gt;').parent['href']
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page not found'))
                        break
            return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False
        
    @logit(log, '_getParentPage')
    def __getParentPage(self):
        page ={}
        try:
            page['title'] = stripHtml(self.soup.find('h1').renderContents())
                
        except:
            log.exception(self.log_msg('LAPTOP doesnt exist'))
            page['title'] = ''
        try:
            ratings=self.soup.find('table', {'cellspacing':'0', 'cellpadding':3}).findAll('td')
            for each in ratings:
                try:
                    if each.b:
                        key = 'ef_rating_'+ each.b.renderContents()[:-1].lower().replace(' ','_')
                        page[key]= float( re.search('rating(\d+)\.gif',each.b.findNext('td').find('img')['src']).group(1))
                except:
                    log.info(self.log_msg('could not parse ratings from parent page'))
        except:
            log.info(self.log_msg('Ratings cannot be found'))
        try:
            post_hash = get_hash(page)
            if checkSessionInfo(self.genre, self.session_info_out,self.parenturi, self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info return True'))
                return False
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo(self.genre, self.session_info_out, self.parenturi, post_hash, 'Post', self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                log.info(self.log_msg('Reult not updataed'))
                return False
            page['path'] = [self.parenturi]
            page['parent_path'] = []
            page['uri'] = self.currenturi
            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
            page['priority']=self.task.priority
            page['level']=self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id                                                        
            page['last_updated_time'] = page['pickup_date']
            page['versioned'] = False
            page['data'] = page['title']
            page['entity'] = 'Post'
            page['category']=self.task.instance_data.get('category','')
            page['task_log_id']=self.task.id
            page['client_name'] = self.task.client_name
            page['versioned']=self.task.instance_data.get('versioned',False)
            self.pages.append(page)
            log.info(self.log_msg('Parent page added'))
            return True
        except:
            log.exception(self.log_msg('error while adding parent page info'))
            return False
                
    @logit(log, 'addReviews')
    def __addReviews(self):
        """ This will add the comments as the data
        """
        try:
            comments =self.soup.find('div',id='posts').findAll('div',style='text-align:left')
        except:
            log.info(self.log_msg('Comments not found'))
            return False

        for comment in comments:
            page={}
            try:
                page['et_author_name'] = stripHtml(comment.find('a','bigusername').renderContents())
            except:
                log.info(self.log_msg('Author name not found'))

            try:
                page['ei_author_reviews_count'] = int(re.search('\d+',stripHtml(comment.find('a','orangehlink').findParent('div').find('b',text='Posts:').next)).group())
                # No more author info,Author links ask for user Registration
            except:
                log.info(self.log_msg('Author Reviews Count not found'))
            try:
                page['data'] = stripHtml(comment.find('div',{'class':'itxt'}).renderContents())
            except:
                log.info( self.log_msg("Exception fetching commens data") )
                continue
            try:
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
            except:
                log.exception(self.log_msg('title not found'))
                page['title'] = ''
            try:
                date=stripHtml(comment.find('a', {'name': re.compile("post\d+")}).renderContents())
                page['posted_date'] = datetime.strftime(datetime.strptime(date,'%m-%d-%Y, %I:%M %p'),'%Y-%m-%dT%H:%M:%SZ')
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                log.exception(self.log_msg('Exception in fetching date'))
            try:
                review_hash = get_hash(page)
                if checkSessionInfo(self.genre, self.session_info_out,review_hash, self.task.instance_data.get('update'),parent_list=[ self.parenturi] ):
                    log.info(self.log_msg('session info return True'))
                    continue
                result=updateSessionInfo(self.genre, self.session_info_out, review_hash, review_hash ,'Review', self.task.instance_data.get('update'),parent_list=[self.parenturi])
                if not result['updated']:
                    log.info(self.log_msg('Update session info return False'))
                    continue
                page['path'] = page['parent_page'] = [self.parenturi]
                page['path'].append(review_hash)
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id  # TODO: Get the client from the project
                page['last_updated_time'] = page['pickup_date']
                page['uri'] = self.currenturi
                page['uri_domain'] =  urlparse.urlparse(page['uri'])[1]
                page['versioned'] = False
                page['client_name'] = self.task.client_name
                page['task_log_id']=self.task.id
                page['category'] = self.task.instance_data.get('category' ,'')
                page['versioned']=self.task.instance_data.get('versioned',False)
                self.pages.append(page)      
            except:
                log.exception(self.log_msg('Exception in add_review (Comment)'))
                
    @logit(log, "_setSoup")
    def __setSoup( self, url = None, data = None, headers = {} ):
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
            

