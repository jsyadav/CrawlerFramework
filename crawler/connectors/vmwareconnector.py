
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''


#Srini
#modified by Ashish

import re
import md5
import copy
import urllib
import datetime
import traceback
import logging
import simplejson
from urllib2 import urlparse, unquote, Request, urlopen
from xml.sax import saxutils
from BeautifulSoup import BeautifulSoup

from tgimport import *
from baseconnector import BaseConnector
from utils.task import Task
from utils.utils import stripHtml, get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from utils.httpconnection import HTTPConnection

log = logging.getLogger('VMWareConnector')

# Someone review this

class VMWareConnector(BaseConnector):
    """Fetch the discussion post which had the specific search term in http://communities.vmware.com

    URLs are of the form
    http://communities.vmware.com/search.jspa?resultTypes=BLOG_POST&resultTypes=DOCUMENT&resultTypes=MESSAGE&resultTypes=BLOG&resultTypes=COMMUNITY&peopleEnabled=true&q=falconstor
    which need to be converted to
    http://communities.vmware.com/search.jspa?q=falconstor&resultTypes=MESSAGE&dateRange=all&communityID=&userID=&numResults=15&rankBy=9 format
    

    """
    @logit(log , 'fetch')
    def fetch(self):                                                                                         
        """Main function to be called. Fetch parent page and get links to individual reviews and goto next page till end
        """
        self.genre="Review"
        try:
            if self.currenturi:
                queryterm = re.search('q=([\w_-]+)', self.currenturi)
                if not queryterm:
                    self.task.status['fetch_status'] = False
                    return False
                queryterm = queryterm.group(1)
                # Get sorted by date
                self.currenturi = 'http://communities.vmware.com/search.jspa?q=%s&resultTypes=MESSAGE&dateRange=all&communityID=&userID=&numResults=15&rankBy=9' %(queryterm)
                
            log.debug(self.currenturi)
            parent_list = [self.currenturi]    
            # Fetch the contents of the parent page
            res = self._getHTML()
            self.rawpage = res['result']
            self._setCurrentPage()
            self.__getParentPage()
            while True:
                # Get the links for search result in this page
                result_blk = self.soup.find(id='jive-search-results-content')
                # Get the next page's soup object
                next_page = result_blk.find('a',{'class':'jive-pagination-next'})
                if next_page is not None:
                    next_page_uri = unicode(next_page['href'])
                else:
                    next_page_uri = None
                links = self.__getResultLinks(result_blk)
                posts_result = self.__addPosts(links, parent_list)
                if not posts_result:
                    break
                log.debug("Next Page: " + str(next_page))
                
                # Process the next_page soup object
                if next_page_uri is None:
                    log.info(self.log_msg('Reached last page of search result'))
                    break
                log.debug(">>>>>" + next_page_uri)
                self.currenturi = 'http://communities.vmware.com' + next_page_uri
                log.debug(">>>>>" + self.currenturi)
                log.debug(self.log_msg("Fetching the next result url %s" %(self.currenturi)))
                res = self._getHTML()
                self.rawpage = res['result']
                self._setCurrentPage()
                log.debug(">>>>> Fetched, going again to extract all")
            self.task.status['fetch_status']=True
            return True
        except:
            print traceback.format_exc()
            self.task.status['fetch_status']=False
            log.exception(self.log_msg('Exception in fetch'))
            return False
            
            

    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """Extract the link from the search result page
        """
        page = {}
        page['uri'] = self.currenturi
        page['title'] = 'No title available'
        try:
            thread_hash = get_hash(page)
        except Exception,e:
            log.exception(self.log_msg('could not build thread_hash'))
            raise e
        try:
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    self.currenturi, self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out, self.currenturi, thread_hash, 
                                         'SearchPage', self.task.instance_data.get('update'), Id=id)
                
#                 if result['updated']:
#                     page['uri'] = normalize(self.currenturi)
#                     page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
#                     page['priority']=self.task.priority
#                     page['level']=self.task.level
#                     page['pickup_date'] = datetime.datetime.strftime(datetime.datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
#                     page['posted_date'] = datetime.datetime.strftime(datetime.datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
#                     page['connector_instance_log_id'] = self.task.connector_instance_log_id
#                     page['connector_instance_id'] = self.task.connector_instance_id
#                     page['workspace_id'] = self.task.workspace_id
#                     page['client_id'] = self.task.client_id
#                     page['client_name'] = self.task.client_name
#                     page['last_updated_time'] = page['pickup_date']
#                     page['versioned'] = False
#                     page['data'] = ''
#                     page['task_log_id']=self.task.id
#                     page['entity'] = 'Thread'
#                     page['category']=self.task.instance_data.get('category','')
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            raise e
            

    @logit(log, '__getResultLinks')
    def __getResultLinks(self, result_blk):
        """Given the div of the results block, give back a list of links
        """
        links = []
        results = result_blk.findAll('span', {'class':'jive-search-result-subject'})
        for result in results:
            link = result.find('a')['href']
            link = 'http://communities.vmware.com' + link
            links.append(link)
        return links

    @logit(log, '__addPosts')
    def __addPosts(self, links, parent_list):
        """Given a list of links to the discussion post, fetch the post contents and the author info
        """
        h = HTTPConnection()
        for link in links:
            try:
                page = {}
                object_id = re.search('objectID=(\d+)', link).group(1)
                link = "http://communities.vmware.com/message/%s#%s" %(object_id, object_id)
                # Using the redirected url instead of the url given by the search page
                self.currenturi = link
                page['uri'] = normalize(link)
                log.debug(self.log_msg("Fetching the post url %s" %(self.currenturi)))
                if checkSessionInfo(self.genre, self.session_info_out, self.currenturi,
                                    self.task.instance_data.get('update'), parent_list=parent_list):
                    # No need to pick this page
                    continue
                res = self._getHTML()

                self.rawpage = res['result']
                self._setCurrentPage()
                # First try extracting from the post body
                if not self.__extractPostBody(page, object_id):
                    # if that fails, extract from the replies
                    self.__extractReplyBody(page, object_id)

            except:
                log.exception(self.log_msg("exception in extracting page"))
                continue
            page['posted_date'] = datetime.datetime.strftime(page['posted_date'], "%Y-%m-%dT%H:%M:%SZ")

            checksum = md5.md5(''.join(sorted(page.values())).encode('utf-8','ignore')).hexdigest()
            id = None
            if self.session_info_out=={}:
                id = self.task.id
            result = updateSessionInfo(self.genre, self.session_info_out, self.currenturi,
                                       checksum, 'Post', self.task.instance_data.get('update'),
                                       parent_list=parent_list, Id=id)
            if result['updated']:
                page['path'] =  page['parent_path'] = parent_list
                page['path'].append(self.currenturi)
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.datetime.strftime(datetime.datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id  # TODO: Get the client from the project 
                page['client_name'] = self.task.client_name
                page['last_updated_time'] = page['pickup_date']
                page['versioned'] = False
                page['entity'] = 'Review'
                page['category'] = self.task.instance_data.get('category','')
                page['task_log_id']=self.task.id
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                

            # Calculate the hash and get the session info thingy
            self.pages.append(page)
        return True
            
            
    @logit(log, '__extractPostBody')
    def __extractPostBody(self, page, object_id):
        """Extract the post(question) body contents and author info
        """
        anchor = self.soup.find('a', {'name':object_id})
        postbody = anchor.parent.parent
        if postbody.name != 'div':
            return False                # It is a reply, go on to the next method
        subj_soup = postbody.find('div', {'class':'jive-thread-post-subject-content'})
        page['title'] = unicode(subj_soup.find('h2').string)
        page['posted_date'] = unicode(subj_soup.find('h3').string)
        page['posted_date'] = datetime.datetime.strptime(page['posted_date'], '%b %d, %Y %I:%M %p')
        page['data'] = stripHtml(postbody.find('div', {'class':'jive-thread-post-message'}).prettify())
        author_blk = postbody.find('div', {'class':'jive-author'})
        self.__extractAuthorInfo(page, author_blk)
        
    @logit(log, '__extractReplyBody')
    def __extractReplyBody(self, page, object_id):
        """Extract the reply to the post's content and author info
        """
        anchor = self.soup.find('a', {'name':object_id})
        postbody = anchor.parent
        if postbody.name != 'td':
            return False                # It is not a post, Must raise an exception
        subj_soup = postbody.find('div', {'class':'thread-reply-bar'})
        page['title'] = unicode(subj_soup.find('strong').string)
        page['posted_date'] = unicode(subj_soup.find('a', {'title':'Link to reply'}).string)
        page['posted_date'] = datetime.datetime.strptime(page['posted_date'], '%b %d, %Y %I:%M %p')
        page['data'] = stripHtml(postbody.find('div', {'class':'jive-thread-reply-message'}).prettify())
        author_blk = postbody.find('div', {'class':'jive-author'})
        self.__extractAuthorInfo(page, author_blk)
                                     
        

    @logit(log, '__extractAuthorInfo')
    def __extractAuthorInfo(self, page, author_blk):
        author_link = author_blk.find('a')
        author_anchor = author_blk.find('a', {'class':'jive-username-link'})
        page['et_author_name'] = unicode(author_anchor.string)
        page['et_author_homepage'] = 'http://communities.vmware.com' + unicode(author_anchor['href'])
        
        
    
        
    
