
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Mohit Ranka
import re
import md5
import logging
import copy

from urlparse import urlparse
from datetime import datetime

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('GoogleGroupsConnector')

#Note - Unfortunately feedparser only picks 10 feeds even if the feednumbers
#specified are more than 10, hence feedparser cannot be used

class GoogleGroupsConnector(BaseConnector):
    
    base_url = "http://groups.google.com/"

    @logit(log , 'fetch')
    def fetch(self):     
        """
        Fetch the data for google groups topic url.
        
        return false if the supplied url is not a topic url
        or and exception occurs.
        """
        self.genre="Review"
        try:
            if re.match("^http://groups\.google\.com/group/.+?/topics$",self.currenturi):
                self.__getParentPage(self.currenturi)
                self.__populateUrlList()
                self.__iteratePosts()
                return True
            else:
                log.info(self.log_msg("%s is NOT a thread url" %self.currenturi))
                return False
        except Exception ,e:
            log.exception(self.log_msg("Exception %s occured"%e.message))
            return False

    @logit(log , '__getParentPage')
    def __getParentPage(self,parent_uri):
        """
        Makes a task log entry for the url.

        Google groups does not have anything to store as 
        parent level information, so it does not have 
        any entries for parent page.
        """
        try:
            if not checkSessionInfo(self.genre, 
                                    self.session_info_out, 
                                    parent_uri,
                                    self.task.instance_data.get('update')):

                #Assigning parent_uri to post_hash for the sake of code clarity
                post_hash = parent_uri 
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre,
                                         self.session_info_out,
                                         parent_uri,
                                         post_hash,
                                         'Search_url', 
                                         self.task.instance_data.get('update'),
                                         Id=id)
            return True
        except Exception,e:
            log.exception(self.log_msg("Exception occured in __getParentPage()"))
            raise e

    @logit (log,'__populateUrlList')
    def __populateUrlList(self):
        """
        populates self.url_list

        fetches the message urls for the topic urls,
        specified by the googlegroups_numresults
        """
        try:
            RESULTS_ITERATIONS = tg.config.get(path='Connector', \
                                                   key='googlegroups_numresults')
            iterator_count = 0
            self.url_list = []
            while True:
                log.debug(self.log_msg("Iterating url %s"%self.currenturi))
                res=self._getHTML(self.currenturi)
                if res:
                    self.rawpage=res['result']
                    self._setCurrentPage()
                    thread_url_list = [link['href'] for link in \
                                           self.soup.findAll('a',attrs={'href':True}) \
                                           if re.match("/group/.+?/browse_thread/thread/.+?$", \
                                                           link['href'])]

                    #This makes sure that duplicate urls dont not get picked up,
                    #in case the same thread url get picked up more than one

                    unique_thread_url_list = list(set(thread_url_list))
                    
                    for each_url in unique_thread_url_list:
                        if iterator_count < RESULTS_ITERATIONS:
                            self.url_list.append(self.base_url+each_url)
                            iterator_count = iterator_count + 1
                        else:
                            log.debug(self.log_msg("All links fetcheded"))
                            return True
                    try:
                        self.currenturi=self.base_url+self.soup.find( \
                            'a',text = 'Older &raquo;').parent['href']
                    except:
                        log.info(self.log_msg("All links fetcheded"))
                        return True
                else:
                    log.debug(self.log_message("No HTML Page set for %s" %(self.currenturi)))
                    return False
        except Exception,e:
            log.exception(self.log_msg("Exception occured in __populateUrlList()"))
            raise e
    
    @logit (log,'__iteratePosts')
    def __iteratePosts(self):
        """
        iterates  all the message urls and fetch data from those urls
        """
        try:
            for url in self.url_list:
                self.currenturi = url
                res=self._getHTML(self.currenturi)
                if res:
                    log.debug(self.log_msg("Fetching url %s" %(self.currenturi)))
                    self.rawpage=res['result']
                    self._setCurrentPage()
                    post_list = self.soup.findAll( \
                        'div',attrs={'id':re.compile('^msg_.+'), \
                                         'class':re.compile('^msg')})
                    log.debug(self.log_msg("Found %d messages for the url %s"%(len(post_list),self.currenturi)))
                    for post_idx,each_post in enumerate(post_list):
                        log.debug(self.log_msg("fetching post number %d"%post_idx))
                        self.current_post = each_post
                        self.__getPosts()
                else:
                    log.debug(self.log_msg("Could not set the HTML page for %s, continuing to" \
                                               + "the next url" %(self.currenturi)))
                    continue
            return True
        except Exception,e:
            log.exception(self.log_msg("Exception occured in __iteratePost"))
            raise e

    @logit(log , '__getPosts')
    def __getPosts(self):
        """
        fetches a individual message
        """
        try:
            try:
                post_id = self.current_post['id']
                log.info(self.log_msg('post_identifier : %s '%(post_id)))
            except:
                log.exception(self.log_msg('could not extract post identifier,' \
                                               + 'so continuing to the next post'))
                return False
            if not checkSessionInfo(self.genre, 
                                    self.session_info_out, 
                                    post_id, 
                                    self.task.instance_data.get('update'),
                                    parent_list=[self.task.instance_data['uri']]):
                page = {}
                try:
                    page['data'] = stripHtml(self.current_post.find \
                                                 ('div',attrs={'id':'inbdy'}).renderContents())
                except:
                    log.info(self.log_msg('could not parse post data'))
                    page['data'] = ''

                try:
                    page['title'] =  stripHtml(self.current_post.find('div',attrs={'class':'exh'}) \
                                                   .find('div',text=re.compile('Subject')). \
                                                   parent.renderContents())
                except:
                    page['title']=page['data'][:100]
                try:
                    page['et_author_name'] = stripHtml(self.current_post.find \
                                                           ('span',attrs={'class':re.compile('author')}) \
                                                           .renderContents())
                except:
                    log.info(self.log_msg('could not parse author name'))
                try:
                    post_hash = get_hash(page)
                except Exception,e:
                    log.exception(self.log_msg("exception in buidling post_hash , moving onto next post"))
                    raise e

                result=updateSessionInfo(self.genre, 
                                         self.session_info_out,
                                         post_id, 
                                         post_hash, 
                                         'Post',
                                         self.task.instance_data.get('update'), 
                                         parent_list=[self.task.instance_data['uri']])
                if result['updated']:
                    try:
                        posted_date = stripHtml(self.current_post.find(\
                                'td',attrs={'nowrap':True}).findAll('span')[-1].renderContents()).strip()
                        try:
                            # First Pattern - "Dec 3, 2:31 am"
                            page['posted_date'] =  datetime.strftime(datetime.strptime( \
                                    str(datetime.utcnow().year) + ' ' + posted_date, \
                                        "%Y %b %d, %H:%M %p"),"%Y-%m-%dT%H:%M:%SZ")
                        except:
                            # Second Pattern - "Apr 15 2004, 6:47 pm"
                            try:
                                page['posted_date'] = datetime.strftime(datetime.strptime( \
                                        posted_date,"%b %d %Y, %H:%M %p"),"%Y-%m-%dT%H:%M:%SZ")
                            except:
                                #Raise the error to store today's datetime
                                raise e
                    except:
                        log.info(self.log_msg('posted_date could not be parsed'))
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['uri'] = normalize(self.currenturi)
                    parent_list = [self.task.instance_data['uri']]
                    page['parent_path'] = copy.copy(parent_list)
                    parent_list.append(page['uri'])
                    page['path'] = parent_list
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['last_updated_time'] = page['pickup_date']
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  
                    page['client_name'] = self.task.client_name
                    page['versioned'] = False
                    page['uri_domain'] = urlparse(self.currenturi)[1]
                    page['task_log_id']=self.task.id
                    page['entity'] = 'Post'
                    page['category'] = self.task.instance_data.get('category' ,'')
                    self.pages.append(page)
                else:
                    log.info(self.log_msg('NOT Appending to self.pages'))
            else:
                log.info(self.log_msg('NOT Appending to self.pages'))
            return True
        except Exception, e:
            log.exception(self.log_msg('Exception in _getPosts'))
            raise e
