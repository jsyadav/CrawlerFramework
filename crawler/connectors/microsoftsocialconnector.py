
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#MOHIT RANKA
import copy
from urlparse import urlparse
import re
import logging
from datetime import datetime,timedelta
import simplejson
from baseconnector import BaseConnector
from tgimport import *
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.sessioninfomanager import updateSessionInfo,checkSessionInfo
from utils.decorators import logit

log = logging.getLogger('MicrosoftSocialConnector')

class MicrosoftSocialConnector(BaseConnector):
    @logit(log,'fetch')
    def fetch(self):
        """
        Fetches the first RESULTS_ITERATIONS results as specified by the attributes, and populate the result links to self.linksOut
        """
        try:
            if re.match(".*\/threads[\/]?$",self.task.instance_data['uri']):
                self.last_timestamp = datetime(1,1,1)
                self.forum_name = re.findall('\/([^\/]+)\/threads\/?$', urlparse(self.task.instance_data['uri'])[2])[0]
                self.crawl_count = int(tg.config.get(path='Connector',key='microsoft_numresults'))
                self.count = 0
                self.done = False
                self.currenturi = self.task.instance_data['uri']+'?sort=firstpostdesc'
                while self.count< self.crawl_count and not self.done:
                    self.__getPageData()
                log.debug(self.log_msg("Length of linksout is %d"%(len(self.linksOut))))
                if self.linksOut:
                    updateSessionInfo('search', self.session_info_out,self.last_timestamp , None,'ForumThreadsPage', self.task.instance_data.get('update'))
                return True
            elif re.match(".*\/thread\/.*?$",self.task.instance_data['uri']):
                self.__getThread()
                self.__getQuestion()
                self.__getAnswers()
                return True
            else:
                log.exception(self.log_msg("Unassociated url %s"%(self.task.instance_data['uri'])))
                return False
        except:
            log.exception(self.log_msg("Exception occured in fetch()"))
            return False

    @logit(log,'__getPageData')
    def __getPageData(self):
        try:
            if self.count<self.crawl_count and not self.done:
                res=self._getHTML(self.currenturi)
                if not res:
                    log.info(self.log_msg("Did not get the main page content"))
                    return False
                self.rawpage=res['result']
                self._setCurrentPage()
                for  thread in self.soup.find('ul',attrs={'class':'threads'}).findAll('li',attrs={'class':'thread'}):
                    self.count = self.count + 1
                    if self.count<self.crawl_count and not self.done:
                        try:
                            # If date is in "Monday, March 30, 2009 11:52 PM" format
                            thread_date  =  datetime.strptime(thread.find('span',attrs={'class':'date'}).renderContents(),"%A, %B %d, %Y %H:%M %p")
                        except:
                            hours = 0
                            minutes = 0
                            try:
                                minutes = int(re.findall("\d+",thread.find('span',attrs={'class':'date'}).renderContents())[-1])
                                if len(re.findall("\d+",thread.find('span',attrs={'class':'date'}).renderContents()))>1:
                                    hours = re.findall("\d+",thread.find('span',attrs={'class':'date'}).renderContents())[0]
                                thread_date = datetime.utcnow()-timedelta(hours = hours) - timedelta(minutes = minutes)
                            except:
                                #if still does not get the thread_date
                                thread_date = datetime.utcnow()
                    if  not checkSessionInfo('search',
                                             self.session_info_out, thread_date,
                                             self.task.instance_data.get('update')):

                        self.last_timestamp = max(thread_date,self.last_timestamp)
                        url = thread.find('h3').find('a')['href']
                        #self.count = self.count + 1
                        temp_task = self.task.clone()
                        temp_task.instance_data['uri']=url
                        temp_task.instance_data['connector_name']='MicrosoftSocialConnector'
                        self.linksOut.append(temp_task)
                    else:
                        log.debug(self.log_msg("Older thread found, quitting"))
                        #self.done = True
                        #break
                if self.count<self.crawl_count and not self.done:
                    self.currenturi = self.soup.find('div',attrs={'class':'pager'}).findAll('a')[-1]['href']
                else:
                    log.debug(self.log_msg("All threads fetched, quitting"))
                    self.done = True
            else:
                log.debug(self.log_msg("All threads fetched, quitting"))
                self.done = True
            return True
        except:
            log.exception(self.log_msg("Exception occured while iterating over the threads"))
            self.done = True
            return False

    @logit(log,'__getThread')
    def __getThread(self):
        """
        Fetches the data from a thread page and appends them self.pages, Always works as update True
        """
        try:
            res=self._getHTML(self.task.instance_data['uri'])
            if not res:
                log.info(self.log_msg("Could not set the thread HTML page"))
                return False
            self.rawpage=res['result']
            self._setCurrentPage()
            page = {}
            try:
                page['et_thread_hierarchy']=[stripHtml(each.renderContents()) for each in self.soup.find('div',attrs={'class':'bread'}).findAll('a')]
                page['title']=page['et_thread_hierarchy'][-1]
            except:
                page['title']=''
                log.info(self.log_msg("Exception occured while fetching thread hierarchy"))
            page['data']= page['title']
            try:
                self.topic = stripHtml(self.soup.find('span',attrs={'name':'subject'}).renderContents())
            except:
                log.info(self.log_msg("Could not pick topic"))
                self.topic = ''
            try:
                thread_hash = get_hash(page)
            except:
                log.debug(self.log_msg("Error occured while creating the thread hash %s" %self.task.instance_data['uri']))
                return False
            if not checkSessionInfo('review', self.session_info_out,
                                    self.task.instance_data['uri'], self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug('got the connector instance first time, sending updatesessioninfo the id : %s' % str(id))
                result=updateSessionInfo('review', self.session_info_out, self.task.instance_data['uri'], thread_hash,
                                         'Thread', True, Id=id)
                if result['updated']:
                    page['parent_path']=[]
                    page['path']=[self.task.instance_data['uri']]
                    page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
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
                    page['entity'] = 'Thread'
                    page['category'] = self.task.instance_data.get('category','')
                    page['task_log_id']=self.task.id
                    page['uri']=normalize(self.currenturi)
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    self.pages.append(page)
                    log.debug(self.log_msg("Thread %s added to self.pages" %(self.task.instance_data['uri'])))
                else:
                    log.debug(self.log_msg("Thread %s NOT added to self.pages" %(self.task.instance_data['uri'])))
            else:
                log.debug(self.log_msg("Thread %s NOT added to self.pages" %(self.task.instance_data['uri'])))
        except Exception,e:
            self.done = False
            log.exception(self.log_msg("Exception occured in __getThread() for thread %s" %self.task.instance_data['uri']))
            return False

    @logit(log,'__getPosts')
    def __getPosts(self,post_type):
        try:
            message_id = self.message['id']
            parent_uri = self.task.instance_data['uri']
            if not checkSessionInfo('review', self.session_info_out,
                                    message_id, self.task.instance_data.get('update'),
                                    parent_list=[parent_uri]):
                page={}
                try:
                    page['et_data_message_id']=message_id
                except:
                    log.info(self.log_msg("Exception occured while message id"))
                try:
                    if post_type=="Suggestion":
                        page['et_data_reply_to']=self.reply_id
                    else:
                        log.debug(self.log_msg("Post is a parent post"))
                except:
                    log.info(self.log_msg("Exception occured while message id"))
                try:
                    page['title'] = self.topic
                except:
                    page['title'] = ''
                    log.info(self.log_msg("Exception occured while fetching post title"))
                try:
                    page['et_author_name'] = stripHtml(self.message.find('span',attrs={'class':'fullbadge'}).\
                                                           find('a',attrs={'class':'author'}).\
                                                           find('span',attrs={'class':'name'}).renderContents())
                except:
                    log.info(self.log_msg("Exception occured while fetching author's name"))
                try:
                    page['data']= stripHtml(self.message.find('div',attrs={'class':'body'}).renderContents())
                except:
                    page['data']=''
                    log.info(self.log_msg("Exception occured while fetching post data"))
                try:
                    page['et_data_post_type']=post_type
                except:
                    log.info(self.log_msg("Post type not found for message %s"%(message_id)))
                try:
                    page['et_data_forum']= stripHtml(self.soup.find('div',attrs={'class':'bread'}).findAll('a')[-3].renderContents())
                except:
                    log.info(self.log_msg("Exception occured while fetching forum name"))

                try:
                    page['et_data_subforum']= stripHtml(self.soup.find('div',attrs={'class':'bread'}).findAll('a')[-2].renderContents())
                except:
                    log.info(self.log_msg("Exception occured while fetching subforum name"))

                try:
                    page['et_data_topic']= stripHtml(self.soup.find('div',attrs={'class':'bread'}).findAll('a')[-1].renderContents())
                except:
                    log.info(self.log_msg("Exception occured while fetching topic"))

                message_hash = get_hash(page)
                result=updateSessionInfo('review', self.session_info_out, message_id, message_hash,
                                         'Post', self.task.instance_data.get('update'), parent_list=[parent_uri])
                if result['updated']:
                    try:
                        message_created = stripHtml(self.message.find('div',attrs={'class':'head'}).\
                                                        find('span',attrs={'class':'date'}).renderContents())
                        page['posted_date'] = datetime.strftime(datetime.strptime(message_created,"%A, %B %d, %Y %H:%M %p"),\
                                                                    "%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg("Error occured while fetching posted date"))
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")

                    page['parent_path']=[parent_uri]
                    parent_list = [parent_uri]
                    parent_list.append(message_id)
                    page['path']=copy.copy(parent_list)
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
                    page['entity'] = 'Post'
                    page['category'] = self.task.instance_data.get('category','')
                    page['task_log_id']=self.task.id
                    page['uri']=normalize(self.currenturi)
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    self.pages.append(page)
                    log.debug(self.log_msg("Post %s added to self.pages" %(message_id)))
                else:
                    log.debug(self.log_msg("Post %s NOT added to self.pages" %(message_id)))
            else:
                log.debug(self.log_msg("Post %s NOT added to self.pages" %(message_id)))
            return True
        except Exception,e:
            log.exception(self.log_msg("Exception occured in __getPosts for post %s" %message_id))
            return False

    @logit(log,'__getQuestion')
    def __getQuestion(self):
        """
        """
        try:
            self.message = self.soup.find('ul',attrs={'class':'message root'}).find('li',attrs={'id':True})
            self.reply_id = self.message['id']
            self.__getPosts("Question")
        except:
            log.info(self.log_msg("Error occured while fetching question from the thread %s" %self.task.instance_data['uri']))
            return False

    @logit(log,'__getAnswers')
    def __getAnswers(self):
        """
        """
        try:
            for answer in self.soup.find('div',attrs={'class':'replies'}).find('ul',attrs={'class':'message'}).findAll('li',attrs={'id':True}):
                self.message = answer
                self.__getPosts("Suggestion")
        except:
            log.exception(self.log_msg("Error occured while fetching question from the thread %s" %self.task.instance_data['uri']))
            return False
