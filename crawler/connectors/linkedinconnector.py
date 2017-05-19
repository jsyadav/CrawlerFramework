
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#MOHIT RANKA
#Ashish Yadav

from urlparse import urlparse
from urllib import quote_plus
import re
import logging
from datetime import datetime
from datetime import timedelta
from baseconnector import BaseConnector
from tgimport import *
import cgi
from utils.sessioninfomanager import *
from utils.task import Task
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import *
from copy import copy
log = logging.getLogger('LinkedInConnector')

class LinkedInConnector(BaseConnector):

    @logit(log,'__createSiteUrl')
    def __createSiteUrl(self):
        """
        Fetches the search data from user network and linkedin network for a given queryterm
        """
        try:
            if self.group_id:
                self.currenturi = 'http://www.linkedin.com/groups?gid=%s'%self.group_id
                return True
            if not self.category_id:
                self.currenturi ='https://www.linkedin.com/searchAnswers?results=&pplSearchOrigin=GLHD&keywords=%s'%quote_plus(self.keyword_term)
            else:
                self.currenturi = 'https://www.linkedin.com/searchAnswers?keywords=%s&searchScope=true&categoryID=%s&fake-category=%s&runSearch=Refine+Search&runSearch='%(quote_plus(self.keyword_term),self.category_id,self.category_id)
            return True
        except:
            log.exception(self.log_msg("Error occured while parsing the keyword for the url: %s" %(self.task.instance_data['uri'])))
            return False

    @logit(log,'fetch')
    def fetch(self):
        """
        Fetches all the search data for a given self.currenturi and returns fetched_status depending
        on the success and faliure of the task
        """
        try:
            self.genre = 'review'
            self.search_result_count = 0
            if self.__linkedinAuth():
                self.group_id = dict(cgi.parse_qsl(self.task.instance_data['uri'].split('?')[-1])).get('gid')
                self.keyword_term = dict(cgi.parse_qsl(self.task.instance_data['uri'])).get('keywords')
                self.category_id = dict(cgi.parse_qsl(self.task.instance_data['uri'])).get('categoryID')
                if self.__createSiteUrl():
                    res=self._getHTML(self.currenturi)
                    if res:
                        self.rawpage=res['result']
                        self._setCurrentPage()
                    else:
                        log.debug(self.log_msg("Could not set the search page url"))
                        return False
                    if self.group_id:
                        try:
                            self.currenturi = 'http://www.linkedin.com' + self.soup.find('a',title='See all general discussions')['href']
                            res=self._getHTML(self.currenturi)
                            self.rawpage=res['result']
                            self._setCurrentPage()
                            self.__fetchGroupDetails()
                            return True
                        except:
                            log.info(self.log_msg('Exception while fetching data'))
                            return False
                    if self.__getParentPage():
                        self.url_list = []
                        self.__iterateSearchPages()
                        for link in self.url_list:
                            self.currenturi = link
                            res=self._getHTML(self.currenturi)
                            if res:
                                self.rawpage=res['result']
                                self._setCurrentPage()
                                self.__getQuestionData(self.keyword_term + str(self.category_id))
                            else:
                                log.debug(self.log_msg("Could not set the search page url"))
                                return False
            if len(self.pages)>0:
                log.debug(self.log_msg("%d Linkedin search results  for uri %s added to self.pages" %(len(self.pages),self.task.instance_data['uri'])))
            else:
                log.debug(self.log_msg("No Linkedin search results fetched for uri %s" %(self.task.instance_data['uri'])))
            return True
        except:
            log.exception(self.log_msg("Exception occured while fetching data"))
            return False

    @logit(log,'__linkedinAuth')
    def __linkedinAuth(self):
        """
        Authenticates and lands self to linkedin Homepage
        """
        try:
            # Authenticate with credentials
            self.currenturi = "https://www.linkedin.com/secure/login"
            auth_data = {'csrfToken' : 'ajax:5573411342227767805',
                         'session_key': tg.config.get(path='Connector',key='linkedin_email'),
                         'session_password': tg.config.get(path='Connector',key='linkedin_password'),
                         'session_login' : 'Sign In'}

            res= self._getHTML(self.currenturi,data=auth_data)
            self.rawpage = res['result']
            self._setCurrentPage()
            return True
        except:
            log.exception(self.log_msg("Exception occured while authenticating to linkedin"))
            return False

    @logit(log,'__getParentPage')
    def __getParentPage(self):
        try:
             #To sort the results in date descending order
            if not self.category_id:
                sort_url ="http://www.linkedin.com/searchAnswers?runSearch=&keywords=%s&searchScope=&questionStatus=all&sortType=dat"%(quote_plus(self.keyword_term))
            else:
                sort_url ="http://www.linkedin.com/searchAnswers?runSearch=&keywords=%s&searchScope=&questionStatus=all&sortType=dat&categoryID=%s"%(quote_plus(self.keyword_term),self.category_id)
            res=self._getHTML(sort_url)
            if res:
                self.rawpage=res['result']
            self._setCurrentPage()

            page={}
            page['title']=self.task.instance_data['uri']
            page['data'] = ''
            try:
                post_hash= get_hash(page)
            except:
                log.info(self.log_msg("Error Occured while making parent post hash, Not fetching the parent page data"))
                return False
            if not checkSessionInfo(self.genre, self.session_info_out,
                                    self.keyword_term + str(self.category_id), self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug(id)
                result=updateSessionInfo(self.genre, self.session_info_out, self.keyword_term + str(self.category_id), post_hash,
                                         'Post', self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    page['path'] = [self.keyword_term + str(self.category_id)]
                    page['parent_path'] = []
                    page['task_log_id']=self.task.id
                    page['versioned']=self.task.instance_data.get('versioned',False)
                    page['category']=self.task.instance_data.get('category','generic')
                    page['last_updated_time']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['client_name']=self.task.client_name
                    page['entity']='post'
                    page['uri'] = normalize(self.currenturi)
                    page['uri_domain'] = unicode(urlparse(self.currenturi)[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    self.pages.append(page)
                    log.debug(self.log_msg("Main page details stored"))
                else:
                    log.debug(self.log_msg("Main page details NOT stored"))
            else:
                log.debug(self.log_msg("Main page details NOT stored"))
            return True
        except:
            log.exception(self.log_msg("Exception occured in __getParentPage()"))
            return False

    @logit(log,'_iterateSearchPages')
    def __iterateSearchPages(self):
        """
        Fetches links from the search result and populates self.url_list
        """
        try:
            while True:
                print self.currenturi
                self.url_list.extend(["http://www.linkedin.com%s"%(each['href']) for each in self.soup.findAll('a',attrs={'title':'View question details'})])
                try:
                    next_url = "%s"%(self.soup.find('a',attrs={'name':'_next'})['href'])
                    if next_url.startswith('/'):
                        next_url = "http://www.linkedin.com%s"%(next_url)
                    print next_url
                    res=self._getHTML(next_url)
                    if res:
                        self.rawpage=res['result']
                        self._setCurrentPage()
                    else:
                        log.debug(self.log_msg("Next page not set, not picking up further link"))
                        break
                except:
                    log.info(self.log_msg("Picked all question links"))
                    break
        except:
            log.info(self.log_msg("Exception occured in _iterateSearchPages."))
            return False

    @logit (log,'__getQuestionData')
    def __getQuestionData(self,parent_uri):
        try:
            page = {}
            try:
                page['title'] = stripHtml(self.soup.find('div',attrs={'class':re.compile('questionbox.*')}).find('div',attrs={'class':'question'}).find('div',attrs={'class':'details-in'}).find('h1').renderContents().strip())
            except:
                log.info(self.log_msg("Exception occured while fetching question title"))
                return False
            try:
                page['data']= stripHtml( self.soup.find('div',attrs={'class':re.compile('questionbox.*')}).find('div',attrs={'class':'question'}).find('div',attrs={'class':'details-in'}).find('p').renderContents())
            except:
                page['data']=''
                log.exception(self.log_msg("Exception occured while fetching review data"))
            try:
                author_soup = self.soup.find('div',attrs={'class':re.compile('questionbox.*')}).find('div',attrs={'class':'who-in'})
                try:
                    page['et_author_profile'] = "http://www.linkedin.com" + author_soup.find('a')['href']
                except:
                    log.info(self.log_msg("Error occurered while fetching author profile link"))

                try:
                    page['et_author_name'] = stripHtml(author_soup.find('a').renderContents().strip())
                except:
                    log.info(self.log_msg("Error occurered while fetching author name"))

                try:
                    page['et_author_designation']= stripHtml(author_soup.find('p',attrs={'class':'title'}).renderContents().strip())
                except:
                    log.info(self.log_msg("Error occured while fetching author designation"))
            except:
                log.info(self.log_msg("Could not get author information"))
            try:
                question_hash =  get_hash(page)
            except:
                log.debug(self.log_msg("Error in creating search result hash, moving to next search result"))
                return False
            if not checkSessionInfo(self.genre, self.session_info_out,
                                    question_hash, self.task.instance_data.get('update'),
                                    parent_list=[parent_uri]):
                result=updateSessionInfo(self.genre, self.session_info_out, question_hash, question_hash,
                                             'Question', self.task.instance_data.get('update'), parent_list=[parent_uri])
                if result['updated']:
                    try:
                        posted_date_text = stripHtml(self.soup.find('p',attrs={'id':'qmeta'}).renderContents())
                        try:
                            match1 = re.findall("posted\s(\d+)\s(\w+)\s",posted_date_text)[0]
                            days = None
                            hours = None
                            if match1[0] == "days":
                                days = int(match1[1])
                            elif match1[0] == "hours":
                                hours = int(match1[1])
                            if days:
                                page['posted_date'] = datetime.strftime(datetime.utcnow()-timedelta(days=days),"%Y-%m-%dT%H:%M:%SZ")
                            elif hours:
                                page['posted_date'] = datetime.strftime(datetime.utcnow()-timedelta(hours=hours),"%Y-%m-%dT%H:%M:%SZ")
                            else:
                                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        except:
                            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        try:
                            match2= re.findall("posted\s(\w+)\s(\d+),\s(\d{4})\s",posted_date)[0]
                            page['posted_date'] = datetime.strftime(datetime.strptime(match2[0]+"-"+match2[1]+"-"+match2[2],"%B-%d-%Y"),"%Y-%m-%dT%H:%M:%SZ")
                        except:
                            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")

                    page['uri']=self.currenturi
                    page['path'] = [parent_uri]
                    page['parent_path'] = [parent_uri,question_hash]
                    page['task_log_id']=self.task.id
                    page['versioned']=self.task.instance_data.get('versioned',False)
                    page['category']=self.task.instance_data.get('category','generic')
                    page['last_updated_time']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['client_name']=self.task.client_name
                    page['entity']='comment'
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    self.pages.append(page)
                    log.info(page)
                else:
                    log.debug(self.log_msg("NOT appending question %s" %self.currenturi))
            else:
                log.debug(self.log_msg("NOT appending question %s" %self.currenturi))

            while True:
                for answer in self.soup.find('div',attrs={'class':'answersbox'}).findAll('div',attrs={'class':re.compile('answer.*')}):
                    self.answer = answer
                    self.__getAnswerData([self.keyword_term + str(self.category_id),question_hash])
                try:
                    next_url = "https://www.linkedin.com%s"%(self.soup.find('a',attrs={'name':'_next'})['href'])
                    res=self._getHTML(next_url)
                    if res:
                        self.rawpage=res['result']
                        self._setCurrentPage()
                    else:
                        break
                except:
                    log.info(self.log_msg("Fetched all the answers"))
                    break
            return True
        except:
            log.exception(self.log_msg("Exception occured in __getQuestionData() for question %s" %(self.currenturi)))
            return False

    @logit (log,'__getAnswerData')
    def __getAnswerData(self,parent_list):
        try:
            answer_id = dict(cgi.parse_qsl(self.answer.find('p',attrs={'class':'meta'}).find('a')['href']))['answerID']
            if not checkSessionInfo(self.genre, self.session_info_out,
                                    answer_id, self.task.instance_data.get('update'),
                                    parent_list=parent_list):
                page={}
                try:
                    author_soup = self.answer.find('div',attrs={'class':'who-in'})
                    try:
                        page['et_author_profile'] = "http://www.linkedin.com" + author_soup.find('a')['href']
                    except:
                        log.info(self.log_msg("Error occurered while fetching author profile link"))

                    try:
                        page['et_author_name'] = stripHtml(author_soup.find('a').renderContents().strip())
                    except:
                        log.info(self.log_msg("Error occurered while fetching author name"))

                    try:
                        page['et_author_designation']= stripHtml(author_soup.find('p',attrs={'class':'title'}).renderContents().strip())
                    except:
                        log.info(self.log_msg("Error occured while fetching author designation"))
                except:
                    log.info(self.log_msg("Could not get author information"))
                try:
                    posted_date_text = stripHtml(self.answer.find('p',attrs={'id':'meta'}).renderContents())
                    try:
                        match1 = re.findall("posted\s(\d+)\s(\w+)\s",posted_date_text)[0]
                        days = None
                        hours = None
                        if match1[0] == "days":
                            days = int(match1[1])
                        elif match1[0] == "hours":
                            hours = int(match1[1])
                        if days:
                            page['posted_date'] = datetime.strftime(datetime.utcnow()-timedelta(days=days),"%Y-%m-%dT%H:%M:%SZ")
                        elif hours:
                            page['posted_date'] = datetime.strftime(datetime.utcnow()-timedelta(hours=hours),"%Y-%m-%dT%H:%M:%SZ")
                        else:
                            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    try:
                        match2= re.findall("posted\s(\w+)\s(\d+),\s(\d{4})\s",posted_date_text)[0]
                        page['posted_date'] = datetime.strftime(datetime.strptime(match2[0]+"-"+match2[1]+"-"+match2[2],"%B-%d-%Y"),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                except:
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                try:
                    try:
                        self.answer.find('div',attrs={'class':'who-in'}).extract()
                    except:
                        log.info(self.log_msg("vcard not found"))
                    try:
                        self.answer.find('p',attrs={'class':'rating'}).extract()
                    except:
                        log.info(self.log_msg("answer rating not found"))

                    page['data']= stripHtml(self.answer.find('p').renderContents())
                except:
                    page['data']=''
                    log.exception(self.log_msg("Exception occured while fetching review data"))

                try:
                    page['title'] = "Answer: %s"%(stripHtml(self.soup.find('div',attrs={'class':re.compile('questionbox.*')}).find('div',attrs={'class':'question'}).find('div',attrs={'class':'details-in'}).find('h1').renderContents().strip()))
                except:
                    log.info(self.log_msg("Exception occured while fetching answer title"))
                    page['title']=''
                try:
                    answer_hash =  get_hash(page)
                except:
                    log.debug(self.log_msg("Error in creating search result hash, moving to next search result"))
                    return False

                result=updateSessionInfo(self.genre, self.session_info_out, answer_id, answer_hash,'Answer', self.task.instance_data.get('update'), parent_list=parent_list)
                if result['updated']:
                    page['path']=page['parent_path'] = parent_list
                    page['path'].append(answer_id)
                    page['uri']=self.currenturi
                    page['task_log_id']=self.task.id
                    page['versioned']=self.task.instance_data.get('versioned',False)
                    page['category']=self.task.instance_data.get('category','generic')
                    page['last_updated_time']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['client_name']=self.task.client_name
                    page['entity']='comment'
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    self.pages.append(page)
                    log.info(page)
                    return True
                else:
                    log.debug(self.log_msg("NOT appending answer %s" %answer_id))
                    return False
            else:
                log.debug(self.log_msg("NOT appending answer %s" %answer_id))
                return False
        except:
            log.exception(self.log_msg("Exception occured in __getAnswerData() for answer %s" %(answer_id)))
            return False

    @logit (log,'__fetchGroupDetails')
    def __fetchGroupDetails(self):
        '''this will fetch the infor from linkedin Groups
        sample URL: http://www.linkedin.com/groups?gid=2117529
        '''
        try:
            self.__getParentPageForGroups()
            self.topic_links = []
            self.__getTopicLinksForGroups()
            self.__getTopicDetails()
        except:
            log.info(self.log_msg('No Groups were found'))

    @logit (log,'__getParentPageForGroups')
    def __getParentPageForGroups(self):
        try:
            page = {}
            self.group_name = page['title']=  stripHtml(self.soup.find('h1','page-title').findAll('a')[-1].renderContents())
            page['et_data_group_name'] = page['title']
            page['data'] = ''
            post_hash= get_hash(page)
            if checkSessionInfo(self.genre, self.session_info_out,
                                    self.group_id, self.task.instance_data.get('update')):
                log.info(self.log_msg('Check session info returns True'))
                return False
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo(self.genre, self.session_info_out, self.group_id, post_hash,
                                     'Post', self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                log.info(self.log_msg('Update session info returns True'))
            page['path'] = [ self.group_id ]
            page['parent_path'] = []
            page['task_log_id']=self.task.id
            page['versioned']=self.task.instance_data.get('versioned',False)
            page['category']=self.task.instance_data.get('category','generic')
            page['last_updated_time']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['client_name']=self.task.client_name
            page['entity']='post'
            page['uri'] = normalize(self.currenturi)
            page['uri_domain'] = unicode(urlparse(self.currenturi)[1])
            page['priority']=self.task.priority
            page['level']=self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            self.pages.append(page)
            log.debug(self.log_msg("Main page details stored"))
            return True
        except:
            log.exception(self.log_msg("Exception occured in __getParentPage()"))
            return False

    @logit (log,'__getTopicLinksForGroups')
    def __getTopicLinksForGroups(self):
        '''this will fetch topic links and add in the self variable
        '''
        try:
            self.topic_links = []
            while True:
                self.topic_links.extend(['http://www.linkedin.com'+x.find('h3').find('a')['href'] for x in self.soup.find('div','contain').findAll('div','content')])
                try:
                    next_url = "%s"%(self.soup.find('a',attrs={'name':'_next'})['href'])
                    if next_url.startswith('/'):
                        next_url = "http://www.linkedin.com%s"%(next_url)
                    print next_url
                    res=self._getHTML(next_url)
                    if res:
                        self.rawpage=res['result']
                        self._setCurrentPage()
                    else:
                        log.debug(self.log_msg("Next page not set, not picking up further link"))
                        break
                except:
                    log.info(self.log_msg("Picked all Group links"))
                    break
        except:
            log.info(self.log_msg("Exception occured in _iterateSearchPages."))
            return False

    @logit (log,'__getTopicDetails')
    def __getTopicDetails(self):
        '''This will fetch the Topic and its comments
        '''
        for topic_link in self.topic_links:
            self.currenturi = topic_link
            try:
                res=self._getHTML()
                self.rawpage=res['result']
                self._setCurrentPage()
                self.__getQuestionDataForGroups()
            except:
                log.exception(self.log_msg('Soup not set , cannot go ahead'))

    @logit (log,'__getTopicDetails')
    def __getQuestionDataForGroups(self):
        '''This will fetch the Question Data for Groups
        '''
        page = {}
        question = self.soup.find('div','question')
        try:
            page['et_data_group_name'] = self.group_name
            page['et_data_discussion_title'] = discussion_title = page['title'] = stripHtml(question.find('h1').renderContents())
            page['data']  = stripHtml(question.find('p','q-details').renderContents())
            discussion_id = page['et_data_discussion_id'] = dict(cgi.parse_qsl(self.currenturi.split('?')[-1])).get('discussionID')
        except:
            log.exception(self.log_msg("Exception occured while fetching question title"))
            return False
        """try:
            author_soup = question.find('div','who-in').find('div','contents')
            aut_name_tag = author_soup.find('a')
            page['et_author_profile'] = "http://www.linkedin.com" + str(aut_name_tag['href'])
            page['et_author_name'] = stripHtml(aut_name_tag.renderContents())
            page['et_author_designation']= stripHtml(author_soup.find('p',attrs={'class':'title'}).renderContents())
        except:
            log.info(self.log_msg("Could not get author information"))"""
        try:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            posted_date = stripHtml(question.find('p','q-timestamp').renderContents())
            try:
                match1 = re.findall("posted\s(\d+)\s(\w+)\s",posted_date,re.I)[0]
                days = None
                hours = None
                if match1[1] == "days":
                    days = int(match1[0])
                elif match1[1] == "hours":
                    hours = int(match1[0])
                elif match1[1].startswith('month'):
                    days = int(match1[0]) * 30
                if days:
                    page['posted_date'] = datetime.strftime(datetime.utcnow()-timedelta(days=days),"%Y-%m-%dT%H:%M:%SZ")
                elif hours:
                    page['posted_date'] = datetime.strftime(datetime.utcnow()-timedelta(hours=hours),"%Y-%m-%dT%H:%M:%SZ")
            except:
                try:
                    match2= re.findall("posted\s(\w+)\s(\d+),\s(\d{4})\s",posted_date)[0]
                    page['posted_date'] = datetime.strftime(datetime.strptime(match2[0]+"-"+match2[1]+"-"+match2[2],"%B-%d-%Y"),"%Y-%m-%dT%H:%M:%SZ")
                except:
                    pass
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            question_hash = get_hash( {'data':page['data'],'title':page['title']} )
        except:
            log.debug(self.log_msg("Error in creating search result hash, moving to next search result"))
            return False
        if not checkSessionInfo(self.genre, self.session_info_out,
                                question_hash, self.task.instance_data.get('update'),
                                parent_list=[self.group_id]):
            result=updateSessionInfo(self.genre, self.session_info_out, question_hash, get_hash(page),
                                         'Question', self.task.instance_data.get('update'), parent_list=[self.group_id])
            if result['updated']:
                page['uri']=self.currenturi
                page['path'] = [self.group_id , question_hash ]
                page['parent_path'] = [ self.group_id ]
                page['task_log_id']=self.task.id
                page['versioned']=self.task.instance_data.get('versioned',False)
                page['category']=self.task.instance_data.get('category','generic')
                page['last_updated_time']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['client_name']=self.task.client_name
                page['entity']='Post'
                page['uri_domain'] = urlparse(page['uri'])[1]
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                self.pages.append(page)
                log.info(self.log_msg('Question data added'))
            else:
                log.debug(self.log_msg("NOT appending question %s" %self.currenturi))
        else:
            log.debug(self.log_msg("NOT appending question %s" %self.currenturi))
        # for Getting Replies for Question in Groups
        while True:
            try:
                for answer in self.soup.find('ol','a-list').findAll('li','comment '):
                    self.__getReplyDataForGroups(answer,question_hash,discussion_title,discussion_id)
                next_url = "https://www.linkedin.com%s"%(self.soup.find('a',attrs={'name':'_next'})['href'])
                res=self._getHTML(next_url)
                if res:
                    self.rawpage=res['result']
                    self._setCurrentPage()
                else:
                    break
            except:
                log.info(self.log_msg("Fetched all the answers"))
                break
        return True

    @logit (log,'__getReplyDataForGroups')
    def __getReplyDataForGroups(self,answer,question_hash,discussion_title='',discussion_id=''):
        page={}
        try:
            answer_id = answer.find('div','answer')['id']
            if checkSessionInfo(self.genre, self.session_info_out,answer_id, self.task.instance_data.get('update'),
                                                                        parent_list=[self.group_id,question_hash]):
                log.info(self.log_msg('check session info returns True'))
                return False
        except:
            log.info(self.log_msg('exception in check session info '))
            return False
        """try:
            author_soup = answer.find('div','who-in').find('div','contents')
            aut_name_tag = author_soup.find('a')
            page['et_author_profile'] = "http://www.linkedin.com" + aut_name_tag['href']
            page['et_author_name'] = stripHtml(aut_name_tag.renderContents())
            page['et_author_designation']= stripHtml(author_soup.find('p',attrs={'class':'title'}).renderContents())
        except:
            log.info(self.log_msg("Could not get author information"))"""
        try:
            page['et_data_discussion_title'] = discussion_title
            page['et_data_discussion_id'] = discussion_id
            page['et_data_group_name'] = self.group_name
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            posted_date = stripHtml(answer.find('p','timestamp').renderContents())
            try:
                match1 = re.findall("posted\s(\d+)\s(\w+)\s",posted_date,re.I)[0]
                days = None
                hours = None
                if match1[1] == "days":
                    days = int(match1[0])
                elif match1[1] == "hours":
                    hours = int(match1[0])
                elif match1[1].startswith('month'):
                    days = int(match1[0]) * 30
                if days:
                    page['posted_date'] = datetime.strftime(datetime.utcnow()-timedelta(days=days),"%Y-%m-%dT%H:%M:%SZ")
                elif hours:
                    page['posted_date'] = datetime.strftime(datetime.utcnow()-timedelta(hours=hours),"%Y-%m-%dT%H:%M:%SZ")
            except:
                try:
                    match2= re.findall("posted\s(\w+)\s(\d+),\s(\d{4})\s",posted_date)[0]
                    page['posted_date'] = datetime.strftime(datetime.strptime(match2[0]+"-"+match2[1]+"-"+match2[2],"%B-%d-%Y"),"%Y-%m-%dT%H:%M:%SZ")
                except:
                    pass
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['data'] = stripHtml(answer.find('div','answer').find('h3').renderContents())
            if len(page['data']) > 50:
                page['title'] = page['data'][:50] + '...'
            else:
                page['title'] = page['data']
        except:
            log.exception(self.log_msg("Exception occured while fetching review data"))
            return False
        try:
            answer_hash =  get_hash(page)
            result=updateSessionInfo(self.genre, self.session_info_out, answer_id, answer_hash,'Answer', self.task.instance_data.get('update'), parent_list=[self.group_id,question_hash])
            if not result['updated']:
                log.info(self.log_msg ('Update session infor returns False'))
            page['path']=page['parent_path'] = [ self.group_id, question_hash ]
            page['path'].append(answer_id)
            page['uri']=self.currenturi
            page['task_log_id']=self.task.id
            page['versioned']=self.task.instance_data.get('versioned',False)
            page['category']=self.task.instance_data.get('category','generic')
            page['last_updated_time']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['client_name']=self.task.client_name
            page['entity']='comment'
            page['uri_domain'] = urlparse(page['uri'])[1]
            page['priority']=self.task.priority
            page['level']=self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            self.pages.append(page)
            return True
        except:
            log.debug(self.log_msg("while adding the answer data"))
            return False