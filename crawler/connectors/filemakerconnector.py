
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#Mohit Ranka

import re
import copy
from datetime import datetime
import logging
import urlparse

from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash

from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('FileMakerConnector')
class FileMakerConnector(BaseConnector):

    @logit(log , 'fetch')
    def fetch(self):                                                                                         
        
        self.genre="review"
        try:
            parent_uri = self.task.instance_data['uri']
            res=self._getHTML()
            if res:
                self.rawpage=res['result']
                self._setCurrentPage()
                if '/board?board.id' in self.task.instance_data['uri']:
                    self.__addThreadUrls()
                elif '/message?board.id' in self.task.instance_data['uri']:
                    self.on_question_page = True
                    self.__getQuestion()
                    while True:
                        try:
                            self.__getAnswers()
                            self.currenturi = [urlparse.urlparse(self.task.instance_data['uri'])[1]\
                                                   +each['href'] for each in self.soup.findAll('a')\
                                                   if each.renderContents()=="Next Page"][0]
                            res=self._getHTML()
                            if res:
                                self.rawpage=res['result']
                                self._setCurrentPage()
                            else:
                                break
                        except:
                            break
                else:
                    log.info(self.log_msg("Unexpected url %s"%(self.task.instance_data['uri'])))
            else:
                log.debug(self.log_msg("Could not set the HTML page for %s"%(self.currenturi)))
            return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False
    
    @logit(log , '__addThreadUrls')
    def __addThreadUrls(self):
        try:
            thread_urls =  ["http://%s%s"%(urlparse.urlparse(self.currenturi)[1],link['href']) for link in self.soup.findAll('a',attrs={'class':'subj_unread'})]
            for url in thread_urls:
                temp_task = self.task.clone()
                temp_task.instance_data['uri'] = url
                log.info(self.log_msg('url : %s'%temp_task.instance_data['uri']))
                self.linksOut.append(temp_task)
            log.info(self.log_msg("%d tasks appened %s"%len(self.linksOut)))
            return True
        except:
            log.info(self.log_msg("Exception occured while creating tasks from the thread urls"))

    @logit(log , '__getQuestion')
    def __getQuestion(self):
        try:
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    self.task.instance_data['uri'], self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug(self.log_msg('checking session info'))
                page={}
                question_soup = self.soup.find('tr',attrs={'id':True})
                try:
                    page['data'] = "\n".join([stripHtml(each.renderContents())\
                                                  for each in question_soup.find('td',attrs={'class':'msg_text_cell'}).findAll('p')])
                except Exception, e:
                    page['data']=''
                    log.exception(self.log_msg('could not parse post data'))
                try:
                    page['title'] =  stripHtml(question_soup.findAll('td',attrs={'class':'subjectbar'})[1].renderContents())
                except Exception, e:
                    page['title'] =page['data'][:50]
                    log.exception(self.log_msg('could not parse page title'))

                try:
                    page['et_thread_hierarchy'] =  [stripHtml(each.renderContents())\
                                                        for self.each in self.soup.findAll(attrs={'class':re.compile('navbar_[^sep]')})]
                except:
                    log.info(self.log_msg('could not parse thread hierarchy'))
                
                try:
                    page['et_author_name'] =  stripHtml(question_soup.find('a',attrs={'class':'auth_text'}).renderContents())
                except:
                    log.info(self.log_msg('could not parse author name'))
                try:
                    page['et_author_profile'] = urlparse.urlparse(self.currenturi)[1] + question_soup.find('a',attrs={'class':'auth_text'})['href']
                except:
                    log.info(self.log_msg('could not parse profile link'))
                try:
                    post_hash = get_hash(page)
                except Exception,e:
                    log.exception(self.log_msg('could not build post_hash'))
                page['et_data_post_type']='Question'
                result=updateSessionInfo(self.genre,self.session_info_out,self.task.instance_data['uri'], post_hash, 
                                         'post', self.task.instance_data.get('update'),Id=id)
                if result['updated']:
                    page['path']=[self.task.instance_data['uri']]
                    page['parent_path']=[]
                    page['uri'] = normalize(self.currenturi)
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    try:
                        date_text = question_soup.find('span',attrs={'class':'date_text'}).renderContents()
                        time_text = question_soup.find('span',attrs={'class':'time_text'}).renderContents()
                        posted_date = datetime.strptime(date_text + "-" + time_text,"%m-%d-%Y-%H:%M %p")
                        page['posted_date'] = datetime.strftime(posted_date,"%Y-%m-%dT%H:%M:%SZ")                          
                    except:
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        log.info(self.log_msg('could not parse posted_date'))

                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['task_log_id']=self.task.id
                    page['entity'] = 'post'
                    page['category']=self.task.instance_data.get('category','')
                    self.pages.append(page)
                    return True
        except Exception,e:
            log.exception(self.log_msg("question post couldn't be parsed"))
            return False

    @logit(log , '__getAnswers')
    def __getAnswers(self):
        try:            
            if self.on_question_page:
                answers_soup = self.soup.findAll('tr',attrs={'id':True})[1:]
                self.on_question_page = False
            else:
                answers_soup = self.soup.findAll('tr',attrs={'id':True})
            for answer_soup in answers_soup:
                answers_id = answer_soup['id']
                log.debug(self.log_msg('checking session info'))
                if not checkSessionInfo(self.genre, self.session_info_out, 
                                        answers_id, self.task.instance_data.get('update'),parent_list=[self.task.instance_data['uri']]):
                    page={}
                    try:
                        page['data'] = "\n".join([stripHtml(each.renderContents())\
                                                  for each in answer_soup.find('td',attrs={'class':'msg_text_cell'}).findAll('p')])
                    except Exception, e:
                        page['data']=''
                        log.exception(self.log_msg('could not parse post data'))
                    try:
                        page['title'] =  stripHtml(answer_soup.findAll('td',attrs={'class':'subjectbar'})[1].renderContents())
                    except Exception, e:
                        page['title'] =page['data'][:50]
                        log.exception(self.log_msg('could not parse page title'))
                    try:
                        page['et_author_name'] =  stripHtml(answer_soup.find('a',attrs={'class':'auth_text'}).renderContents())
                    except:
                        log.info(self.log_msg('could not parse thread hierarchy'))
                    try:
                        page['et_author_profile'] = urlparse.urlparse(self.currenturi)[1]+ answer_soup.find('a',attrs={'class':'auth_text'})['href']
                    except:
                        log.exception(self.log_msg('could not parse profile link'))
                    page['et_data_post_type']='Suggestion'
                    try:
                        post_hash = get_hash(page)
                    except Exception,e:
                        log.exception(self.log_msg('could not build post_hash'))
                        raise e
                    result=updateSessionInfo(self.genre,self.session_info_out,answers_id, post_hash, 
                                             'comment', self.task.instance_data.get('update'), parent_list =[self.task.instance_data['uri']])
                    if result['updated']:
                        parent_list = [self.task.instance_data['uri']]
                        page['parent_path'] = copy.copy(parent_list)
                        parent_list.append(answers_id)
                        page['path'] = parent_list
                        page['uri'] = normalize(self.currenturi)
                        page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        try:
                            date_text = answer_soup.find('span',attrs={'class':'date_text'}).renderContents()
                            time_text = answer_soup.find('span',attrs={'class':'time_text'}).renderContents()
                            posted_date = datetime.strptime(date_text + "-" + time_text,"%m-%d-%Y-%H:%M %p")
                            page['posted_date'] = datetime.strftime(posted_date,"%Y-%m-%dT%H:%M:%SZ")                          
                        except:
                            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                            log.info(self.log_msg('could not parse posted_date'))
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id
                        page['client_name'] = self.task.client_name
                        page['last_updated_time'] = page['pickup_date']
                        page['versioned'] = False
                        page['task_log_id']=self.task.id
                        page['entity'] = 'comment'
                        page['category']=self.task.instance_data.get('category','')
                        self.pages.append(page)
        except Exception,e:
                log.exception(self.log_msg("posts could not be extracted"))
                return False
