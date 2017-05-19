
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#Mohit Ranka

import re
from datetime import datetime
import logging
import urlparse

from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash

from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('TekTipsConnector')
class TekTipsConnector(BaseConnector):

    @logit(log , 'fetch')
    def fetch(self):
        self.genre="review"
        try:
            parent_uri = self.task.instance_data['uri']
            res=self._getHTML()
            if res:
                self.rawpage=res['result']
                self._setCurrentPage()
                if '/threadminder.cfm?pid=' in self.task.instance_data['uri']:
                    self.__addThreadUrls()
                elif '/viewthread.cfm?qid=' in self.task.instance_data['uri']:
                    self.on_question_page = True
                    self.__getQuestion()
                    self.__getAnswers()
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
        count = 0
        thread_count = tg.config.get(path='Connector',key='tektips_num_threads')
        done = False
        try:
            while not done:
                thread_urls =  ["http://%s%s"%(urlparse.urlparse(self.currenturi)[1],link['href'])\
                                    for link in self.soup.findAll('a')\
                                    if 'viewthread' in link['href']]
                for url in thread_urls:
                    if count < thread_count and not done:
                        count = count + 1
                        temp_task = self.task.clone()
                        temp_task.instance_data['uri'] = url
                        log.info(self.log_msg('url : %s'%temp_task.instance_data['uri']))
                        self.linksOut.append(temp_task)
                    else:
                        done = True
                        break
                if not done:
                    try:
                        self.currenturi = "http://%s%s"%(urlparse.urlparse(self.task.instance_data['uri'])[1],\
                                                             [link['href'] for link in self.soup.findAll('a',attrs={'title':True})\
                                                                  if link['title']=="Next Page"][0])
                        res=self._getHTML(self.currenturi)
                        if res:
                            self.rawpage=res['result']
                            self._setCurrentPage()
                        else:
                            done = True
                            break
                    except:
                        done = True
                        break
            log.info(self.log_msg("%d tasks appeneded"%len(self.linksOut)))
            return True
        except:
            log.exception(self.log_msg("Exception occured while creating tasks from the thread urls"))
            return False

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
                question_soup = self.soup.find('table',attrs={'class':'post'})
                try:
                    page['data'] =  stripHtml(question_soup.find('div',attrs={'class':'wordwrap'}).renderContents())
                except Exception, e:
                    page['data']=''
                    log.exception(self.log_msg('could not parse post data'))
                try:
                    page['title'] =  stripHtml(self.soup.find('div',attrs={'class':'title'}).find('h1').renderContents())
                except Exception, e:
                    page['title'] =page['data'][:50]
                    log.exception(self.log_msg('could not parse page title'))

                try:
                    page['et_thread_hierarchy'] =  [stripHtml(each.renderContents()) \
                                                        for each in self.soup.find('div',attrs={'class':'bcrumb'}).findAll('a')]
                except:
                    log.info(self.log_msg('could not parse thread hierarchy'))
                
                try:
                    page['et_author_name'] =  stripHtml(question_soup.find('td',attrs={'class':'handle'}).find('a').renderContents())
                except:
                    log.info(self.log_msg('could not parse author name'))
                try:
                    page['et_author_profile'] = urlparse.urlparse(self.currenturi)[1] +\
                        question_soup.find('td',attrs={'class':'handle'}).find('a')['href']
                except:
                    log.info(self.log_msg('could not parse profile link'))
                try:
                    post_hash = get_hash(page)
                except Exception,e:
                    log.exception(self.log_msg('could not build post_hash'))
                page['et_data_post_type']='Question'
                result=updateSessionInfo(self.genre,self.session_info_out,self.task.instance_data['uri'], post_hash, 
                                         'Question', self.task.instance_data.get('update'),Id=id)
                if result['updated']:
                    page['path'] = page['parent_path'] = []
                    page['path'].append(self.task.instance_data['uri'])
                    page['uri'] = normalize(self.currenturi)
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    try:
                        posted_date_text = question_soup.find('td',attrs={'class':'date'}).\
                            find('div',attrs={'title':True}).renderContents().strip()
                        posted_date = datetime.strptime(posted_date_text,"%d %b %y %H:%M")
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
            self.fetch_next_message = True
            answers_soup = self.soup.findAll('table',attrs={'class':'post'})[1:]
            for answer_soup in answers_soup:
                if not self.fetch_next_message:
                    return True
                page={}
                try:
                    page['data'] =  stripHtml(answer_soup.find('div',attrs={'class':'wordwrap'}).renderContents())
                except Exception, e:
                    page['data']=''
                    log.exception(self.log_msg('could not parse post data'))
                page['title'] = ''
                try:
                    page['et_author_name'] =  stripHtml(answer_soup.find('td',attrs={'class':'handle'}).find('a').renderContents())
                except:
                    log.info(self.log_msg('could not parse author name'))
                try:
                    page['et_author_profile'] =  urlparse.urlparse(self.currenturi)[1] +\
                        answer_soup.find('td',attrs={'class':'handle'}).find('a')['href']
                except:
                    log.exception(self.log_msg('could not parse profile link'))
                page['et_data_post_type']='Suggestion'
                try:
                    post_hash = get_hash(page)
                except Exception,e:
                        log.exception(self.log_msg('could not build post_hash'))
                        raise e
                if not checkSessionInfo(self.genre, self.session_info_out, 
                                        post_hash, self.task.instance_data.get('update'),parent_list=[self.task.instance_data['uri']]):
                    result=updateSessionInfo(self.genre,self.session_info_out,post_hash, post_hash, 
                                             'Answer', self.task.instance_data.get('update'), parent_list =[self.task.instance_data['uri']])
                    if result['updated']:
                        page['path'] = page['parent_path'] = [self.task.instance_data['uri']]
                        page['path'].append(post_hash)
                        page['uri'] = normalize(self.currenturi)
                        page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        try:
                            posted_date_text = answer_soup.find('td',attrs={'class':'date'}).\
                                find('div',attrs={'title':True}).renderContents().strip()
                            posted_date = datetime.strptime(posted_date_text,"%d %b %y %H:%M")
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
                        log.debug(self.log_msg("Answer added to self.pages"))
                    else:
                        if not self.task.instance_data.get('update'):
                            self.fetch_next_message = False
                        log.debug(self.log_msg("Not appending to self.pages"))
                else:
                    if not self.task.instance_data.get('update'):
                        self.fetch_next_message = False
                    log.debug(self.log_msg("Not appending to self.pages"))
        except Exception,e:
            log.exception(self.log_msg("posts could not be extracted"))
            return False
