
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''


#ASHISH YADAV


import re
import md5
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import logging
from urllib2 import urlparse,unquote

from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash

from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('DaumKnowledgeConnector')
class DaumKnowledgeConnector(BaseConnector):

    @logit(log , 'fetch')
    def fetch(self):                                                                                         
        self.genre="Review"
        try:
            #for sentiment extraction
            code = None
            parent_uri = self.currenturi
            # for sentiment extraction
            res=self._getHTML()
            review_next_page_list = []
            self.rawpage=res['result']
            self._setCurrentPage()
#            self.currenturi = self.soup.find('frame',{'name':'down','id':'down'})['src']
            self._getParentPage(parent_uri) #checks for parent page ,and appends a empty dictionay or a modified post.
            res=self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            page = {}
            if (not checkSessionInfo(self.genre, self.session_info_out, 
                                     parent_uri, self.task.instance_data.get('update'),
                                     parent_list=[])):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    
                page['uri'] = self.currenturi
                try:
                    page['et_author_name'] = stripHtml(self.soup.find('div',{'class':'writer'}).find('a').renderContents())
                except:
                    log.info('could not parse Author Name')

                try:
                    page['ei_num_page_views'] = int(stripHtml(self.soup.find('div',{'class':'writer'}).find('span',{'class':'skinTxt'})))
                except:
                    log.info('could not parse number of page views')
        
                try:
                    page['data'] = stripHtml(self.soup.find('td',{'id':'user_contents'}).renderContents())
                except:
                    log.info('could not parse page data')
                    
                try:
                    page['title'] = stripHtml(self.soup.find('strong',{'id':'q_title'}).renderContents())
                except:
                    log.info('could not parse number of page views')
                    page['title'] = page['data'][:100]
                    
                try:
                    page['posted_date'] = datetime.strptime(self.soup.find('li',{'class':'skinTxt'}).\
                                                          renderContents().strip(),'%y.%m.%d %H:%M')
                except:
                    log.info('could not parse number of posted_date')

                review_hash = get_hash(page)
                result=updateSessionInfo(self.genre, self.session_info_out, parent_uri, review_hash,
                                         'Review', self.task.instance_data.get('update'),Id=id )
                if result['updated']:
                    page['uri'] = normalize(self.currenturi)
                    page['path'] = [parent_uri]
                    page['parent_path'] = []
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project       
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['task_log_id']=self.task.id
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')                        
                    self.pages.append(page)
            self.addAnswers(parent_uri)
            self.addcomments(parent_uri)
            return True
        except:
            log.exception(self.log_msg('exception in daumknowledgeconnector'))
            return False
            
    @logit(log , '_getParentPage')
    def _getParentPage(self,parent_uri):#NAMING CONVENTION IS WRONG
            ##J- I think these needs to be in a try except- if th title fails or rating fails - coz the html changed---what crash?
            ## a try-except-raise
            try:
                page={}
                try:
                    page['data']= re.sub('\n{2,}','\n',stripHtml(self.soup.find('dd',{'class':'rContent'}).renderContents()))
                except:
                    log.exception(self.log_msg('data could not be parsed'))
                    raise e

                try:
                    page['title'] = stripHtml(self.soup.find('strong',{'id':'q_title'}).renderContents())
                except Exception, e:
                    log.exception(self.log_msg('could not parse page title'))
                    raise e

                try:
                    page['et_author_name'] = stripHtml(self.soup.find('p',{'class':'nickArea'}).a.renderContents())
                except:
                    log.info('could not parse author name')

                try:
                    page['ei_num_views'] = int(self.soup.find('span',{'id':'viewCount'}).renderContents())
                except Exception, e:
                    log.info(self.log_msg('could not parse number of views'))

                try:
                    page['ei_num_answers'] = int(soup.find('span',{'class':re.compile('bR11_[0-9]+')}).renderContents())
                except Exception, e:
                    log.info(self.log_msg('could not parse number of answers'))

                
                try:
                    post_hash = get_hash(page)
                except Exception,e:
                    log.exception(self.log_msg('could not build post_hash'))
                    raise e

                log.debug(self.log_msg('checking session info'))

                if not checkSessionInfo(self.genre, self.session_info_out, 
                                        parent_uri, self.task.instance_data.get('update')):
                    id=None
                    if self.session_info_out=={}:
                        id=self.task.id
                    result=updateSessionInfo(self.genre, self.session_info_out,parent_uri, post_hash, 
                                             'Post', self.task.instance_data.get('update'), Id=id)
                    if result['updated']:
                        page['uri'] = normalize(self.currenturi)
                        page['path'] = [parent_uri]
                        page['parent_path'] = []
                        page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        try:
                            posted_date = stripHtml(self.soup.find('span',{'class':'bG11'}).renderContents())
                            posted_date = datetime.strptime(posted_date,"%Y-%m-%d %H:%M")
                            page['posted_date'] = datetime.strftime(posted_date,"%Y-%m-%dT%H:%M:%SZ")
                        except:
                            log.info(self.log_msg('could not parse posted_date'))
                            page['pickup_date'] = datetime.strftime(posted_date,"%Y-%m-%dT%H:%M:%SZ")
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id  # TODO: Get the client from the project       
                        page['client_name'] = self.task.client_name
                        page['last_updated_time'] = page['pickup_date']
                        page['versioned'] = False
                        page['entity'] = 'Question'
                        page['category']=self.task.instance_data.get('category','')                        
                        self.pages.append(page)
            except Exception,e:
                log.exception(self.log_msg("parent post couldn't be parsed"))
                raise e

    @logit(log,'addAnswers')
    def addAnswers(self,parenturi):
        self.genre = 'Generic'
        answers = self.soup.findAll('div',{'id':re.compile('answer_.*')})
        for answer in answers:
            page = {}
            try:
                posted_date = stripHtml(answer.find('div',{'class':'nickArea'}).span.renderContents()).strip('| ')
                posted_date = datetime.strptime(posted_date,'%Y-%m-%d %H:%M')
                page['posted_date'] = datetime.strftime(posted_date,"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.info('could not parse posted_date')
                continue

            if (not checkSessionInfo(self.genre, self.session_info_out, 
                                     posted_date, self.task.instance_data.get('update'),
                                     parent_list=[parenturi]) ):
                
                if self.session_info_out=={}:
                    id=self.task.id
                    

                try:
                    page['data'] = stripHtml(self.soup.find('div',{'id':re.compile('answer_*')}).\
                                                 find('td',{'class':'tx-content-container'}).renderContents())
                except:
                    log.info('could not parse answer data')

                if len(page['data']) > 80:
                    page['title'] = page['data'][:80] + '...'
                else:
                    page['title'] = page['data']

                try:
                    page['et_author_name'] = stripHtml(answer.find('div',{'class':'nickArea'}).a.renderContents())
                except:
                    log.info('could not parse author name')

                try:
                    page['ei_num_scraps'] = int(stripHtml(answer.find('a',{'class':re.compile('scrap .*')}).\
                                                              parent.find('strong',{'class':True}).renderContents() ))
                except:
                    log.info('could not parse number of scraps')

                try:
                    page['ei_num_recommendation'] = int(answer.find('span',text=u'\ucd94\ucc9c').parent.\
                                                            parent.find('strong').renderContents())
                except:
                    log.info('could not parse number of recommendation')

                try:
                    answer_hash = get_hash(page)
                except:
                    log.exception(self.log_msg("exception in buidling answer_hash , moving onto next comment"))
                    continue
                
                parent_list=[parenturi]
                result=updateSessionInfo(self.genre, self.session_info_out, posted_date, answer_hash, 
                                         'Generic', self.task.instance_data.get('update'), parent_list=parent_list)
                if result['updated']:
                    page['uri'] = self.currenturi
                    page['parent_path'] = parent_list[:]
                    parent_list.append(posted_date)
                    page['path'] = parent_list
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project 
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['entity'] = 'Answer'
                    page['category'] = self.task.instance_data.get('category','')
                    page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                    self.pages.append(page)
        return True



    #addReviews
    @logit(log , 'addreviews')
    def addcomments(self, parenturi):
        comments = self.soup.findAll('div',{'id':re.compile('comment_.*'),'class':'txt'})
        log.info(self.log_msg('no. of comments found on page %d'%(len(comments))))
        for comment in comments:
            try:
                page ={}
                url = parenturi
                ##ODD JV
                #for cases in which review has been deleted , no permalink is present hence we skip for 
                 #that review,same applies for comments
                page['uri'] = normalize(url)
                comment_id = comment['id']
                if (not checkSessionInfo(self.genre, self.session_info_out, 
                                         comment_id, self.task.instance_data.get('update'),
                                         parent_list=[parenturi]) ):

                    if self.session_info_out=={}:
                        id=self.task.id

                    try:
                        page['data'] = stripHtml(comment.renderContents())
                        page['title'] = page['data'][:80]   
                    except:
                        log.info(self.log_msg("could not get data of the comment "))
                        continue

                    try:
                        page['et_author_name'] =  stripHtml(comment.parent.find('div',{'class':'nick'}).a.renderContents())
                    except:
                        log.info(self.log_msg("review author name couldn't be extracted"))

                    try:
                        comment_hash = get_hash(page)
                    except:
                        log.exception(self.log_msg("exception in buidling review_hash , moving onto next comment"))
                        continue
                    parent_list=[parenturi]
                    result=updateSessionInfo(self.genre, self.session_info_out, comment_id, comment_hash, 
                                             'Review', self.task.instance_data.get('update'), parent_list=parent_list)
                    if result['updated']:
                        page['uri'] = self.currenturi
                        page['parent_path'] = parent_list[:]
                        parent_list.append(comment_id)
                        page['path'] = parent_list
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id  # TODO: Get the client from the project 
                        page['client_name'] = self.task.client_name
                        page['last_updated_time'] = page['pickup_date']
                        page['versioned'] = False
                        page['entity'] = 'Comment'
                        page['category'] = self.task.instance_data.get('category','')
                        try:
                            posted_date = comment.parent.find('div',{'class':'date'}).renderContents()
                            posted_date = datetime.strptime(posted_date,'%y.%m.%d')
                            page['posted_date'] = datetime.strftime(posted_date ,"%Y-%m-%dT%H:%M:%SZ")
                        except:
                            log.info(self.log_msg("couldn't parse posted_date"))
                            page['posted_date'] = page['pickup_date']
                        page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                        self.pages.append(page)
                
                else:
                    log.info(self.log_msg('reached already parsed review so returning'))
                    return False
            except:
                log.exception(self.log_msg("exception in addreviews"))
                continue
        return True
