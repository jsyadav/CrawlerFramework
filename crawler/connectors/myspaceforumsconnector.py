
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#ASHISH YADAV

import re
import cgi
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import logging
from urllib2 import urlparse,unquote
import copy

from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('MySpaceForumsConnector')
class MySpaceForumsConnector(BaseConnector):

    @logit(log , 'fetch')
    def fetch(self):
        try:
            self.genre = 'Generic'
            parent_uri = self.currenturi
            post_count = 0
#            self.POSTS_ITERATIONS = tg.config.get(path='Connector',key='myspaceforum_numposts') 
            parent_list = [parent_uri]
            res=self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            category = cgi.parse_qs(urlparse.urlparse(self.currenturi)[4])['fuseaction'][0]
            if category.lower() == 'messageboard.viewcategory': #is a category page parse all the thread links from this page.
                urls = self._getThreadLinks()
            elif category.lower() == 'messageboard.viewthread': #is the thread page itself.
                urls = [self.currenturi]
            if len(urls) > 1:
                for url in urls:
                    temp_task=self.task.clone()
                    temp_task.instance_data['uri']= url
                    self.linksOut.append(temp_task)
                    log.info(self.log_msg('creating task : %s'%url))
                log.info(self.log_msg('.no of Tasks added %d'%(len(self.linksOut))))

            else:
                self._getParentPage(parent_uri)
                self._addAnswers(parent_uri)
            return True
        except:
            log.exception(self.log_msg('fetch failed'))
            return False

    @logit(log,'_getThreadLinks')
    def _getThreadLinks(self):
        nextLink = True
        links = []
        num_threadconfig = tg.config.get(path='Connector', key='myspaceforum_numthreads')
        num_threads = 0
        while nextLink and num_threadconfig > num_threads:
            threadLinks = self.soup.find('table',{'id':'catstbl'}).\
                findAll('a',{'href':re.compile('fuseaction=messageboard.viewThread',re.I)})
            for each in threadLinks:
                num_threads += 1
                links.append(each['href'])
                if num_threads >= num_threadconfig:
                    break
            nextLink = self._getNextLink()
            if not links:
                break
            if nextLink:
                res=self._getHTML()
                self.rawpage=res['result']
                self._setCurrentPage()
        return links 

    @logit(log , '_getParentPage')
    def _getParentPage(self,parent_uri):
        try:
            page={}
            question = self.soup.find('div',{'class':'postbody'}).parent.parent
            
            try:
                posted_date = stripHtml(question.find('span',{'class':'redtext8'}).renderContents())
                posted_date = datetime.strptime(posted_date,'%d %b %Y, %H:%M')
                page['posted_date'] = datetime.strftime(posted_date,"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.info(self.log_msg('could not parse posted_date for question'))
                raise 

            try:
                page['et_thread_title'] = stripHtml(self.soup.find('div',{'id':'forumctr'}).h1.renderContents()).split('\n')[-1].strip()
            except:
                log.info(self.log_msg('could not parse thread title'))

            try:
                page['data'] = stripHtml(question.find('div',{'class':'postbody'}).renderContents())
            except:
                log.exception(self.log_msg('could not parse question data'))
                raise 

            try:
                page['title'] = stripHtml(self.soup.find('div',{'id':'forumctr'}).h1.renderContents()).split('\n')[-1].strip()
            except:
                page['title'] = page['data'][:80]
                log.info(self.log_msg('could not find thread title'))
                
            try:
                page['et_author_name'] = stripHtml(question.find('td',{'class':'author'}).a.renderContents())
            except:
                log.info(self.log_msg('could not find question author name'))

            try:
                page['et_author_profile'] = question.find('td',{'class':'author'}).a['href']
            except:
                log.info(self.log_msg('could not parse author profile link'))

            try:
                match = re.match(re.compile(r'([MF])/([0-9]+)(.*)',re.DOTALL),
                                 stripHtml(question.find('div',{'class':'aboutuser'}).renderContents()))
                match = list(match.groups())
                if match[0]:
                    page['et_author_gender'] = match[0]
                if match[1]:
                    page['et_author_age'] = match[1]
                if match[2]:
                    page['et_author_location'] = ''.join([each.strip() for each in match[2].strip().splitlines() if each.strip()])
            except:
                log.info(self.log_msg('could not find question author detail'))

            try:
                question_hash = get_hash(page)
            except Exception,e:
                log.exception(self.log_msg('could not build question_hash'))
                raise e

            #continue if returned true
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    page['posted_date'], self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out,page['posted_date'],question_hash, 
                                         'Question', self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    page['path']=[parent_uri]
                    page['parent_path']=[]
                    page['uri'] = normalize(self.currenturi)
                    page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
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
                    page['entity'] = 'Question'
                    page['category']=self.task.instance_data.get('category','')                        
                    self.pages.append(page)
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            raise e


        
    @logit(log , '_addAnswers')
    def _addAnswers(self,parent_uri):
        try:
            try:
                end_page_num = max([int(re.findall(r"[0-9]+",stripHtml(each['href']))[0]) for each in self.soup.findAll\
                                        ('a',{'href':re.compile("javascript:NextPage\(\'[0-9]+\'\)")})])
                self.currenturi = self._getNextLink(end_page_num-1)
                res=self._getHTML()
                self.rawpage=res['result']
                self._setCurrentPage()
            except:
                log.info('no next page')
            num_commentsconfig = tg.config.get(path='Connector', key='myspaceforum_numcomments')
            prev_link = True
            num_comments = 0
            while num_commentsconfig > num_comments and prev_link:
                answers = self.soup.findAll('div',{'class':'postbody'})[::-1]
                for each in answers:
                    page = {}
                    each = each.parent.parent
                    num_comments +=1
                    try:
                        posted_date = stripHtml(each.find('span',{'class':'redtext8'}).renderContents())
                        posted_date = datetime.strptime(posted_date,'%d %b %Y, %H:%M')
                        page['posted_date'] = datetime.strftime(posted_date,"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.exception(self.log_msg('could not parse posted_date for question'))
                        raise 

                    try:
                        page['et_thread_title'] = stripHtml(self.soup.find('div',{'id':'forumctr'}).h1.\
                                                                renderContents()).split('\n')[-1].strip()
                    except:
                        log.info(self.log_msg('could not parse thread title'))

                    try:
                        data =  each.find('div',{'class':'postbody'}).renderContents()
                        data = re.sub(re.compile('<blockquote>.*?</blockquote>',re.DOTALL),'',data)
                        page['data'] = stripHtml(data)
                        if not page['data']:
                            continue
                    except:
                        log.exception(self.log_msg('could not parse question data'))
                        continue

                    try:
                        page['title'] = stripHtml(self.soup.find('div',{'id':'forumctr'}).h1.renderContents()).split('\n')[-1].strip()
                    except:
                        page['title'] = page['data'][:80]
                        log.info(self.log_msg('could not find thread title'))

                    try:
                        page['et_author_name'] = stripHtml(each.find('td',{'class':'author'}).a.renderContents())
                    except:
                        log.info(self.log_msg('could not find question author name'))

                    try:
                        page['et_author_profile'] = each.find('td',{'class':'author'}).a['href']
                    except:
                        log.info(self.log_msg('could not parse author profile link'))

                    try:
                        match = re.match(re.compile(r'([MF])/([0-9]+)(.*)',re.DOTALL),
                                         stripHtml(each.find('div',{'class':'aboutuser'}).renderContents()))
                        match = list(match.groups())
                        if match[0]:
                            page['et_author_gender'] = match[0]
                        if match[1]:
                            page['et_author_age'] = match[1]
                        if match[2]:
                            page['et_author_location'] = ''.join([each.strip() for each in match[2].strip().splitlines() if each.strip()])
                    except:
                        log.exception(self.log_msg('could not find question author detail'))

                    try:
                        question_hash = get_hash(page)
                    except Exception,e:
                        log.exception(self.log_msg('could not build question_hash'))
                        raise e

                    #continue if returned true
                    if not checkSessionInfo(self.genre, self.session_info_out, 
                                            page['posted_date'], self.task.instance_data.get('update')):
                        id=None
                        if self.session_info_out=={}:
                            id=self.task.id
                        result=updateSessionInfo(self.genre, self.session_info_out,page['posted_date'],question_hash, 
                                                 'Answer', self.task.instance_data.get('update'), Id=id)
                        if result['updated']:
                            page['path']=[parent_uri]
                            page['parent_path']=[]
                            page['uri'] = normalize(self.currenturi)
                            page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
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
                            page['category']=self.task.instance_data.get('category','')                        
                            self.pages.append(page)
                        else:
                            log.info(self.log_msg('post already present and not updated , so not adding to self.pages'))
                    else:
                        log.info(self.log_msg('post already present and update is set false , so not adding to self.pages'))
                    
                prev_link = self._getNextLink(inc=-1)
                if prev_link:
                    self.currenturi = prev_link
                    res=self._getHTML()
                    self.rawpage=res['result']
                    self._setCurrentPage()
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            raise e


    @logit(log , '_getNextLink')
    def _getNextLink(self,inc=1,pageNum=None):
        #implements pagination javascript function equivalent in python
        url_unparsed = list(urlparse.urlparse(self.currenturi))
        params = cgi.parse_qs(url_unparsed[4])
        link = ''
        if self.soup.findAll('a',text='Next') or self.soup.findAll('a',text='Previous'):
            if not params.get('page'):
                params['page'] = ['0']
            if pageNum:
                params['page'][0] = str(pageNum)
            else:
                params['page'][0] = str(int(params['page'][0]) + inc)
            params['lastpagesent'] = params['page']
            url_unparsed[4] = '&'.join(['%s=%s'%(k,v[0])for k,v in params.items()])
            if int(params['page'][0]) >= 0:
                link = urlparse.urlunparse(url_unparsed)
        return link
