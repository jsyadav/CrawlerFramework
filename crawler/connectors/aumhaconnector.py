
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
import copy
from urllib2 import urlparse,unquote
import cgi

from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash

from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('AumhaConnector')
class AumhaConnector(BaseConnector):

    @logit(log , 'fetch')
    def fetch(self):                                                                                         
        
        self.genre="Review"
        try:
            self.__base_uri = 'http://www.aumha.net'
            code = None
            parent_uri = self.currenturi
            res=self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            self.POSTS_ITERATIONS = tg.config.get(path='Connector',key='aumha_numposts')
            self.curiter = 0
            if '/viewtopic.php' not in self.currenturi:
                next_page = self.soup.find('a',text="Next")
                while self.addQuestionUrls(parent_uri) and next_page:
                    try:
                        self.currenturi = self._removeSid(normalize(self.__base_uri + next_page.parent['href']))
                        log.debug(self.log_msg("Fetching url %s" %(self.currenturi)))
                        res=self._getHTML()
                        self.rawpage=res['result']
                        self._setCurrentPage()
                        next_page = self.soup.find('a',text="Next")
                    except Exception, e:
                        log.exception(self.log_msg('exception in iterating pages in fetch'))
                        return False
            else:
                self._getParentPage(parent_uri)
                self.iteratePosts(parent_uri)
            return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False
    
    @logit(log , 'addQuestionsUrls')
    def addQuestionUrls(self, parenturi):
        question_urls = self.soup.findAll('a',{'class':'topictitle','href':True})
        for url in question_urls:
            self.curiter+=1
            if self.curiter > self.POSTS_ITERATIONS:
                return False
            ins_url = normalize(self.__base_uri + url['href'])
            temp_task = self.task.clone()
            temp_task.instance_data['uri'] = self._removeSid(ins_url)
            log.info(self.log_msg('url : %s'%temp_task.instance_data['uri']))
            try:
                topic_info = url.parent.parent.findAll('p',{'class':'topicdetails'} , text = re.compile('^[0-9]+$'))
                temp_task.instance_data['replies'] = stripHtml(topic_info[0])
            except:
                log.exception(self.log_msg('could not parse replies for the topic'))
            try:
                temp_task.instance_data['views'] = stripHtml(topic_info[1])
            except:
                log.exception(self.log_msg('could not parse views for the topic'))
            try:
                temp_task.instance_data['last_posted_date'] = stripHtml(url.parent.parent.find('p',{'class':'topicdetails'} , 
                                                                    text = re.compile(' (am|pm)$')))
            except:
                log.exception(self.log_msg('could not parse views for the topic'))
            self.linksOut.append(temp_task)
        log.info(self.log_msg("No. of tasks appened %s"%len(self.linksOut)))
        return True

    @logit(log , '_getParentPage')
    def _getParentPage(self,url):
        try:
                #continue if returned true
#                if self.session_info_out != {}:
#                     self.currenturi = normalize(self.currenturi + '&start=0&sd=d&sk=t&sort=go&st=0')
#                     res=self._getHTML()
#                     self.rawpage=res['result']
#                     self._setCurrentPage()
            self._parentIdentifier = self.__base_uri + re.sub('^\.*','',self.soup.find('a',
                                                            {'href':re.compile('./viewtopic.*?#.*?$')})['href'])
            self._parentIdentifier = self._removeSid(self._parentIdentifier)
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    url, self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug(self.log_msg('checking session info'))
                page={}
                
                try:
                    page['data'] = stripHtml(self.soup.find('div',{'class':'postbody'}).renderContents())
                except Exception, e:
                    log.exception(self.log_msg('could not parse post data'))
                    raise e

                try:
                    page['title'] = stripHtml(self.soup.find('a',{'class':'titles'}).renderContents())
                except Exception, e:
                    log.exception(self.log_msg('could not parse page title'))
                    page['title'] =page['data'][:50]

                try:
                    thread_hierarchy = self.soup.find('p',{'class':'breadcrumbs'}).findAll('a')[1:]
                    page['et_thread_hierarchy'] = [stripHtml(t.renderContents()) for t in thread_hierarchy]
                except:
                    log.info(self.log_msg('could not parse thread hierarchy'))

                try:
                    page['et_author_name'] = stripHtml(self.soup.find('b',{'class':'postauthor'}).renderContents())
                except:
                    log.info(self.log_msg('could not parse author name'))

                try:
                    page['et_author_type'] = stripHtml(self.soup.find('td',{'class':'postdetails'}).renderContents())
                except:
                    log.info(self.log_msg('could not parse author detail'))

                try:
                    page['et_author_footer'] = re.sub('^_*','',stripHtml(self.soup.find('span',
                                                                                        {'class':'postbody'}).renderContents()))
                except:
                    log.info(self.log_msg('could not parse author footer'))

                try:
                    page['ei_data_number_views'] = int(self.task.instance_data.get('views'))
                except:
                    log.info(self.log_msg('could not get number of views for this topic'))

                try:
                    page['ei_data_number_replies'] = int(self.task.instance_data.get('replies'))
                except:
                    log.info(self.log_msg('could not get number of replies for this topic'))

                try:
                    last_posted_date = self.task.instance_data.get('last_posted_date')
                    last_posted_date = datetime.strptime(last_posted_date,'%a %m/%d/%y %I:%M %p')
                    page['edate_data_last_posted_date'] = datetime.strftime(last_posted_date,"%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.info(self.log_msg('could not get last posted date for this topic'))

                try:
                    author_information = stripHtml(self.soup.find('span',{'class':'postdetails'}).renderContents())
                except:
                    log.info(self.log_msg("could't get author information"))

                try:
                    author_joined_date =  re.search('Joined:.*',author_information)
                    author_joined_date = author_joined_date.group().split(':',1)[-1].strip()
                    author_joined_date = datetime.strptime(author_joined_date,'%a %m/%d/%y %I:%M %p')
                    page['edate_author_member_since'] = datetime.strftime(author_joined_date,"%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.info(self.log_msg('could not parse author joined date'))

                try:
                    page['ei_author_number_posts'] = int(re.search('Posts:.*',author_information).group().split(':',1)[-1].strip())
                except:
                    log.info(self.log_msg('could not parse author number of posts'))

                try:
                    post_hash = get_hash(page)
                except Exception,e:
                    log.exception(self.log_msg('could not build post_hash'))

                result=updateSessionInfo(self.genre,self.session_info_out,url, post_hash, 
                                         'Answer', self.task.instance_data.get('update'),Id=id)
                if result['updated']:
                    page['path']=[url]
                    page['parent_path']=[]
                    page['uri'] = normalize(url)
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    try:
                        posted_date = stripHtml(self.soup.find(text='Posted:').parent.parent.renderContents()).split(':',
                                                                                                                1)[-1].strip()
                        posted_date = datetime.strptime(posted_date, '%a %m/%d/%y %I:%M %p')
                        page['posted_date'] = datetime.strftime(posted_date,"%Y-%m-%dT%H:%M:%SZ")                          
                    except:
                        log.info('could not parse posted_date')
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")

                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project       
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['task_log_id']=self.task.id
                    page['entity'] = 'post'
                    page['category']=self.task.instance_data.get('category','')                        
                    self.pages.append(page)
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            raise e


    @logit(log,'iteratePosts')
    def iteratePosts(self,parent_url):
        try:
            self.currenturi = normalize(parent_url + '&start=0&sd=d&sk=t&sort=go&st=0')
            res=self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            next = self.soup.find('a',text='Next')
            while self.getPosts(parent_url) and next:
                self.currenturi = self.__base_uri + re.sub('^\.*','',next.parent['href'])
                self.currenturi = self._removeSid(self.currenturi)
                res=self._getHTML()
                self.rawpage=res['result']
                self._setCurrentPage()
                next = self.soup.find('a',text='Next')
        except:
            log.exception(self.log_msg('exception in iteratePosts'))

    @logit(log , 'getPosts')
    def getPosts(self,parent_url):
        try:            
            posts = self.soup.findAll('div',{'class':'postbody'})
            for post in posts:
                post = post.parent.parent.parent.parent.parent.parent
                post_ident = self.__base_uri + re.sub('^\.*','',post.find('a',
                                                                          {'href':re.compile('./viewtopic.*?#.*?$')})['href'])
                post_ident = self._removeSid(post_ident)
                log.info(post_ident)
                log.info(self._parentIdentifier)
                if post_ident == self._parentIdentifier:
                    continue
                if not checkSessionInfo(self.genre, self.session_info_out, 
                                        post_ident, self.task.instance_data.get('update'),parent_list=[parent_url]):
                    log.debug(self.log_msg('checking session info'))
                    page={}
                    try:
                        page['data'] = stripHtml(post.find('div',{'class':'postbody'}).renderContents())
                    except Exception, e:
                        log.exception(self.log_msg('could not parse post data'))
                        raise e
                    try:
                        page['title'] = page['data'][:50].strip() 
                    except Exception, e:
                        log.exception(self.log_msg('could not parse page title'))
                        page['title'] = ''


                    try:
                        page['et_author_name'] = stripHtml(post.find('b',{'class':'postauthor'}).renderContents())
                    except:
                        log.info(self.log_msg('could not parse author name'))

                    try:
                        page['et_author_type'] = stripHtml(post.find('td',{'class':'postdetails'}).renderContents())
                    except:
                        log.info(self.log_msg('could not parse author detail'))

                    try:
                        page['et_author_footer'] = re.sub('^_*','',stripHtml(post.find('span',
                                                                                            {'class':'postbody'}).renderContents()))
                    except:
                        log.info(self.log_msg('could not parse author footer'))
                    try:
                        author_information = stripHtml(post.find('span',{'class':'postdetails'}).renderContents())
                    except:
                        log.info(self.log_msg("could't get author information"))

                    try:
                        author_joined_date =  re.search('Joined:.*',author_information)
                        author_joined_date = author_joined_date.group().split(':',1)[-1].strip()
                        author_joined_date = datetime.strptime(author_joined_date,'%a %m/%d/%y %I:%M %p')
                        page['edate_author_member_since'] = datetime.strftime(author_joined_date,"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg('could not parse author joined date'))

                    try:
                        page['ei_author_number_posts'] = int(re.search('Posts:.*',author_information).group().split(':',1)[-1].strip())
                    except:
                        log.info(self.log_msg('could not parse author number of posts'))

                    try:
                        post_hash = get_hash(page)
                    except Exception,e:
                        log.exception(self.log_msg('could not build post_hash'))
                        raise e

                    result=updateSessionInfo(self.genre,self.session_info_out,post_ident, post_hash, 
                                             'Answer', self.task.instance_data.get('update'), parent_list =[parent_url])
                    if result['updated']:
                        parent_list = [parent_url]
                        page['parent_path']=copy.copy(parent_list)
                        parent_list.append(post_ident)
                        page['path']=parent_list
                        page['uri'] = post_ident
                        page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        try:
                            posted_date = stripHtml(post.find(text='Posted:').parent.parent.renderContents()).split(':',
                                                                                                                    1)[-1].strip()
                            posted_date = datetime.strptime(posted_date, '%a %m/%d/%y %I:%M %p')
                            page['posted_date'] = datetime.strftime(posted_date,"%Y-%m-%dT%H:%M:%SZ")                          
                        except:
                            log.info('could not parse posted_date')
                            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id  # TODO: Get the client from the project       
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
        
            
    def _removeSid(self,url):
        parsed_url = [u for u in urlparse.urlparse(url)]
        params = dict(cgi.parse_qsl(parsed_url[4]))
        if params.get('sid'):
            del params['sid']
        parsed_url[4] = str('&'.join('%s=%s'%(k,v) for k,v in params.items()))
        return urlparse.urlunparse(parsed_url)
