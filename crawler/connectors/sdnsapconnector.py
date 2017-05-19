
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
import cgi

from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash

from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('SdnSapConnector')
class SdnSapConnector(BaseConnector):

    @logit(log , 'fetch')
    def fetch(self):                                                                                         
        
        self.genre="Review"
        try:
            self.__base_uri = 'https://www.sdn.sap.com/'
            code = None
            parent_uri = self.currenturi
            self.POSTS_ITERATIONS = tg.config.get(path='Connector',key='sdnsap_numposts')
            self.curiter = 0
            if 'forum?forumID' in self.currenturi:
                self.setIframeData(iframe_present=True)
                next_page = self.soup.find('a',text="Next")
                while self.addQuestionUrls(parent_uri) and next_page:
                    try:
#                        log.debug(self.log_msg("Fetching url %s" %(self.currenturi)))
                        self.currenturi =  normalize('https://forums.sdn.sap.com/' + next_page.parent['href'])
                        self.setIframeData(iframe_present=True)
                        next_page = self.soup.find('a',text="Next")
                    except Exception, e:
                        log.exception(self.log_msg('exception in iterating pages in fetch'))
                        return False
            elif 'thread.jspa?threadID' in self.currenturi:
                self.setIframeData()
                self._getParentPage(parent_uri)
                self.iteratePosts(parent_uri)
            return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False
    
    @logit(log , 'addQuestionsUrls')
    def addQuestionUrls(self, parenturi):
        question_urls = [each.parent for each in self.soup.findAll('td',{'class':'jive-thread-name'})]
        log.info(self.log_msg('number of threads found on the page : %s'%len(question_urls)))
        for url in question_urls:
            try:
                self.curiter+=1
                if self.curiter > self.POSTS_ITERATIONS:
                    return False
                ins_url = normalize('https://forums.sdn.sap.com/' + url.find('td',{'class':'jive-thread-name'}).a['href'])
                temp_task = self.task.clone()
                temp_task.instance_data['uri'] = ins_url
                log.info(self.log_msg('url : %s'%temp_task.instance_data['uri']))
                try:
                    temp_task.instance_data['replies'] = stripHtml(url.find('td',{'class':'jive-msg-count'}).renderContents())
                except:
                    log.exception(self.log_msg('could not parse replies for the topic'))
                try:
                    temp_task.instance_data['views'] = stripHtml(url.find('td',{'class':'jive-view-count'}).renderContents())
                except:
                    log.exception(self.log_msg('could not parse views for the topic'))
                self.linksOut.append(temp_task)
            except:
                log.exception(self.log_msg('exception in function question urls'))
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
            parent = self.soup.findAll('div',{'class':'jive-message-list'})[1]
            self._parentIdentifier = re.sub('^.*messageID=','',parent.find('a',href=re.compile('^abuse!.*$'))['href'])
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    url, self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug(self.log_msg('checking session info'))
                page={}
                
                try:
                    page['data'] = stripHtml(parent.find('div',{'class':'jive-message-body'}).renderContents().replace('<br />',''))
                except Exception, e:
                    log.exception(self.log_msg('could not parse post data'))
                    raise e

                try:
                    page['title'] = stripHtml(parent.find('span',{'class':'jive-subject'}).renderContents())
                except Exception, e:
                    log.exception(self.log_msg('could not parse page title'))
                    page['title'] =page['data'][:50]

                try:
                    page['et_thread_hierarchy'] = [stripHtml(each.renderContents()) for each in 
                                        self.soup.find('p',{'id':'jive-breadcrumbs'}).findAll('a',href=True)]
                except:
                    log.info(self.log_msg('could not parse thread hierarchy'))

                try:
                    page['et_author_name'] = stripHtml(parent.find('a',href=re.compile('^profile\.jspa\?userID.*$')).renderContents())
                except:
                    log.info(self.log_msg('could not parse author name'))

                try:
                    page['et_author_profile'] = normalize( 'https://forums.sdn.sap.com/' + parent.find('a',
                                                                                 href=re.compile('^profile\.jspa\?userID.*$'))['href'])
                except:
                    log.info(self.log_msg('could not parse author profile link'))

                try:
                    author_type = [each['alt'] for each in parent.find('a',href=re.compile('^profile\.jspa\?userID.*$')).parent.\
                         findAll('img',src=True)]
                    if author_type:
                        page['et_author_type'] = author_type
                except:
                    log.info(self.log_msg('could not parse author detail'))

                try:
                    page['ei_data_number_views'] = int(self.task.instance_data.get('views'))
                except:
                    log.info(self.log_msg('could not get number of views for this topic'))

                try:
                    page['ei_data_number_replies'] = int(self.task.instance_data.get('replies'))
                except:
                    log.info(self.log_msg('could not get number of replies for this topic'))

                try:
                    author_information = stripHtml(parent.find('span',{'class':'jive-description'}).renderContents())
                except:
                    log.info(self.log_msg("could't get author information"))

                try:
                    author_joined_date =  re.search('Registered:.*',author_information.replace('\r\n',''))
                    author_joined_date = author_joined_date.group().split(':',1)[-1].strip()
                    author_joined_date = datetime.strptime(author_joined_date,'%m/%d/%y')
                    page['edate_author_member_since'] = datetime.strftime(author_joined_date,"%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.info(self.log_msg('could not parse author joined date'))

                try:
                    page['ei_author_number_posts'] = int(re.search('Posts:.*',
                                                               author_information.replace('\r\n','')).group().split(':',1)[-1].strip())
                except:
                    log.info(self.log_msg('could not parse author number of posts'))

                try:
                    page['ei_author_forum_points'] = int(re.search('Forum Points:.*',
                                                             author_information.replace('\r\n','')).group().split(':',1)[-1].strip())
                except:
                    log.info(self.log_msg('could not parse author forum points'))


                try:
                    post_hash = get_hash(page)
                except Exception,e:
                    log.exception(self.log_msg('could not build post_hash'))

                result=updateSessionInfo(self.genre,self.session_info_out,url, post_hash, 
                                         'Question', self.task.instance_data.get('update'),Id=id)
                if result['updated']:
                    page['path'] = page['parent_path'] = []
                    page['path'].append(url)
                    page['uri'] = normalize(url)
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    try:
                        posted_date = parent.find('span',{'class':'jive-description'},text=re.compile('Posted:'))
                        posted_date = re.search('Posted:.*',posted_date.replace('\r\n','')).group().split(':',1)[-1].strip()
                        posted_date = datetime.strptime(posted_date, '%b %d, %Y %I:%M %p')
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
#            self.currenturi = self.__base_uri + self.soup.find('a',title=re.compile('^Last Post By.*$'))
#            self.currenturi = self.currenturi.replace('//','/')
            next = self.soup.find('span',{'class':'jive-paginator-bottom'}).find('a',text='Next')
            while self.getPosts(parent_url) and next:
                next = self.soup.find('span',{'class':'jive-paginator-bottom'}).find('a',text='Next')
                self.currenturi = normalize('https://forums.sdn.sap.com/' + next.parent['href'])
                self.setIframeData()
        except:
            log.exception(self.log_msg('exception in iteratePosts'))

    @logit(log , 'getPosts')
    def getPosts(self,parent_url):
        try:            
            posts = self.soup.findAll('div',{'class':'jive-message-list'})
            if not posts:
                return False
            for post in posts[1:]:
                try:
                    post_ident = re.sub('^.*messageID=','',post.find('a',href=re.compile('^abuse!.*$'))['href'])
                    log.info(self.log_msg(post_ident))
                    if post_ident == self._parentIdentifier:
                        continue
                    if not checkSessionInfo(self.genre, self.session_info_out, 
                                            post_ident, self.task.instance_data.get('update'),parent_list=[parent_url]):
                        id=None
                        if self.session_info_out=={}:
                            id=self.task.id
                            log.debug(self.log_msg('checking session info'))
                        page={}
                        try:
                            page['data'] = stripHtml(post.find('div',
                                                               {'class':'jive-message-body'}).renderContents().replace('<br />',''))
                        except Exception, e:
                            log.exception(self.log_msg('could not parse post data'))
                            continue

                        try:
                            page['title'] = stripHtml(post.find('span',{'class':'jive-subject'}).renderContents())
                        except Exception, e:
                            log.exception(self.log_msg('could not parse page title'))
                            page['title'] =page['data'][:50]

                        try:
                            page['et_author_name'] = stripHtml(post.find('a',
                                                                         href=re.compile('^profile\.jspa\?userID.*$')).renderContents())
                        except:
                            log.info(self.log_msg('could not parse author name'))

                        try:
                            page['et_reply_message_url'] = 'https://forums.sdn.sap.com/' + \
                                post.find('a',title=re.compile('in response to'))['href']
                        except:
                            log.info(self.log_msg('could not parse reply message url'))

                        try:
                            page['et_reply_author_name'] = stripHtml(post.find('a',
                                                                title=re.compile('in response to'))['title'].split(':')[-1].strip())
                        except:
                            log.info(self.log_msg('could not parse reply author name'))

                        try:
                            page['et_author_profile'] = normalize('https://forums.sdn.sap.com/' + post.find('a',
                                                                                  href=re.compile('^profile\.jspa\?userID.*$'))['href'])
                        except:
                            log.info(self.log_msg('could not parse author profile link'))

                        try:
                            author_type = [each['alt'] for each in post.find('a',href=re.compile('^profile\.jspa\?userID.*$')).parent.\
                                 findAll('img',src=True)]
                            if author_type:
                                page['et_author_type'] = author_type
                        except:
                            log.info(self.log_msg('could not parse author detail'))

                        try:
                            author_information = stripHtml(post.find('span',{'class':'jive-description'}).renderContents())
                        except:
                            log.info(self.log_msg("could't get author information"))

                        try:
                            author_joined_date =  re.search('Registered:.*',author_information.replace('\r\n',''))
                            author_joined_date = author_joined_date.group().split(':',1)[-1].strip()
                            author_joined_date = datetime.strptime(author_joined_date,'%m/%d/%y')
                            page['edate_author_member_since'] = datetime.strftime(author_joined_date,"%Y-%m-%dT%H:%M:%SZ")
                        except:
                            log.info(self.log_msg('could not parse author joined date'))


                        try:
                            page['ei_author_number_posts'] = int(re.search('Posts:.*',
                                                                       author_information.replace('\r\n',
                                                                       '')).group().split(':',1)[-1].strip())
                        except:
                            log.info(self.log_msg('could not parse author number of posts'))

                        try:
                            page['ei_author_forum_points'] = int(re.search('Forum Points:.*',
                                                                     author_information.replace('\r\n',
                                                                     '')).group().split(':',1)[-1].strip())
                        except:
                            log.info(self.log_msg('could not parse author forum points'))

                        try:
                            post_hash = get_hash(page)
                        except Exception,e:
                            log.exception(self.log_msg('could not build post_hash'))

                        result=updateSessionInfo(self.genre,self.session_info_out,post_ident, post_hash, 
                                                 'answer', self.task.instance_data.get('update'),parent_list=[parent_url])
                        if result['updated']:
                            page['path'] = page['parent_path']= []
                            page['path'].append(post_ident)
                            page['uri'] = normalize(self.currenturi)
                            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                            page['priority']=self.task.priority
                            page['level']=self.task.level
                            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                            try:
                                posted_date = stripHtml(post.find('span',{'class':'jive-description'},text=re.compile('Posted:')))
                                posted_date = re.search('Posted:.*',posted_date.replace('\r\n','')).group().split(':',1)[-1].strip()
                                posted_date = datetime.strptime(posted_date, '%b %d, %Y %I:%M %p')
                                page['posted_date'] = datetime.strftime(posted_date,"%Y-%m-%dT%H:%M:%SZ")                          
                            except:
                                log.exception('could not parse posted_date')
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
                except:
                    log.exception(self.log_msg("forum post couldn't be parsed"))
        except Exception,e:
            log.exception(self.log_msg("forum post couldn't be parsed"))
            return False
        return True

    @logit(log,'setIframeData')
    def setIframeData(self,iframe_present=False):
        log.info(self.log_msg('initial url = %s'%self.currenturi))
        res=self._getHTML()
        self.rawpage=res['result']
        self._setCurrentPage()
        if self.soup.find('iframe'):
            self.currenturi = normalize(self.__base_uri + self.soup.find('iframe')['src'])
            log.info(self.log_msg('modified = %s'%self.currenturi))
            res=self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
        
