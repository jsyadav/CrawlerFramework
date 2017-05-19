'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Krtihka

import re
import copy
from datetime import datetime,timedelta
import logging
from urllib2 import urlparse
from tgimport import tg

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('VmWareScriptingConnector')
class VmWareScriptingConnector(BaseConnector):
    '''
    This will fetch the info for smallbizserver.net forums
    '''
    @logit(log , 'fetch')
    def fetch(self):
            """
            Fetch of http://forum.xda-developers.com
            """
            self.genre="Review"
            try:
                #self.currenturi ='http://forum.xda-developers.com/showthread.php?t=483836'
                self.parent_uri = self.currenturi
                if not self.currenturi.strip().startswith('http://www.vmwarescripting.com/index.php/board'):
                    if not self.__setSoup():
                        log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                        return False
                    self.__getParentPage()
                    self.__addPosts()
                    return True
                else:
                    self.total_posts_count = 0
                    self.last_timestamp = datetime( 1980,1,1 )
                    self.max_posts_count = int(tg.config.get(path='Connector',key='vmwarescript_numresults'))
                    if not self.__setSoup():
                        log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                        return False
                    while True:
                        if not self.__getThreads():
                            break
##                    self.linksOut=None        
                    if self.linksOut:
                        updateSessionInfo('Search', self.session_info_out,self.last_timestamp , None,'ForumThreadsPage', self.task.instance_data.get('update'))
                    return True
            except:
                log.exception(self.log_msg('Exception in fetch'))
                return False
    @logit(log , '__getThreads')
    def __getThreads( self ):
                """
                It will fetch each thread and its associate infomarmation
                """
                try:
                    threads = self.soup.find('div','tborder').findAll('tr')
                except:
                    log.exception(self.log_msg('No thread found, cannot proceed'))
                    return False
                for thread in threads[1:]:
                    if  self.max_posts_count <= self.total_posts_count :
                        log.info(self.log_msg('Reaching maximum post,Return false'))
                        return False
                    self.total_posts_count = self.total_posts_count + 1
                    page={}
                    try:
                        page['title'] = stripHtml(str(thread.find('td','windowbg').find('span').find('a')))
                    except:
                        log.exception(self.log_msg("No titles found!"))    
                    try:
                        page['et_author_name']=stripHtml(str(thread.findAll('td','windowbg2')[2]))    
                    except:
                        log.exception(self.log_msg("Author name not mentioned"))    
                    try:
                          page['ei_thread_replies_count']= int(stripHtml(str(thread.findAll('td','windowbg')[1])))    
                    except:
                        log.exception(self.log_msg("No replies found"))
                    try:
                          page['ei_thread_views_count'] = int(stripHtml(str(thread.findAll('td','windowbg')[2])))    
                    except:
                        log.exception(self.log_msg("No views found"))    
                    try:
                        date_str = re.sub('\s+',' ',stripHtml(str(thread.find('span','smalltext')))).split('by')[0].strip()
                        thread_time = datetime.strptime(date_str,'%B %d, %Y, %I:%M:%S %p')
                        page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%B %d, %Y, %I:%M:%S %p'),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.exception(self.log_msg("Posted date is not mentioned"))        
                    try:
                        if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')):
                            log.info(self.log_msg('Session info return True or Reaches max count'))
                            continue
                        self.last_timestamp = max(thread_time , self.last_timestamp )
                        temp_task=self.task.clone()
                        #log.info(normalize (re.sub('PHPSESSID=421c923f222030b1dac56e90488adaa4&',' ',thread.find('span',attrs={'id':re.compile('msg')}).a['href'])))
                        
                        temp_task.instance_data[ 'uri' ] = normalize (re.sub('/?PHPSESSID=.*?&','',thread.find('span',attrs={'id':re.compile('msg')}).a['href']))
                        temp_task.pagedata.update(page)
                        log.info(temp_task.pagedata)
                        self.linksOut.append( temp_task )
                        log.info(self.log_msg('Task Added'))
                    except:
                        log.exception(self.log_msg('Cannot add the Task'))
                
                return True 
    @logit(log, '__getParentPage')
    def __getParentPage(self):
            """
            This will get the parent info
            thread url.split('&')[-1].split('=')[-1]

            """
            if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                             self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info return True, Already exists'))
                return False
            page = {}
            try:
                page['et_thread_hierarchy'] = re.sub('\xa0\xbb\xa0','>>',stripHtml(str(self.soup.find('div',id='linktree'))))
                
            except:
                log.info(self.log_msg('Thread hierarchy is not found'))
                page['title']=''
            for each in ['title','et_author_name','ei_thread_replies_count','ei_thread_views_count','posted_date']:
                try:
                    page[each] = self.task.pagedata[each]
                except:
                    log.info(self.log_msg('page data cannot be extracted'))
            try:
                post_hash = get_hash( page )
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo( self.genre, self.session_info_out, self.\
                       parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
                if not result['updated']:
                    return False
                page['path']=[self.parent_uri]
                page['parent_path']=[]
                page['uri'] = normalize( self.currenturi )
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
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
                #page['first_version_id']=result['first_version_id']
                page['data'] = ''
                #page['id'] = result['id']
                page['task_log_id']=self.task.id
                page['entity'] = 'Post'
                page['category']=self.task.instance_data.get('category','')
                self.pages.append(page)
                log.info(page)
                log.info(self.log_msg('Parent Page added'))
                return True
            except:
                log.exception(self.log_msg("parent post couldn't be parsed"))
                return False
                
    @logit(log , '__addPosts')
    def __addPosts(self):
            """ It will add Post for a particular thread
            """
            try:
                posts = [x.findParent('table') for x in self.soup.findAll('div','post')]
            except:
                log.exception(self.log_msg('posts are not found'))
                return False
            post_type = True
            for post in posts:
                page={}
                try:
                    if post_type:
                        page['entity']='question'
                    else:
                        page['entity'] = 'answer'
                    post_type=False
                    page['data'] = stripHtml(str(post.find('div','post')))
                    #page['title'] = page['data'][:20]
                except:    
                    log.exception(self.log_msg("No discussions found"))
                try:
                    page['posted_date']=datetime.strftime(datetime.strptime(re.sub('\xbb','',stripHtml(str(post.findAll('div','smalltext')[1])).split('on:')[-1]).strip(),'%B %d, %Y, %I:%M:%S %p'),"%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.exception(self.log_msg("Posted_date not found"))    
                try:
                    users_info = [x.strip() for x in stripHtml(post.find('div','smalltext').renderContents()).split('\n') if not x.strip()=='']     
                    if len(users_info)>1:
                        try:
                            page['et_author_type'] = ' '.join(users_info[0:-1])
                        except:
                            log.exception(self.log_msg("Information on user type is not found"))    
                        try:
                            log.info(users_info[-1].strip())
                            page['ei_author_posts_count'] = int(users_info[-1].split(':')[-1].strip())
                        except:
                            log.exception(self.log_msg("Total posts count not found"))    
                    else:
                        try:
                            page['et_author_type'] = users_info[0]
                        except:
                            log.exception(self.log_msg("user type not mentioned"))
                except:
                    log.exception(self.log_msg("User information not specified"))            
                try:
                    page['ef_rating_overall']=float(len(post.find('div','smalltext').findAll('img',alt='*')))                 
                except:    
                    log.exception(self.log_msg("Individual rating not found"))
                try:
                    title_tag = post.find('div',attrs={'id':re.compile('subject')}).a
                    page['uri'] = title_tag['href']     
                    page['title']=stripHtml(title_tag.renderContents())
                except:
                    log.exception(self.log_msg("Permalink not found"))    
                    page['uri'] = self.currenturi
                    page['title']=''
                try:
                    review_hash = get_hash( page )
                    unique_key = get_hash( {'data':page['data'],'title':page['title']})
                    if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                                 self.task.instance_data.get('update'),parent_list\
                                                                =[self.parent_uri]):
                        continue
                    result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                                review_hash,'Review', self.task.instance_data.get('update'),\
                                                            parent_list=[self.parent_uri])
                    if not result['updated']:
                        continue
                    #page['id'] = result['id']
                    #page['first_version_id']=result['first_version_id']
                    #page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                    parent_list = [ self.parent_uri ]
                    page['parent_path'] = copy.copy(parent_list)
                    parent_list.append( unique_key )
                    page['path']=parent_list
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    #page['entity'] = 'Review'
                    page['category'] = self.task.instance_data.get('category','')
                    page['task_log_id']=self.task.id
                    #page['uri'] = self.currenturi
                    page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                    self.pages.append( page )
                    log.info(page)
                    log.info(self.log_msg('Review Added'))
                except:
                    log.exception(self.log_msg('Error while adding session info'))       
    @logit(log, "_setSoup")
    def __setSoup( self, url = None, data = None, headers = {} ):
            """
                It will set the uri to current page, written in seperate
                method so as to avoid code redundancy
            """
            if url:
                self.currenturi = url
            try:
                log.info(self.log_msg( 'for uri %s'%(self.currenturi) ))
                res = self._getHTML( data = data, headers=headers  )
                if res:
                    self.rawpage = res[ 'result' ]
                else:
                    log.info(self.log_msg('self.rawpage not set.... so Sorry..'))
                    return False
                self._setCurrentPage()
                return True
            except Exception, e:
                log.exception(self.log_msg('Page not for  :%s'%url))
                raise e                                 