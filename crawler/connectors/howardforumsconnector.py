
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#ASHISH YADAV

import re
import cgi
import copy
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import logging
from urllib2 import urlparse#,unquote,Request,urlopen not required now
import urllib#unused
#import traceback#dupe/unused import
#import simplejson#not required now
#import copy#unused import
#from xml.sax import saxutils#not required now

from tgimport import *
from baseconnector import BaseConnector

#from utils.task import Task#unused
from utils.utils import stripHtml,get_hash#incorrect naming of methods
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('HowardforumsConnector')
class HowardforumsConnector(BaseConnector):

    @logit(log , 'fetch')
    def fetch(self):
        try:
            self.genre = 'Review'
            parent_uri = self.currenturi
            post_count = 0
            #why self??
            #why such a long method?? 
            ##follow ur own set precedence - addReviews and not addreviews
            self.POSTS_ITERATIONS = tg.config.get(path='Connector',key='howardforums_numposts') 
            ##
            self.currenturi = normalize(parent_uri + '&goto=newpost')
            parent_list = [parent_uri]
            res=self._getHTML()
            #missing check for res None
            self.rawpage=res['result']
            self._setCurrentPage()
            self._getParentPage(parent_uri) 
            #needs better exception handling
            while True:
                self.currenturi = normalize(self.currenturi)
                post_section = self.soup.find('div',{'id':'posts'})
                posts = post_section.findAll('div',{'align':'center'},recursive=False)
                posts.reverse() 
                log.debug('number of posts found: %d' % len(posts))
                for index,post in enumerate(posts):
                    log.debug('going for post: %d' %(index+1))
                    log.debug(self.currenturi)
                    try:
                        page = {}
                        try:
                            data = stripHtml(post.find('span',{'id':'intelliTxt'}).div.renderContents())
#                            data = stripHtml(re.sub(re.compile('<table.*?</table>',re.DOTALL|re.MULTILINE),'',str(data)))
#                            page['data'] =  stripHtml(re.sub('Quote:','',data).strip())
                            page['data'] = re.sub(re.compile(\
                                    'Quote:[\n\s]+',re.DOTALL|re.MULTILINE),'Quote:\n',data)
                        except:
                            log.exception(self.log_msg('could not parse post data'))
                            #DOCUMENT the subtlity of this action
                            page['data'] = ''
                            ##

                        try:
                            title = [stripHtml(title.renderContents()) for title in post.findAll('div',{'class':'smallfont'}) if title.strong]
                            if title:
                                page['title'] = title[0]
                            elif len(page['data']) > 50: #title is set to first 50 characters or the post whichever is less
                                page['title'] = page['data'][:50] + '...'
                            else:
                                page['title'] = page['data']
                            page['title'] = re.sub('\n+',' ',page['title'])
                        except:
                            log.info(self.log_msg('could not parse title'))
                        try:
                            author_info = post.find('div',{'class':'smallfont'}).findParent('tr')
                        except:
                            log.exception(self.log_msg('could not parse author_info'))
                            author_info = None
                        try:
                            author_phones = stripHtml(author_info.find('b',text='Phone(s):').parent.parent.renderContents())
                            author_phones = [re.sub('^[0-9]+:','',phone.strip()) for phone in \
                                                 author_phones.splitlines() if phone.strip()][1:]
                            page['et_author_phones'] = [phone.strip() for phone 
                                                        in author_phones if phone.strip()] 
                        except:
                            log.exception(self.log_msg('could not parse author_phones'))
                        try:
                            author_providers = stripHtml(\
                                author_info.find(\
                                    'b',text='Provider(s):').\
                                    parent.parent.renderContents())
                            author_providers = [re.sub('^[0-9]+:','',provider.strip()) 
                                                for provider in author_providers.splitlines()\
                                                    if provider.strip()][1:]#why? - write a coment
                            page['et_author_phone_service_providers'] = [provider.strip() for provider in author_providers if provider.strip()] 
                        except:
                            log.exception(self.log_msg('could not parse author phone service providers'))
                        try:
                            page['et_author_name'] = stripHtml(author_info.find('a',{'class':'bigusername'}).renderContents())
                        except:
                            log.exception(self.log_msg('could not parse author name'))
                        try:
                            page['et_author_profile'] = 'http://www.howardforums.com/'+author_info.find('a',{'class':'bigusername'})['href']
                        except:
                            log.info(self.log_msg('could not parse author profile link'))
                        try:
                            page['et_author_type'] = stripHtml(post.find('div',{'class':'smallfont'}).renderContents())
                        except:
                            log.info(self.log_msg('could not determine author type'))
                        try:
                            author_joined_date_str = post.find('div',{'class':'smallfont'},text=re.compile('^Joined:.*'))
                            author_joined_date = datetime.strptime(author_joined_date_str.replace('Joined:','').strip(),'%b %Y')
                            page['edate_author_member_since'] = datetime.strftime(author_joined_date,"%Y-%m-%dT%H:%M:%SZ")
                        except:
                            log.info(self.log_msg('could not parse author joined date'))
                        try:
                            author_posts = re.sub('Posts:|,','',post.find('div',text=re.compile(r'Posts:'))).strip()
                            page['ei_author_posts_count'] = int(author_posts)
                        except:
                            log.info(self.log_msg('could not parse author posts count'))
                        try:
                            author_location = re.sub('From:','',post.find('div',text=re.compile('From:')))
                            page['et_author_location'] = author_location.strip()
                        except:
                            log.info(self.log_msg('could not parse author location'))
                        try:
                            post_permalink = 'http://www.howardforums.com/'+post.find('a',{'target':'new'})['href']
                            params = cgi.parse_qsl(urlparse.urlparse(post_permalink)[4]) #gets p=# parameter from the url,unique to a post
                            post_id = dict(params)['p']
                            post_permalink = re.sub('&postcount=[0-9]+$','',post_permalink)
                        except:
                            log.exception(self.log_msg('could not extract post_id, so continuing'))
                            continue

                        try:
                            post_hash = get_hash(page)
                        except:
                            log.exception(self.log_msg("exception in buidling post_hash , moving onto next post"))
                            continue
                        ##could have been long back in code...it does not need hash to be present
                        if not checkSessionInfo(self.genre, self.session_info_out,
                                                post_id, self.task.instance_data.get('update'),
                                                parent_list=parent_list):
                            result=updateSessionInfo(self.genre, self.session_info_out,post_id, post_hash, 
                                                     'Post', self.task.instance_data.get('update'), 
                                                     parent_list=parent_list)
                            if result['updated']:
                                page['parent_path'] = copy.copy(parent_list)
                                parent_list.append(post_id)
                                page['path'] = parent_list
                                page['priority']=self.task.priority
                                page['level']=self.task.level
                                page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                                page['last_updated_time'] = page['pickup_date']
                                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                                page['connector_instance_id'] = self.task.connector_instance_id
                                page['workspace_id'] = self.task.workspace_id
                                page['client_id'] = self.task.client_id  # TODO: Get the client from the project 
                                page['client_name'] = self.task.client_name
                                page['versioned'] = False
                                page['uri'] = normalize(post_permalink)
                                page['uri_domain'] = urlparse.urlparse(post_permalink)[1]
                                page['task_log_id']=self.task.id
                                page['entity'] = 'Post'
                                page['category'] = self.task.instance_data.get('category' ,'')
                                try:
                                    now = datetime.utcnow()
                                    date = stripHtml(post.find('a',{'name':re.compile('post[0-9]+')}).parent.renderContents())
                                    if date.split(',')[0].strip() == 'Yesterday':
                                        posted_time = datetime.strptime(date.split(',')[-1].strip(),'%H:%M')
                                        posted_date =  posted_time.replace(now.year,now.month,now.day - 1)
                                    elif date.split(',')[0].strip() == 'Today':
                                        posted_time = datetime.strptime(date.split(',')[-1].strip(),'%H:%M')
                                        posted_date =  posted_time.replace(now.year,now.month,now.day)
                                    else:
                                        posted_date = datetime.strptime(date ,'%m-%d-%Y, %H:%M')
                                    page['posted_date'] = datetime.strftime(posted_date ,"%Y-%m-%dT%H:%M:%SZ")
                                except:
                                    log.exception(self.log_msg('posted_date could not be parsed'))
                                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                                self.pages.append(page)
                            else:
                                log.info(self.log_msg('NOT Appending to self.pages, returning from here'))
                        else:
                            log.info(self.log_msg('NOT Appending to self.pages'))
                            return True
                        post_count = post_count + 1
                        if post_count >= self.POSTS_ITERATIONS:
                            log.debug(self.log_msg("Limit Exceeded for posts for this thread, %s" %(self.currenturi)))
                            return True
                    except:
                        log.exception(self.log_msg('Exception in addPosts'))
                        continue
                prev_page = self.soup.find('a',title=re.compile('Prev Page '))
                if prev_page:
                    log.info(self.log_msg('moving to previous page in Posts'))
                    self.currenturi = 'http://www.howardforums.com/' + prev_page['href']
                    res=self._getHTML()
                    self.rawpage=res['result']
                    self._setCurrentPage()
                else:
                    log.info(self.log_msg('reached first page in posts'))
                    return True
            return True
        except:
            log.exception(self.log_msg('exception in addPosts'))
            
    @logit(log , '_getParentPage')
    def _getParentPage(self,parent_uri):
            try:
                page={}
                try:
                    thread_hierarchy = self.soup.findAll('span',{'class':'navbar'})
                    thread_hierarchy = [stripHtml(title.a.renderContents()) for title in thread_hierarchy]
                    page['et_thread_hierarchy'] = thread_hierarchy
                except:
                    log.info(self.log_msg('could not parse thread hirearchy'))
                try:
                    page['title'] = stripHtml(self.soup.find('td',{'class':'navbar','colspan':'3'}).strong.renderContents())
                except:
                    log.info(self.log_msg('could not find thread title'))

                try:
                    thread_hash = get_hash(page)
                except Exception,e:
                    log.exception(self.log_msg('could not build thread_hash'))
                    raise e
                ##se previous comment
                #continue if returned true
                if not checkSessionInfo(self.genre, self.session_info_out, 
                                        parent_uri, self.task.instance_data.get('update')):
                    id=None
                    if self.session_info_out=={}:
                        id=self.task.id
                    result=updateSessionInfo(self.genre, self.session_info_out,parent_uri,thread_hash, 
                                             'Thread', self.task.instance_data.get('update'), Id=id)
                    if result['updated']:
                        page['path']=[parent_uri]
                        page['parent_path']=[]
                        page['uri'] = normalize(self.currenturi.replace('&goto=newpost',''))
                        page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id  # TODO: Get the client from the project       
                        page['client_name'] = self.task.client_name
                        page['last_updated_time'] = page['pickup_date']
                        page['versioned'] = False
                        page['data'] = ''
                        page['task_log_id']=self.task.id
                        page['entity'] = 'Thread'
                        page['category']=self.task.instance_data.get('category','')                        
                        self.pages.append(page)
            except Exception,e:
                log.exception(self.log_msg("parent post couldn't be parsed"))
                raise e
