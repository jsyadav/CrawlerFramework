
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#ASHISH YADAV

import re
import md5
import cgi
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import logging
from urllib2 import urlparse,unquote

from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('TreoCentralConnector')
class TreoCentralConnector(BaseConnector):

    @logit(log,'_createSiteUrl')
    def getSearchResults(self):
        ''' 
        url to be given in this form 
        http://discussion.treocentral.com/tcforum/search.php?query=iphone&forumchoice=-1&searchdate=0&beforeafter=after&sortby=lastpost&sortorder=descending&action=simplesearch
        just put approrite queryterm for query parameter
        '''
        try:
            params = self.currenturi.split('?')[-1]
            data = dict(cgi.parse_qsl(params))
            headers = {'Referer':self.currenturi}
            post_url = 'http://discussion.treocentral.com/tcforum/search.php?do=process'
            res=self._getHTML(post_url,data=data,headers=headers)
            log.info(self.log_msg('seedurl : %s'%self.currenturi))
            self.rawpage=res['result']
            self._setCurrentPage()
            return 
        except Exception,e:
            log.exception(self.log_msg('could not get search results'))
            raise e

    @logit(log , 'fetch')
    def fetch(self):                                                                                         
        self.genre="Review"
        parent_uri = self.currenturi
        self.getSearchResults()
        try:
            self._getParentPage(parent_uri) #checks for parent page ,and appends a empty dictionay or a modified post.
            self.THREADS_ITERATIONS = tg.config.get(path='Connector',key='treocentral_numthreads') 
            self.POSTS_ITERATIONS = tg.config.get(path='Connector',key='treocentral_numposts') 
            self.threads_count = 0
            self.threads_done = False
            while not self.threads_done:
                try:
                    next_page = self.soup.find('a',title=re.compile('Next Page '))
                    if self.addThreads(parent_uri) and next_page and not self.threads_done:
                        self.currenturi = 'http://discussion.treocentral.com/tcforum/' + next_page['href']
                        res=self._getHTML()
                        self.rawpage=res['result']
                        self._setCurrentPage()
                    else:
                        log.info(self.log_msg('Reached last page of reviews'))
                        break
                except Exception, e:
                    log.exception(self.log_msg('exception in iterating pages in fetch'))
                    break
            return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log , 'addThreads')
    def addThreads(self, parenturi):
        try:
            threads = []
            for each_thread in self.soup.find('table',{'id':'threadslist'}).findAll('a',{'href':True,'id':re.compile('thread_title_')}):
                threads.append(each_thread)
                self.threads_count = self.threads_count + 1
                if self.threads_count >= self.THREADS_ITERATIONS:
                    self.threads_done = True
                    break
            log.info(self.log_msg('number of threads found : %d '%len(threads)))
            for thread in threads:
                try:
                    print thread['href']
                    thread_url = normalize('http://discussion.treocentral.com/tcforum/' + thread['href'])
                    if not checkSessionInfo(self.genre, self.session_info_out,
                                            thread_url, self.task.instance_data.get('update'),
                                            parent_list=[parenturi]):
                        page = {}
                        try:
                            page['title'] = stripHtml(thread.renderContents())
                        except:
                            log.info(self.log_msg('could not parse thread title'))
                            page['title'] = ''
                        try:
                            post_section = thread.parent.parent.parent       
                        except:
                            log.info(self.log_msg('could not get to post section , continue from next_post'))
                            continue
                        try:
                            page['et_author_name'] = stripHtml(post_section.find('div',{'class':'smallfont'}).renderContents())
                        except:
                            log.info(self.log_msg("could not parse thread's author name"))
                        try:
                            page['et_author_profile']='http://discussion.treocentral.com/'+author_info.find('a',{'class':'bigusername'})['href']
                        except:
                            log.info(self.log_msg('could not parse author profile link'))
                        try:
                            post_metadata = post_section.find('td',{'class':'alt2','title':re.compile('Replies: ')})
                            metadata = re.match(re.compile('Replies: ([0-9]+), Views: ([0-9]+)'),post_metadata['title'])
                            if metadata.group(1):
                                page['ei_data_reply_count'] = int(metadata.group(1))
                            if metadata.group(2):
                                page['ei_data_view_count'] = int(metadata.group(2))
                        except:
                            log.info(self.log_msg('could not parse post replies , views'))
                        try:
                            thread_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                                         page.values()))).encode('utf-8','ignore')).hexdigest()
                        except:
                            log.exception(self.log_msg("exception in buidling thread_hash , moving onto next thread"))
                            continue
                        result=updateSessionInfo(self.genre, self.session_info_out, thread_url, thread_hash, 
                                                 'Thread', self.task.instance_data.get('update'), parent_list=[parenturi])
                        if result['updated']:
                            page['path'] = page['parent_path'] = [parenturi]
                            page['path'].append(thread_url)
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
                            page['entity'] = 'thread'
                            page['data']=''
                            page['category'] = self.task.instance_data.get('category','')
                            page['task_log_id']=self.task.id
                            page['uri'] = thread_url
                            page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                            try:
                                last_posted_date_pattern =  stripHtml(post_metadata.renderContents())
                                last_posted_date_pattern = re.search('([^ ]+)(.*?)$',last_posted_date_pattern)
                                last_posted_date = datetime.today()
                                last_posted_day = last_posted_date.day #because datetime obj is not modifieable , 
                                                                        # i am keeping day variable so that can be modified and put on later.
                                if last_posted_date_pattern.group(1):
                                    if last_posted_date_pattern.group(1) == 'Yesterday':
                                        last_posted_day -= 1
                                    elif '/' in last_posted_date_pattern.group(1):
                                        last_posted_date = datetime.strptime(last_posted_date_pattern.group(1),'%m/%d/%Y')
                                        last_posted_day = last_posted_date.day
                                    page['posted_date'] = datetime.strftime(last_posted_date,"%Y-%m-%dT%H:%M:%SZ")
                                elif last_posted_date_pattern.group(2):
                                    posted_time  = datetime.strptime(last_posted_date_pattern.group(2).strip(),'%I:%M %p')
                                    last_posted_date = posted_time.replace(last_posted_date.year,last_posted_date.month,last_posted_date.day)
                                    page['posted_date'] = datetime.strftime(last_posted_date,"%Y-%m-%dT%H:%M:%SZ")
                                else:
                                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                            except:
                                log.info(self.log_msg('posted_date could not be parsed'))
                                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                            self.pages.append(page)
                    try:
                        self.addPosts(thread_url,parent_list=[parenturi,thread_url])
                    except:
                        log.exception(self.log_msg('exception in calling add_posts'))    
                except:
                    log.info(self.log_msg('exception in add_threads , continuing from next thread'))
                    continue
            return True    
        except:
            log.exception(self.log_msg('exception in calling add_threads'))   
            return False

    @logit(log , 'addPosts')
    def addPosts(self,thread_url, parent_list):
        try:
            post_count = 0
            self.currenturi = normalize(thread_url + '&goto=newpost')
            while True:
                res=self._getHTML()
                self.rawpage=res['result']
                self._setCurrentPage()
                self.currenturi = normalize(self.currenturi)
                post_section = self.soup.find('div',{'id':'posts'})
                posts = post_section.findAll('div',{'align':'center'},recursive=False)
                # As we are adding the posts in reverse direction 
                posts.reverse() 
                log.debug('number of posts found: %d' % len(posts))
                for index,post in enumerate(posts):
                    log.debug('going for post: %d' %(index+1))
                    log.debug(self.currenturi)
                    try:
                        page = {}
                        try:
                            data = post.find('div',{'id':re.compile('post_message_')})
                            page['data'] = re.sub(re.compile('Quote:[\n\s]+',re.DOTALL|re.MULTILINE),'Quote:\n',stripHtml(data.renderContents())) 
                        except:
                            log.info(self.log_msg('could not parse post data'))
                            page['data'] = ''
                        try:
                            if len(page['data']) > 50: #title is set to first 50 characters or the post whichever is less
                                page['title'] = page['data'][:50] + '...'
                            else:
                                page['title'] = page['data']
                        except:
                            log.info(self.log_msg('could not parse title'))
                        try:
                            author_info = post.find('div',{'class':'smallfont'}).parent
                        except:
                            log.exception(self.log_msg('could not parse author_info'))
                            author_info = None

                        try:
                            page['et_author_name'] = stripHtml(author_info.find('a',{'class':'bigusername'}).renderContents())
                        except:
                            log.info(self.log_msg('could not parse author name'))
                        try:
                            page['et_author_type'] = stripHtml(post.find('div',{'class':'smallfont'}).renderContents())
                        except:
                            log.info(self.log_msg('could not determine author type'))

                        try:
                            author_metadata = author_info.findAll('div',{'class':'smallfont'})[-1]
                            author_joined_date = datetime.strptime(author_metadata.find('div',text=re.compile('Join Date:')).replace('Join Date:','').strip(),'%b %Y')
                            author_joined_date = datetime.strftime(author_joined_date,"%Y-%m-%dT%H:%M:%SZ")
                            page['edate_author_member_since'] = author_joined_date
                        except:
                            log.info(self.log_msg('could not parse author information'))
                        try:
                            author_posts = re.sub('Posts:|,','',author_metadata.find('div',text=re.compile('Posts:'))).strip()
                            page['ei_author_posts_count'] = int(author_posts)
                        except:
                            log.info(self.log_msg('could not parse author posts count'))
                        try:
                            author_location = author_metadata.find('div',text=re.compile('Location:')).replace('Location:','').strip()
                            page['et_author_location'] = author_location
                        except:
                            log.info(self.log_msg('could not parse author location'))
                        try:
                            post_permalink = 'http://discussion.treocentral.com/' + post.find('a',{'id':re.compile('postcount')})['href']
                            post_permalink = re.sub('&postcount=[0-9]+$','',post_permalink)
                        except:
                            log.exception(self.log_msg('could not extract post identifier, so continuing'))
                            continue

                        try:
                            post_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                                                    page.values()))).encode('utf-8','ignore')).hexdigest()
                        except:
                            log.exception(self.log_msg("exception in buidling post_hash , moving onto next post"))
                            continue
                        if not checkSessionInfo(self.genre, self.session_info_out,
                                                post_permalink, self.task.instance_data.get('update'),
                                                parent_list=parent_list):
                            result=updateSessionInfo(self.genre, self.session_info_out,post_permalink, post_hash, 
                                                     'Post', self.task.instance_data.get('update'), 
                                                     parent_list=parent_list)
                            if result['updated']:
                                page['path'] = page['parent_path'] = parent_list
                                page['path'].append(post_permalink)
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
                                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                                page['task_log_id']=self.task.id
                                page['entity'] = 'Thread'
                                page['category'] = self.task.instance_data.get('category' ,'')
                                try:
                                    date = stripHtml(post.find('a',{'name':re.compile('post[0-9]+')}).parent.renderContents())
                                    last_posted_date_pattern = re.search('([^ ]+),(.*?)$',date)
                                    last_posted_date = datetime.today()
                                    last_posted_day = last_posted_date.day
                                    if last_posted_date_pattern.group(1):
                                        if last_posted_date_pattern.group(1) == 'Yesterday':
                                            last_posted_day -= 1
                                        elif '/' in last_posted_date_pattern.group(1):
                                            last_posted_date = datetime.strptime(last_posted_date_pattern.group(1),'%m/%d/%Y')
                                            last_posted_day = last_posted_date.day
                                    if last_posted_date_pattern.group(2):
                                        posted_time  = datetime.strptime(last_posted_date_pattern.group(2).strip(),'%I:%M %p')
                                    last_posted_date = posted_time.replace(last_posted_date.year,last_posted_date.month,last_posted_day)
                                    page['posted_date'] = datetime.strftime(last_posted_date,"%Y-%m-%dT%H:%M:%SZ")
                                except:
                                    log.exception(self.log_msg('posted_date could not be parsed'))
                                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                                self.pages.append(page)
                            else:
                                log.info(self.log_msg('NOT Appending to self.pages updates false'))
                        else:
                            log.info(self.log_msg('NOT Appending to self.pages'))
                            return True
                        post_count = post_count + 1
                        if post_count >= self.POSTS_ITERATIONS:
                            log.debug(self.log_msg("Limit Exceeded for posts for this thread, %s" %(thread_url)))
                            return True
                    except:
                        log.exception(self.log_msg('Exception in addPosts'))
                        continue
                prev_page = self.soup.find('a',title=re.compile('Prev Page '))
                if prev_page:
                    log.info(self.log_msg('moving to previous page in Posts'))
                    self.currenturi = 'http://discussion.treocentral.com/tcforum/' + prev_page['href']
                else:
                    log.info(self.log_msg('reached first page in posts'))
                    return True
            return True
        except:
            log.exception(self.log_msg('exception in addPosts'))
            
    @logit(log , '_getParentPage')
    def _getParentPage(self,parent_uri):#NAMING CONVENTION IS WRONG
            try:
                #continue if returned true
                if not checkSessionInfo(self.genre, self.session_info_out, 
                                        parent_uri, self.task.instance_data.get('update')):
                    post_hash = md5.md5(parent_uri).hexdigest() #kinda dummy value
                    id=None
                    if self.session_info_out=={}:
                        id=self.task.id
                    result=updateSessionInfo(self.genre, self.session_info_out,parent_uri, post_hash, 
                                             'Search_url', self.task.instance_data.get('update'), Id=id)
            except Exception,e:
                log.exception(self.log_msg("parent post couldn't be parsed"))
                raise e
