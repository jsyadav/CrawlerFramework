
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#ASHISH YADAV
#Sudharshan S

import re
import cgi
import copy
import md5
from datetime import datetime,timedelta
from BeautifulSoup import BeautifulSoup
import logging
from urllib2 import urlparse
import urllib
import traceback

from tgimport import *
from baseconnector import BaseConnector

from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

# Sample URI
# http://www.mobilyz.com/forum/f7/merken/f375/samsung/

log = logging.getLogger('mobilyzforumconnector')
class MobilyzForumConnector(BaseConnector):

    @logit(log , 'fetch')
    def fetch(self):
        try:
            self.genre = 'Review'
            parent_uri = self.currenturi
            res=self._getHTML()
            #missing check for res None
            self.rawpage=res['result']
            self._setCurrentPage()
            self.POSTS_ITERATIONS = tg.config.get(path='Connector',key='mobilyzforum_numposts') 
            self.THREAD_ITERATIONS = tg.config.get(path='Connector',key='mobilyzforum_numthreads') 

            if not parent_uri.endswith('.html'):#it's a page having links to individual threads, 
                self.__getThreads()
                return True

            if '/forum/' not in self.currenturi: #not a forum url
                return True

            if not self.__getParentPage(parent_uri):
                return True
            
            parent_list = [parent_uri]
            self.currenturi = normalize(self.task.pagedata['last_url'])
            res = self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            self.post_count=0
            
            #while self.__getPosts(parent_list):
                #prev_page = self.soup.find('a',text='previous')
                #if prev_page:
                #    log.info(self.log_msg('moving to previous page in Posts'))
            #    self.currenturi = prev_page.parent['href']
            #    res = self._getHTML()
            #    self.rawpage = res['result']
            #    self._setCurrentPage()
            self.__getPosts(parent_list)
                #else:
                #    log.info(self.log_msg('reached first page in posts'))
                #    break
        except:
            log.exception(self.log_msg('exception in addPosts'))
            
        return True


    @logit(log,'__getPosts')
    def __getPosts(self, parent_list):
        try:

            pagenav = self.soup.find("div", "pagenav")
            no_pages = None
            if pagenav:
                # Number of pages.. Not 'No' pages :)
                no_pages = int(pagenav.find("td", "vbmenu_control").renderContents().split()[-1])

            # So that we will have atleast one iteration in the loop below
            post_pages = []
            post_segment = self.soup.find(id='posts')
            posts_soup = post_segment.findAll("div", "page")
            if posts_soup:
                post_pages.append(posts_soup)

            # In case of pagination, extract the soups corresponding to the posts from each page and
            # and append it to a list for use below
            if no_pages:
                log.info(self.log_msg('Found %d pages' %(no_pages)))
                for i in range(2, no_pages):
                    self.currenturi = ".".join(self.currenturi.split(".")[0:-1]) + "-%s.html" %(i)
                    res=self._getHTML()
                    self.rawpage=res['result']
                    self._setCurrentPage()
                    post_segment = self.soup.find(id='posts')
                    posts_soup = post_segment.findAll("div", "page")
                    if not posts_soup:
                        post_pages.append(posts_soup)
            else:
                log.info(self.log_msg("No pagination for %s" %self.currenturi))

            # Handle all post_soups in the pages
            log.info(self.log_msg("\n%s\nBeginning to process %d pages\n%s" %("="*25, len(post_pages), "="*25)))
            for posts_soup in post_pages:
                if not posts_soup:
                    log.info(self.log_msg("No posts in here. Ignoring the current thread"))
                    raise
                log.info("Found %d posts. Processing them" %(len(posts_soup) - 1)) # last div is junk

                # Process all the posts one by one
                for idx, post in enumerate(posts_soup):
                    if post.find("h3"):
                        if post.find("h3").find("a")['href'].startswith("https://adwords"):
                            log.debug(self.log_msg("This is an ad space. Ignoring and moving on"))
                            continue

                    page = {}
                    post_permalink = ''
                    post_id = None
                    try:
                        post_permalink = post.find('a', id = re.compile("postcount\d+"))['href']
                        page['uri'] = post_permalink
                        # Unique identifier for this post. Take the MD5 of the post's permalink
                        post_id = md5.md5(post_permalink).hexdigest()
                    except:
                        log.info(self.log_msg('could not extract post_permalink. Cannot continue'))
                        continue
                                            
                    try:
                        page['et_author_name'] = post.find("a", "bigusername").renderContents()
                    except Exception, e:
                        log.info("Couldn't author name for %d" %idx)
                        continue

                    # Extract additional data
                    post_title_info_soup = post.find('div', {'id' : re.compile(r'postmenu_\d+')})
                    if post_title_info_soup:
                        try:
                            page['et_author_title'] = post_title_info_soup.find('div', 'smallfont').renderContents()
                        except Exception, e:
                            log.info("Couldn't author title for %d." %idx)

                        try:
                            page['et_author_profile'] = post_title_info_soup.a['href']
                        except:
                            log.info(self.log_msg('could not parse author profile link'))
                    else:
                        log.info(self.log_msg("No additional userdata found.. Prolly site layout changed?"))

                    try:
                        post_hash = get_hash(page)
                    except:
                        log.exception(self.log_msg("exception in building post_hash , moving onto next post"))
                        continue

                    try:
                        ad_txt = post.find("div", "ad_txt")
                        # Rip quoted text
                        if ad_txt.findAll("a", {"rel" : "nofollow"}):
                            # Md5's of the permalinks (posts) that were quoted/referred
                            page['referenced_posts'] = [md5.md5(x['href']).hexdigest() \
                                                        for x in ad_txt.findAll("a", {"rel" : "nofollow"})]

                            log.info(self.log_msg('quotes == %s'%page['referenced_posts']))
                    except:
                        log.info(self.log_msg('could not extract quoted message.'))


                    try:
                        posted_date_dutch = post.find("div", "quote").find("span").renderContents().strip()
                        if ',' in posted_date_dutch:
                            strp_date = posted_date_dutch
                            posted_date = datetime.strptime(' '.join([each.strip().capitalize() for \
                                                                      each in strp_date.split()[1:]]),'%d %B %Y, %H:%M')
                            
                        elif self.get_date(posted_date_dutch): # Some posts have '2 weeks since', '2 days since'.. etc
                            posted_date = self.get_date(posted_date_dutch)
                        else: # Normal Dates in english, 09 March 2009.. etc
                            posted_date = datetime.strptime(posted_date_dutch.strip(),'%d %B %Y')

                        #This is some dead code from ashish's version to find the posted_date
                        #if author_info:
                        #    if ',' in author_info[-1].strip():
                        #        strp_date = author_info[-1].strip()
                        #        posted_date = datetime.strptime(' '.join([each.strip().capitalize() for \
                        #                                                  each in strp_date.split()[1:]]),'%d %B %Y, %H:%M')
                        #    elif self.get_date(author_info[-1].strip()):
                        #        posted_date = self.get_date(author_info[-1].strip())
                        #    else:
                        #posted_date= datetime.strptime(author_info[-1].strip(),'%d %B %Y %H:%M')
                        page['posted_date'] = datetime.strftime(posted_date, "%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.exception(self.log_msg('could not parse posted date'))


                    try:
                        ad_txt = post.find("div", "ad_txt")
                        
                        # Rip quoted text. We don't need quoted text in the post data
                        if ad_txt.find("div", {"style" : "margin:20px; margin-top:5px; " }):
                            ad_txt.find("div", {"style" : "margin:20px; margin-top:5px; " }).extract()
                            page['data'] = stripHtml(ad_txt.renderContents())
                        else:
                            page['data'] = stripHtml(ad_txt.renderContents())
                                                    
                    except Exception, e:
                        page['data'] = ''
                        log.info("Couldn't get data for %d. Continuing on to the next post %s" %(idx, e))
                        continue

                    try:
                        # Hopefully this won't screw up
                        page['title'] = page['data'][0:50] + "..."
                        page['title'] = re.sub('\n+',' ',page['title']) 
                    except Exception, e:
                        log.info("Couldn't get title for %d. Continuing on to the next post. %s. %s" %(idx, e, traceback.format_exc()))
                        continue



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
                            page['posted_date'] = page.get('posted_date',page['pickup_date'])
                            page['last_updated_time'] = page['pickup_date']
                            page['connector_instance_log_id'] = self.task.connector_instance_log_id
                            page['connector_instance_id'] = self.task.connector_instance_id
                            page['workspace_id'] = self.task.workspace_id
                            page['client_id'] = self.task.client_id  # TODO: Get the client from the project 
                            page['client_name'] = self.task.client_name
                            page['versioned'] = False
                            page['uri_domain'] = urlparse.urlparse(post_permalink)[1]
                            page['task_log_id']=self.task.id
                            page['entity'] = 'Answer'
                            page['category'] = self.task.instance_data.get('category' ,'')
                            self.pages.append(page)
                        else:
                            log.info(self.log_msg('NOT Appending to self.pages, returning from here'))
                    else:
                        log.info(self.log_msg('NOT Appending to self.pages'))
                        return True

                    self.post_count = self.post_count + 1
                    if self.post_count >= self.POSTS_ITERATIONS:
                        log.debug(self.log_msg("Limit Exceeded for posts for this thread, %s" %(self.currenturi)))
                        return False
                    log.debug(self.log_msg("Added %d" %self.post_count))
        except:
            log.exception(self.log_msg('Exception in addPosts'))
            
        return True
    


    @logit(log , '__getParentPage')
    def __getParentPage(self,parent_uri):
        
        """Gather all thread related metadata like title and number of replies here
        """
        page = {}
        try:
            if self.soup.find("ul", "tabset"):
                log.info("This link %s is not a forum thread. Must be either a review or news link. Moving on" %parent_uri)
                return False 

            log.info(self.log_msg("Extracting posts from %s" %parent_uri))
            try:
                page['title'] = self.soup.find(id='content-primary').find("h1").renderContents()
                page['data'] = page['title'] # Base connector will whine
            except Exception, e:
                log.info(self.log_msg("Couldn't parse title. Ignoring the post"))
                #raise e

            # Get the number of posts
            try:
                post_segment = self.soup.find(id='post')
                page['ei_thread_num_replies'] = len(post_segment.findChildren()) - 2 # The last div is junk
            except Exception, e:
                log.info(self.log_msg("Couldn't get the number of replies for %s" %parent_uri))

            # Get the thread hierarchy.
            try:
                thread_hierarchy = self.soup.find('ul',{'class':'breadcrumb'}).findAll('li')[3:-1]
                thread_hierarchy = [stripHtml(title.a.renderContents()) for title in thread_hierarchy]
                page['et_thread_hierarchy'] = ';'.join(thread_hierarchy)
            except Exception, e:
                log.info(self.log_msg("Couldn't get thread hierarchy"))

            try:
                thread_hash = get_hash(page)
            except Exception,e:
                log.exception(self.log_msg('could not build thread_hash'))
                raise e

            #continue if returned true
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    parent_uri, self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out,parent_uri,thread_hash, 
                                             'Thread', self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    if self.task.pagedata.get('ei_num_comments'):
                        page['ei_num_comments'] = self.task.pagedata['ei_num_comments']
                    if self.task.pagedata.get('ei_num_views'):
                        page['ei_num_views'] = self.task.pagedata['ei_num_views']
                    page['path']=[parent_uri]
                    page['parent_path']=[]
                    page['uri'] = normalize(self.currenturi)
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
                    page['task_log_id']=self.task.id
                    page['entity'] = 'Question'
                    page['category']=self.task.instance_data.get('category','')                        
                    self.pages.append(page)
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed. URL is %s" %self.currenturi))
            raise e
        return True



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

        
    def get_date(self, date):
        try:
            posted_date = None
#            date_exp = re.search(re.compile(r'([0-9]*) (hour|hours|minute|minutes|day|days|month|months) ago'),date)
            if len(date.split()[-1].split(':'))>1:
                hours,minutes = map(int,date.split()[-1].split(':'))
            else:
                hours,minutes=None,None
            log.info(date)
            date_exp = re.search(re.compile(r'([0-9]+) (uur|minute|minuten|dagen|dag|week|weken|minuut) geleden'),date)
            if date_exp:
                if date_exp.group(2) in ['dag','dagen']:
                    posted_date = datetime.utcnow()-timedelta(days=int(date_exp.group(1)))
                elif date_exp.group(2) in ['uur']:
                    posted_date =  datetime.utcnow()-timedelta(hours=int(date_exp.group(1)))
                elif date_exp.group(2) in '[minute,minuten]':
                    posted_date =  datetime.utcnow()-timedelta(minutes=int(date_exp.group(1)))
                elif date_exp.group(2) in '[week,weken]':
                    posted_date =  datetime.utcnow()-timedelta(weeks=int(date_exp.group(1)))
            if hours and minutes and posted_date:
                posted_date = posted_date + timedelta(hours=hours,minutes=minutes)

        except:
            log.info(traceback.format_exc())
        return posted_date


    @logit(log,'__getThreads')
    def __getThreads(self):
        """
        Get all the thread links and add them as separate tasks. Ignore reviews and news
        """
        unique_urls=set()
        
        def isValidForumURL(url):
            """The starting pages has all sorts of forum, review and news urls mixed together. Taking only forum urls
            to be valid
            """            
            log.debug(self.log_msg("Checking if the %s is a valid forum thread before enqueuing the task" %url.a['href']))
            _soup = BeautifulSoup(urllib.urlopen(url.a['href']))
            if _soup.find("ul", "tabset"):
                log.info(self.log_msg("This link %s is not a forum thread. Must be either a review or news link. "
                                          "Moving on" %url.a['href']))
                return False
            return True

        # Hold the parenturi temporarily while we get all the urls of the threads
        parenturi = self.currenturi

        # Gather urls in the first thread page
        log.info(self.log_msg("Gathering %d thread urls from %s" %(self.THREAD_ITERATIONS, self.currenturi)))
        thread_urls = self.soup.findAll('td',{'id':re.compile('td_threadtitle_[0-9]+')})
        urls = []
        
        threadCount = 0
        for url in thread_urls:
            if threadCount < self.THREAD_ITERATIONS:
                if isValidForumURL(url):
                    urls.append(url)
                    threadCount += 1
            else:
                log.debug(self.log_msg("Maximum thread count threshold reached in the first page"))
                break

        # Don't bother processing other urls if thread count limit is reached in hte first page itself
        if len(urls) < self.THREAD_ITERATIONS:
            # Gather subsequent thread urls
            try:
                no_pages = int(self.soup.find("div", "pagenav").find("td", "vbmenu_control").renderContents().split()[-1])
            except:
                no_pages = 1
            log.info(self.log_msg('Found %d pages' %no_pages))

            threadCount = len(urls)  
            log.info(self.log_msg('Found %d thread urls in the first page' %threadCount))

            if no_pages > 1:
                for i in range(2, no_pages + 1):
                    if threadCount >= self.THREAD_ITERATIONS:
                        log.debug(self.log_msg("\n========================\nMaximum Thread cound %d reached\n====================" %self.THREAD_ITERATIONS))
                        break
                    
                    self.currenturi = "%s/index%d.html" %(parenturi.strip('/'), i)
                    res=self._getHTML()
                    self.rawpage=res['result']
                    self._setCurrentPage()
                    
                    thread_urls = self.soup.findAll('td',{'id':re.compile('td_threadtitle_[0-9]+')})
                    for url in thread_urls: # Check if the url is a valid forum url. Else ignore them
                        if threadCount >= self.THREAD_ITERATIONS:
                            break
                        if not isValidForumURL(url):
                            continue
                        threadCount += 1
                        urls.append(url)
                
        # # Reset things. Set the soup to the parenturi
        self.currenturi = parenturi
        res=self._getHTML()
        self.rawpage=res['result']
        self._setCurrentPage()
                    
        log.info(self.log_msg('urls == %s'%len(urls)))
        lastPage = False
        threadCount = 0
        for each in urls:
            #threadCount+=1
            #if threadCount < self.THREAD_ITERATIONS:
                url = each.a
                key = md5.md5(url['href']).hexdigest()
                last_url = each.parent.find(text='Laatste pagina')                        
                try:
                    if not checkSessionInfo(self.genre,self.session_info_out, key,
                                            self.task.instance_data.get('update')) \
                                            and not url['href'] in unique_urls:
                        unique_urls.add(url['href'])
                        temp_task = self.task.clone()
                        temp_task.instance_data['uri'] = normalize(url['href'])
                        temp_task.pagedata['title'] = stripHtml(url.renderContents())
                        #log.debug("Last_URL Type %s : %s" %(type(last_url), last_url))
                        #if last_url:
                        #    last_url = last_url['href']
                        #else:
                        last_url = url['href']
                        try:
                            metadata = each.parent.find('td',{'class':'alt1','title':\
                                                            re.compile('Reacties: [0-9]+?, Bekeken: [0-9]+?')})
                            if metadata:
                                comments,views = re.findall('Reacties: ([0-9]+), Bekeken: ([0-9]+)',
                                                        metadata['title'])[0]
                                temp_task.pagedata['ei_num_comments'] = int(comments)
                                temp_task.pagedata['ei_num_views'] = int(views)
                        except:
                            log.exception(self.log_msg('could not extract num_views,num_comments'))

                        temp_task.pagedata['last_url'] = normalize(last_url)
                        updateSessionInfo(self.genre, self.session_info_out, key, '',
                                              'Post', self.task.instance_data.get('update'))

                        self.linksOut.append(temp_task)
                except:
                    log.exception(self.log_msg("exception in adding temptask to linksout"))
                    continue

        self.log_msg('number of tasks appended  == %s'%(len(self.linksOut)))
        return
    
