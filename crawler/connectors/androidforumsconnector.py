'''
Copyright (c)2008-2010 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#prerna

import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime,timedelta

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('AndroidForumsConnector')
class AndroidForumsConnector(BaseConnector):
    '''
    This will fetch the info for androidforums.com
    Sample uris is
    http://androidforums.com/motorola-droid/
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of android forums
        """
        try:
            if '.html' in self.currenturi:
                if re.search('index\d+.html',self.currenturi):
                    return self.__createTasksForThreads()
                else:
                    return self.__addLinksAndPosts()
            else:
                return self.__createTasksForThreads()
                #this will fetch the thread links and Adds Tasks                
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'\
                                                            %self.currenturi))
        return True
    
    @logit(log, '__createTasksForThreads')
    def __createTasksForThreads(self):
        
        """
        This will create Tasks for the threads found on the given url
        The # of Tasks are limited by Config Variable
        """
        try:
                    
            self.__total_threads_count = 0
            self.__baseuri = 'http://androidforums.com/motorola-droid/'
            self.__last_timestamp = datetime( 1980,1,1 )
            self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                                'androidforums_maxthreads'))
            self.__setSoupForCurrentUri()
            main_page_soup = copy.copy(self.soup)
            #c = 0
            while self.__getThreads():
                try:
                    self.currenturi =  main_page_soup.find('a', rel='next')['href']
                    self.__setSoupForCurrentUri()
                    main_page_soup = copy.copy(self.soup)
##                    c += 1
##                    if c >1:
##                        break 
                except:
                    log.exception(self.log_msg('Next Page link not found for url \
                                                        %s'%self.currenturi))
                    break                                  
            log.info(self.log_msg('LINKSOUT: ' + str(len(self.linksOut))))
            #self.linksOut = [] # To Remove
            if self.linksOut:
                updateSessionInfo('Search', self.session_info_out, \
                            self.__last_timestamp , None, 'ForumThreadsPage', \
                            self.task.instance_data.get('update'))
            return True  
        except:
            log.exception(self.log_msg('Exception while creating tasks for the url %s'\
                                                         %self.currenturi)) 
            return False

    
    def __getThreads(self):
            """
            It will fetch each thread and its associate infomarmation
            and add the tasks
            """
            threads = [each.findParent('tr') for each in self.soup.find('table',\
                id='threadslist').findAll('td', id=re.compile('td_threadtitle_'))]
            if not threads:
                log.info(self.log_msg('No threads are found for url %s'%\
                                                            self.currenturi))
                return False
            for thread in threads:
                try:
                    #pagedata = {}
                    if thread.find('b', text='Sticky:'):
                        # Sticky posts are not needed (for this Site), It contains the post about Guidelines
                        log.info(self.log_msg('It is Sticky Thread, Not required'))
                        continue
                    
                    self.__total_threads_count += 1
                    if  self.__total_threads_count > self.__max_threads_count:
                        log.info(self.log_msg('Reaching maximum post,Return false \
                                            from the url %s'%self.currenturi))
                        return False  
                    thread_info = thread.findAll('td', recursive=False)
                    if not len(thread_info)==6:
                        # Each thread is table record, containing 6 table data
                        log.info(self.log_msg('No Enough Fields, Ignoring the thread'))
                        continue
                    try:
                        date_str , last_post_author = [x.strip() for x in stripHtml\
                            (thread_info[3].find('div','smallfont').renderContents())\
                                                .split('\n') if not x.strip()=='']
                        #Posted date cannot be fetched exactly, since Timezone is not not known
                        #So, Ignore the post, During Next Crawl, It will be picked up
                        if date_str.startswith('Today') or date_str.startswith('Yesterday'):
                            date_str = self.__getDateStr(date_str)
                            thread_time = datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",date_str).\
                                            strip(),"%B %d, %Y%I:%M %p")
                        else:
                            thread_time = datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",date_str).\
                                            strip(),"%B %d, %Y %I:%M %p")                    
                        log.info(thread_time)                    
                            
    ##                        pagedata['edate_last_post_date']= datetime.\
    ##                                                        strptime(date_str, '%B %d, %Y, %I:%M %p').\
    ##                                                        strftime("%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.exception(self.log_msg('Cannot fetch the date'))
                        #continue
                    if checkSessionInfo('Search', self.session_info_out, thread_time, \
                                    self.task.instance_data.get('update')):
                        log.info(self.log_msg('Session info Returns True for %s'%\
                                                            self.currenturi))
                        log.info(thread_time)
                        return False
                    self.__last_timestamp = max(thread_time , self.__last_timestamp )
                    temp_task=self.task.clone() 
                    try:
                        link = thread_info[2].find\
                                ('a', id=re.compile('thread_title'))['href']
                        temp_task.instance_data[ 'uri' ] = link
                    except:
                        log.exception(self.log_msg('Thread votes not found for url\
                                                            %s'%self.currenturi)) 
                        continue
                    try:
                        if last_post_author.startswith('by '):
                            temp_task.pagedata['et_thread_last_post_author']  =  last_post_author.\
                                                                        replace('by ','')
                             
                    except:
                        log.exception(self.log_msg('last_post_auhor not found'))                                                                                      
                    try:
                        author_and_rating_tag = thread_info[2].find('div', 'smallfont')
                        temp_task.pagedata['et_author_name'] = stripHtml(author_and_rating_tag.\
                                                    renderContents())                                                            
                       
                        temp_task.pagedata['ei_thread_num_views'] = int(stripHtml(thread_info[-1].\
                                                            renderContents()).replace(',',''))
                        temp_task.pagedata['ei_thread_num_replies'] = int\
                                (stripHtml(thread_info[-2].renderContents()).replace(',',''))
                                
                        temp_task.pagedata['edate_last_post_date']= datetime.strftime\
                                                (thread_time, "%Y-%m-%dT%H:%M:%SZ")    
                        try:            
                            rating_str = author_and_rating_tag.find('img', \
                                                    'inlineimg')['alt'].split(',')
                            temp_task.pagedata['ef_thread_rating'] =  float(\
                                        rating_str[1].replace('average.',''))
                        except:
                            log.exception(self.log_msg('Thread rating not found for url\
                                                        %s'%self.currenturi))
                        try:
                            temp_task.pagedata['ei_thread_votes_count'] = int(re.search\
                                                ('\d+',rating_str[0]).group())
                        except:
                            log.exception(self.log_msg('Thread votes not found for url\
                                                        %s'%self.currenturi))
                    except:
                        log.exception(self.log_msg('page data not found for url\
                                                            %s'%self.currenturi))
                    self.linksOut.append(temp_task)                                        
                except:
                    log.exception(self.log_msg('no link found'))
                                                                                                                                  
            return True
        
    @logit(log, '__addLinksAndPosts')
    def __addLinksAndPosts(self): 
        """
        This will add the link info from setParentPage method and 
        Add the posts  addPosts mehtod
        
        """
        try:
            #self.currenturi =  link
            self.genre = "Review"
            self.__task_elements_dict = {
                            'priority':self.task.priority,
                            'level': self.task.level,
                            'last_updated_time':datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ"),
                            'pickup_date':datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ"),
                            'connector_instance_log_id': self.task.connector_instance_log_id,
                            'connector_instance_id':self.task.connector_instance_id,
                            'workspace_id':self.task.workspace_id,
                            'client_id':self.task.client_id,
                            'client_name':self.task.client_name,
                            'versioned':False,
                            'category':self.task.instance_data.get('category',''),
                            'task_log_id':self.task.id }
            self.__setSoupForCurrentUri()
            self.__setParentPage()
            try:
                question_post = self.soup.find('div', id=re.compile ('^edit\d+')).extract()
            except:
                log.exception(self.log_msg('Question post cannot be added'))
            main_page_soup = copy.copy(self.soup)
            try:
                self.__addPage(question_post, True)
            except:
                log.exception(self.log_msg('page not added'))
            self.__goToLastPage(main_page_soup)
            #c =0
            while True:
                main_page_soup = copy.copy(self.soup)
                if not self.__addPosts():
                    break
                try:
                    self.currenturi =  main_page_soup.find('a',text='&lt;').parent['href']
                    self.__setSoupForCurrentUri()
                    main_page_soup = copy.copy(self.soup)
##                    c +=1
##                    if c >1:
##                        break
                except:
                    log.exception(self.log_msg('No Previous URL found for url \
                                                        %s'%self.currenturi))
                    break
            return True
        except:
            log.exception(self.log_msg('Exception while add the theread posts \
                                            for the url %s'%self.currenturi))
            return False
        
    @logit(log, '__setParentPage')
    def __setParentPage(self):
        """
        This will get the parent info
        """
        page = {}
        try:
            hierarchies = [each for each in [stripHtml(x.renderContents()) for x in self.soup.find('table','tborder').table.findAll('td')] if each]
            self.hierarchy = [x.strip() for x in hierarchies[0].split('>')]
            page['title'] = hierarchies[1]
            self.hierarchy.append(page['title'])
            page['et_thread_hierarchy'] = self.hierarchy
        except:
            log.exception(self.log_msg('Thread hierarchy is not found'))
            return False
        if checkSessionInfo(self.genre, self.session_info_out, self.task.instance_data['uri'],\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        page_data_keys = ['et_author_name', 'ei_thread_num_views', \
                            'ei_thread_num_replies','ef_thread_rating','edate_last_post_date',\
                            'ei_thread_votes_count','et_thread_last_post_author']
        [page.update({each:self.task.pagedata.get(each)}) for each in \
                                page_data_keys if self.task.pagedata.get(each)] 
        try:
            date_str = stripHtml(self.soup.find('div', id='posts').find('td', \
                                                    'thead').renderContents())
            if date_str.startswith('Today') or date_str.startswith('Yesterday'):
                date_str = self.__getDateStr(date_str)
            page['posted_date'] =  datetime.strftime(datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",date_str).\
                                        strip(),'%B %d, %Y, %I:%M %p'), "%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            log.exception(self.log_msg('Posted date not found for the url %s'%self.currenturi))
        try:
            result = updateSessionInfo('review', self.session_info_out, self.\
                task.instance_data['uri'], get_hash( page ), 'Forum', self.task.instance_data.get('update'))
            if not result['updated']:
                return False
            page['path']=[ self.task.instance_data['uri'] ] 
            page['parent_path']=[]
            page['uri'] = self.currenturi
            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
            page['data'] = ''
            page['entity'] = 'thread'
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            log.info(self.log_msg('Parent Page added'))
            return True
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False    
        
    @logit(log , '__addPosts')
    def __addPosts(self):
        """
        It will add the posts for a particular thread
        """
        try:
            posts = self.soup.findAll('div', id=re.compile ('^edit\d+'))
            posts.reverse()
            if not posts:
                log.info(self.log_msg('No posts found'))
                return False
            log.debug(self.log_msg('Total No of Reviews found is %d'%len(posts)))
            for post in posts:
                if not self.__addPage(post):
                    return False
            return True
        except:
            log.exception(self.log_msg('Reviews are not found for url %s'%self.currenturi))
            return False
        
    @logit(log, '__addPage')
    def __addPage(self, post, question=False):
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            unique_key = post.find('a', text='permalink').parent['href']
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                         self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True'))
                return False
            page = self.__getData(post, question, unique_key)
        except:
            log.exception(self.log_msg('Cannot add the post for the url %s'%\
                                                            self.currenturi))
            return False
        try:
            result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                get_hash( page ),'Review', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if not result['updated']:
                log.info(self.log_msg('Update session info returns False'))
                return False
            page['parent_path'] = page['path'] = [self.task.instance_data['uri']]
            page['path'].append(unique_key)
            page['uri'] = unique_key
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            log.info(self.log_msg('Post Added'))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False   
        
    @logit(log,'__getDateStr')
    def __getDateStr(self,date_str):
        '''
        This get the str which starts with Today or Yesterday and return
        the timestamp
        '''
        try:
            today_str = 'Today'
            yester_day_str = 'Yesterday'
            if date_str.startswith(today_str):
                #date_str = datetime.strftime(datetime.utcnow(),"%m-%d-%Y ") + \
                 #                       date_str.replace(today_str, '').strip()
                date_str = datetime.strftime(datetime.utcnow(),"%B %d, %Y") + \
                                    date_str.replace(today_str, '').strip()
            elif date_str.startswith(yester_day_str):
                #date_str = datetime.strftime(datetime.utcnow()-timedelta(days=1)\
                 #   ,"%m-%d-%Y ") + date_str.replace(yester_day_str, '').strip()
                date_str = datetime.strftime(datetime.utcnow()-timedelta(days=1)\
                    ,"%B %d, %Y") + date_str.replace(yester_day_str, '').strip()
            log.info(date_str)        
            return date_str
        except:
            log.exception(self.log_msg('date cannot be formed %s'%date_str))     
    
    @logit(log, '__setLastPage')
    def __goToLastPage(self, main_page_soup):
        """
        This will set the soup the last page of the post, from here 
        It needs to be navigated in the reverse order.
        If the posts are in within 3 pages, There won't be any Last Page Tag
        Then find max of page no ( 2 or 3) and Navigate to the page
        find out the List of posts and reverse it and then start adding it
        First add the Question, then above steps will be done, 
        session info will take of duplicates posts not adding to pages
        """
        last_page_tag = None
        try:
            last_page_tag = main_page_soup.find('div', 'pagenav').find(title=re.compile('Last Page'))
        except:
            log.exception(self.log_msg('Last page tag not found for url %s'%self.task.instance_data['uri']))
        try:
            if not last_page_tag:
                last_page_tag = main_page_soup.find('div', 'pagenav').findAll('a',\
                                        text=re.compile ('^\d+$'))[-1].parent
        except:
            log.exception(self.log_msg('Last page cannot find from the given page no'))  
        try:
            if not last_page_tag:
                log.info(self.log_msg('Posts exists only in the current page'))
                self.soup = main_page_soup
                return True
            self.currenturi = last_page_tag['href']
            self.__setSoupForCurrentUri()
            return True
        except:
            log.exception(self.log_msg('Exception occurred while Setting the last page'))
            return False
        
    @logit(log, '__getData')
    def __getData(self, post, question, permalink):
        """ This will return the page dictionry
        """
        page = {}
        try:
            page['entity'] = 'question' if question else 'answer'
        except:
            log.exception(self.log_msg('question type not found for url %s'%permalink))
            page['entity'] = 'thread'
        try:
            data_tag = post.find('div', id=re.compile('post_message_'))
            previous_message_tag = data_tag.find('div', text='Quote:')
            if previous_message_tag:
                previous_message_tag.parent.findParent('div').extract()
            page['data'] = stripHtml(data_tag.renderContents())
        except:
            log.exception(self.log_msg('Title not found for the url %s'%permalink))
            page['data'] = ''        
        try:
            author_tag = post.find('a','bigusername')
            page['et_author_name'] = stripHtml(author_tag.renderContents())
        except:
            log.exception(self.log_msg('author name not found for the url %s'%permalink))            
        try:
            page['et_author_category'] = stripHtml(post.find('div','smallfont')\
                                                            .renderContents())
        except:
            log.exception(self.log_msg('Author category not found for url %s'\
                                                         %self.currenturi))
        copycurrenturi = self.currenturi                                                   
        try:
            self.currenturi = page['et_author_profile'] = author_tag['href']
            if self.__setSoupForCurrentUri():
                info_tags = self.soup.find('div', id='profile_tabs').findAll(True,'shade')
                for each in info_tags:
                    try:
                        if each.name == 'span':
                            span_value = stripHtml(each.renderContents())
                            if span_value.endswith(':'):
                                key = span_value[:-1].lower().replace(':','').strip().replace(' ','_')
                                value = str(each.next.next).strip()
                                page[ 'et_author_' + key ] = value
                        elif each.name =='dt':
                            key = 'et_author_' + stripHtml(each.renderContents()).lower().replace(' ','_')
                            page[key] = stripHtml(each.findNext('dd').renderContents())
                        else:
                            log.info(self.log_msg('Unknown Tag'))
                    except:
                        log.exception(self.log_msg('Author Info not found from the url %s'%self.currenturi))
        except:
            log.exception(self.log_msg('author name not found for the url %s'%self.currenturi))
        try:
            page['et_thread_forum'] = self.hierarchy[-3]
            page['et_thread_subforum'] = self.hierarchy[-2]
            page['title'] = page['et_thread_topic'] = self.hierarchy[-1]
            if not question:
                 page['title'] = 'Re: ' + page['title']
        except:
            log.exception(self.log_msg('data forum not found'))
            page['title'] = ''
            
        if not page['title'] and not page['data']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%(permalink)))
            return False
        try:
            date_str = stripHtml(post.find('td', 'thead').renderContents())
            if date_str.startswith('Today') or date_str.startswith('Yesterday'):
                date_str = self.__getDateStr(date_str)
            #page['posted_date'] =  datetime.strftime(datetime.strptime(date_str.replace(' ',''), \
                                #'%m-%d-%Y,%I:%M%p'), "%Y-%m-%dT%H:%M:%SZ")
            log.info(date_str)                    
            page['posted_date'] =datetime.strftime(datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",date_str).\
                                        strip(),"%B %d, %Y, %I:%M %p"),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            log.exception(self.log_msg('Posted date not found for the url %s'%permalink))
        self.currenturi = copycurrenturi       
        return page
            
    @logit(log,'__setSoupForCurrentUri')
    def __setSoupForCurrentUri(self, data=None, headers={}):
        """It will set soup object for the Current URI
        """
        res = self._getHTML(data=data, headers=headers)
        if res:
            self.rawpage = res['result']
        else:
            log.info(self.log_msg('Page Content cannot be fetched for the url: \
                                                            %s'%self.currenturi))
            raise Exception('Page content not fetched for th url %s'%self.currenturi)
        self._setCurrentPage()
        return True        