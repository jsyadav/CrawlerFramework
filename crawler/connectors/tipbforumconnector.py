#prerna

import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime,timedelta
from cgi import parse_qsl
from urllib import urlencode

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('TipbForumConnector')
class TipbForumConnector(BaseConnector):
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        '''
        try:
            if '.html' in self.currenturi:
                return self.__addThreadAndPosts()
            else:
                return self.__createTasksForThreads()
            
                #this will fetch the thread links and Adds Tasks               
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'\
                                                            %self.currenturi))
        return True
    
    @logit(log, '__addThreadAndPosts')
    def __addThreadAndPosts(self): 
        """
        This will add the Thread info from setParentPage method and 
        Add the posts of the thread in addPosts mehtod
        """
        try:
            self.genre = "Review"
            self.__hierarchy = []
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
            self.__setHierarchy()
            question_post = self.soup.find('div', id=re.compile('^edit.*?'))
            self.__addPost(question_post, True)
            self.__goToLastPage() 
            main_page_soup = copy.copy(self.soup)  
            while self.__iteratePosts():
                try:
                    self.currenturi = main_page_soup.find('a', title = re.compile('Prev Page -'))['href']
                    self.__setSoupForCurrentUri()
                    main_page_soup = copy.copy(self.soup)
                except:
                    log.exception(self.log_msg('Next Page link for aadtrhead not found for url %s'%self.currenturi))
                    break
            return True
        except:
            log.exception(self.log_msg('Exception while add the theread posts \
                                            for the url %s'%self.currenturi))
        return True
        
    @logit(log, '__createTasksForThreads')
    def __createTasksForThreads(self):
        
        """
        This will create Tasks for the threads found on the given url
        The # of Tasks are limited by Config Variable
        """
        try:
                    
            self.__total_threads_count = 0
            self.__last_timestamp = datetime( 1980,1,1 )
            self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                                'tipbforum_maxthreads'))
            self.__setSoupForCurrentUri()
            while self.__processForumUrl():
                try:
                    self.currenturi = self.soup.find('a',title = re.compile('Next Page - '))['href']
                    self.__setSoupForCurrentUri()
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
            log.info(self.log_msg('Exception while creating tasks for the url %s'\
                                                         %self.currenturi)) 
            return False
        
    @logit(log, '__processForumUrl')
    def __processForumUrl(self):
        """
        It will fetch each thread and its associate infomarmation
        and add the tasks
        """
        threads = [each.findParent('tr') for each in self.soup.find('table',\
             id='threadslist').findAll('td', id=re.compile('td_threadtitle_'))]
        for thread in threads:
            try:
                if thread.find('img', src=re.compile('sticky.gif$')):
                    log.info(self.log_msg('Its a Sticky Thread, Ignore it in the \
                                       url %s'%self.currenturi))  
                    continue
                self.__total_threads_count += 1
                if  self.__total_threads_count > self.__max_threads_count:
                    log.info(self.log_msg('Reaching maximum post,Return false \
                                        from the url %s'%self.currenturi))
                    return False  
                try:
                    th=thread.findAll('td',recursive=False)
                    date_tag = stripHtml(th[3].renderContents()).splitlines()\
                                                                  [0] .strip() 
                    if date_tag.startswith('Today'):
                        date_tag = date_tag.replace('Today', datetime.strftime\
                                                (datetime.utcnow(),'%d %B %Y'))
                        thread_time = datetime.strptime(date_tag,"%d %B %Y  %H:%M %p")                                     
                    elif date_tag.startswith('Yesterday'):
                        date_tag = date_tag.replace('Yesterday', datetime.\
                                    strftime((datetime.utcnow()-timedelta\
                                         (days=1)),'%d %B %Y')) 
                        thread_time = datetime.strptime(date_tag,"%d %B %Y  %H:%M %p")
                    else:                                                      
                        thread_time = datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",date_tag).\
                                        strip(),"%m-%d-%Y  %H:%M %p") 
                except:
                    log.exception(self.log_msg('last post not found in the url\
                                                    %s'%self.currenturi)) 
                if checkSessionInfo('Search', self.session_info_out, thread_time, \
                                self.task.instance_data.get('update')):
                    log.info(self.log_msg('Session info Returns True for %s'%\
                                                        self.currenturi))
                    log.info(thread_time)
                    return False
                self.__last_timestamp = max(thread_time , self.__last_timestamp )
                temp_task=self.task.clone() 
                try:
                    s=th[2].find('a', id=re.compile('thread_title_\d+'))['href']                                           
                    temp_task.instance_data[ 'uri' ] = s
                except:
                    log.exception(self.log_msg('uri not found in the url\
                                                    %s'%self.currenturi)) 
                    continue 
                self.linksOut.append(temp_task)
            except:
                log.exception(self.log_msg('links not found'))      
        return True                                    
    
    @logit(log, '__iteratePosts')
    def __iteratePosts(self):   
        
        try:
            posts = self.soup.find('div', id='posts').findAll('table',id = re.compile('^post\d+'))
            if not posts:
                log.info(self.log_msg('No posts found'))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(posts)))
            posts.reverse()
            for post in posts:
                if not self.__addPost(post):
                    return False
            return True    
        except:
            log.exception(self.log_msg('can not  find the data'))
            return False    
        
    @logit(log,'__setHierarchy')
    def __setHierarchy(self):
        """
        """
        page = {}
        try: 
            hierarchies = [each for each in [stripHtml(x.renderContents())\
                            for x in self.soup.find('table','tborder').table.findAll('td')] if each]
            self.hierarchy = [x.strip() for x in hierarchies[0].split('>')]
            page['data'] = page['title'] = hierarchies[-1].splitlines()[-1].strip()
            self.hierarchy.append(page['title'])
            page['et_thread_hierarchy'] = self.hierarchy
        except:
            log.exception(self.log_msg('Thread hierarchy is not found'))
            return False    
        if checkSessionInfo(self.genre, self.session_info_out, self.task.instance_data['uri'],\
                                        self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        try:
            date_str =  stripHtml(self.soup.find('table',id=re.compile('post\d+')).\
                            find('td').renderContents()).split('\t')[-1].strip()  
            
            if date_str.startswith('Today'):
                date_str = date_str.replace('Today', datetime.strftime\
                                                (datetime.utcnow(),'%d %B %Y')) 
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str,\
                                    '%d %B %Y, %H:%M %p'),"%Y-%m-%dT%H:%M:%SZ")                                              
            elif date_str.startswith('Yesterday'):
                date_str = date_str.replace('Yesterday', datetime.\
                                    strftime((datetime.utcnow()-timedelta\
                                    (days=1)),'%d %B %Y'))
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%d %B %Y, %H:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
            else:
                page['posted_date'] = datetime.strftime(datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",date_str).\
                                        strip(),"%m-%d-%Y, %H:%M %p"),"%Y-%m-%dT%H:%M:%SZ") 
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), \
                                                         "%Y-%m-%dT%H:%M:%SZ")                            
        try:
            result = updateSessionInfo('review', self.session_info_out, self.\
                task.instance_data['uri'], get_hash(page), 'forum', \
                                    self.task.instance_data.get('update'))
            if result['updated']:
                page['path'] = [self.task.instance_data['uri']] 
                page['parent_path'] = []
                page['uri'] = self.currenturi
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])                
                page['entity'] = 'thread'
                page.update(self.__task_elements_dict)
                self.pages.append(page)
            else:
                log.exception(self.log_msg('Result[updated] returned True for \
                                                        uri'%self.currenturi))
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))

    @logit(log, '__goToLastPage')
    def __goToLastPage(self):
        """This will set the soup the last page of the post
        """
        try:
            pagination_tag = self.soup.find('div', 'pagenav')
            if not pagination_tag:
                return
            uri = None
            last_page_tag = pagination_tag.find('a', title=re.compile('Last Page'))
            if last_page_tag:
                uri = last_page_tag['href']
            else:
                last_page_tag = pagination_tag.findAll('a', href=True, text=re.compile('^\d+$'))
                if last_page_tag:
                    uri = last_page_tag[-1].parent['href']
            if not uri:
                log.info(self.log_msg('Post found in only one page'))
                return
            self.currenturi = uri
            self.__setSoupForCurrentUri()
        except:
            log.exception(self.log_msg('Last page cannot find from the given page no \
                                    for url %s'%self.task.instance_data['uri']))

    
    @logit(log,'__addPost')    
    def __addPost(self, post, is_question = False): 
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            post_id = post.find('a',id=re.compile('postcount\d+'))            
            unique_key = post_id['id'].replace('postcount','')
            #log.info(unique_key)
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                         self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True'))
                return False
            page = self.__getData(post, is_question)
        except:
            #log.info(post)
            log.exception(self.log_msg('Cannot add the post for the url %s'%\
                                                            self.currenturi))
            return False
        try:
            page['uri'] = self.currenturi + '#' + stripHtml(post_id.renderContents()) 
        except:
            log.exception(self.log_msg('Cannot find the uri'))
            page['uri'] = self.currenturi
        try:
            result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                get_hash( page ),'Review', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if not result['updated']:
                log.exception(self.log_msg('Update session info returns False'))
                return True
            page['parent_path'] = [self.task.instance_data['uri']]
            page['path'] = [self.task.instance_data['uri'] ,unique_key ] 
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            #log.info(page)
            log.info(self.log_msg('Post Added'))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False  
          
    
    def __getData(self, post, is_question):
        page = {'entity':'question' if is_question else 'answer'}
        
        try:
            page['title']=self.hierarchy[-1]
            if not is_question:
                 page['title'] = 'Re: ' + page['title']
        except:
            log.exception(self.log_msg('data forum not found'))
            page['title'] = ''
            
        try:
            data_tag = post.find('div', id=re.compile('post_message_\d+'))
            previous_message_tag = data_tag.find('div', text='Quote:')
            if previous_message_tag:
                previous_message_tag.parent.findParent('div').extract()
            page['data'] = stripHtml(data_tag.renderContents())
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''        
        
        if not page['title'] and not page['data']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False                    
        try:
            date_str = stripHtml(post.find('td').renderContents()).split('\t')[-1].strip()
            if date_str.startswith('Today'):
                date_str = date_str.replace('Today', datetime.strftime\
                                                (datetime.utcnow(),'%d %B %Y')) 
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str,\
                                    '%d %B %Y, %H:%M %p'),"%Y-%m-%dT%H:%M:%SZ")                                              
            elif date_str.startswith('Yesterday'):
                date_str = date_str.replace('Yesterday', datetime.\
                                    strftime((datetime.utcnow()-timedelta\
                                    (days=1)),'%d %B %Y'))
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%d %B %Y, %H:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
            else:
                page['posted_date'] = datetime.strftime(datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",date_str).\
                                        strip(),"%m-%d-%Y, %H:%M %p"),"%Y-%m-%dT%H:%M:%SZ") 
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), \
                                                         "%Y-%m-%dT%H:%M:%SZ")             
        try:
            page['et_author_name'] = stripHtml(post.find('a','bigusername').\
                                               renderContents()) 
        except:
            log.exception(self.log_msg('author name not found'))
            
        try:
            page['et_author_category'] = stripHtml(post.find('div',id=re.\
                    compile('postmenu_\d+')).findNext('div').renderContents())
        except:
            log.info(self.log_msg('author category not found'))  
            
        try:              
            author_info = stripHtml(post.find('a','bigusername').findParent('td').\
                            findNext('td',valign = 'top',nowrap = 'nowrap').renderContents()).\
                            splitlines()
            for each in author_info:
                try:
                    if re.search('Join Date:',each):
                        join_date = '01 ' + re.sub('Join Date:', '', each).strip()
                        page['edate_author_joined_date'] =datetime.strftime\
                        (datetime.strptime(join_date,'%d %b %Y'),"%Y-%m-%dT%H:%M:%SZ")
                    if re.search('Location:',each):  
                        page['et_author_location'] = re.sub('Location:', '', each).strip()
                    if re.search('Age:',each):     
                        page['ei_author_age'] = int(re.sub('Age:','', each).strip()) 
                    if re.search('Posts:',each):     
                        page['ei_author_posts_count'] = int(re.sub('Posts:', '', each).\
                                                        replace(',','').strip()) 
                    if re.search('Thanks:',each):     
                        page['ei_author_thanks'] = int(re.sub('Thanks:','', each).replace(',','').strip())     
                    if re.search('Thanked',each):     
                        page['et_author_thanked'] = re.sub('Thanked','', each).split('Times')[0].strip()
                except:
                    log.exception(self.log_msg('cannot find the author info'))
        except:
            log.exception(self.log_msg('author posts count not found')) 
        
        return page                                                                                                                                                                                      
    @logit(log, '__setSoupForCurrentUri')                                                                                 
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
          
