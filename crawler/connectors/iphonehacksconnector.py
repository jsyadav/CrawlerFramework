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

log = logging.getLogger('IphoneHacksConnector')
class IphoneHacksConnector(BaseConnector):
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        sample url :http://www.iphone-hacks.com/forums/forum-6.html
        '''
        try:
            if 'forums/forum-' in self.currenturi:
                return self.__createTasksForThreads()
               
            else:
                return self.__addThreadAndPosts()
            
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
            self.baseuri = 'http://www.iphone-hacks.com/forums/'
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
            question_post = self.soup.find('div','KonaBody').find('div',id = re.compile('p+'))
            self.__addPost(question_post, True)
            self.__goToLastPage() 
            page_links = self.soup.find('p', 'pagelink conl').findAll('a')
            if page_links:
                current_page = int(stripHtml(page_links[-1].renderContents()))
            main_page_soup = copy.copy(self.soup)  
            while self.__iteratePosts():
                try:
                    current_page -= 1
                    previouspage_link = [each['href'] for each in main_page_soup.\
                                        find('p', 'pagelink conl').findAll('a') if int(stripHtml(each\
                                        .renderContents()))==current_page][-1]
                    self.currenturi = self.baseuri + previouspage_link 
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
            self.baseuri = 'http://www.iphone-hacks.com/forums/'
            self.__last_timestamp = datetime( 1980,1,1 )
            self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                                'iphonehacks_maxthreads'))
            self.__setSoupForCurrentUri()
            current_page = 1
            while self.__processForumUrl():
                try:
                    current_page += 1
                    nextpage_link = [each['href'] for each in self.soup.find('p', 'pagelink conl').\
                                    findAll('a') if int(stripHtml(each.renderContents()))==current_page][0]
                    self.currenturi = self.baseuri + nextpage_link
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
        threads = self.soup.find('div','blocktable').find('div','inbox').findAll('tr')
        for thread in threads[1:]:
            try:
                if thread.find('span','stickytext'):
                    log.info(self.log_msg('Its a Sticky Thread, Ignore it in the \
                                       url %s'%self.currenturi))  
                    continue
                self.__total_threads_count += 1
                if  self.__total_threads_count > self.__max_threads_count:
                    log.info(self.log_msg('Reaching maximum post,Return false \
                                        from the url %s'%self.currenturi))
                    return False  
                try:
                    date_str  = stripHtml(thread.find('td','tcr').find('a').renderContents())
                    thread_time = datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",date_str).\
                                    strip(),"%Y-%m-%d  %H:%M:%S")
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
                    s = thread.find('div','tclcon').find('a')['href']
                    temp_task.instance_data[ 'uri' ] =  self.baseuri + s                                        
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
            posts = self.soup.find('div','KonaBody').findAll('div',id = re.compile('p+'))
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
            hierarchies = [each for each in [stripHtml(x.renderContents())for x in \
                            self.soup.find('div','KonaBody').find('div','linkst').\
                                find('ul').findAll('li')] if each]
            self.hierarchy = hierarchies
            page['data'] = page['title'] = hierarchies[-1].strip()
            page['et_thread_hierarchy'] = self.hierarchy
        except:
            log.exception(self.log_msg('Thread hierarchy is not found'))
            return False    
        if checkSessionInfo(self.genre, self.session_info_out, self.task.instance_data['uri'],\
                                        self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        try:
            date_str = stripHtml(self.soup.find('span','conr').findParent('span').\
                        find('a').renderContents()).strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str.\
                                        strip(),"%Y-%m-%d %H:%M:%S"),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('Posted date not found'%self.currenturi))
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
            pagination_tag = self.soup.find('p','pagelink conl')
            if not pagination_tag:
                return
            uri = None
            last_page_tag = pagination_tag.findAll('a')
            if last_page_tag:
                uri = self.baseuri + last_page_tag[-1]['href']
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
            unique_key = post['id'].replace('p','')
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
            page['uri'] = self.currenturi + stripHtml(post.find('span','conr').renderContents())
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
            page['title'] = self.hierarchy[-1]
            if not is_question:
                page['title'] = 'Re: ' + page['title']
        except:
            log.exception(self.log_msg('data forum not found'%self.currenturi))
            page['title'] = ''
        try:
            data_tag = post.find('div','postmsg')
            quote_tag =  data_tag.find('blockquote')
            lastedit_tag = data_tag.find('p','postedit')
            if quote_tag:
                quote_tag.extract()
            if lastedit_tag:
                lastedit_tag.extract()
            page['data'] = stripHtml(data_tag.renderContents())
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''        
        
        if not page['title'] and not page['data']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False                    
        try:
            date_str = stripHtml(self.soup.find('span','conr').findParent('span').
                        find('a').renderContents()).strip()
            page['posted_date'] =  page['posted_date'] = datetime.strftime(datetime.strptime(date_str.\
                                        strip(),"%Y-%m-%d %H:%M:%S"),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('Posted date not found'%self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), \
                                                         "%Y-%m-%dT%H:%M:%SZ")             
        try:
            page['et_author_name'] = stripHtml(post.find('div','postleft').find('dt').\
                                        renderContents())
        except:
            log.exception(self.log_msg('author name not found'%self.currenturi))
            
        try:
            page['et_author_category'] = stripHtml(post.find('dd','usertitle').renderContents())
        except:
            log.exception(self.log_msg('author category not found')%self.currenturi)  
            
        try:              
            author_info = [stripHtml(each.renderContents())for each in post.\
                            find('div','postleft').findAll('dd')]
            for each in author_info:
                if re.search('Registered:', each):
                    joined_date = each.replace('Registered:','').strip()
                    page['edate_author_joined_date'] = datetime.strftime(datetime.strptime(joined_date.\
                                        strip(),"%Y-%m-%d"),"%Y-%m-%dT%H:%M:%SZ")
                if re.search('Posts:', each):                        
                    page['ei_author_posts_count'] = int(each.replace('Posts:','').replace(',','').strip())
                if re.search('From:', each):
                    page['et_author_location'] = each.replace('From:','').strip() 
        except:
            log.exception(self.log_msg('cannot find the author info'%self.currenturi))
        
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
          
