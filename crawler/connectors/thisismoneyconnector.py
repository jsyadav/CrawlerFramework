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

log = logging.getLogger('ThisIsMoneyConnector')
class ThisIsMoneyConnector(BaseConnector):
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        '''
        try:
            if 'forum.jsp' in self.currenturi:
                return self.__createTasksForThreads()
            elif 'search.jsp' in self.currenturi:
                log.info('search page')
                return self.__createTaskForSearchResults()
            else:
                return self.__addThreadAndPosts()
                #this will fetch the thread links and Adds Tasks                
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'\
                                                            %self.currenturi))
            return False
    
    @logit(log, '__addThreadAndPosts')
    def __addThreadAndPosts(self): 
        """
        This will add the Thread info from setParentPage method and 
        Add the posts of the thread in addPosts mehtod
        """
        try:
            self.genre = "Review"
            self.__hierarchy = []
            self.__baseuri = 'http://boards.thisismoney.co.uk/tim/'
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
            self.__iteratePosts()
        except:
            log.exception(self.log_msg('Exception while add the theread posts \
                                            for the url %s'%self.currenturi))
            return False
        return True
    
    @logit(log, '__createTaskForSearchResults')
    def __createTaskForSearchResults(self):
        '''This will get the search link url and add the tasks
        '''
        try:
            self.__total_threads_count = 0
            self.__baseuri = 'http://boards.thisismoney.co.uk/tim/'
            self.__last_timestamp = datetime( 1980,1,1 )
            self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                                'thisismoney_maxthreads'))
            self.__setSoupForCurrentUri()
            while self.__processSearchPage():
                try:
                    self.currenturi = self.__baseuri + self.soup.find('a', title='Next Page')['href']
                    self.__setSoupForCurrentUri()
                except:
                    log.info(self.log_msg('Processed all the pages'))
                    break
                                                
        except:
            log.exception(self.log_msg('Cannot add tasks'))
        #log.info(self.log_msg('Total len of linksout %d'%len(self.linksOut)))
        #self.linksOut = []
        return True
    
    @logit(log, '__processSearchPage')
    def __processSearchPage(self):
        '''This will process the search page
        '''
        try:
            results = self.soup.find('div', 'mbPanel').findAll('div', recursive=False)
            if not results:
                return False
            for result in results:
                self.__total_threads_count += 1
                if self.__total_threads_count > self.__max_threads_count:
                    log.info(self.log_msg('Reached Max thread count'))
                    return False
                try:
                    temp_task = self.task.clone()
                    temp_task.instance_data['uri'] = self.__baseuri + result.findAll('a', 'bd')[1]['href'].split('#')[0]
                    self.linksOut.append(temp_task)
                except:
                    log.info(self.log_msg('task cannot be added '))
            return True
        except:
            log.exception(self.log_msg('No Results found'))
            return False
            
            
    
        
    @logit(log, '__createTasksForThreads')
    def __createTasksForThreads(self):
        
        """
        This will create Tasks for the threads found on the given url
        The # of Tasks are limited by Config Variable
        """
        try:
                    
            self.__total_threads_count = 0
            self.__baseuri = 'http://boards.thisismoney.co.uk/tim/'
            self.__last_timestamp = datetime( 1980,1,1 )
            self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                                'thisismoney_maxthreads'))
            self.__setSoupForCurrentUri()
            #temp_count = 0
            while self.__processForumUrl():
                try:
                    next_page_uri =self.__baseuri + self.soup.find('a', title='Next Page')['href']
                    data_dict = dict(parse_qsl(next_page_uri.split('?')[-1]))
                    if 's' in data_dict.keys():
                        data_dict.pop('s')
                    self.currenturi = self.__baseuri + 'forum.jsp =?'+ urlencode(data_dict)                    
                    self.__setSoupForCurrentUri()
##                    temp_count += 1
##                    if temp_count>=200:
                        #break
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
        
    @logit(log, '__processForumUrl')
    def __processForumUrl(self):
        """
        It will fetch each thread and its associate infomarmation
        and add the tasks
        """
        threads = self.soup.find('table',summary='List of all the current boards').\
                    find('tbody').findAll('tr')
        for thread in threads:
            try:
                self.__total_threads_count += 1
                if  self.__total_threads_count > self.__max_threads_count:
                    log.info(self.log_msg('Reaching maximum post,Return false \
                                        from the url %s'%self.currenturi))
                    return False  
                
                try:
                    th = stripHtml(thread.renderContents()).split('\n')
                    date_tag = th[0].strip()
                                                
                    thread_time = datetime.strptime(date_tag,'%d/%m/%y')
                except:
                    log.exception(self.log_msg('last post date not found in the url\
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
                    
                    s= thread.find('td','link ').find('a')['href']                                         
                    temp_task.instance_data[ 'uri' ] = self.__baseuri + re.sub\
                                ('\?s=.*?&','?',s)
                except:
                    log.exception(self.log_msg('uri not found in the url\
                                                    %s'%self.currenturi)) 
                    continue 
                try:
                    temp_task.pagedata['edate_last_post_date'] = datetime.strftime\
                                                (thread_time, "%Y-%m-%dT%H:%M:%SZ")
                    log.info(temp_task.pagedata['edate_last_post_date'])                                                                                       
                except: 
                    log.info(self.log_msg('last post date not found in the url\
                                                    %s'%self.currenturi))   
                try:    
                    temp_task.pagedata['et_first_author_name'] =th[-2].strip()
                    log.info(temp_task.pagedata['et_first_author_name'])
                except:
                    log.info(self.log_msg('author name  not found in the url\
                                                    %s'%self.currenturi))    
                
                try:    
                    temp_task.pagedata['ei_thread_replies_count'] = int(th[-1].strip())
                    log.info(temp_task.pagedata['ei_thread_replies_count'])
                except:
                    log.info(self.log_msg('reply  not found in the url\
                                                    %s'%self.currenturi)) 
                self.linksOut.append(temp_task)
            except:
                log.exception(self.log_msg('links not found'))      
        return True                                    
    
    @logit(log,'__setParentPage')
    def __setParentPage(self):
        """ this will set parent page info """
        
        page = {}
        try: 
            page['title']  = stripHtml(self.soup.find('div','brdSubHd grey top botOne').renderContents()).split('replies')[-1].strip()
            #log.info(page['title'])
            
            page['data'] = stripHtml(self.soup.find('div','mbPanel clearPanel').renderContents())
             
            try:
                date_str = stripHtml(self.soup.find('div','brdSubHd blue').renderContents()).split('on')[-1].strip()
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%d/%m/%y at %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")             
   
            except:
                log.exception(self.log_msg('Posted date not found'))
                page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('main page title  not found'))
            return False  
        unique_key = get_hash({'title': page['title'],'data' : page['data']})
        if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
            self.task.instance_data.get('update')):
                    
            log.info(self.log_msg('Session info returns True for uri %s'\
                                                                           %self.currenturi))
            return False
        page_data_keys = ['et_first_author_name', 'ei_thread_replies_count', \
                            'edate_last_post_date']
        [page.update({each:self.task.pagedata.get(each)}) for each in \
                                page_data_keys if self.task.pagedata.get(each)] 
        try:
            result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                    get_hash( page ),'Review', self.task.instance_data.get('update'))
            if not result['updated']:
                log.exception(self.log_msg('Update session info returns False'))
                return True
            page['parent_path'] = page['path'] = [self.task.instance_data['uri']]
##            page['path'] = [unique_key]
            #page['path'].append(unique_key)
            page['uri'] = self.currenturi
            page['entity'] = 'Review'
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            #log.info(page)
            log.info(self.log_msg('Post Added'))
            return True        
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False  
    
    @logit(log, '__iteratePosts')
    def __iteratePosts(self):   
        """It will Iterate Over the Posts found in the Current URI
        """
        
        try:
            dates = [stripHtml(each.renderContents()).split('on')[-1]for each in self.soup.findAll('div','brdSubHd blue')]
            reviews = [stripHtml(each.renderContents())for each in self.soup.findAll('div','mbPanel clearPanel')] 
            #authors = [stripHtml(each.renderContents()).split('on')[0].split(':')[-1].strip()for each in self.soup.findAll('div','brdSubHd blue')]
            author_links = [each.find('a')['href']for each in self.soup.findAll('div','brdSubHd blue')]
            zipped = zip(dates,author_links,reviews)
            if not zipped:
                log.info(self.log_msg('No posts found'))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(zipped)))
            #posts.reverse()
            for my_index,zipp in enumerate(zipped[:5]):
##                if my_index==0:
                    
                if not self.__addPost(my_index,zipp):
                    return False
            return True    
        except:
            log.exception(self.log_msg('can not  find the data'))
            return False    
        
    def __addPost(self, my_index, zipp): 
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            page = self.__getData(my_index, zipp)
            unique_key = get_hash({'data': page['data']}) 
            #log.info(unique_key)
            
            if checkSessionInfo(self.genre, self.session_info_out,unique_key,\
                         self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info returns True'))
                return False
            
        except:
            #log.info(post)
            log.exception(self.log_msg('Cannot add the post for the url %s'%\
                                                            self.currenturi))
            return False
        page['uri'] = self.currenturi 
        try:
            result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                get_hash( page ),'Review', self.task.instance_data.get('update'))
            if not result['updated']:
                log.info(self.log_msg('Update session info returns False'))
                return True
            page['parent_path'] = page['path'] = [self.task.instance_data['uri']]
            #page['path'].append(unique_key)
            #page['uri'] = unique_key
            page['entity'] = 'Review'
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            #log.info(page)
            log.info(self.log_msg('Post Added'))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False  
          
    
    def __getData(self, my_index, zipp):
        #page = {'entity':'question' if is_question else 'answer'}
        page = {}
        try:
            page['title'] = stripHtml(self.soup.find('div','brdSubHd grey top botOne').renderContents()).split('replies')[-1].strip()
            if my_index != 0:
                page['title'] = 'Re: ' + page['title']
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
            
        try:
            page['data'] = zipp[-1]
            
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''        
        
        if not page['title'] and not page['data']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False   
        try:
            page['et_review'] = zipp[-1]
            
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
                                     
        try:
            date_str = zipp[0].strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%d/%m/%y at %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")             
   
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        copycurrenturi = self.currenturi
        try:
            
            author_link = zipp[1]
            #log.info(self.log_msg('author_info not found'))      
            self.currenturi = self.__baseuri + author_link
            self.__setSoupForCurrentUri()
            info = self.soup.findAll('div','mbPanel clearPanel')

            try:
                page['et_author_name'] =stripHtml(info[0].renderContents())
            except:
                log.exception(self.log_msg('author name not found'))
            try:
                date_str  = stripHtml(info[1].renderContents())
                page['et_author_joined_date'] = datetime.strftime(datetime.strptime(date_str,'%d %B %Y'),"%Y-%m-%dT%H:%M:%SZ") 
            except:
                log.exception(self.log_msg('author joined date not found'))
            try:              
            
                page['ei_author_posts_count'] = int(stripHtml(info[-1].renderContents()).replace(',','').strip()) 
                                      
            except:
                #import traceback
                #print traceback.format_exc()
                log.exception(self.log_msg('cannot find the author posts'))
            #self.currenturi = copycurrenturi
        except:
            log.exception(self.log_msg('author posts count not found')) 
        self.currenturi = copycurrenturi
        self.__setSoupForCurrentUri()    
        
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
          
    