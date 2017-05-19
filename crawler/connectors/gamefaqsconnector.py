#prerna
#reviewed by saravana
import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('GameFaqsConnector')
class GameFaqsConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        sample urls :http://www.gamefaqs.com/xbox360/931482-medal-of-honor-airborne/reviews
                    http://www.gamefaqs.com/boards/931482-medal-of-honor-airborne
        '''
        
        if 'reviews' in self.currenturi:
            return self.__processReviewUrl()
        elif re.search('/\d+$', self.currenturi):
            return self.__addThreadAndPosts()
            return self.__createTasksForThreads()
        else:
            return self.__createTasksForThreads()
    
    @logit(log, '__createTasksForThreads')
    def __createTasksForThreads(self):
        """
        This will create Tasks for the threads found on the given url
        The # of Tasks are limited by Config Variable
        """
        self.baseuri = 'http://www.gamefaqs.com'
        try:
            self.__total_threads_count = 0
            self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                                'gamefaqs_maxthreads'))
            self.__setSoupForCurrentUri()
            while self.__processBoardUrl():
                try:
                    next_page_tag = self.soup.find('a',text='Next')
                    if next_page_tag:
                        self.currenturi = self.baseuri + next_page_tag.parent['href']
                    else: 
                        last_page_tag =self.soup.find('a',text='Last')
                        if last_page_tag:
                            self.currenturi = self.baseuri + last_page_tag.parent['href']
                        else:
                            log.info('there in no next page')
                            break
                            
                    self.__setSoupForCurrentUri()
                except:
                    log.exception(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                    break                
                    
            log.info(self.log_msg('LINKSOUT: ' + str(len(self.linksOut))))
            #self.linksOut = [] # To Remove
            if self.linksOut:
                updateSessionInfo('Search', self.session_info_out, None, 'ForumThreadsPage', \
                            self.task.instance_data.get('update'))
            return True  
        except:
            log.exception(self.log_msg('Exception while creating tasks for the url %s'\
                                                         %self.currenturi)) 
            return True
        
    @logit(log, '__processBoardUrl')
    def __processBoardUrl(self):
        """
        It will fetch each thread and its associate infomarmation
        and add the tasks
        """
        try:
            threads  = self.soup.find('table','board topics').find('tbody').findAll('tr',recursive = False)
        except:
            log.info(self.log_msg('No Threads found in the url %s'%self.currenturi))
            return False    
        for thread in threads:
            self.__total_threads_count += 1
            if  self.__total_threads_count > self.__max_threads_count:
                log.info(self.log_msg('Reaching maximum post,Return false \
                                    from the url %s'%self.currenturi))
                return False  
            temp_task=self.task.clone() 
            try:
                temp_task.instance_data[ 'uri' ] = self.baseuri + thread.\
                                                    findAll('td',recursive =False)[1].\
                                                    find('a')['href']                                           
            except:
                log.exception(self.log_msg('uri not found in the url\
                                                %s'%self.currenturi)) 
                continue 
            try:    
                temp_task.pagedata['et_author_name'] = stripHtml(thread.\
                                                        findAll('td',recursive =False)[2].\
                                                        renderContents())
            except:
                log.exception(self.log_msg('author name  not found in the url\
                                                %s'%self.currenturi))    
            try:    
                temp_task.pagedata['ei_thread_posts_count'] = int(stripHtml(thread.\
                                                                findAll('td',recursive =False)[3].\
                                                                renderContents()).replace(',',''))
            except:
                log.exception(self.log_msg('posts count  not found in the url\
                                                %s'%self.currenturi))    
            self.linksOut.append(temp_task)
        return True                                    
    
    @logit(log, '__addThreadAndPosts')
    def __addThreadAndPosts(self): 
        """
        This will add the Thread info from setParentPage method and 
        Add the posts of the thread in addPosts mehtod
        """
        try:
            self.genre = "Review"
            self.baseuri = 'http://www.gamefaqs.com'
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
            self.__getBoardParentPage()
            question_post = self.soup.find('table','board message').find('tr',recursive = False)
            self.__addBoardPost(question_post, True)
            self.__goToLastPage() 
            main_page_soup = copy.copy(self.soup)           
            while self.__iteratePosts():
                try:
                    previous_page_tag = main_page_soup.find('a',text='Previous')
                    if previous_page_tag :
                        self.currenturi = self.baseuri + previous_page_tag.parent['href']
                    else: 
                        first_page_tag =main_page_soup.find('a',text='First')
                        if first_page_tag:
                            self.currenturi = self.baseuri + first_page_tag.parent['href']
                        else:
                            log.info('there in no first page')
                            break
                    self.__setSoupForCurrentUri()
                    main_page_soup = copy.copy(self.soup)    
                except:
                    log.exception(self.log_msg('Next Page link for aadtrhead not found for url %s'%self.currenturi))
                    break
            return True
        except:
            log.exception(self.log_msg('Exception while add the theread posts \
                                            for the url %s'%self.currenturi))
            return False
     
    @logit(log, '__goToLastPage')
    def __goToLastPage(self):
        """This will set the soup the last page of the post
        """
        try:
            pagination_tag = self.soup.find('div', 'pages')
            if not pagination_tag:
                return
            last_page_tag = pagination_tag.find('a', text ='Last', href= True)
            if not last_page_tag:
                return
            self.currenturi = self.baseuri + last_page_tag.parent['href']
            self.__setSoupForCurrentUri()
        except:
            log.exception(self.log_msg('Last page cannot find from the given page no \
                                    for url %s'%self.task.instance_data['uri']))
   
        
    @logit(log, '__iteratePosts')
    def __iteratePosts(self):   
        try:
            posts = self.soup.find('table','board message').findAll('tr',recursive = False)
            if not posts:
                log.info(self.log_msg('No posts found'))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(posts)))
            posts.reverse()
            for post in posts:
                if not self.__addBoardPost(post):
                    return False
            return True    
        except:
            log.exception(self.log_msg('can not  find the data'))
            return False    
        
    @logit(log,'__getBoardParentPage')
    def __getBoardParentPage(self):
        """
        """
        page = {}
        try: 
            page['data'] = page['title'] = stripHtml(self.soup.find('h2','title').renderContents())
        except:
            log.exception(self.log_msg('Thread hierarchy is not found'))
            return False    
        if checkSessionInfo(self.genre, self.session_info_out, self.task.instance_data['uri'],\
                                        self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        page_data_keys = ['et_author_name', 'ei_thread_posts_count']
        [page.update({each:self.task.pagedata.get(each)}) for each in \
                                page_data_keys if self.task.pagedata.get(each)] 
        try:
            date_str = stripHtml(self.soup.find('div','msg_stats_left').\
                        prettify().__str__())[:-1].strip().split('Posted')[-1]\
                                            .replace('\n','/').strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,\
                                    '%m/%d/%Y  %I:%M:%S %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), \
                                                         "%Y-%m-%dT%H:%M:%SZ")                              
        try:
            result = updateSessionInfo(self.genre, self.session_info_out, self.\
                task.instance_data['uri'], get_hash(page), 'thread', \
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
            
    @logit(log,'__addBoardPost')    
    def __addBoardPost(self, post,is_question=False): 
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            page = self.__getBoardData(post,is_question)  
            unique_key = get_hash({'data': page['data']}) 
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                         self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True'))
                return False
        except:
            log.exception(self.log_msg('Cannot add the post for the url %s'%\
                                                            self.currenturi))
            return False
        try:
            result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                get_hash( page ),'forum', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if not result['updated']:
                log.exception(self.log_msg('Update session info returns False'))
                return True
            page['parent_path'] = [self.task.instance_data['uri']]
            page['path'] =  [self.task.instance_data['uri'],unique_key]
            page['uri'] = self.currenturi
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            #log.info(page)
            log.info(self.log_msg('Post Added'))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False  
        
    @logit(log, '__getBoardData')
    def __getBoardData(self,post,is_question):
        page = {'entity':'question' if is_question else 'answer'}
        
        try:
            page['title']= stripHtml(self.soup.find('h2','title').renderContents())
            if not is_question:
                 page['title'] = 'Re: ' + page['title']
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
        try:
            page['data'] = stripHtml(post.find('div','msg_body').renderContents()\
            .replace('/>>','/>'))
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''      
        
        if not page['title'] and not page['data']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False  
        try:
            date_str = stripHtml(post.find('div','msg_stats_left').\
                        prettify().__str__())[:-1].strip().split('Posted')[-1].\
                        replace('\n','/').strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,\
                                    '%m/%d/%Y  %I:%M:%S %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), \
                                                         "%Y-%m-%dT%H:%M:%SZ")        
        try:
            page['et_author_name'] =stripHtml(post.find('div','msg_stats_left').\
                                    renderContents()).split('Posted')[0].split('\n>')[0]                      
        except:
            log.exception(self.log_msg('author name not found')) 
        return page 
    
    @logit(log, '__processReviewUrl')
    def __processReviewUrl(self):
        ''' it will process reviews url
        sample url  :'http://www.gamefaqs.com/xbox360/931482-medal-of-honor-airborne/reviews'
        '''     
        try:
            self.baseuri = 'http://www.gamefaqs.com'
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
            self.__iterateLinks()
        except:
            log.exception(self.log_msg('page not fetched'))
        return True
    @logit(log, '__iterateLinks')
    def __iterateLinks(self):   
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            tables =self.soup.findAll('table','contrib')
            for each in tables:
                links = each.findAll('tr',recursive=False)
                if not links:
                    log.info(self.log_msg('No posts found'))
                    return False
                log.info(self.log_msg('Total No of Posts found is %d'%len(links)))
                for each in links:
                    link = each.find('a')['href']
                    if not self.__addPost(link):
                        return False
            return True
        except:
            log.exception(self.log_msg('can not  find the data'))
            return False          
        
    @logit(log, '__addPost')  
    def __addPost(self, link): 
        """
        This will take the post tag , and fetch data 
        """
        try:
            self.currenturi = self.baseuri + link
            unique_key = self.currenturi
            log.info(unique_key)
            if checkSessionInfo('review', self.session_info_out, unique_key,\
                                        self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info returns True'))
                return False
            self.__setSoupForCurrentUri()  
            page = self.__getData()
                    
            if not page:
                return True
            log.info(unique_key)
            result=updateSessionInfo('review', self.session_info_out, unique_key, \
                get_hash( page ),'Review', self.task.instance_data.get('update'))
            if not result['updated']:
                log.info(self.log_msg('Update session info returns False'))
                return True
            page['path'] = [unique_key] 
            page['parent_path'] = []
            #page['path'].append(unique_key)
            page['uri'] = unique_key
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page['entity'] = 'Review'
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            #log.info(page)
            log.info(self.log_msg('Post Added'))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False  
          
    def __getData(self):
        page = {}
        try:
            page['title'] = stripHtml(self.soup.find('div','details').find('h3').\
                            renderContents())
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
            
        try:
            page['data'] = stripHtml(self.soup.find('div','details').find('p').\
                            renderContents().replace('/>>','/>'))
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''        
        
        if not page['title'] and not page['data']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False  
        try:
            info = stripHtml(self.soup.find('div','details').find('p',text = re.compile('Reviewer'))).\
                    split(',')
            for each in info:
                if 'Reviewer' in each:
                    try:
                        page['et_author_score'] = each.split(':')[-1].strip()
                    except:
                        log.exception(self.log_msg('review score  not found'))  
                    
                elif 'Originally' in each:   
                    try: 
                        date_str =each.split(':')[-1].strip()
                        tag = date_str.split('/')
                        if tag[1]>12 or tag[1]<12 :
                            page['posted_date']= datetime.strftime(datetime.strptime(date_str,'%m/%d/%y'),\
                                                    "%Y-%m-%dT%H:%M:%SZ")
                        else:    
                            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%d/%m/%y'),\
                                                    "%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.exception(self.log_msg('Posted date not found'))
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),\
                                                "%Y-%m-%dT%H:%M:%SZ")     
                elif 'Updated' in each:
                    try:
                        date_str = each.split('Updated')[-1].strip()
                        tag = date_str.split('/')
                        if tag[1]>12 or tag[1]<12 :
                            page['edate_data_last_update']= datetime.strftime(datetime.strptime(date_str,\
                                                            '%m/%d/%y'),"%Y-%m-%dT%H:%M:%SZ")
                        else:    
                            page['edate_data_last_update'] = datetime.strftime(datetime.strptime(date_str,\
                                                                '%d/%m/%y'),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.exception(self.log_msg('last update not found'))
                       
        except:
            log.exception(self.log_msg('info not found'))
        try:            
            date_str =stripHtml(self.soup.find('div','details').find('p',text = re.compile('Game Release:'))).\
                    split(':')[-1].split('(')[-1].split(',')[-1].replace(')','').strip() 
            page['edate_game_released_date'] = datetime.strftime(datetime.strptime(date_str,'%m/%d/%y'),\
                                                "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('Posted date not found'))
        try:
            page['et_game_name'] = stripHtml(self.soup.find('div','details').\
                                    find('p',text = re.compile('Game Release:'))).\
                                    split(':')[-1].split('(')[0].strip()
            page['et_game_release_country'] = stripHtml(self.soup.find('div','details').\
                                                find('p',text = re.compile('Game Release:'))).\
                                                split(':')[-1].split('(')[-1].split(',')[0]
        except:
            log.exception(self.log_msg('difficulty and time spent not found'))     
        # author info 
        try:
            page['et_author_name'] = stripHtml(self.soup.find('h2','title').renderContents()).\
                                    split('Review by')[-1].strip()
            copycurrenturi = self.currenturi
            try:
                auth_link = self.soup.find('h2','title').find('a')['href']
                if auth_link:
                    #page.update(self.__addAuthorInfo(auth_link))
                    self.currenturi = self.baseuri + auth_link
                    self.__setSoupForCurrentUri()
                    try:
                        row = stripHtml(self.soup.find('div','details').renderContents()).split('\n')
                        for each in row:
                            if 'Name:' in each:
                                page['et_author_full_name']= each.split(':')[-1].strip()
                            elif'E-mail Address:' in each:    
                                page['et_author_email'] =each.split(':')[-1].strip().replace(' ','@')
                            elif 'Web Site Address:' in each:
                                page['et_author_website'] = each.split('Address:')[-1].strip()
                    except:
                        log.exception(self.log_msg('author info not found'))
            except:
                log.exception(self.log_msg('author link not found'))
            self.currenturi = copycurrenturi    
        except:
            log.exception(self.log_msg('author name not found'))                    
                
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
            
                                 
                 