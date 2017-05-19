#Prerna
#reviewd by saravana
import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime
from urllib import urlencode 
from BeautifulSoup import BeautifulSoup

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('GameSpotConnector')
class GameSpotConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        '''
        if 'players.html' in self.currenturi:
            return self.__processReviewUrl()
        elif 'forum.html' in self.currenturi:
            return self.__createTasksForThreads()
        else:
            return self.__addThreadAndPosts()
                #this will fetch the thread links and Adds Tasks                
    
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
                                                'gamespot_maxthreads'))
            self.__setSoupForCurrentUri()
            while self.__processForumUrl():
                try:
                    self.currenturi = self.soup.find('a','next')['href']
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
            log.exception(self.log_msg('Exception while creating tasks for the url %s'\
                                                         %self.currenturi)) 
            return False
        
    @logit(log, '__processForumUrl')
    def __processForumUrl(self):
        """
        It will fetch each thread and its associate infomarmation
        and add the tasks
        """
        try:
            threads  = self.soup.find('tbody','topics ').findAll('tr',recursive=False)
        except:
            log.info(self.log_msg('No Threads found in the url %s'%self.currenturi))
            return False
        for thread in threads:
            try:
                self.__total_threads_count += 1
                if  self.__total_threads_count > self.__max_threads_count:
                    log.info(self.log_msg('Reaching maximum post,Return false \
                                        from the url %s'%self.currenturi))
                    return False  
                
                try:
                    date_str = stripHtml(thread.find('td','lastpost').find('br').\
                                next).split('\n')[-1].split('PT')[0].strip()
                    thread_time = datetime.strptime(date_str,"%b %d, %Y  %I:%M %p")
                except:
                    log.exception(self.log_msg('last post not found in the url\
                                                    %s'%self.currenturi)) 
                    continue
                if checkSessionInfo('Search', self.session_info_out, thread_time, \
                                self.task.instance_data.get('update')):
                    log.info(self.log_msg('Session info Returns True for %s'%\
                                                        self.currenturi))
                    return False
                self.__last_timestamp = max(thread_time, self.__last_timestamp )
                temp_task=self.task.clone() 
                try:
                    temp_task.instance_data['uri'] = thread.find('td', 'topic').find('a')['href']                                           
                except:
                    log.exception(self.log_msg('uri not found in the url\
                                                    %s'%self.currenturi)) 
                    continue 
                try:
                    temp_task.pagedata['edate_thread_last_post_date'] = datetime.strftime\
                                                (thread_time, "%Y-%m-%dT%H:%M:%SZ")                                                           
                except: 
                    log.exception(self.log_msg('last post date not found in the url\
                                                    %s'%self.currenturi))   
                try:    
                    temp_task.pagedata['et_author_name'] = stripHtml(thread.find('span','author').\
                                                            find('a').renderContents())
                except:
                    log.exception(self.log_msg('author name  not found in the url\
                                                    %s'%self.currenturi))    
                try:    
                    temp_task.pagedata['et_thread_last_post_author'] = stripHtml(thread.find('td','lastpost').\
                                                                find('a').renderContents())
                except:
                    log.exception(self.log_msg('last post author not found in the url\
                                                    %s'%self.currenturi))                    
                try:    
                    temp_task.pagedata['ei_thread_posts_count'] = int(stripHtml(thread.find('td','posts last').renderContents()).replace(',',''))
                except:
                    log.info(self.log_msg('views  not found in the url\
                                                    %s'%self.currenturi))    
                self.linksOut.append(temp_task)
            except:
                log.exception(self.log_msg('links not found'))      
        return True                                    
    
    @logit(log, '__addThreadAndPosts')
    def __addThreadAndPosts(self): 
        """
        This will add the Thread info from setParentPage method and 
        Add the posts of the thread in addPosts mehtod
        """
        try:
            self.genre = "Review"
            self.hierarchy = []
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
            self.__getForumParentPage()
            main_page_soup = copy.copy(self.soup)           
            main_page_uri = self.currenturi
            question_post = self.soup.find('div',id =re.compile('msg_\d+'))
            self.__addForumPost(question_post, True)
            self.soup = copy.copy(main_page_soup)
            copy_soup = copy.copy(self.soup)
            self.currenturi = main_page_uri
            self.__goToLastPage()
            copy_soup = copy.copy(self.soup)
            while self.__iteratePosts():
                try:
                    self.currenturi = copy_soup.find('a','prev end')['href']
                    self.__setSoupForCurrentUri()
                    copy_soup = copy.copy(self.soup)
                    #self.soup = copy.copy(main_page_soup)
                except:
                    log.exception(self.log_msg('Next Page link for aadtrhead not found for url %s'%self.currenturi))
                    break
            return True
        except:
            log.exception(self.log_msg('Exception while add the theread posts \
                                            for the url %s'%self.currenturi))
            return False
        
    @logit(log, '__iteratePosts')
    def __iteratePosts(self):   
        
        try:
            posts = self.soup.findAll('div',id =re.compile('msg_\d+'))
            if not posts:
                log.info(self.log_msg('No posts found'))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(posts)))
            posts.reverse()
            for post in posts:
                if not self.__addForumPost(post):
                    return False
            return True    
        except:
            log.exception(self.log_msg('can not  find the data'))
            return False    
        
    @logit(log,'__getForumParentPage')
    def __getForumParentPage(self):
        """
        """
        page = {}
        try: 
            self.hierarchy = stripHtml(self.soup.find('div','crumbs forum_crumbs').\
                            renderContents()).replace('\n','').split(u'\u203a')
            page['data'] = page['title'] = self.hierarchy[-1]
            page['et_thread_hierarchy'] = self.hierarchy
        except:
            log.exception(self.log_msg('Thread hierarchy is not found'))
            return False    
        if checkSessionInfo(self.genre, self.session_info_out, self.task.instance_data['uri'],\
                                        self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        page_data_keys = ['et_author_name', 'ei_thread_posts_count', \
                            'edate_last_post_date',\
                                    'et_last_post_author']
        [page.update({each:self.task.pagedata.get(each)}) for each in \
                                page_data_keys if self.task.pagedata.get(each)] 
        try:
            date_str = stripHtml(self.soup.find('div','posted').renderContents()).split('PT')[0].strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%b %d, %Y   %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), \
                                                         "%Y-%m-%dT%H:%M:%SZ")                              
        try:
            result = updateSessionInfo(self.genre, self.session_info_out, self.\
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
        '''This is move the current page and soup to last page
        '''
        try:
            pagination_tag = self.soup.find('ul','pages')
            if not pagination_tag:
                return
            pages = pagination_tag.findAll('a', text=re.compile('\d+'))
            if len(pages)==1:
                return             
            self.currenturi = pages[-1].parent['href']
            self.__setSoupForCurrentUri()
        except:
            log.exception(self.log_msg('Last page cannot find'))
            
    @logit(log,'__addPost')    
    def __addForumPost(self, post, is_question = False): 
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            post_id = post.find('div',id=re.compile('m-\d+'))
            unique_key = post_id['id'].replace('m-1-','')
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                         self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True'))
                return False
            page = self.__getForumData(post, is_question)           
        except:
            log.exception(self.log_msg('Cannot add the post for the url %s'%\
                                                            self.currenturi))
            return False
        try:
            
            result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                get_hash( page ),'posts', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if not result['updated']:
                log.exception(self.log_msg('Update session info returns False'))
                return True
            page['uri'] = self.currenturi + '#' + unique_key 
            page['parent_path'] = [self.task.instance_data['uri']]
            page['path'] =  [self.task.instance_data['uri'], unique_key]
            page['path'].append(unique_key)
            #page['uri'] = self.currenturi
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            #log.info(page)
            log.info(self.log_msg('Post Added'))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False  
    @logit(log, '__getForumData')
    def __getForumData(self,post, is_question):
        page = {'entity':'question' if is_question else 'answer'}
        try:
            page['title']=self.hierarchy[-1]
            if not is_question:
                 page['title'] = 'Re: ' + page['title']
        except:
            log.exception(self.log_msg('data forum not found'))
            page['title'] = ''
        try:
            data_tag = post.find('div','msg_wrap')
            quote_tag = data_tag.find('div','bb_quote')
            if quote_tag:
                quote_tag.extract()
            try:
                previous_message_tag = data_tag.findAll('strong', text=re.compile('From:\s*'))
                if previous_message_tag:
                    [each.extract()for each in previous_message_tag]
            except:
                pass
            page['data'] = stripHtml(data_tag.renderContents().replace('/>>','/>'))
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''      
        
        if not page['title'] and not page['data']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False  
        try:
            page['et_author_signature'] =   stripHtml(post.find('div','usersig').renderContents())  
        except:
            log.exception(self.log_msg('signature not found'))                  
        try:
            date_str = stripHtml(post.find('div','posted').renderContents()).split('PT')[0].strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%b %d, %Y   %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), \
                                                         "%Y-%m-%dT%H:%M:%SZ")      
        try:
            page['et_author_name'] = stripHtml(post.find('div','username').renderContents())
            log.info(page['et_author_name'])
            try:
                auth_link = post.find('div','username').find('a')['href']
                if auth_link:
                    page.update(self.__addAuthorInfo(auth_link))
            except:
                log.exception(self.log_msg('author link not found'))
        except:
            log.exception(self.log_msg('author name not found')) 
        return page             
                                 
             
    @logit(log, '__processReviewUrl')
    def __processReviewUrl(self):     
        try:
            self.__baseuri = 'http://www.gamespot.com'
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
            self.__getParentPage()
            main_page_soup = copy.copy(self.soup)
            while self.__iterateLinks():
                try:
                    self.currenturi = self.__baseuri + main_page_soup.find('a','next',rel='next')['href']
                    self.__setSoupForCurrentUri()
                    main_page_soup = copy.copy(self.soup)
                except:
                    log.exception(self.log_msg('can not fetch next page links %s'))
                    break
            return True
        except:
            log.exception(self.log_msg('page not fetched'))
            return False        
    
    @logit(log, '__iterateLinks')
    def __iterateLinks(self):   
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            links = [each['href']for each in self.soup.findAll('a','continue')]
            if not links:
                log.info(self.log_msg('No posts found'))
                return False
            log.info(self.log_msg('Total No of Posts found is %d'%len(links)))
            for link in links:
                if not self.__addPost(link):
                    return False
            return True    
        except:
            log.exception(self.log_msg('can not  find the data'))
            return False
    
    @logit(log,'__getParentPage')
    def __getParentPage(self):
        """ this will set parent page info """
        
        page = {}
        
        try: 
            page['data'] = page['title']  = stripHtml(self.soup.find('h2','module_title').renderContents())
        except:
            log.exception(self.log_msg('main page title  not found'))
        
        if not page['title'] and not page['data']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False   
        try:
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('Posted date not found'))
        try:
            page['ei_player_reviews_count'] = int(stripHtml(self.soup.find('dl','player_stats').find('dd','reviews').renderContents()).replace(',','').strip())        
            page['ei_player_ratings_count'] = int(stripHtml(self.soup.find('dl','player_stats').find('dd','ratings').renderContents()).replace(',','').strip())
            page['ei_users_now_playing'] = int(stripHtml(self.soup.find('dl','player_stats').find('dd','now_playing').renderContents()).replace(',','').strip())
        except:
            log.exception(self.log_msg('player info not found'))
                
        unique_key = self.currenturi
        if checkSessionInfo('review', self.session_info_out, unique_key,\
            self.task.instance_data.get('update')):
                    
            log.info(self.log_msg('Session info returns True for uri %s'\
                                                                           %self.currenturi))
            return False
        try:
            result=updateSessionInfo('review', self.session_info_out, unique_key, \
                    get_hash( page ),'Review', self.task.instance_data.get('update'))
            if not result['updated']:
                log.exception(self.log_msg('Update session info returns False'))
                return True
            page['parent_path'] = [self.task.instance_data['uri']] 
            page['path'] = [self.task.instance_data['uri']]
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
    
    @logit(log, '__addPost')  
    def __addPost(self, link,): 
        """
        This will take the post tag , and fetch data 
        """
        try:
            self.currenturi = link
            unique_key = self.currenturi
            if checkSessionInfo('review', self.session_info_out, unique_key,\
                         self.task.instance_data.get('update'),parent_list\
                                            = [self.currenturi]):
                log.info(self.log_msg('Session info returns True'))
                return False
            self.__setSoupForCurrentUri()  
            page = self.__getData()
                    
            if not page:
                return True
        
            result=updateSessionInfo('review', self.session_info_out, unique_key, \
                get_hash( page ),'Review', self.task.instance_data.get('update'),\
                                parent_list=[self.currenturi])
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
            page['title'] = stripHtml(self.soup.find('p','deck').renderContents())
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
            
        try:
            page['data'] = stripHtml(self.soup.find('div',id=re.compile('player_review_body')).renderContents())
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''        
        
        if not page['title'] and not page['data']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False                    
        try:
            date_str = stripHtml(self.soup.find('span',property=re.compile('v:dtreviewed')).renderContents()).replace("PT","").strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%b %d, %Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")            
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            
        try:
            page['et_review_recommended_by'] = stripHtml(self.soup.find('ul','details').renderContents()).split('by')[-1]
            
        except:
            log.exception(self.log_msg('review recommended by not found'))
            
        try:
            page['et_review_difficulty'] = stripHtml(self.soup.find('dt',text = re.compile('Difficulty:')).findNext('dd').renderContents())   
            page['et_review_time_spent'] = stripHtml(self.soup.find('dt',text = re.compile('Time Spent:')).findNext('dd').renderContents())
            page['et_review_score'] =  stripHtml(self.soup.find('div','score').renderContents())
            page['et_review_bottom_line'] = stripHtml(self.soup.find('dt',text = re.compile('The Bottom Line:')).findNext('dd').renderContents())
        except:
            log.exception(self.log_msg('difficulty and time spent not found'))     
        # author info 
        try:
            page['et_author_name'] = stripHtml(self.soup.find('p','username').renderContents())
            try:
                auth_link = self.soup.find('p','username').find('a')['href']
                if auth_link:
                    page.update(self.__addAuthorInfo(auth_link))
            except:
                log.exception(self.log_msg('author link not found'))
        except:
            log.exception(self.log_msg('author name not found'))                    
                
        return page         
        
    @logit(log, '__addAuthorInfo')  
    def __addAuthorInfo(self, auth_link):
        """ this will fetch all the author info """
        auth_info = {}
        copycurrenturi = self.currenturi
        
        try:
            self.currenturi = auth_link
            self.__setSoupForCurrentUri()
            try:
                info = self.soup.find('ul','profile_stats').findAll('li')
                #dict([('et_' + k.lower().replace(' ', '_') ,v) for k,v in dict([[stripHtml(xx.renderContents()) for xx in each.findAll('td')] for each in stat_table.findAll('tr') if len(each.findAll('td'))==2][1:]).iteritems()])
                for each in info[3:]:
                    try:
                        field = 'et_author_'+ stripHtml(each.find('span','label').renderContents()).lower().replace(' ','_').split(':')[0]
                        if 'last_online' in field:
                            date_str = stripHtml(each.find('span','data').renderContents()).split('PT')[0].strip() 
                            auth_info[field] = datetime.\
                                            strftime(datetime.strptime(date_str,'%m/%d/%y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
                        elif 'member_since' in field:
                            date_str = stripHtml(each.find('span','data').renderContents()).strip()
                            auth_info[field] = datetime.strftime(datetime.strptime(date_str,'%b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")                      
                        else:
                            auth_info[field] = stripHtml(each.find('span','data').renderContents()) 
                    except:
                        log.exception(self.log_msg('auth info not found'))                 
            except:
                log.exception(self.log_msg('info not found'))
            try:
                auth_info['et_author_level'] = stripHtml(self.soup.find('div','text').\
                                                renderContents()).split(':')[-1].replace('\n','').strip()
            except:
                log.exception(self.log_msg('authoe level not found'))
            try:    
                self.currenturi = self.soup.find('li','about_me').find('a')['href']
                self.__setSoupForCurrentUri()
                # about author
                try:
                    row = self.soup.find('div','module about_me_module first').findAll('li')
                    for each1 in row:
                        field = 'et_author_'+ stripHtml(each1.find('span','label').renderContents()).lower().replace(' ','_').split(':')[0]
                        if 'name' in field:
                            field = 'et_author_full_name'
                        auth_info[field] = stripHtml(each1.find('span','data').renderContents())
                except:
                    log.exception(self.log_msg('not found'))  
                data_row = self.soup.findAll('div','module')
                for each in data_row:
                    data = each.findAll('li')
                    for each1 in data:
                        field = 'et_author_'+ stripHtml(each1.find('span','label').renderContents()).lower().replace(' ','_').split(':')[0]
                        auth_info[field] = stripHtml(each1.find('span','data').renderContents())
            except:
                log.exception(self.log_msg('about author not found'))
        except:
            log.exception(self.log_msg('author info not found'))
        self.currenturi = copycurrenturi    
        return auth_info                                                                                                                                                                                            
    
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
