import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime
from BeautifulSoup import BeautifulSoup
from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('BaliForumConnector')
class BaliForumConnector(BaseConnector):
    @logit(log, 'fetch')    
    def fetch(self):
        try:
            
            if 'postlist.php' in self.currenturi:
                return self.__createTasksForThreads()
            else:
                return self.__addThreadAndPosts()
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'\
                                                            %self.currenturi))
            return False    
        
    @logit(log, '__addThreadAndPosts')
    def __addThreadAndPosts(self):    
        self.__genre = "Review"
        self.__hierarchy = []
        self.__baseuri = 'http://baliforum.com'
        self.currenturi = self.currenturi + '&fpart=all&vc=1'
        self.__setSoupForCurrentUri()
        self.__task_elements_dict= {
                        'priority':self.task.priority,
                        'level': self.task.level,
                        'last_updated_time':datetime.strftime(datetime.utcnow()
                                                , "%Y-%m-%dT%H:%M:%SZ"), 
                        'pickup_date':datetime.strftime(datetime.utcnow(),  
                                                        "%Y-%m-%dT%H:%M:%SZ"), 
                        'connector_instance_log_id': \
                                        self.task.connector_instance_log_id, 
                        'connector_instance_id':
                                            self.task.connector_instance_id, 
                        'workspace_id':self.task.workspace_id, 
                        'client_id':self.task.client_id, 
                        'client_name':self.task.client_name, 
                        'versioned':False, 
                        'category':self.task.instance_data.get('category',''), 
                        'task_log_id':self.task.id }  
        self.__setParentPage()
        question_title = self.soup.find('td','subjecttable').findParent('tr')
        question_post = BeautifulSoup(question_title.__str__() + question_title.findNextSibling('tr').__str__())
        self.__addPost(question_post, True)   
        self.__itratePosts()
        return True 
    
    @logit(log, '__createTasksForThreads')
    def __createTasksForThreads(self):
            """
            This will create Tasks for the threads found on the given url
                The # of Tasks are limited by Config Variable
            """
            self.__setSoupForCurrentUri()
            self.__total_threads_count = 0
            self.__baseuri = 'http://baliforum.com'
            self.__last_timestamp =datetime(1980, 1, 1) 
            #The Maximum No of threads to process, Bcoz, not all the forums get
            #updated Everyday, At maximum It will 100
            self.__max_threads_count = int(tg.config.get(path='Connector', key=\
                                            'baliforum_maxthreads'))
            while self.__processForumUrl():
                try:
                    self.currenturi =self.soup.find('img', alt='Next page').findParent('a')['href']                    
                    self.__setSoupForCurrentUri()
                except:
                    log.info(self.log_msg('Next Page link not found for url \
                                                        %s'%self.currenturi))
                    break
            log.debug(self.log_msg('LINKSOUT: ' + str(len(self.linksOut))))
            #self.linksOut = [] # To Remove
            if self.linksOut:
                updateSessionInfo('Search', self.session_info_out, \
                            self.__last_timestamp , None, 'ForumThreadsPage', \
                            self.task.instance_data.get('update'))
            return True
        
    @logit(log, '__processForumUrl')
    def __processForumUrl(self):
        """
         It will fetch each thread and its associate infomarmation
        and add the tasks
        """
        try:
            thread_table = self.soup.findAll('table', 'tablesurround')[4]
            threads=thread_table.findAll('tr')[2:-1]
            #thread_table=self.soup.findAll('table','tablesurround')[4].findAll('tr')[2:-1]
            log.debug(self.log_msg('The Total # of threads found is %d'%len(threads)))
        except:
            log.exception(self.log_msg('No threads found in %s'%self.currenturi))
            return False
        for each in threads:
            try:
                if each.find('img', src=re.compile('sticky.gif$')):
                    log.info(self.log_msg('Its a Sticky Thread, Ignore it in\
                                            the url %s'%self.currenturi))
                    continue
                self.__total_threads_count += 1
                if  self.__total_threads_count > self.__max_threads_count:
                    log.info(self.log_msg('Reaching maximum post,Return false \
                                        from the url %s'%self.currenturi))
                    return False
                try:  
                    time=stripHtml(each.findAll('td',recursive=False)[7].\
                                            renderContents()).splitlines()[0]
                    time = datetime.strptime(time, '%a %b %d %Y %I:%M %p')
                except:
                    log.info(self.log_msg('lastpost not found in the url\
                                                    %s'%self.currenturi)) 
                if checkSessionInfo('Search', self.session_info_out, time, \
                                self.task.instance_data.get('update')):
                    log.info(self.log_msg('Session info Returns True for %s'%\
                                                        self.currenturi))
                    #log.info(time)
                    #log.info(self.session_info_out)                    
                    return False   
                self.__last_timestamp = max(time, self.__last_timestamp)
                temp_task = self.task.clone()   
                try:
                    temp_task.instance_data['uri'] = each.findAll('td',\
                                    recursive=False)[2].a['href'].split('#')[0]
                    #log.info(temp_task.instance_data['uri'])
                except:
                    log.exception(self.log_msg('uri not found in the url\
                                                    %s'%self.currenturi)) 
                    continue
                temp_task.pagedata['edate_last_post_date'] = datetime.strftime(time, "%Y-%m-%dT%H:%M:%SZ")
                temp_task.pagedata['et_last_post_author']=stripHtml\
                                (each.findAll('td',recursive=False)[7]\
                                    .renderContents()).splitlines()[-1]
                try:  
                    temp_task.pagedata['et_author_name'] = stripHtml(each.\
                            findAll('td',recursive=False)[3].renderContents())
                except:
                    log.info(self.log_msg('Author name not found in the url\
                                                    %s'%self.currenturi))    
                try:    
                    temp_task.pagedata['ei_thread_views_count'] = int(stripHtml(each.findAll\
                                    ('td',recursive=False)[4].renderContents()))
                except:
                    log.info(self.log_msg('views not found in the url\
                                                    %s'%self.currenturi)) 
                 
                try:    
                    temp_task.pagedata['ei_thread_replies_count'] = int(stripHtml(each.findAll\
                                    ('td',recursive=False)[5].renderContents()))
                except:
                    log.info(self.log_msg('reply  not found in the url\
                                                    %s'%self.currenturi))  
                self.linksOut.append(temp_task)
            except:
                log.exception(self.log_msg('links not found'))      
        return True
    @logit(log, '__setParentPage')
    def __setParentPage(self):
        """This will get the parent info
        """
        page = {}
        try:
            page['et_thread_hierarchy'] = [x.strip() for x \
                in stripHtml(self.soup.find('font','catandforum')\
                                                .renderContents())]
            page['data']=page['title'] = stripHtml(self.soup.find('span', \
                            id=re.compile('subject\d+')).renderContents())
            page['et_thread_hierarchy'].append(page['title'])
        except:
            log.exception(self.log_msg('Thread hierarchy and Title Not found \
                                                for uri%s'%self.currenturi))
            return 
        if checkSessionInfo('review', self.session_info_out, self.task.\
                instance_data['uri'], self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return
        page_data_keys = ['et_author_name', 'ei_thread_replies_count', \
                            'ei_thread_views_count','edate_last_post_date','et_last_post_author']
        [page.update({each:self.task.pagedata.get(each)}) for each in \
                                page_data_keys if self.task.pagedata.get(each)] 
        try:
            date_str = self.soup.find('span', id=re.compile('number\d+')).next\
                                        .next.__str__().split('-')[-1].strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,\
                                '%a %b %d %Y %I:%M %p'), "%Y-%m-%dT%H:%M:%SZ")
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
                log.info(self.log_msg('Result[updated] returned True for \
                                                        uri'%self.currenturi))
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))
    
    @logit(log, '__itratePosts')
    def __itratePosts(self):
        try:
            quotes=self.soup.findAll('blockquote')
            for quote in quotes:
               quote.extract()
            #[each.extract() for each in self.soup.findAll('blockquote')]
            posts = [BeautifulSoup(each.findParent('tr').__str__() + each.findParent('tr').findNextSibling('tr').__str__()) for each in self.soup.findAll('td',\
                                                        'subjecttable')[1:]]
            if not posts:
                log.info(self.log_msg('No posts found'))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(posts)))
            posts.reverse()
            for post in posts:
                self.__addPost(post)
        except:
            log.exception(self.log_msg('can find the data'))
        return True
    
    @logit(log, '__addPost')
    def __addPost(self, post, is_question=False):
        try:
            unique_key = post.find('a')['name'].replace('Post','')
            log.debug(self.log_msg('POST: ' + str(unique_key)))
            if checkSessionInfo('review', self.session_info_out, unique_key, \
                             self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'\
                                                                %unique_key))
                return False
            page = self.__getData(post, is_question)
            if not page:
                return True
            result = updateSessionInfo('review', self.session_info_out, 
                unique_key,get_hash( page ),'forum', self.task.instance_data.get\
                    ('update'),parent_list=[self.task.instance_data['uri']])
            if result['updated']:
                page['path'] = [ self.task.instance_data['uri'], unique_key]
                page['parent_path'] = [self.task.instance_data['uri']]
                page['uri']= self.currenturi + '#' + unique_key
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                #page['entity'] = ''
                #log.info(page)
                page.update(self.__task_elements_dict)
                self.pages.append(page)
                log.info(self.log_msg('Page added'))
            else:
                log.info(self.log_msg('Update session info returns False for \
                                                    url %s'%self.currenturi))
        except:
            log.exception(self.log_msg('Cannot add the post for the uri %s'\
                                                            %self.currenturi))
        return True
    @logit(log, '__getData')
    def __getData(self,post,is_question):
        page = {'entity':'question' if is_question else 'answer'}
        try:
            #page['uri'] = self.currenturi
            page['title'] = stripHtml(post.find('span', id=re.compile\
                                            ('subject\d+')).renderContents())
        except:
            log.exception(self.log_msg('Title not found'))
            page['title'] = ''
        try:
            
            page['data'] = stripHtml('\n'.join([x.__str__() for x in post.findAll('font','post')])) 
            
            if not page['data']:
                log.info(self.log_msg('No data found'))
                log.info(post)
                
        except:
            log.info(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''
        if not page['data'] and not page['title']: 
            log.info(self.log_msg("Data and Title are not found for %s,\
                                    discarding this Post"%(self.currenturi)))
            return False 
        try:  
            page['et_author_name'] = stripHtml(post.find('a', title=re.compile\
                                                ('Member')).renderContents())
        except:
            log.info(self.log_msg('author name not found'))
        try:
            date_str = post.find('span', id=re.compile('number\d+')).next.next\
                                            .__str__().split('-')[-1].strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,\
                                '%a %b %d %Y %I:%M %p'), "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-\
                                                            %m-%dT%H:%M:%SZ")
        try:
            page['et_author_category'] = post.find('span', 'small').next\
                                                            .__str__().strip()
        except:
            log.info(self.log_msg('author category not found'))
        try:
            author_info=stripHtml(post.find('span', 'small').renderContents())\
                                                                .splitlines()
            for each in author_info:
                try:
                    if each.startswith('Posts:'):
                        page['ei_author_posts_count'] = int(each.replace('Posts:','')\
                                                                        .strip())
                    if each.startswith('Reged:'):
                        page['edate_author_member_since'] = each.replace('Reged:','').strip()
                    if each.startswith('Loc:'):            
                        page['edate_author_location'] =each.replace('Loc:','').strip()
                except:
                    log.info(self.log_msg('cannot find the author info'))
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