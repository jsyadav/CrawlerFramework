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

log = logging.getLogger('CardRatingsConnector')
class CardRatingsConnector(BaseConnector):
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        sample url:http://www.cardratings.com/forum/search.php?st=0&sk=t&sd=d&sr=posts&keywords=american+express
        '''
        try:
            self.genre = "Review"
            self.__baseuri = 'http://www.cardratings.com/forum'
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
            #c = 0
            main_page_soup = copy.copy(self.soup)
            while self.__processForumUrl():
                try:
                    next_page_uri = main_page_soup.find('a','right-box right',text = 'Next').\
                                    parent['href'].replace('.','')
                    data_dict = dict(parse_qsl(next_page_uri.split('?')[-1]))
                    if 'sid' in data_dict.keys():
                        data_dict.pop('sid')
                    self.currenturi = self.__baseuri + '/search.php?'+ \
                                        urlencode(data_dict)  
                    self.__setSoupForCurrentUri()
                    main_page_soup = copy.copy(self.soup)
##                    c += 1
##                    if c > 100:
##                        break
                except:
                    log.exception(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                    break                
            return True  
        except:
            log.info(self.log_msg('Exception while  fetchin  review url %s'\
                                                         %self.currenturi)) 
        return True
        
    @logit(log, '__processForumUrl')
    def __processForumUrl(self):
        """
        It will fetch each thread and its associate infomarmation
        and add the tasks
        """
        try:
            threads = self.soup.find('div',id = 'page-body').findAll('div','inner')
            for thread in threads:
                try:
                    post_uri = thread.find('div','postbody').find('h3').\
                                find('a')['href'].replace('.','')
                    data_dict = dict(parse_qsl(post_uri.split('?')[-1]))
                    post_id = 'p' + data_dict['p']
                    if 'sid' in data_dict:
                        data_dict.pop('sid')
                    if 'hilit' in data_dict:
                        data_dict.pop('hilit')    
                    review_page_link = self.__baseuri + '/viewtopic.php?' + \
                                        urlencode(data_dict)
                    self.__addPost(review_page_link, post_id)
                except:     
                    log.exception(self.log_msg('uri not found in the url\
                                                        %s'%self.currenturi)) 
                    continue 
            return True                 
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'\
                                                            %self.currenturi))
            return True
    
    @logit(log,'__addPost')    
    def __addPost(self, review_page_link, post_id): 
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            self.currenturi = review_page_link
            self.__setSoupForCurrentUri()
            page = self.__getData(post_id)
            if not page:
                return True 
            unique_key = get_hash({'data' : page['data']})
            log.info(unique_key)
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
            page['uri'] = self.currenturi 
        except:
            log.info(self.log_msg('Cannot find the uri'))
            page['uri'] = self.currenturi
        try:
            result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                get_hash( page ),'Review', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if not result['updated']:
                log.exception(self.log_msg('Update session info returns False'))
                return True
            page['parent_path'] =  []
            page['path'] = [self.task.instance_data['uri'], unique_key]
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            log.info(page)
            log.info(self.log_msg('Post Added'))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
        return True  
          
    
    def __getData(self, post_id):
        post = self.soup.find('div',id=post_id)
        page = {}
        try:
            page['title']= stripHtml(post.find('div','postbody').find('h3').\
                            find('a').renderContents())
            if page['title'].startswith('Re:'):
                page['entity'] = 'question'
            else:
                page['entity'] = 'answer'    
        except:
            log.exception(self.log_msg('data title not found'))
            page['title'] = ''
            
        try:
            page['data'] = stripHtml(post.find('div','content').renderContents())
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''        
        
        if not page['title'] and not page['data']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False                    
        try:
            auth_link = post.find('p','author').find('a')
            if auth_link:
                auth_link.extract()
            author = post.find('p','author').find('strong')
            if author:
                author.extract()
            try:
                page['et_author_name'] = stripHtml(author.renderContents()) 
            except:
                log.exception(self.log_msg('author name not found'))
            try:
                date_str = stripHtml(post.find('p','author').renderContents()).\
                            split('by')[-1].strip()
                page['posted_date'] =  datetime.strptime(date_str,'%a %b %d, %Y %I:%M %p').\
                                        strftime("%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception(self.log_msg('Posted date not found'))
                page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
                
        except:
            log.exception(self.log_msg('author not found'))
        
        try:
            lists =[stripHtml(each.renderContents())for each in post.\
                    find('dl','postprofile').findAll('dd',recursive = False)]
            for each in lists:
                if each.startswith('Posts:'):
                    page['et_author_posts_count'] = int(each.split(':')[-1].\
                                                    replace(',',''))
                if each.startswith('Joined:'):
                    date_str =  each.split('Joined:')[-1].strip()
                    try:
                        page['edate_author_joined_date'] = datetime.strptime(date_str,'%a %b %d, %Y %I:%M %p').\
                                                        strftime("%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.exception(self.log_msg('joined datet not found'))
                if each.startswith('Location:'):
                    page['et_author_locatiion'] = each.split('Location:')[-1]
                                                                                               
        except:
            log.exception(self.log_msg('author info not found'))
            
        
        return page                                                                                                                                                                                      
    @logit(log,'__setSoupForCurrentUri')
    def __setSoupForCurrentUri(self, data=None, headers={}):
        """It will set soup object for the Current URI
        """
        res = self._getHTML(data=data, headers=headers)
        if res:
            self.rawpage = res['result']
        else:
            raise Exception('Page content not fetched for th url %s'%self.currenturi)
        self._setCurrentPage()                                              
          