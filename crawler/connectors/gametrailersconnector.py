#prerna
#reviewed by ashish
import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime, timedelta

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('GameTrailersConnector')
class GameTrailersConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        sample url: http://www.gametrailers.com/video/review-medal-of/25655
        '''
        try:
            self.genre = "Review"
            self.baseuri = 'http://www.gametrailers.com/ajax/player_comments_ajaxfuncs_read.php?do=get_list_page&type=movies&id=25655&page=1&count=10'
            #params = dict(type='movies',id=1000,page=100)
            #self.baseuri%params
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
            posts_url_id = 'movies&id=' + self.currenturi.split('/')[-1]
            self.currenturi = re.sub('movies&id=\d+',posts_url_id,self.baseuri) 
            self.__setSoupForCurrentUri()
            try:
                page_tag  = int(stripHtml(self.soup.find('div','comment_head_text_right').\
                    findAll('a')[-2].renderContents()))
                log.debug(self.log_msg(page_tag))
            except:
                log.exception(self.log_msg('page_tag not found %s'%self.currenturi))
            count = 1
            while self.__iteratePosts():
                count += 1
                next_page = 'page=' + str(count)
                self.currenturi = re.sub('&page=\d+&','&' + next_page + '&' ,self.currenturi)
                if count > page_tag: #for pagination
                    break
                self.__setSoupForCurrentUri()
        except:
            log.exception(self.log_msg('Exception while add the theread posts \
                                            for the url %s'%self.currenturi))
        return True

    @logit(log, '__setParentPage')
    def __setParentPage(self):
        """ this will set parent page info """
        page = {}
        try:
            page['title']  = stripHtml(self.soup.find('h2','gameTitle').renderContents()) 
        except:
            log.exception(self.log_msg('main page title  not found %s'%self.currenturi))
            page['title'] = ''
        try:
            page['data']  = stripHtml(self.soup.find('div','description').renderContents())
        except:
            log.exception(self.log_msg('data not found %s'%self.currenturi)) 
            page['data'] = ''  
            
        if not page['title'] and not page['data']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False  
        try:
            date_str = stripHtml(self.soup.find('span', 'posted').renderContents()).\
                        split(':')[-1].strip()
            page['posted_date'] = datetime.strptime(date_str,'%b %d, %Y').strftime(\
                                    "%Y-%m-%dT%H:%M:%SZ") #another way of python calling convention
        except:
            log.exception(self.log_msg('Posted date not found %s'%self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), \
                                    "%Y-%m-%dT%H:%M:%SZ")
        try:
            page['ei_data_views_count'] = int(stripHtml(self.soup.find('span', 'views').\
                                        renderContents()).split(':')[-1].strip().\
                                        replace(',',''))
        except:
            log.exception(self.log_msg('views count not found %s'%self.currenturi))                                
        try:
            field = self.soup.find('div', 'content').findAll('strong')
            for each in field:
                tag = 'et_game_' + stripHtml(each.renderContents()).replace(':','').\
                        lower().replace(' ','_')
                if 'platforms' in tag:
                    page[tag] = stripHtml(each.findParent('div').renderContents()).\
                                split(':')[-1].strip()
                elif 'release' in tag:
                    tag = tag.replace('et','edate')
                    date_str = stripHtml(each.next.next.__str__()).strip()
                    page[tag] = datetime.strftime(datetime.strptime(date_str,'%b %d, %Y'),\
                                "%Y-%m-%dT%H:%M:%SZ")
                else:
                    page[tag] = stripHtml(each.next.next.__str__()) 
        except:
            log.exception(self.log_msg('game info not found %s'%self.currenturi))
                
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
                log.exception(self.log_msg('Update session info returns False %s'%self.currenturi))
                return True
            page['parent_path'] = [] #parent path empty..recheck why not product page!!
            page['path'] = [self.task.instance_data['uri']]
            page['uri'] = self.currenturi
            page['entity'] = 'Review'
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            #log.info(page)
            log.info(self.log_msg('Post Added %s'%self.currenturi))
            return True        
        except:
            log.exception(self.log_msg('Error while adding session info %s'%self.currenturi))
            return False    

    @logit(log, '__iteratePosts')
    def __iteratePosts(self):   
        try:
            posts = self.soup.findAll('div','comment_text_container')
            if not posts:
                log.info(self.log_msg('No posts found %s'%self.currenturi))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(posts)))
            for post in posts[:]:#use some range for few data  
                if not self.__addPost(post):
                    return False 
            return True    
        except:
            log.exception(self.log_msg('can not  find the data %s'%self.currenturi))
            return False     
    
    @logit(log, '__addPost')    
    def __addPost(self, post): 
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            page = self.__getData(post)
            if not page:
                return True 
            unique_key = get_hash({'posted_date' : page['posted_date'], 'data': page['data']}) 
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
                get_hash( page ),'review', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if not result['updated']:
                log.exception(self.log_msg('Update session info returns False'))
                return True
            page['parent_path'] = [self.task.instance_data['uri']]
            page['path'] =  [self.task.instance_data['uri'],unique_key]
            page['uri'] = self.currenturi
            page['entity'] = 'review'
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            #log.info(page)
            log.info(self.log_msg('page added  %s'%self.currenturi))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False  
    
    @logit(log, '__getData')
    def __getData(self, post):
        page = {}
        try:
            data_tag = post.find('div', 'comment_text')
            quote_tag = data_tag.find('div', 'quoted_msg')
            if quote_tag:
                quote_tag.extract()
            page['title'] = page['data'] = stripHtml(data_tag.renderContents()).\
                            replace('/>>','/>')
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['title'] = page['data'] = ''        
        
        if not page['title'] and not page['data']: 
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False  
        try:
            date_str = stripHtml(post.find('div','comment_date').renderContents()).\
                        split('Posted')[-1].strip()
            page['posted_date']= datetime.strftime(datetime.strptime(date_str,'%m-%d-%Y %I:%M%p'),\
                                "%Y-%m-%dT%H:%M:%SZ")   
        except:
            log.exception(self.log_msg('posted_date nt found %s'%self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")                                 
        
        # author info 
        try:
            page['et_author_name'] = stripHtml(post.find('div','comment_username').\
                                        renderContents())
        except:
            log.exception(self.log_msg('author_name not found %s'%self.currenturi))
                                            
        copycurrenturi = self.currenturi
        try:
            auth_link = post.find('div','comment_username').find('a')['href']
            self.currenturi = auth_link
            self.__setSoupForCurrentUri()  
            try:
                page['et_author_level'] = stripHtml(self.soup.find('div','gamepad_leftnav_level').\
                                            renderContents()).split('Level')[-1]
            except:
                log.exception(self.log_msg('author level  not found %s'%self.currenturi))                                              
            try:     
                auth_info = stripHtml(self.soup.find('div','info_box_text').\
                        renderContents()).split('\n') 
                for each in auth_info:         
                    if 'Join' in each:
                        try:
                            date_str =  stripHtml(each.__str__()).\
                                        split(':')[-1].strip()
                            page['edate_author_join_date'] = datetime.\
                                                            strftime(datetime.strptime(date_str,'%b %d, %Y'),\
                                                            "%Y-%m-%dT%H:%M:%SZ")                                   
                        except:
                            log.exception(self.log_msg('Join date not found %s'%self.currenturi))
                    elif 'Experience' in each:
                        page['ei_author_experience'] = int(stripHtml(each.__str__()).\
                                                        split(':')[-1].replace(',',''))
                    elif 'GTP' in each:
                        page['ei_author_gtp'] = int(stripHtml(each.__str__()).\
                                                split(':')[-1].replace(',','')) 
                    elif 'Last' in each:
                        date = stripHtml(each.__str__()).split(':')[-1].strip() 
                        date_exp = re.search(re.compile(r'([0-9]*) (hour|hours|minute|mins|minutes|day|days|month|months) ago'),date)
                        if date_exp:
                            if date_exp.group(2) in ['day','days']:
                                page['edate_author_last_online'] = datetime.\
                                                strftime(datetime.utcnow()-\
                                                timedelta(days=int(date_exp.group(1))),"%Y-%m-%dT%H:%M:%SZ")
                            elif date_exp.group(2) in ['hour','hours']:
                                page['edate_author_last_online'] =  datetime.\
                                strftime(datetime.utcnow()-timedelta(seconds=3600*int(date_exp.group(1))),"%Y-%m-%dT%H:%M:%SZ")
                            elif date_exp.group(2) in '[minute,mins,minutes]':
                                page['edate_author_last_online'] =  datetime.\
                                strftime(datetime.utcnow()-timedelta(seconds=60*int(date_exp.group(1))),"%Y-%m-%dT%H:%M:%SZ")
                            elif date_exp.group(2) in '[month,months]':
                                page['edate_author_last_online'] = datetime.\
                                strftime(datetime.utcnow()-timedelta(days=30*int(date_exp.group(1))),"%Y-%m-%dT%H:%M:%SZ")
                                
            except:
                log.exception(self.log_msg('auth_info not found %s'%self.currenturi))
            try:
                rating_info = stripHtml(self.soup.find('div','info_box_middle').\
                                find('div','info_box_title',text = re.compile('Thumb Ratings')).\
                                findNext('div').renderContents()).split('\n')
                for each in rating_info:
                    if 'Total' in each:
                        try:
                            page['ei_author_total_ratings_score'] = int(stripHtml(each.__str__()).\
                                                                    split(':')[-1].replace(',',''))
                        except:
                            log.exception(self.log_msg('auth total score not found %s'%self.currenturi))         
                    elif '+' in each:
                        try:
                            page['ei_author_total_positive_thumbs_given'] =\
                                            int(stripHtml(each.__str__()).split(':')[-1].\
                                            replace(',','').replace('-',''))
                        except:
                            log.exception(self.log_msg('positive thumbs no not found %s'%self.currenturi))                   
                    elif '-' in each:
                        try:
                            page['ei_author_total_negative_thumbs_given'] =\
                                          int(stripHtml(each.__str__()).split(':')[-1].replace(',','').replace('-','')) 
                        except:
                            log.exception(self.log_msg('negative thumbs not found %s'%self.currenturi))                
                                                                               
            except:
                    log.exception(self.log_msg('rating info not found %s'%self.currenturi))                                                 
        except:
            log.exception(self.log_msg('author link not found %s'%self.currenturi))
        self.currenturi = copycurrenturi   
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
            
                                 
                     
           
          
        
    