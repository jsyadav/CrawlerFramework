import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime
from urllib import urlencode 

from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('DooYooConnector')
class DooYooConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        '''
        try:
            self.baseuri = 'http://www.dooyoo.co.uk'
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
            main_page_soup = copy.copy(self.soup)
            while self.__iteratePosts():
                try:
                    pagination_tag = [each for each in main_page_soup.find('div', 'i7paging').findAll('li') if not each.string=='Page:']
                    if not pagination_tag:
                        break
                    current_page_index = 0
                    for i, next_page_tag in enumerate(pagination_tag):
                        if next_page_tag.get('id', '')=='current':
                            current_page_index = i
                            break
                    if current_page_index==len(pagination_tag) -1:
                        break
                    else:
                        self.currenturi = self.baseuri + pagination_tag[current_page_index + 1].find('a')['href']
                    #main_page_soup.find('div', 'i7paging').find('li', id='current').next.next.next.a['href']
                    self.__setSoupForCurrentUri()
                    main_page_soup = copy.copy(self.soup)
                except:
                    log.exception(self.log_msg('can not fetch next page links %s'))
                    break
        except:
            log.exception(self.log_msg('page not fetched'))
        return True
    
    @logit(log, '__iteratePosts')
    def __iteratePosts(self):   
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = self.soup.findAll('p','review description')
            if not posts:
                log.info(self.log_msg('No posts found'))
                return False
            log.info(self.log_msg('Total No of Posts found is %d'%len(posts)))
            #return True
            #posts.reverse()
            main_page_uri = self.currenturi
            for post in posts:
                links = post.find('a')
                if not links:
                    if not self.__addReview(post, main_page_uri):
                        False
                else:
                    if not self.__addPost(links['href']):
                        False
            return True    
        except:
            log.exception(self.log_msg('can not  find the data'))
            return False
    
    @logit(log, '__addReview')
    def __addReview(self, post, main_page_uri):
        """ this will add the post if there is  no other page link""" 
        try:
            page = {}
            #self.currenturi = main_page_uri
            #self.__setSoupForCurrentUri()
            try:
                page['title'] = stripHtml(post.findParent('div').renderContents()).split('\n')[0]
                log.info(page['title'])
            except:
                log.exception(self.log_msg('title not found'))
                page['title'] = ''
                
            try:
                page['data'] = stripHtml(post.renderContents())
            except:
                log.info(self.log_msg('Data not found for the url %s'%self.currenturi))
                page['data'] = ''        
            
            if not page['title'] and not page['data']:
                log.info(self.log_msg("Data and title not found for %s,"\
                                        " discarding this review"%self.currenturi))
                return True
            try:
                date_str = stripHtml(post.findParent('div').find('abbr','dtreviewed').renderContents())
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%d/%m/%y'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception(self.log_msg('posted date not found')) 
                page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")    
            try:
                name = stripHtml(post.findParent('div').renderContents()).\
                                    split('\n')[2].split('-')[0].split('by')[-1]
                if 'member_link' in name:
                    page['et_author_name'] = name.split(';')[-1].strip()
                    page.update(self.__addAuthorInfo(page['et_author_name']))
                else:
                    page['et_author_name'] = name
            except:
                log.exception(self.log_msg('author name not found'))            
            unique_key  = get_hash({'data' : page['data']})
            if checkSessionInfo('review', self.session_info_out, unique_key,\
                         self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True'))
                return False
        
            result=updateSessionInfo('review', self.session_info_out, unique_key, \
                get_hash( page ),'Review', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if not result['updated']:
                log.info(self.log_msg('Update session info returns False'))
                return True
            page['path'] = [self.task.instance_data['uri'], unique_key] 
            page['parent_path'] = [self.task.instance_data['uri']]
            page['uri'] = main_page_uri
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page['entity'] = 'review'
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            log.info(page)
            log.info(self.log_msg('Post Added'))
        except:
            log.exception(self.log_msg('Error while adding session info'))
        return True
            
    @logit(log, '__addAuthorInfo')  
    def __addAuthorInfo(self, auth_name):
        """ this will fetch all the author info """
        auth_info = {}
        try:
            log.info(auth_name)
            log.info(type(auth_name))
            self.currenturi = self.baseuri + '/member/' +  str(auth_name)
            self.__setSoupForCurrentUri()
            try:
                stat_table = self.soup.find('td','pp_head_sm').findParent('table')
                #dict([('et_' + k.lower().replace(' ', '_') ,v) for k,v in dict([[stripHtml(xx.renderContents()) for xx in each.findAll('td')] for each in stat_table.findAll('tr') if len(each.findAll('td'))==2][1:]).iteritems()])
                try:
                    date_str = stripHtml(stat_table.find('td',text = re.compile('dooyoo member since')).\
                            findNext('td').renderContents())
                    auth_info['edate_author_membership_date'] = datetime.strftime(datetime.strptime(date_str,'%d/%m/%y'),"%Y-%m-%dT%H:%M:%SZ")                    
                except:
                    log.exception(self.log_msg('member date not found'))
                try:
                    auth_info['et_author_primium_reviews'] = stripHtml(stat_table.\
                                        find('td',text = re.compile('Premium Reviews')).\
                                        findNext('td').renderContents())
                except:
                    log.exception(self.log_msg('primium review not found'))
                try:
                    auth_info['et_author_express_reviews'] = stripHtml(stat_table.\
                                        find('td',text = re.compile('Express Reviews')).\
                                        findNext('td').renderContents())
                except:
                    log.exception(self.log_msg('express review not found'))                        
                try:
                    auth_info['et_author_comment_written'] = stripHtml(stat_table.\
                                        find(   'td',text = re.compile('Comments Written')).\
                                        findNext('td').renderContents())
                                        
                except:
                    log.exception(self.log_msg('comment_written not found'))
                try:
                    auth_info['et_author_reviews_rated'] = stripHtml(stat_table.find('td',text = re.compile('Reviews Rated')).\
                                                    findNext('td').renderContents())
                except:
                    log.exception(self.log_msg('review rated not found')) 
                try:
                    auth_info['et_author_crowns'] = stripHtml(stat_table.find('td',text = re.compile('Crowns')).\
                                                    findNext('td').renderContents())
                except:
                    log.exception(self.log_msg('Crowns  not found')) 
                try:
                    auth_info['et_author_community_ratings'] = stripHtml(stat_table.find('td',text = re.compile('Community Ratings')).\
                                                    findNext('td').renderContents())
                except:
                    log.exception(self.log_msg('Community Ratings not found')) 
                try:
                    auth_info['et_author_very_useful'] = stripHtml(stat_table.find('td',text = re.compile('Very useful')).\
                                                    findNext('td').renderContents())
                except:
                    log.exception(self.log_msg('Very useful not found')) 
                try:
                    auth_info['et_author_useful'] = stripHtml(stat_table.find('td',text = re.compile('Useful')).\
                                                    findNext('td').renderContents())
                except:
                    log.exception(self.log_msg('Useful not found')) 
                try:
                    auth_info['et_author_somewhat_useful'] = stripHtml(stat_table.find('td',text = re.compile('Somewhat useful')).\
                                                    findNext('td').renderContents())
                except:
                    log.exception(self.log_msg('Somewhat useful not found')) 
                try:
                    auth_info['et_author_not_useful'] = stripHtml(stat_table.find('td',text = re.compile('Not useful')).\
                                                    findNext('td').renderContents())
                except:
                    log.exception(self.log_msg('Not useful not found')) 
            except:
                log.exception(self.log_msg('stat not found'))
            try:
                personal_table = self.soup.find('div',text = 'Member Details').\
                                findParent('tr').findNext('tr').findNext('tr').findAll('p')
                if personal_table:                
                    for each in personal_table:
                        info = stripHtml(each.renderContents())
                        if 'Gender:' in info:
                            auth_info['et_author_gender'] = info.split(':')[-1]
                        if 'city:' in info:     
                            auth_info['et_author_city'] = info.split(':')[-1]
                        if 'Surname:' in info:
                            auth_info['et_author_last_name'] = info.split(':')[-1]
                        if 'Occupation:' in info:        
                            auth_info['et_author_occuption'] = info.split(':')[-1]
                        if 'Industry:' in info:    
                            auth_info['et_author_industry'] = info.split(':')[-1]
                        if 'Country:' in info:    
                            auth_info['et_author_country'] = info.split(':')[-1] 
                        if 'Homepage:' in info:    
                            auth_info['et_author_homepage'] = info.split(':')[-1]
                        if 'Date of birth:' in info:    
                            auth_info['et_author_dob'] = info.split(':')[-1]
                        if 'Email address:' in info:    
                            auth_info['et_author_email'] = info.split(':')[-1]               
                            
            except:
                log.exception(self.log_msg('personal_info not found')) 
        except:
            log.exception(self.log_msg('author info not found'))
        return auth_info
                   
            
    @logit(log, '__addPost')  
    def __addPost(self, link): 
        """
        This will take the post tag , and fetch data 
        """
        try:
            self.currenturi = self.baseuri + link
            if checkSessionInfo('review', self.session_info_out, self.currenturi,\
                         self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True'))
                return False
            self.__setSoupForCurrentUri()  
            review_url = self.currenturi
            page = self.__getData()
            if not page:
                return True
            self.currenturi = review_url
            result=updateSessionInfo('review', self.session_info_out, self.currenturi, \
                get_hash( page ),'Review', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if not result['updated']:
                log.info(self.log_msg('Update session info returns False'))
                return True
            page['path'] = [self.task.instance_data['uri'], self.currenturi] 
            page['parent_path'] = [self.task.instance_data['uri']]
            page['uri'] = self.currenturi
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page['entity'] = 'review'
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
        baseuri = 'http://www.dooyoo.co.uk'
        try:
            page['title'] = stripHtml(self.soup.find('h2', attrs={'class':re.compile('i7_revheader_')}).next.__str__())
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
            
        try:
            page['data'] = stripHtml(self.soup.find('div','description').renderContents())
        except:
            log.info(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''        
        
        if not page['title'] and not page['data']:
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False                    
        try:
            date_str = stripHtml(self.soup.find('abbr','dtreviewed').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%d/%m/%y'),"%Y-%m-%dT%H:%M:%SZ")             
        except:
            log.exception(self.log_msg('Posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
            
        try:
            page['et_data_pros'] = stripHtml(self.soup.find('b',text ='Advantages:').findParent('p').renderContents()).split(':')[-1]   
            page['et_data_cons'] = stripHtml(self.soup.find('b',text ='Disadvantages:').findParent('p').renderContents()).split(':')[-1]
        except:
            log.exception(self.log_msg('adv and disadv not found'))     
        
        try:
            auth_name = self.soup.find('span','reviewer vcard')
            if auth_name:
                page['et_author_name'] = stripHtml(auth_name.renderContents())
                page.update(self.__addAuthorInfo(page['et_author_name']))
            else:
                page['et_author_name'] = stripHtml(self.soup.find('strong',text =  re.compile('Author Name:')).\
                                        findParent('p').renderContents()).split(':')[-1].strip()     
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