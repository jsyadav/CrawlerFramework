#-------------------------------------------------------------------------------
# Name:        Module1
# Purpose:     
#
# Author:      Pratik
#
# Created:     25/05/2009
# Copyright:   (c) Pratik 2009
# Licence:     <your licence>
#-------------------------------------------------------------------------------
#!/usr/bin/env python

import re
from datetime import datetime
import logging
from urllib2 import urlparse
from urllib import urlencode
import urllib
from BeautifulSoup import BeautifulSoup
from cgi import parse_qsl
import copy

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('NateForumConnector')

class NateForumConnector(BaseConnector):
        'add'
       
        def fetch(self):
            """
            http://ask.nate.com
             """
            self.genre="Review"
            try:
            #self.currenturi ='http://ask.nate.com/qna/holdlist.html?c=2583'
                self.parent_uri = self.currenturi
                if self.currenturi.startswith('http://ask.nate.com/qna/view.html'):
                    if not self.__setSoup():
                        log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                        return False
                    self.__getParentPage("Forum")
                    self.post_type= True
                    while True:
                        self.__addPosts()
                        try:
                            self.currenturi = 'http://ask.nate.com/' + self.soup.find('span',attrs={'class':'next'}).find('a')['href']
                        except:
                            log.info(self.log_msg('Next page not set'))
                            break
                    return True
                elif self.currenturi.startswith('http://news.nate.com/view/'):
                      if not self.__setSoup():
                        log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                        return False
                      self.__getParentPage("News")
                      self.post_type= True
                      while True:
                        self.__addPosts()
                        try:
                            self.currenturi = 'http://news.nate.com/' + self.soup.find('span',attrs={'class':'next'}).find('a')['href']
                        except:
                            log.info(self.log_msg('Next page not set'))
                            break
                      return True
                elif self.currenturi.startswith('http://search.nate.com/search/ok.html'):
                        self.total_posts_count = 0
                        self.last_timestamp = datetime( 1980,1,1 )
                        self.max_posts_count = int(tg.config.get(path='Connector',key='nateforum_numresults'))
                        self.currenturi = self.currenturi + '&rv=1'
                        temp_str = 'http://search.nate.com/search/ok.html'
                        if not self.__setSoup():
                           log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                           return False
                        count =0;
                        while True:
                            if not self.__getThreadsSearchForum():
                                break
                            try:
                                self.currenturi = temp_str + str(self.soup.findAll('span',attrs={'class':'paging'})[0].findAll('a')[count]['href'])
                                log.info(self.currenturi)
                                count = count + 1
                                #self.currenturi = self.currenturi + "&p=" + str(count)
                                 #'http://ask.nate.com/' + self.soup.find('span',attrs={'class':'next'}).find('a')['href']
                                if not self.__setSoup():
                                    break
                            except:
                                log.info(self.log_msg('Next Page link not found'))
                                break
                        if self.linksOut:
                            updateSessionInfo('Search', self.session_info_out,self.last_timestamp , None,'ForumThreadsPage', self.task.instance_data.get('update'))
                        return True
                elif self.currenturi.startswith('http://search.nate.com/search/obp.html'):
                     
                        self.total_posts_count = 0
                        self.last_timestamp = datetime( 1980,1,1 )
                        self.max_posts_count = int(tg.config.get(path='Connector',key='nateforum_numresults'))
                        self.currenturi = self.currenturi + '&rv=1'
                        temp_str = 'http://search.nate.com/search/'
                        if not self.__setSoup():
                           log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                           return False
                        count =0;
                        while True:
                            if not self.__getThreadsSearchBlog():
                                break
                            try:
                                self.currenturi = temp_str + str(self.soup.findAll('span',attrs={'class':'paging'})[0].findAll('a')[count]['href'])
                                log.info(self.currenturi)
                                count = count + 1
                                if not self.__setSoup():
                                    break
                            except:
                                log.info(self.log_msg('Next Page link not found'))
                                break
                        if self.linksOut:
                            updateSessionInfo('Search', self.session_info_out,self.last_timestamp , None,'ForumThreadsPage', self.task.instance_data.get('update'))
                        return True
                elif self.currenturi.startswith('http://search.nate.com/search/news.html'):

                        self.total_posts_count = 0
                        self.last_timestamp = datetime( 1980,1,1 )
                        self.max_posts_count = int(tg.config.get(path='Connector',key='nateforum_numresults'))
                        #self.currenturi = self.currenturi + '&daysprune=-1&order=desc&sort=lastpost'
                        temp_str = 'http://search.nate.com/search/news.html'
                        if not self.__setSoup():
                           log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                           return False
                        count =0;
                        while True:
                            if not self.__getThreadsSearchNews():
                                break
                            try:
                                self.currenturi = temp_str + str(self.soup.findAll('span',attrs={'class':'paging'})[0].findAll('a')[count]['href'])
                                log.info(self.currenturi)
                                count = count + 1
                                if not self.__setSoup():
                                    break
                            except:
                                log.info(self.log_msg('Next Page link not found'))
                                break
                        if self.linksOut:
                            updateSessionInfo('Search', self.session_info_out,self.last_timestamp , None,'ForumThreadsPage', self.task.instance_data.get('update'))
                        return True
                else:
                    log.info(self.log_msg('Url format is not recognized, Please verify the url'))
            except:
                log.exception(self.log_msg('Exception in fetch'))
                return False
                        
        @logit(log, '__getThreads')
        def __getThreads(self):
            try:
                ''
                threads = [each.findParent('tr') for each in self.soup.findAll('td',attrs={'class':'chek'})]
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False
            
            for thread in threads:
                
                if  self.max_posts_count <= self.total_posts_count :
                        log.info(self.log_msg('Reaching maximum post,Return false'))
                        return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    post_date =  stripHtml(thread.find('td',attrs={'class':'time'}).renderContents())
                    log.info(post_date)
                    try:
                        if post_date.__contains__('.'):
                            thread_time = datetime.strptime (post_date,'%y.%m.%d')
                        else:
                            thread_time = datetime.strptime (post_date,'%y.%m.%d %H:%M')
                        post_date = datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    except:
                       try:
                            match_object = re.match('^(\d{2}):(\d{2})$',post_date)
                            today_date =datetime.utcnow()
                            thread_time = datetime(year=today_date.year,month=today_date.month,day=today_date.day,hour=int(match_object.group(1)),minute=int(match_object.group(2)))
                       except:
                            log.info(self.log_msg('Cannot add the post continue'))
                       post_date = datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    
                    temp_task = self.task.clone()
                    temp_task.pagedata['et_data_category'] = stripHtml((thread.find('td',attrs={'class':'chek'})).renderContents())
                    temp_task.instance_data['uri'] = normalize( 'http://ask.nate.com/' + thread.find('td',attrs={'class':'titl'}).find('a')['href'] )
                    temp_task.pagedata['title'] = stripHtml((thread.find('td',attrs={'class':'titl'})).renderContents())
                    temp_task.pagedata['ei_thread_replies_count'] = stripHtml((thread.find('td',attrs={'class':'repl'})).renderContents())
                    temp_task.pagedata['posted_date'] = post_date
                    log.info(temp_task.pagedata['uri'])
                    self.linksOut.append( temp_task )
                except:
                        log.info(self.log_msg('Post details not be found'))
                        continue
                    
            return True
        @logit(log, '__getData')
        def __getData(self, review, post_type ):
               page = {'title':''}
##               try:
##                page['et_author_name'] = stripHtml((review.find('a',attrs={'class':'nickname'})).renderContents())
##
##               except:
##                log.info(self.log_msg('author name not found'))

               #if post_type== 'Question':
               try:
                   tempv = review.findAll('span',attrs={'class':'num'})
                   page['ei_thread_replies_count'] = stripHtml(tempv[1].renderContents())
                   page['ei_thread_views_count'] = stripHtml(tempv[2].renderContents())
                   page['ei_reply_shared_count'] =  int(stripHtml((soup.find('div',attrs ={'class':'btnUpCount'}).find('strong')).renderContents()))
               except:
                   log.info(self.log_msg('author post status not found'))
               try:
                 page['title'] = stripHtml((review.find('div',attrs={'class':'titleInfo'}).find('strong')).renderContents())
               except:
                   ''
               try:
                   date_str = stripHtml((review.find('span',attrs ={'class':'num'})).renderContents())
                   #date_str = stripHtml(date.renderContents())
               except:
                   ''
               try:

                    thread_time = datetime.strptime (date_str,'%y.%m.%d %H:%M')
                    page['posted_date'] = datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
               except:
                   try:
                        match_object = re.match('^(\d{2}):(\d{2})$',date_str)
                        today_date =datetime.utcnow()
                        thread_time = datetime(year=today_date.year,month=today_date.month,day=today_date.day,hour=int(match_object.group(1)),minute=int(match_object.group(2)))
                   except:
                        log.info(self.log_msg('Cannot add the post continue'))
                   page['posted_date'] = datetime.strftime(thread_time,"%Y-%m-dT%H:%M:%SZ")
                   log.info(page['posted_date'])
                   log.info("data posted date")
               try:
                page['data']  =  stripHtml((review.find('div',attrs={'class':'ViewContents'})).renderContents())
               except:
                page['data'] = ''
                log.info(self.log_msg('Data not found for this post'))

               try:
                   author_prof_link = stripHtml((review.find('div',id =re.compile('^memberInfo_.*$')).findAll('a')[0]['href']))
                   log.info(str(author_prof_link))
                   page.update(self.__getAuthorInfo(author_prof_link))
               except:
                    log.exception(self.log_msg('Data not found for author_info'))
                
               return page
           
        
            
        @logit(log, '__addPosts')
        def __addPosts(self):
            
                try:
                    reviews =self.soup.findAll('div',attrs = {'class':'AnswerWrap'})
                except:
                    log.exception(self.log_msg('Reviews are not found'))
                    return False

                for i, review in enumerate(reviews):
                    post_type = "Question"
                    if i==0 and self.post_type:
                        post_type = "Question"
                        self.post_type = False
                    else:
                        post_type = "Answer"
                    page = self.__getData( review , post_type )
                    #log.info(page)
                    try:
                        review_hash = get_hash( page )
                        unique_key = get_hash( {'data':page['data'],'title':page['title']})
                        if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                                     self.task.instance_data.get('update'),parent_list\
                                                                    =[self.parent_uri]):
                            continue
                        result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                                    review_hash,'Review', self.task.instance_data.get('update'),\
                                                                parent_list=[self.parent_uri])
                        if not result['updated']:
                            continue
                        #page['id'] = result['id']
                        #page['first_version_id']=result['first_version_id']
                        #page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                        parent_list = [ self.parent_uri ]
                        page['parent_path'] = copy.copy(parent_list)
                        parent_list.append( unique_key )
                        page['path']=parent_list
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                            ,"%Y-%m-%dT%H:%M:%SZ")
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id
                        page['client_name'] = self.task.client_name
                        page['last_updated_time'] = page['pickup_date']
                        page['versioned'] = False
                        page['entity'] = 'Review'
                        page['category'] = self.task.instance_data.get('category','')
                        page['task_log_id']=self.task.id
                        page['uri'] = self.currenturi
                        page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                        self.pages.append( page )
                        log.info(page)
                        log.info(self.log_msg('Review Added'))
                    except:
                        log.exception(self.log_msg('Error while adding session info'))

        @logit(log, '__getParentPage')
        def __getParentPage(self,u_type):
            """
            This will get the parent info
            thread url.split('&')[-1].split('=')[-1]

            """
            if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                             self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info return True, Already exists'))
                return False
            page = {}
            if u_type == "Forum":
                log.info("entering forum")
                try:
                    tempLis=[]
                    tempr = self.soup.findAll('a',attrs={'class':'location'})
                    for each in tempr:
                         tempLis.append(stripHtml(each.renderContents()))

                    tempLis.append(stripHtml((self.soup.find('a',attrs={'class':'dir_on'})).renderContents()))

                    page['et_thread_hierarchy'] = tempLis
                    page['title']= stripHtml((self.soup.find('div',attrs={'class':'titleInfo'}).find('strong')).renderContents())
                    page['ei_question_agreed']= int(stripHtml((self.soup.find('div',attrs ={'class':'totalUpCount'}).find('strong')).renderContents()))
                    log.info(page['title'])
                except:
                    log.info(self.log_msg('Thread hierarchy is not found'))
                try:
                    review_parent =self.soup.findAll('div',attrs = {'class':'QuestionWrap'})
                    for each in review_parent:
                         post_type = 'Question'
                         page.update(self.__getData( each , post_type ))
                except:
                     log.info(self.log_msg('page data cannot be extracted'))
                    #page['title']=''
                for each in ['ei_thread_replies_count','edate_last_post_date','et_data_category']:
                    try:
                        page[each] = self.task.pagedata[each]
                    except:
                        log.exception(self.log_msg('page data cannot be extracted'))

                try:
                    page['et_thread_id'] = self.currenturi.split('=')[1].split('&')[0]
                except:
                    log.info(self.log_msg('Thread id not found'))
                log.info(page)
                log.info("page status parent")
            elif u_type=="News":
                log.info("entering news")
                try:
                    ''
                    page['title'] = stripHtml(self.soup.find('h3',attrs={'class':'articleSubecjt'}).renderContents())
                    page['posted_date'] = self.task.pagedata['edate_last_post_date']
                    page['data'] = stripHtml(self.soup.find('div',attrs={'id':'articleContetns'}).renderContents())
                    page['et_search_category'] = self.task.pagedata['et_search_category']
                except:
                    ''
                    log.info("news data cannot be extracted")
            try:
                post_hash = get_hash( page )
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo( self.genre, self.session_info_out, self.\
                       parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
                if not result['updated']:
                    return False
                page['path']=[self.parent_uri]
                page['parent_path']=[]
                page['uri'] = normalize( self.currenturi )
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                #page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                page['client_name'] = self.task.client_name
                page['last_updated_time'] = page['pickup_date']
                page['versioned'] = False
                #page['first_version_id']=result['first_version_id']
                if u_type == "Forum":
                    page['data'] = ''
                #page['id'] = result['id']
                page['task_log_id']=self.task.id
                page['entity'] = 'Post'
                page['category']=self.task.instance_data.get('category','')
                self.pages.append(page)
                log.info(page)
                log.info(self.log_msg('Parent Page added'))
                return True
            except :
                log.exception(self.log_msg("parent post couldn't be parsed"))
                return False
            
        @logit(log, '__getAuthorInfo')
        def __getAuthorInfo(self,urlpath):
            
                 tempage = urllib.urlopen(urlpath)
                 tempSoup = BeautifulSoup(tempage)
                 pages ={}
                 try:
                     author_level_str = stripHtml(tempSoup.find('div',attrs={'class':'mylevel'}).find('img')['alt']).split(' ')[1]
                     pages['ei_author_level'] = author_level_str.strip()
                     pages['ei_author_rank'] = int(re.search('\d+',((stripHtml(tempSoup.find('ul',attrs={'class':'myPoint'}).findAll('li')[0].renderContents()).split(':')[1]).strip()).replace(',','')).group())
                     pages['ei_author_index'] = int(re.search('\d+',((stripHtml(tempSoup.find('ul',attrs={'class':'myPoint'}).findAll('li')[1].renderContents()).split(':')[1]).strip()).replace(',','')).group())
                     pages['ei_author_mileage']= int(re.search('\d+',((stripHtml(tempSoup.find('ul',attrs={'class':'myPoint'}).findAll('li')[2].renderContents()).split(':')[1]).strip()).replace(',','')).group())
                     pages['ei_author_questions_count'] = int((stripHtml((tempSoup.find('dd',attrs={'class':'quest'})).renderContents()).split(' ')[0]).strip())
                     pages['et_author_name'] = tempSoup.find('div',attrs={'class':'myInfo'})['title']
                 except:
                     log.exception("author info cant be fetched")

                 log.info(pages)
                 return pages
             
        @logit(log, '__setSoup')
        def __setSoup( self, url = None, data = None, headers = {} ):
            """
                It will set the uri to current page, written in seperate
                method so as to avoid code redundancy
            """
            if url:
                self.currenturi = url
            try:
                log.info(self.log_msg( 'for uri %s'%(self.currenturi) ))
                res = self._getHTML( data = data, headers=headers  )
                #log.info(res)
                #res = self._getHTML()
                if res:
                    self.rawpage = res[ 'result' ]
                else:
                    log.info(self.log_msg('self.rawpage not set.... so Sorry..'))
                    return False
                self._setCurrentPage()
                
                return True
            except Exception, e:
                log.exception(self.log_msg('Page not for  :%s'%url))
                raise e
        @logit(log, '__getThreadsSearchForum')
        def __getThreadsSearchForum(self):
            try:
                ''
                threads = [each.findParent('dl') for each in self.soup.findAll('dt',attrs={'class':'text-inline'})]
                #log.info(threads)
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False

            for thread in threads:
                if  self.max_posts_count <= self.total_posts_count :
                        log.info(self.log_msg('Reaching maximum post,Return false'))
                        return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    tempLis = thread.findAll('i')
                    post_date =  tempLis[0].nextSibling
                    log.info(post_date)
                    post_date = post_date.strip()
                    log.info(post_date)

                    try:
                        if post_date.__contains__('.'):
                            thread_time = datetime.strptime (post_date,'%Y.%m.%d')
                        else:
                            thread_time = datetime.strptime (post_date,'%y.%m.%d %H:%M')
                        post_date = datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                        log.info(post_date)
                        log.info(thread_time)
                        if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')):
                            log.info(self.log_msg('Session info return True or Reaches max count'))
                            return False
                        self.last_timestamp = max(thread_time , self.last_timestamp )
                    except:
                        log.info("date cannot be parsed")
                        continue
                        
                    
                    temp_task = self.task.clone()
                    try:
                        temp_task.pagedata['ei_thread_replies_count'] = int(re.search('\d+$',tempLis[1].nextSibling.strip()).group())
                        temp_task.pagedata['ei_thread_views_count'] = int(re.search('\d+$',tempLis[2].nextSibling.strip()).group())
                    except:
                        ''
                    dir_lis = thread.findAll('cite')
                    temp_task.pagedata['et_data_category'] = stripHtml((dir_lis[1]).renderContents())
                    temp_task.pagedata['et_search_category'] = stripHtml((dir_lis[0]).renderContents())
                    url_str = normalize(thread.find('a')['href'] )
                    if url_str.__contains__('ask.nate.com'):
                        temp_task.instance_data[ 'uri' ] = url_str
                    log.info(url_str)
                    temp_task.pagedata['edate_last_post_date'] = post_date
                    log.info(temp_task.pagedata)
                    self.linksOut.append( temp_task )
                except:
                        log.exception(self.log_msg('Post details not be found'))
                        continue

            return True
        @logit(log, '__getThreadsSearchBlog')
        def __getThreadsSearchBlog(self):
            try:
                ''
                tempS =self.soup.find('div',attrs={'class':"search-section data-blog"})
                threads = [each.findParent('dl') for each in tempS.findAll('dt',attrs={'class':'text-inline'})]
                #log.info(threads)
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False

            for thread in threads:
                if  self.max_posts_count <= self.total_posts_count :
                        log.info(self.log_msg('Reaching maximum post,Return false'))
                        return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    
                    tempLis = thread.findAll('i')
                    post_date =  tempLis[0].nextSibling
                    log.info(post_date)
                    post_date = post_date.strip()
                    log.info(post_date)
                    try:
                        if post_date.__contains__('.'):
                            thread_time = datetime.strptime (post_date,'%Y.%m.%d')
                        else:
                            thread_time = datetime.strptime (post_date,'%Y.%m.%d %H:%M')
                        post_date = datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                        log.info(post_date)
                        if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')):
                            log.info(self.log_msg('Session info return True or Reaches max count'))
                            return False
                        self.last_timestamp = max(thread_time , self.last_timestamp )
                    except:
                        log.info('date cannot be parsed')
                        continue
                    temp_task = self.task.clone()
                    dir_lis = thread.findAll('cite')
                    temp_task.pagedata['et_search_category'] = stripHtml((dir_lis[0]).renderContents())
                    url_str = normalize(thread.find('a')['href'] )
                    log.info(url_str)
                    #if url_str.__contains__('ask.nate.com'):
                    temp_task.instance_data[ 'uri' ] = url_str
                    #temp_task.pagedata['title'] = stripHtml((thread.find('td',attrs={'class':'titl'})).renderContents())

                    temp_task.pagedata['edate_last_post_date'] = post_date
                    log.info(temp_task.pagedata)
                    self.linksOut.append( temp_task )
                except:
                        log.info(self.log_msg('Post details not be found'))
                        log.exception('mesage')
                        continue

            return True
        @logit(log, '__getThreadsSearchNews')
        def __getThreadsSearchNews(self):
            log.info("entering news section")
            try:
                ''
                threads = [each.findParent('dl') for each in self.soup.findAll('dt',attrs={'class':'text-inline'})]
                #log.info(threads)
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False

            for thread in threads:
                if  self.max_posts_count <= self.total_posts_count :
                        log.info(self.log_msg('Reaching maximum post,Return false'))
                        return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    tempLis = thread.findAll('i')
                    post_date =  tempLis[0].nextSibling
                    post_date = post_date.strip()
                    log.info(post_date)
                    #log.info(post_date)
                    try:
                        if post_date.__contains__('.'):
                            thread_time = datetime.strptime (post_date,'%Y.%m.%d %H:%M')
                        else:
                            thread_time = datetime.strptime (post_date,'%Y.%m.%d')
                        post_date = datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                        log.info(post_date)
                        if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')):
                            log.info(self.log_msg('Session info return True or Reaches max count'))
                            return False
                        self.last_timestamp = max(thread_time , self.last_timestamp )
                    except:
                        log.info("cant parse date")
                        continue
                    temp_task = self.task.clone()
                    dir_lis = thread.findAll('cite')
                    temp_task.pagedata['et_search_category'] = stripHtml(dir_lis[0].renderContents())
                    url_str = normalize(thread.find('a')['href'] )
                    #if url_str.__contains__('news.nate.com'):
                    temp_task.instance_data[ 'uri' ] = url_str
                    #temp_task.pagedata['title'] = stripHtml((thread.find('td',attrs={'class':'titl'})).renderContents())

                    temp_task.pagedata['edate_last_post_date'] = post_date
                    log.info(temp_task.pagedata)
                    self.linksOut.append( temp_task )
                except:
                        log.info(self.log_msg('Post details not be found'))
                        continue

            return True