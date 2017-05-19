'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#prerna


import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime
from cgi import parse_qsl

from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.httpconnection import HTTPConnection
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging. getLogger("ReutersConnector")

class ReutersConnector(BaseConnector):    
    """Connector for reuters.com 
    """
    @logit(log,'fetch')
    def fetch(self):
        """
        It fetches the data from the url mentioned
        sample news_url : http://www.reuters.com/search?blob=groupon
               blog_uri : http://www.reuters.com/search/blog?blob=groupon
        """
        self.__genre = "Review"
        self.base_uri = 'http://www.reuters.com'
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
        try:
            self.__setSoupForCurrentUri()
            c = 0
            main_page_soup= copy.copy(self.soup)
            while True:
                self.__fetchArticle()
                #self.__fetchComments()
                try:
                    self.currenturi = self.base_uri + main_page_soup.find('li','next').\
                                        find('a')['href']
                    self.__setSoupForCurrentUri()
                    main_page_soup = copy.copy(self.soup)
##                    c +=1
##                    if c >=5:
##                        break
                except:
                    log.exception(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                    break                
            return True
        except:
            log.exception(self.log_msg("Exception in fetch %s"%self.currenturi)) 
        return True
        
    @logit(log,'fetchArticle')    
    def __fetchArticle(self):
        """It fetches the product information
        """
        try:
            articles = [each.find('a')['href']for each in self.soup.find('div',id ='searchCenter').findAll('li','searchHeadline')]
            if not articles:
                log.info(self.log_msg("No Articles Found"))
                return False
            log.info(self.log_msg('total article found in %s :%d'%(self.currenturi,len(articles))))
            for article_uri in articles:
                if re.search('blog?',article_uri):
                    self.__fetchBlogs(article_uri)
                else:
                    self.__fetchNews(article_uri)
            return True    
        except:
            log.exception(self.log_msg("there is exception in fetching article %s"%self.currenturi))
        return True            
                   
    @logit(log,'fetchNews')    
    def __fetchNews(self, article_uri):         
        self.currenturi = article_uri
        page = {'uri':self.currenturi}
        try:
            self.__setSoupForCurrentUri()
        except:
            log.exception(self.log_msg('Exception in setting soup for %s'%self.currenturi))
            return False
        if not checkSessionInfo(self.__genre, self.session_info_out, self.currenturi,  \
                            self.task.instance_data.get('update')):
            try:
                page['title'] = stripHtml(self.soup.find('div','column2 gridPanel grid8').\
                    find('h1').renderContents())
            except:
                log.exception(self.log_msg("Exception Occurred while fetching the title!! for %s"%self.currenturi))
                page['title'] =''
            try:
                data = None
                data_tag = self.soup.find('span','articleLocation')
                if data_tag:
                    data = [stripHtml(data_tag.next.next.__str__())] or \
                            stripHtml(self.soup.find('span',id ='midArticle_0').\
                            findPrevious('p').renderContents())
                if data:
                    data.append(stripHtml('\n'.join([each.next.__str__() for each in self.\
                        soup.findAll('span', {'id':re.compile('midArticle_\d+')})])))
                    
                    page['data'] = '\n'.join(data)
                else:
                    page['data'] = stripHtml('\n'.join([each.next.__str__() for each in self.\
                                    soup.findAll('span', {'id':re.compile('midArticle_\d+')})]))
            except:
                log.exception(self.log_msg('Article data not found %s'%self.currenturi))        
                page['data'] =''
                        
            if not page['title'] and not page['data']:
                log.info(self.log_msg("title not found for %s,discarding this review"%self.currenturi))
                return False    
            try:
                author_tag = stripHtml(self.soup.find('p','byline').renderContents())
                if re.search(' at ',author_tag):
                    author_tag = self.soup.find('p','byline')
                    unwanted_tag = author_tag.find('a')
                    if unwanted_tag:
                        unwanted_tag.extract()
                    page['et_author_name'] = stripHtml(self.soup.find('p','byline').\
                            renderContents()).replace('By ','').replace(' at','')        
                else:    
                    page['et_author_name'] = ','.join([stripHtml(each.renderContents())for each in \
                            self.soup.find('p','byline').findAll('a')])
            except:
                log.exception(self.log_msg("author name not found %s"%self.currenturi))
            try:
                page['et_author_location'] = stripHtml((self.soup.find('span','location') or self.soup.find('p','byline').find('a')).\
                            renderContents())
            except:
                log.exception(self.log_msg("Author location not found for %s"\
                                                                %self.currenturi ))
            try:
                #date_str = stripHtml(self.soup.find('span','location').findNext('span','timestamp').renderContents()).replace('EDT','').replace('EST','').strip()
                date_str = stripHtml(self.soup.find('div',id='articleInfo').\
                            findNext('span','timestamp').renderContents())
                date_str = re.split(re.compile(' am| pm |am|pm', re.IGNORECASE),date_str)[0].strip()                    
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str\
                                                    ,'%a %b %d, %Y %I:%M'),"%Y-%m-%dT%H:%M:%SZ")
                log.info(self.log_msg('posted: %s'%page['posted_date']))                                
                    
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                log.exception(self.log_msg('Posted date cannot be fetched for url\
                                                                    %s'%self.currenturi))
            try:
                result = updateSessionInfo(self.__genre, self.session_info_out, self.currenturi, \
                    get_hash(page) ,'Post', self.task.instance_data.get('update'))
                if result['updated']:
                    page['path'] = [self.task.instance_data['uri']]
                    page['parent_path'] = []
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])            
                    page['entity'] = 'News'
                    page.update(self.__task_elements_dict)
                    self.pages.append(page)
                    log.info(self.log_msg('Parent Page added'))
                else:
                    log.exception(self.log_msg('Result[updated] returned True for \
                                                    uri'%self.currenturi))    
            except:
                log.exception(self.log_msg("parent post couldn't be parsed for url \
                                                            %s"%self.currenturi))   
        try:
            self.__fetchNewsComments()
        except:
            log.exception(self.log_msg('problem in fetching comments %s'%self.currenturi))    

    @logit(log,'fetchBlogs')    
    def __fetchBlogs(self, article_uri):         
        self.currenturi = article_uri
        page = {'uri':self.currenturi}

        try:
            self.__setSoupForCurrentUri()
        except:
            log.exception(self.log_msg('Exception in setting soup for %s'%self.currenturi))
            return False#continue
        if not checkSessionInfo(self.__genre, self.session_info_out, self.currenturi,  \
                            self.task.instance_data.get('update')):
            #return False
            try:
                title_tag = self.soup.find('div','columnRight grid8')
                page['title'] = stripHtml((title_tag.find('h1') or  title_tag.find('h2')).renderContents())
                #log.info(page['title'])
            except:
                log.exception(self.log_msg("Exception Occurred while fetching the title!! for %s"%self.currenturi))
                page['title'] =''
            
            try:
                page['data'] = '\n'.join([stripHtml(each.renderContents())for each in self.soup.find('div',id='postcontent').findAll('p')])
            except:
                log.exception(self.log_msg('Article data not found %s'%self.currenturi))        
                page['data'] =''
                        
            if not page['title'] and not page['data']:
                log.info(self.log_msg("title not found for %s,discarding this review"%self.currenturi))
                return False    
            try:
                #date_str = stripHtml(self.soup.find('span','location').findNext('span','timestamp').renderContents()).replace('EDT','').replace('EST','').strip()
                date_str = stripHtml(self.soup.find('div','timestamp').renderContents())
                date_str = re.split('\d+:\d+',date_str)[0].strip()
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
                log.info(self.log_msg('posted: %s'%page['posted_date']))                                
                    
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                log.exception(self.log_msg('Posted date cannot be fetched for url\
                                                                    %s'%self.currenturi))
            try:
                result = updateSessionInfo(self.__genre, self.session_info_out, self.currenturi, \
                    get_hash(page) ,'Post', self.task.instance_data.get('update'))
                if result['updated']:
                    page['path'] = [self.task.instance_data['uri']]
                    page['parent_path'] = []
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])            
                    page['entity'] = 'Blog'
                    page.update(self.__task_elements_dict)
                    self.pages.append(page)
                    log.info(page)
                    log.info(self.log_msg('Parent Page added'))
                else:
                    log.exception(self.log_msg('Result[updated] returned True for \
                                                    uri'%self.currenturi))    
            except:
                log.exception(self.log_msg("parent post couldn't be parsed for url \
                                                            %s"%self.currenturi))   
        try:
            self.__fetchBlogsComments()
        except:
            log.exception(self.log_msg('problem in fetching comments %s'%self.currenturi)) 

    @logit(log,'fetchBlogsComments')     
    def __fetchBlogsComments(self):
        
        try:
            comments = self.soup.find('div','commentlist').findAll('div',id = re.compile('comment-\d+'))
            if not comments:
                return False
            log.info(self.log_msg('no of comments:%s in %s'%(len(comments),self.currenturi)))
            for comment in comments:
                page = {}                                                          
                try:
                    page['et_author_name'] = stripHtml(comment.find('span','commentsAuthor').\
                        renderContents()).split('Posted by')[-1]
                    #log.info(self.log_msg('comment author:%s'%page['et_author_name']))
                except:
                    log.exception(self.log_msg('comment author not found %s'%self.currenturi))
                        
                try:
                    page['data'] = '\n'.join([stripHtml(each.renderContents())for each in comment.find('div','commentsBody').findAll('p')])
                    page['title'] = ''
                    #log.info(self.log_msg('comment data:%s'%page['data']))
                except:
                    log.exception(self.log_msg('comment data not found %s'%self.currenturi))
                    page['data'] =''
                    page['title'] =  ''
                    continue 
                        
                try:
                    date = stripHtml(comment.find('div','commentdate timestamp').renderContents())
                    date_str = re.split('\d+:\d+',date)[0].strip()
                    page['posted_date'] = datetime.strftime(datetime.strptime(date_str\
                                                ,'%b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
                    #log.info(self.log_msg('comment date:%s'%page['posted_date']))
                except:
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    log.exception(self.log_msg("comment posted_date not found %s"%self.currenturi))
                    
                    
                unique_key = comment['id']
                try: 
                    if checkSessionInfo(self.__genre, self.session_info_out, unique_key,  \
                                        self.task.instance_data.get('update')):
                        log.info( self.log_msg('Session info return True for the url %s\
                                                                    '%self.currenturi) )
                        return False
                    result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                            get_hash(page) ,'Post', self.task.instance_data.get('update'))
                    if not result['updated']:
                        return False
                    page['parent_path'] = [self.task.instance_data['uri']]
                    page['path'] = [self.task.instance_data['uri'],unique_key]
                    page['uri'] = self.currenturi + '#' + unique_key
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])            
                    #page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['entity'] = 'Comment'
                    page.update(self.__task_elements_dict)
                    self.pages.append(page)
                    log.info(self.log_msg('comment Page added'))
                except:
                    log.exception(self.log_msg("parent post couldn't be parsed for url \
                                                                %s"%self.currenturi))
            return True                                                    
        except:
            log.exception(self.log_msg("no comments found for thid article %s"%self.currenturi))              
    
    @logit(log,'fetchNewsComments')     
    def __fetchNewsComments(self):
        
        try:
            comment_tag = self.soup.find('div','articleComments').findAll('div','singleComment')
            if not comment_tag:
                return False
            log.info(self.log_msg('no of comments:%s in %s'%(len(comment_tag),self.currenturi)))
            for each in comment_tag:
                page = {}                                                          
                try:
                    page['et_author_name'] = stripHtml(each.find('div','commentAuthor').find('a').\
                                                renderContents())
                    #log.info(self.log_msg('comment author:%s'%page['et_author_name']))
                except:
                    log.exception(self.log_msg('comment author not found %s'%self.currenturi))
                        
                try:
                    page['data'] = stripHtml(each.find('div','commentsBody').renderContents())
                    page['title'] = ''
                    #log.info(self.log_msg('comment data:%s'%page['data']))
                except:
                    log.exception(self.log_msg('comment data not found %s'%self.currenturi))
                    page['data'] =''
                    page['title'] =''
                    continue 
                        
                try:
                    date_str = stripHtml(each.find('div','timestamp').\
                                renderContents())
                    date_str = re.split(re.compile(' am| pm |am|pm', re.IGNORECASE),date_str)[0].strip()                    
                    page['posted_date'] = datetime.strftime(datetime.strptime(date_str\
                                                    ,'%b %d, %Y %I:%M'),"%Y-%m-%dT%H:%M:%SZ")
                    #log.info(self.log_msg('comment date:%s'%page['posted_date']))
                except:
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    log.exception(self.log_msg("comment posted_date not found %s"%self.currenturi))
                    
                    
                unique_key = get_hash({'data': page['data']})    
                try: 
                    if checkSessionInfo(self.__genre, self.session_info_out, unique_key,  \
                                        self.task.instance_data.get('update')):
                        log.info( self.log_msg('Session info return True for the url %s\
                                                                    '%self.currenturi) )
                        return False
                    result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                            get_hash(page) ,'Post', self.task.instance_data.get('update'))
                    if not result['updated']:
                        return False
                    page['parent_path'] = [self.task.instance_data['uri']]
                    page['path'] = [self.task.instance_data['uri'],unique_key]
                    page['uri'] = self.currenturi + '#' + unique_key
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])            
                    
                    page['entity'] = 'Comment'
                    page.update(self.__task_elements_dict)
                    self.pages.append(page)
                    log.info(self.log_msg('comment Page added'))
                except:
                    log.exception(self.log_msg("parent post couldn't be parsed for url \
                                                                %s"%self.currenturi))
            return True                                                    
        except:
            log.exception(self.log_msg("no comments found for thid article %s"%self.currenturi))            
        
    @logit(log,'__setSoupForCurrentUri')
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
    