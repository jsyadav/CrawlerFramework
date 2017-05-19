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
log = logging. getLogger("WsjConnector")

class WsjConnector(BaseConnector):    
    """Connector for blogs.wsj.com 
    """
    @logit(log,'fetch')
    def fetch(self):
        """
        It fetches the data from the url mentioned
        sample url :'http://online.wsj.com/public/search/term.html?KEYWORDS=iphone+4s&x=32&y=9&media=Blogs&source=WSJ.com' 
        http://blogs.wsj.com/marketbeat/2011/11/11/apple-getting-bruised-again/?KEYWORDS=iphone+4s
        http://online.wsj.com/video/kindle-touch-leads-amazon-new-wave-of-e-readers/BD39C950-8D2E-4275-979D-8CB0BB1CB197.html?mod=WSJ_Article_Videocarousel_2"""
        self.__genre = "Review"
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
            if re.search('media=Blogs',self.currenturi):
                self.__setSoupForCurrentUri()
            else:
                data_dict = dict(parse_qsl(self.currenturi.split('?')[-1]))
                if 'KEYWORDS' in data_dict:
                    search_term = data_dict.pop('KEYWORDS')
                else:
                    log.info('no search_term found')
                    return False
                conn = HTTPConnection()
                data = {'KEYWORDS':search_term,'media':'Articles','sort_by':'relevance'}
                headers = {'Host':'online.wsj.com','Referer':self.currenturi}
                self.__setSoupForCurrentUri(data=data,headers=headers)
                
            self.parenturi = self.currenturi
            c = 0
            main_page_soup= copy.copy(self.soup)
            while True:
                self.__fetchArticle()
                self.currenturi = self.parenturi
                try:
                    data = {}
                    headers = {'Host':'online.wsj.com'}
                    if main_page_soup.find('a','_nextLink'):
                        data['page_no'] = int(main_page_soup.find('input',id = 'page_no')['value'])+1
                        self.__setSoupForCurrentUri(data=data,headers=headers)
                        log.info(self.log_msg('now in %s page'%data['page_no']))
                        main_page_soup = copy.copy(self.soup)
##                        c +=1
##                        if c >=1:
##                            break
                    else:
                        log.info('next page not found')
                        return False
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
            articles =  [each.find('a',attrs ={'class':re.compile('mjLinkItem')})['href']for each in \
                        self.soup.findAll('li', attrs = {'class': re.compile('headlineList-item mjItemMain')})]
            
            if not articles:
                log.info(self.log_msg("No Articles Found"))
                return False
            log.info(self.log_msg('total article found in %s :%d'%(self.currenturi,len(articles))))
            for article_uri in articles:
                self.currenturi = article_uri
                page = {'uri':self.currenturi}
                try:
                    self.__setSoupForCurrentUri()
                except:
                    log.exception(self.log_msg('Exception in setting soup for %s'%self.currenturi))
                    continue
                if not checkSessionInfo(self.__genre, self.session_info_out, self.currenturi,  \
                                    self.task.instance_data.get('update')):
                    log.info( self.log_msg('Session info return True for the url %s\
                                                                '%self.currenturi) )
                    try:
                        page['title'] = stripHtml(self.soup.find('div','articleHeadlineBox headlineType-newswire').\
                                          find('h1').renderContents())
                        log.info(self.log_msg('title = %s'%page['title']))
                    except:
                        log.exception(self.log_msg("Exception Occurred while fetching the title!! for %s"%self.currenturi))
                        page['title'] = ''
                
                    try:
                        author_tag = self.soup.find('div','articlePage').find('h3','byline')
                        if author_tag:
                            author_tag.extract()
                            page['et_author_name'] = stripHtml(author_tag.renderContents()).replace('By ','')                  
                    except:
                        log.exception(self.log_msg("Exception Occurred while fetchin author for %s"%self.currenturi))
                    try:
                        data_tag = self.soup.find('div','articlePage') or self.soup.find('div','article story').find('div','liveBlog-header')
                        page['data'] = stripHtml('\n'.join([each.renderContents()for each in data_tag.findAll('p')]))
                        log.info(self.log_msg('data = %s'%page['data']))
                    except:
                        log.exception(self.log_msg('Article data not found %s'%self.currenturi))        
                        page['data'] = ''
                            
                    if not page['title'] and not page['data']:
                        log.info(self.log_msg("title not found for %s,discarding this review"%self.currenturi))
                        return False    
                    try:
                        date_str = stripHtml(self.soup.find('div','articleHeadlineBox headlineType-newswire').\
                                       find('li',attrs = {'class':re.compile('dateStamp')}).renderContents())
                        date_str = re.split(re.compile(' am| pm', re.IGNORECASE),date_str)[0]
                        page['posted_date'] = datetime.strftime(datetime.strptime(date_str\
                                                        ,'%B %d, %Y, %I:%M'),"%Y-%m-%dT%H:%M:%SZ")
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
                            page['entity'] = 'Post'
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
                    self.__fetchComments()
                except:
                    log.exception(self.log_msg('problem in fetching comments %s'%self.currenturi))    
            return True    
        except:
            log.exception(self.log_msg("there is exception in fetching article %s"%self.currenturi))
        return True    
                                                                                                                     
        ###fetching comments         
    @logit(log,'fetchComments')     
    def __fetchComments(self):
        
        try:
            comment_uri = self.soup.find('div','postcomments').find('a','viewall')['href']
            if not comment_uri:
                log.info(self.log_msg('no comments found for %s'%self.currenturi))
                return False
            self.currenturi = comment_uri
            self.__setSoupForCurrentUri()
            comment_tag = self.soup.find('ul','commentlist').findAll('li', attrs = {'class':re.compile('commententry\s+')})
            if not comment_tag:
                log.info('comments not found')
                return False
            log.info(self.log_msg('no of comments:%s in %s'%(len(comment_tag),self.currenturi)))
            for each in comment_tag:
                page = {}    
                unique_key = each.find('a',attrs = {'name':re.compile('comment-\d+')})['name']                                                      
                try:
                    page['et_author_name'] = stripHtml(each.find('li','posterName').\
                                                find('cite').renderContents())
                    #log.info(self.log_msg('comment author:%s'%page['et_author_name']))
                except:
                    log.exception(self.log_msg('comment author not found %s'%self.currenturi))
                        
                try:
                    page['data'] = stripHtml(each.find('div','commentContent').renderContents())
                    page['title'] = ''
                    #log.info(self.log_msg('comment data:%s'%page['data']))
                except:
                    log.exception(self.log_msg('comment data not found %s'%self.currenturi))
                    continue 
                        
                try:
                    date_str = stripHtml(each.find('li','postStamp').renderContents()).strip()
                    page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%I:%M %p %B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
                    #log.info(self.log_msg('comment date:%s'%page['posted_date']))
                except:
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    log.exception(self.log_msg("comment posted_date not found %s"%self.currenturi))
                    
                    
                #unique_key = get_hash({'data': page['data']})    
                try: 
                    if checkSessionInfo(self.__genre, self.session_info_out, unique_key,  \
                                        self.task.instance_data.get('update')):
                        log.info( self.log_msg('Session info return True for the url %s\
                                                                    '%self.currenturi) )
                        continue
                    result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                            get_hash(page) ,'Post', self.task.instance_data.get('update'))
                    if not result['updated']:
                        continue
                    page['parent_path'] = [self.task.instance_data['uri']]
                    page['path'] = [self.task.instance_data['uri'],unique_key]
                    page['uri'] = self.currenturi + '#' + unique_key
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])            
                    page['entity'] = 'Post'
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
    
