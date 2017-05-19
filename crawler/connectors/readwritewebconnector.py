'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#prerna dubey


import re
import copy
import logging
#import urllib2
from urllib2 import urlparse,urlopen,quote
from datetime import datetime
from cgi import parse_qsl
import simplejson
from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.httpconnection import HTTPConnection
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging. getLogger("ReadWriteWebConnector")

class ReadWriteWebConnector(BaseConnector):    
    """Connector for readwriteweb 
    """
    @logit(log,'fetch')
    def fetch(self):
        """
        It fetches the data from the url mentioned      
        'http://www.readwriteweb.com/lijitsearch/?uri=http%253A%252F%252Fwww.lijit.com%252Fusers%252Frww&start_time=1322459942141&p=l&blog_uri=http%253A%252F%252Fwww.readwriteweb.com%252F&blog_platform=&view_id=&link_id=60597&flavor=&q=IPHONE+4S&lijit_q=IPHONE+4S'
        'http://www.lijit.com/search?uri=http%3A%2F%2Fwww.lijit.com%2Fusers%2Frww&q=iphone%204s&type=blog'
        its permalink"""
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
            self.__max_results_count = int(tg.config.get(path='Connector', key=\
                                                'readwriteweb_numresults'))
            data_dict = dict(parse_qsl(self.currenturi.split('?')[-1]))
            if 'q' in data_dict.keys():
                query_term = data_dict['q']
                log.info(query_term)
            else:
                log.info('query term not found')    
                return False
            self.parenturi = self.currenturi
            self.currenturi = 'http://www.lijit.com/search?uri=http://www.lijit.com/users/rww&q=%s&type=blog'%query_term
            self.__setSoupForCurrentUri()
            log.info(self.log_msg('search uri is : %s'%self.currenturi))
            start_index = 0
            total_result_count = int('\n'.join([each.split('"totalResults":')[-1].replace(',','')for each in re.findall('\"totalResults.*?,', self.soup.__str__())]))
            log.info(total_result_count)
            conn = HTTPConnection()
            headers = {'Host':'www.lijit.com'}
            while True:
                self.__fetchArticle()
                self.currenturi = self.parenturi
                try:
                    start_index +=10
                    if start_index >self.__max_results_count or start_index>total_result_count:
                        break
                    self.currenturi = 'http://www.lijit.com/api/json/search_uri/blog?start='+ str(start_index) +'&q='+ quote(query_term) +'&uri=http%3A%2F%2Fwww.lijit.com%2Fusers%2Frww&blog_uri=http%3A%2F%2Fwww.readwriteweb.com%2F&blog_platform=RWW&p=l'
                    data = {'blog_uri':'http://www.readwriteweb.com/','p':'l','q':query_term,'start':start_index,'uri':'http://www.lijit.com/users/rww','blog_platform':'RWW'}
                    conn.createrequest(self.currenturi,data = data,headers =headers)
                    res = conn.fetch().read()
                    self.rawpage = res
                    self._setCurrentPage()
                except:
                    log.exception(self.log_msg('Next Page link not found for url \
                                                    %s'%self.currenturi))
                    break                
        except:
            log.exception(self.log_msg("Exception in fetch %s"%self.currenturi)) 
        return True
        
    @logit(log,'fetchArticle')    
    def __fetchArticle(self):
        """It fetches the product information
        """
        try:
            links = set([each.split('link":')[-1].replace('\\','').\
                    replace('"','')for each in re.findall('\"link.*?http.*?"', self.soup.__str__())])
            if not links:
                log.info(self.log_msg("No links Found"))
                return False
            log.info(self.log_msg('total links found in %s :%d'%(self.currenturi,len(links))))
            for link in links:
                self.currenturi = link
                page = {'uri':self.currenturi}
                try:
                    self.__setSoupForCurrentUri()
                except:
                    log.exception(self.log_msg('Exception in setting soup for %s'%self.currenturi))
                    continue
                if not checkSessionInfo(self.__genre, self.session_info_out, self.currenturi,  \
                                    self.task.instance_data.get('update')):
                    try:
                        page['title'] = stripHtml(self.soup.find('h1','titlelink').renderContents())
                        log.info(self.log_msg('title = %s'%page['title']))
                    except:
                        log.exception(self.log_msg("Exception Occurred while fetching the title!! for %s"%self.currenturi))
                        page['title'] = ''
                
                    try:
                        author_tag = self.soup.find('div','author-date-comments')
                        if author_tag:
                            page['et_author_name'] = stripHtml(author_tag.find('a').renderContents())
                            page['et_author_profile'] = author_tag.find('a')['href']
                    except:
                        log.exception(self.log_msg("Exception Occurred while fetchin author for %s"%self.currenturi))
                    try:
                        data_tag = self.soup.find('div','asset-content')
                        if data_tag:
                            data_tag.find('div','related-entries').extract()
                            data_tag.find('div', id = 'like-and-retweet').extract()
                        page['data'] =  stripHtml(data_tag.renderContents()).replace('<!--//   -->','')
#                        page['data'] =  stripHtml(self.soup.find('div','asset-content').renderContents()).replace('<!--//   -->','')
                        log.info(self.log_msg('data = %s'%page['data']))
                    except:
                        log.exception(self.log_msg('Article data not found %s'%self.currenturi))        
                        page['data'] = ''
                            
                    if not page['title'] and not page['data']:
                        log.info(self.log_msg("title not found for %s,discarding this review"%self.currenturi))
                        continue #return False    
                    try:
                        date_str = stripHtml(self.soup.find('div','author-date-comments').\
                                    renderContents()).split('/')[1].strip()
                        
                        page['posted_date'] = datetime.strftime(datetime.strptime(date_str\
                                                        ,'%B %d, %Y %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
                        log.info(self.log_msg('posted: %s'%page['posted_date']))                                
                        
                    except:
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        log.exception(self.log_msg('Posted date cannot be fetched for url\
                                                                        %s'%self.currenturi))
#                    try:
 #                       total_comments = stripHtml(self.soup.find('div','author-date-comments').\
  #                                  renderContents()).split('/')[-1].strip()                                                    
   #                     page['ei_data_comments_count'] = int(re.sub('Comments','',total_comments))            
    #                except:
     #                   log.exception(self.log_msg('total comments count not found for %s'%self.currenturi))   
                        
                    try:
                        result = updateSessionInfo(self.__genre, self.session_info_out, self.currenturi, \
                                  get_hash(page) ,'Post', self.task.instance_data.get('update'))
                        if result['updated']:
                            page['path'] = [self.task.instance_data['uri']]
                            page['parent_path'] = []
                            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])            
                            page['entity'] = 'post'
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
            self.comment_uri = 'http://readwritewebmain.disqus.com/thread.js?url=' + self.currenturi + '&api_key=p2un7eqgWE3PqfEe0VkRKsMseUYqbc5DvRQGRHsGfxcPZUxITNxMTXVVfOsEpTnm'
            log.info(self.log_msg('comment uri : %s'%self.comment_uri))
            s = urlopen(self.comment_uri).read() 
            try:
                comment_data = '\n'.join(re.findall('/* */ jsonData = {"reactions":.*?\[.*?; /* */',s.__str__())).replace('/ jsonData = ','').replace('; /','')
                comment_tag = simplejson.loads(comment_data)
                log.info(self.log_msg('total comments found: %s'% len(comment_tag['posts'].keys())))
                if not comment_data or not comment_tag:
                    log.info('comments not found')
                    return False
                for each in  comment_tag['posts'].keys():
                    data_tag = comment_tag['posts'][each]
                    page = {} 
                    unique_key = each
                    try:
                        page['et_author_name'] = stripHtml(data_tag['user_key'])
                    except:
                        log.exception(self.log_msg('comment author not found %s'%self.currenturi))
                            
                    try:
                        page['data'] = stripHtml(data_tag['message'])
                        page['title'] = ''
                        #log.info(self.log_msg('comment data:%s'%page['data']))
                    except:
                        log.exception(self.log_msg('comment data not found %s'%self.currenturi))
                        continue 
                            
                    try:
                        date_str = stripHtml(data_tag['real_date']).replace('_',' ').strip()
                        page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%Y-%m-%d %H:%M:%S'),"%Y-%m-%dT%H:%M:%SZ")
                        #log.info(self.log_msg('comment date:%s'%page['posted_date']))
                    except:
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        log.exception(self.log_msg("comment posted_date not found %s"%self.currenturi))
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
                        page['entity'] = 'comment'
                        page.update(self.__task_elements_dict)
                        self.pages.append(page)
                        log.info(self.log_msg('comment Page added'))
                    except:
                        log.exception(self.log_msg("parent post couldn't be parsed for url \
                                                                    %s"%self.currenturi))
                return True 
            except:
                log.exception(self.log_msg("Exceptionin in getting comment string  %s"%self.currenturi))                                                     
        except:
            log.exception(self.log_msg("no comments found for this article %s"%self.currenturi))            
        
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
    
