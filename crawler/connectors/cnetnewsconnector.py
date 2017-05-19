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
from urllib2 import urlopen
from BeautifulSoup import BeautifulSoup
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.httpconnection import HTTPConnection
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging. getLogger("CnetNewsConnector")

class CnetNewsConnector(BaseConnector):    
    """Connector for news.cnet.com 
    """
    @logit(log,'fetch')
    def fetch(self):
        """
        It fetches the data from the url mentioned
        http://news.cnet.com/1770-5_3-0.html?query=iphone4s&tag=srch&searchtype=news
        http://news.cnet.com/8301-13579_3-57329405-37/ipad-3-could-make-apple-the-worlds-top-pc-vendor-next-year/?tag=rtcol;dis
        http://news.cnet.com/8614-13579_3-57329405.html?assetTypeId=12"""
        self.__genre = "Review"
        self.base_uri = 'http://news.cnet.com'
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
            self.parenturi = self.currenturi
            self.currenturi = self.currenturi + '&rpp=10&source=news&activityname=cnetSearchUpdater&nomesh=true'
            #sample_ure:http://news.cnet.com/1770-5_3-0.html?query=iphone4s&tag=srch&searchtype=news&rpp=10&source=news&activityname=cnetSearchUpdater&nomesh=true
            s = urlopen(self.currenturi).read()
            ss1 = s.replace('<![CDATA[','').replace(']]>','')
            self.soup = BeautifulSoup(ss1)
            c = 0
            main_page_soup= copy.copy(self.soup)
            while True:
                self.__fetchNews()
                try:
                    self.currenturi = self.base_uri + main_page_soup.find('li','next').\
                                        find('a')['href']
                    s = urlopen(self.currenturi).read()
                    ss1 = s.replace('<![CDATA[','').replace(']]>','')
                    self.soup = BeautifulSoup(ss1)
                    main_page_soup = copy.copy(self.soup)
                    
##                    c +=1
##                    if c >=1:
##                        break
                except:
                    log.exception(self.log_msg('Next Page link not found for News url \
                                                    %s'%self.currenturi))
                    break                
            return True
        except:
            log.exception(self.log_msg("Exception in fetch %s"%self.currenturi)) 
        return True
        
    @logit(log,'fetchNews')    
    def __fetchNews(self):
        """It fetches the product information
        """
        try:
            newslinks =  [each.find('a')['href']for each in self.soup.findAll(attrs={'class':re.compile('resultInfo')})]
            
            if not newslinks:
                log.info(self.log_msg("No News Articles Found"))
                return False
            log.info(self.log_msg('total News article found in %s :%d'%(self.currenturi,len(newslinks))))
            for news_uri in newslinks:
                self.currenturi = self.base_uri + news_uri
                page = {'uri':self.currenturi}
                try:
                    self.__setSoupForCurrentUri()
                except:
                    log.exception(self.log_msg('Exception in setting soup for %s'%self.currenturi))
                    continue
                if not checkSessionInfo(self.__genre, self.session_info_out, self.currenturi,  \
                                    self.task.instance_data.get('update')):
                    try:
                        page['title'] = stripHtml(self.soup.find('header',section = 'title').\
                                        find('h1').renderContents())
                        log.info(self.log_msg('title = %s'%page['title']))
                    except:
                        log.exception(self.log_msg("Exception Occurred while fetching the title!! for %s"%self.currenturi))
                        page['title'] = ''
                    try:
                        page['et_author_name'] = stripHtml(self.soup.find('div','postByline').\
                                                        find('span','author').renderContents()).\
                                                        replace('by ','').strip()                 
                    except:
                        log.exception(self.log_msg("Exception Occurred while fetchin author for %s"%self.currenturi))
                    
                    try:
                        page['et_author_profile'] = self.soup.find('div','postByline').\
                                                        find('span','author').find('a')['href']
                    except:
                        log.exception(self.log_msg("Exception Occurred while fetchin author profile for %s"%self.currenturi))
                    try:
                        page['data'] = stripHtml('\n'.join([each.renderContents()for each in self.soup.find('div','postBody txtWrap').findAll('p')]))
                        log.info(self.log_msg('data = %s'%page['data']))
                    except:
                        log.exception(self.log_msg('Article data not found %s'%self.currenturi))        
                            
                    if not page['title'] and not page['data']:
                        log.info(self.log_msg("title not found for %s,discarding this review"%self.currenturi))
                        return False    
                    try:
                        date_str = stripHtml(self.soup.find('div','postByline').\
                                    find('time','datestamp').renderContents())
                        date_str = re.split(re.compile(' am| pm', re.IGNORECASE),date_str)[0].strip()
                        page['posted_date'] = datetime.strftime(datetime.strptime(date_str\
                                                        ,'%B %d, %Y %I:%M'),"%Y-%m-%dT%H:%M:%SZ")
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
                    s = urlopen(self.currenturi).read()
                    asset_id = re.search('assetId\: \'\d+\'',s).group().split(':')[-1].replace("'",'').strip()
                    node_id = re.search('nodeId\: \'\d+\'',s).group().split(':')[-1].replace("'",'').strip()
                    site_id = re.search('siteId\: \'\d+\'',s).group().split(':')[-1].replace("'",'').strip()
                    comment_uri = 'http://news.cnet.com/'+'8614-'+node_id+'_'+site_id+'-'+asset_id+'.html?assetTypeId=12'
                    if not comment_uri:
                        log.info(self.log_msg('no comments found for %s'%self.currenturi))
                        return False
                    self.currenturi = comment_uri
                    self.__setSoupForCurrentUri()
                    c =0
                    while True:
                    
                        self.__fetchComments()
                        try:
                            next_uri = self.soup.find('li','next').find('a')['href']
                            if re.match('^http://news.cnet.com',next_uri):
                                self.currenturi = next_uri
                            else:    
                                self.currenturi= self.base_uri+next_uri
                                self.__setSoupForCurrentUri()
##                            c +=1
##                            if c >=2:
##                                break
                        except:
                            log.exception(self.log_msg('Next Page link not found for comments url \
                                                        %s'%self.currenturi))
                            break
                except:
                   log.exception(self.log_msg('comments url not found for %s'%self.currenturi)) 
            return True    
        except:
            log.exception(self.log_msg("there is exception in fetching article %s"%self.currenturi))
        return True    
                                                                                                                        
    ###fetching comments         
    @logit(log,'fetchComments')     
    def __fetchComments(self):
        
        try:
            
##            reply_tag = [each.find('dd','replieswrapper')for each in soupc.findAll('dl',messageid =re.compile('\d+'))]
##            reply_tag = [each.find('dd','replieswrapper').findAll('dl',messageid = re.compile('\d+'))for \
##                        each in self.soup.findAll('dl',messageid =re.compile('\d+')) if each.find('dd','replieswrapper')]
##            reply = [[each1.extract() for each1 in each] for each in reply_tag if each]
##            comment = [each for each in soupc.findAll('dl',messageid =re.compile('\d+'))]
##            comment_tag = comment + reply
            comment_tag = self.soup.findAll('dl',messageid =re.compile('\d+'))
            if not comment_tag:
                log.info('comments not found')
                return False
            log.info(self.log_msg('no of comments:%s in %s'%(len(comment_tag),self.currenturi)))
            for each in comment_tag:
                page = {}    
                unique_key = each['messageid']
                try:
                    page['et_author_name'] = stripHtml(each.find('a','commenter').renderContents())
                    #log.info(self.log_msg('comment author:%s'%page['et_author_name']))
                except:
                    log.exception(self.log_msg('comment author not found %s'%self.currenturi))
                try:
                    page['et_author_profile'] = each.find('a','commenter')['href']    
                except:
                    log.exception(self.log_msg('comment author profile not found %s'%self.currenturi))    
                try:
                    page['data'] = stripHtml(each.find('dd','commentBody').renderContents())
                    page['title'] = ''
                    #log.info(self.log_msg('comment data:%s'%page['data']))
                except:
                    log.exception(self.log_msg('comment data not found %s'%self.currenturi))
                    continue 
                        
                try:
                    date_str = stripHtml(each.find('dd','tool clearfix').find('time').renderContents())
                    date_str = re.split(re.compile(' am| pm', re.IGNORECASE),date_str)[0].strip()
                    page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%B %d, %Y %I:%M'),"%Y-%m-%dT%H:%M:%SZ")
                    #log.info(self.log_msg('comment date:%s'%page['posted_date']))
                except:
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    log.exception(self.log_msg("comment posted_date not found %s"%self.currenturi))
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
        return True
        
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
    
