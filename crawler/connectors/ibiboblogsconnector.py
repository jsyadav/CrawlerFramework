
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

##MOHIT RANKA
#JV
import urllib
from urllib2 import *
from urlparse import urlparse
import re
import cgi
import logging
from logging import config
from datetime import datetime
import md5
from BeautifulSoup import BeautifulSoup
import copy
from baseconnector import BaseConnector

from tgimport import *
from utils.sessioninfomanager import *
from utils.utils import stripHtml
from utils.utils import removeJunkData
from utils.urlnorm import normalize
from utils.decorators import *

log = logging.getLogger('IbiboBlogsConnector')

class IbiboBlogsConnector(BaseConnector):
   
    @logit(log,'_createSiteUrl')
    def _createSiteUrl(self):
        curr_url = "http://crawlx.bixee.com/isearch/result/provider/ibiboblogs2/1/?q="+urllib.quote(self.keyword_term)
        return curr_url

    @logit(log,'fetch')
    def fetch(self):
        """
        Fetched all the blogs for a given self.currenturi and returns Fetched staus depending 
        on the success and faliure of the task
        """
        try:
            self.genre = 'Review'
            try:
                self.keyword_term =  cgi.parse_qsl(self.task.instance_data['uri'])[0][1]
                if not self.keyword_term:
                    return False
            except:
                log.exception(self.log_msg("Unexpected format of the url %s for blogs.ibibo.com"%self.task.instance_data['uri']))
                return False
            
            self._getParentPage()
            self.currenturi = self._createSiteUrl()
            res=self._getHTML(self.currenturi)
            self.rawpage=res['result']
            self._setCurrentPage()
            self.url_list = []
            self._getUrlList()
            log.debug(self.log_msg("Number of blog urls fetched %d"%(len(self.url_list))))
            self.new_blog_count = 0
            self.all_blog_count = 0
            for each_url in self.url_list:
                try:
                    self.all_blog_count = self.all_blog_count + 1
                    log.debug(self.log_msg("processing blog number %d" %(self.all_blog_count)))
                    self._fetchBlog(each_url,self.task.instance_data['uri'])
                except:
                    log.exception(self.log_msg("Exception occured while fetching blog from url"))
                    continue # To the next url
            if len(self.pages) > 0:
                log.debug(self.log_msg("%d new blogs fetched" %self.new_blog_count))
            else: 
                log.debug(self.log_msg("No new blogs fetched"))
            return True
        except Exception,e:
            log.exception(self.log_msg("Exception occured in fetch()"))
            return False

    def _getParentPage(self):
        """
        Gets the data for the parent page and appends it to self.pages if the parent page has changed since the last crawl. 
        An empty dictionay is added to self.pages if the parent page is not changed since the last crawl
        """
        '''
        As no data is being fetched from parent page HTML, we are not going to parent page
        and saving 1 fetch. How smart of us! :D
        '''
        try:
            page={}
           #  if not self.rawpage:
#                 res=self._getHTML(self.currenturi) 
#                 self.rawpage=res['result']
#             self._setCurrentPage()
            page['title'] = self.keyword_term
            page['data']=''
            try:
                post_hash= md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                                                page.values()))).encode('utf-8','ignore')).hexdigest()
            except:
                log.exception(self.log_msg("Exception occured while creating parent page hash for url %s" %self.currenturi))
                return False

            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    self.currenturi, self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug('got the connector instance first time, sending updatesessioninfo the id : %s' % str(id))
                result=updateSessionInfo(self.genre, self.session_info_out, self.currenturi, post_hash, 
                                             'Post', self.task.instance_data.get('update'), Id=id)

                if result['updated']:
                    page['path']=[self.currenturi]
                    page['parent_path']=[]
                    page['uri'] = normalize(self.currenturi)
                    page['uri_domain'] = unicode(urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project       
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['task_log_id']=self.task.id
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')
                    self.pages.append(page)
                    log.debug(self.log_msg("Parent page %s info added to self.pages" %(self.currenturi)))
                else:
                    log.debug(self.log_msg("Parent page %s info NOT added to self.pages" %(self.currenturi)))
            else:
                log.debug(self.log_msg("Parent page %s info NOT added to self.pages" %(self.currenturi)))
            return True
        except:
            log.exception(self.log_msg("Exception occured in _getParentPage()"))
            return False
        
    @logit(log,'_getUrlList')
    def _getUrlList(self):
        """
        Iterates through all the blog pages by next button and populates self.url_list
        """
        # Assumption - self.soup exists (and set to the First page of the blog)
        try:
             #This is supposed to be a constant and not a variable, hence its in capitalcase
            BLOG_COUNT = tg.config.get(path='Connector', key='ibiboblogs_numresults')
            blog_count_iterator = 0
            while blog_count_iterator<=BLOG_COUNT:
                log.debug(self.log_msg(self.currenturi))
                log.debug(self.log_msg("Before Extending "+str(len(self.url_list))))
                for each in self.soup.findAll('div',attrs={'class':'searchResult'}):   
                    try:
                        permalink_url = each.find('div',attrs={'class':'resultHead'}).find('a')['href']
                        blog_count_iterator = blog_count_iterator + 1
                        if permalink_url in self.url_list: # Duplicate post
                            log.debug(self.log_msg("Duplicate url found, continuing to get other blog url"))
                            continue
                        else:
                            if blog_count_iterator<=BLOG_COUNT:
                                self.url_list.append(permalink_url)
                            else: 
                                log.debug(self.log_msg("All Urls are captured, Exiting the While loop"))
                                return True
                    except:
                        log.exception(self.log_msg("Exception while fetching permalink/titleurl, not appending the blog"))
                        continue

                log.debug(self.log_msg("After Extending "+str(len(self.url_list))))
                try:
                    try:
                        next_link =  self.soup.find('div',attrs={'class':'paginator'}).find('img',attrs={'src':'/img/ibibo/right-arrow.gif'}).parent.get('href')

                        log.debug(self.log_msg("Next Link is: "+next_link))
                    except:
                        log.info(self.log_msg("Next link not found"))
                        break
                    if next_link:
                        self.currenturi = next_link
                        res=self._getHTML(self.currenturi)
                        self.rawpage=res['result']
                        self._setCurrentPage()
                    else:
                        log.debug(self.log_msg("All Urls are captured, Exiting the While loop"))
                        break
                except:
                    log.exception(self.log_msg("Exception occured while fetching next link from the page"))
                    break
            return True
        except:
            log.exception(self.log_msg("Exception occured in _getUrlList()"))
            return False

    @logit(log,'_fetchBlog')
    def _fetchBlog(self,blog_url,parent_uri):
        """
        Fetches the data from a blog page and appends them self.pages
        """
        try:
            self.currenturi = blog_url
            res=self._getHTML(self.currenturi)
            self.rawpage=res['result']
            self._setCurrentPage()
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    blog_url, self.task.instance_data.get('update'),
                                    parent_list=[parent_uri]):
                page={}
                try:
                    page['et_author_name'] =  self.soup.find('div',attrs={'class':'post'}).find('small').renderContents().split('|')[1].strip().replace('by ','')
                except:
                    log.info(self.log_msg("Exception occured while fetching author name from the blog"))
                try:
                    page['title'] = self.soup.find('div',attrs={'class':'post'}).find('a',attrs={'title':re.compile("Permanent Link.*")}).renderContents()
                except:
                    page['title'] = ''
                    log.info(self.log_msg("Exception occured while fetching blog title"))
                    
                try:
                    page['ef_rating_overall'] = str(float(len(self.soup.find('div',attrs={'class':'post'}).findAll('img',attrs={'src':'http://blogs.ibibo.com/wp-includes/images/star2.gif'}))))
                except:
                    log.info(self.log_msg("Exception occured while fetching overall rating"))

                try:
                    page['data']= removeJunkData(self.soup.find('div',attrs={'class':'post'}),False)
                except:
                    page['data']=''
                    log.exception(self.log_msg("Exception occured while fetching blog data"))
                try:
                    blog_hash =  md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                                                page.values()))).encode('utf-8','ignore')).hexdigest()
                except:
                    log.exception(self.log_msg("Exception occured while creating blog hash, not appending this  blog"))
                    return False
                result=updateSessionInfo(self.genre, self.session_info_out, self.currenturi, blog_hash, 
                                         'Blog', self.task.instance_data.get('update'), parent_list=[parent_uri])
                if result['updated']:
                    try:
                        date_match = re.match("(\w+)\s*?(\d{1,2})(st|nd|rd|th)\s*?\,\s*?(\d{4})",stripHtml(self.soup.find('div',attrs={'class':'post'}).find('small').renderContents().split('|')[0].strip()))
                        post_date=date_match.group(1)+":"+date_match.group(2)+":"+date_match.group(4)
                        page['posted_date'] = datetime.strftime(datetime.strptime(post_date,"%B:%d:%Y"),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        log.info(self.log_msg("Exception occured while fetching post date from blog"))

                    parent_list=[parent_uri]
                    page['parent_path']=copy.copy(parent_list)
                    parent_list.append(self.currenturi)
                    page['path'] = parent_list
                    page['versioned']=self.task.instance_data.get('versioned',False)
                    page['category']=self.task.instance_data.get('category','generic')
                    page['client_name']=self.task.client_name
                    page['last_updated_time']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ") 
                    page['task_log_id']=self.task.id
                    page['entity']='blog'
                    page['uri'] = normalize(self.currenturi)
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    self.new_blog_count = self.new_blog_count + 1
                    self.pages.append(page)
                    log.debug(self.log_msg("Blog %s added to self.pages" %(blog_url)))
                else:
                    log.debug(self.log_msg("Blog %s NOT added to self.pages" %(blog_url)))
            else:
                log.debug(self.log_msg("Blog %s NOT added to self.pages" %(blog_url)))
                
            if self.task.instance_data.get('pick_comments'):
                try:
                    self.comment_list=[]
                    self.new_comment_count = 0
                    self.all_comment_count = 0
                    self.comment_list = self.soup.find('ol',attrs={'class':'commentlist'}).findAll('li',attrs={'class':'odd'})
                    for each_comment in self.comment_list:
                        self.current_comment = each_comment
                        self._getBlogComment([self.task.instance_data['uri'],blog_url])
                except:
                    log.debug(self.log_msg("No comments found for this blog"))
            return True
        except Exception,e:
            log.exception(self.log_msg("Exception occured in _fetchBlog() for url %s" %(blog_url)))
            return False
        
    @logit(log,'_getBlogComments')
    def _getBlogComment(self,parent_list):
        """
        Gets new/updated comments from a particular blog and appends to self.pages
        """
        try:
            comment_iden = self.current_comment.get('id')
            if not checkSessionInfo(self.genre, self.session_info_out,
                                    comment_iden, self.task.instance_data.get('update'),
                                    parent_list=parent_list):
                page={}
                try:
                    page['et_author_name']=self.current_comment.find('div',attrs={'class':'commentTxt'}).strong.renderContents()
                except:
                    log.info(self.log_msg("Could not fetch comment author name"))
                try:
                    page['data']= ' '.join(stripHtml(each_para.renderContents().strip()) for each_para in self.current_comment.find('div',attrs={'class':'commentTxt'}).findAll('p')[1:]) 
                    page['title']=str(page['data'])[:50]
                except:
                    page['data']=''
                    page['title']=''
                    log.info(self.log_msg("Blog data not found"))
                comment_hash =  md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                                                page.values()))).encode('utf-8','ignore')).hexdigest()
                result=updateSessionInfo(self.genre, self.session_info_out, comment_iden, comment_hash,
                                         'Comment', self.task.instance_data.get('update'),
                                         parent_list=parent_list)
                if result['updated']:
                    try:
                        page['posted_date']= datetime.strftime(datetime.strptime(self.current_comment.find('a',attrs={'href':re.compile('^#comment-\d+$')}).renderContents(),"%b %d, %Y"),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        log.info(self.log_msg("Exception occured while fetching post date from blog"))

                    page['parent_path']=copy.copy(parent_list)
                    parent_list.append(comment_iden)
                    page['path']=parent_list
                    page['versioned']=self.task.instance_data.get('versioned',False)
                    page['category']=self.task.instance_data.get('category','generic')
                    page['client_name']=self.task.client_name
                    page['last_updated_time']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ") 
                    page['task_log_id']=self.task.id
                    page['entity']='comment'
                    page['uri'] = normalize(self.currenturi)
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project    
                    self.new_comment_count = self.new_comment_count + 1
                    self.pages.append(page)
                    log.debug(self.log_msg("Appending comment %s" %(comment_iden)))
                    return True
                else:
                    log.debug(self.log_msg("NOT appending comment %s has been fetched" %(comment_iden)))
                    return False
            else:
                log.debug(self.log_msg("NOT appending comment %s has been fetched" %(comment_iden)))
                return False
        except:
            log.exception(self.log_msg("Exception occured while fetching comment %s" %(comment_iden)))
            return False
