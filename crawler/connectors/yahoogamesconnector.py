'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved
This software is confidential and proprietary information of Serendio Software.
It is disclosed pursuant to a non-disclosure agreement between the recipient
and Serendio. This source code is provided for informational purposes only, and
Serendio makes no warranties, either express or implied, in this. Information 
in this program, including URL and other Internet website references, is 
subject to change without notice. The entire risk of the use or the results of 
the use of this program  with the user. Complying with all applicable 
copyright laws is the responsibility of the user. Without limiting the rights 
under copyright, no part of this program may be reproduced, stored in, or 
introduced into a retrieval system, or distributed or transmitted in any form 
or by any means (electronic, mechanical, photocopying, recording, on a website,
or otherwise) or for any purpose, without the express written permission of 
Serendio Software.
Serendio may have patents, patent applications, trademarks, copyrights, or 
other intellectual property rights covering subject matter in this program. 
Except as expressly provided in any written license agreement from Serendio, 
the furnishing of this program does not give you any license to these patents, 
trademarks, copyrights, or other intellectual property.
'''
#prerna

import re, copy, logging
from urllib2 import urlparse, urlopen
from urllib import urlencode
from datetime import datetime, timedelta


from tgimport import tg
from utils.httpconnection import HTTPConnection
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from BeautifulSoup import BeautifulSoup
log = logging.getLogger('YahooGamesConnector')
class YahooGamesConnector(BaseConnector):
    '''
    This will fetch the info for 
    Sample uris is
    http://get.games.yahoo.com/reviews/games/bejeweled-2
    '''
        
    @logit(log , 'fetch')
    def fetch(self):
        """Fetch of games.yahoo.com
        """
        try:
            self.__genre = 'review'
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
            while True:
                try:
                    if not self.__iteratePosts():
                        log.info(self.log_msg('No more links found'))
                        break
                    self.currenturi = self.soup.find('li','next').find('a')['href']
                    self.__setSoupForCurrentUri() 
                except:
                    log.exception(self.log_msg('next page not found')) 
                    break   
        except:
            log.exception(self.log_msg('Exception in fetch for the url %s'%self.currenturi))
        return True
        
    @logit(log , '__iteratePosts')
    def __iteratePosts(self):
        """It will Iterate Over the Posts found in the Current URI
        """
        try:
            posts = self.soup.find('ul','reviews').findAll('li',recursive = False)
            if not posts:
                log.info(self.log_msg('No posts found in url %s'%self.currenturi))
                return False
            for post in posts:
                if not self.__addPost(post):
                    log.debug(self.log_msg('Post not added to self.pages for url\
                                                            %s'%self.currenturi))
                    break
            return True
        except:
            log.exception(self.log_msg('Reviews are not found for url %s'%self.currenturi))
            return True
    
    @logit(log, '__addPost')
    def __addPost(self, post):
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            unique_key = post.findNext('a')['name']
            if checkSessionInfo(self.__genre, self.session_info_out, \
                        unique_key, self.task.instance_data.get('update'),\
                        parent_list = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True for uri %s'% \
                                                                unique_key))
                return False
            page = self.__getData(post)
            if not page:
                log.info(self.log_msg('page contains empty data, getdata \
                                    returns  False for uri %s'%self.currenturi))
                return True
            result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                get_hash( page ),'post', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if result['updated']:
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [self.task.instance_data['uri'], unique_key ]
                page['uri'] = self.currenturi + '#' + unique_key
                page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
                page ['entity'] = 'post'
                page.update(self.__task_elements_dict)
                self.pages.append(page)
                log.info(page)
            else:
                log.info(self.log_msg('Update session info returns False for \
                                                    url %s'%self.currenturi))
        except:
            log.exception(self.log_msg('Cannot add the post for the uri %s'%self.currenturi))
        return True
    
    @logit(log, '__getData')
    def __getData(self, post):
        """ This will return the page dictionry
        """
        page = {}
        try:
            date_str = date_str = stripHtml(post.find('div','byline').find('span','date').\
                        renderContents()).strip()
            page['posted_date']= datetime.strptime(date_str,"%B %d, %Y").strftime("%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('posted_date not be found in %s'% self.currenturi))
            #log.info(date_str)
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['title'] = stripHtml(post.find('div','bottomline').find('span').\
                            renderContents())
        except:
            log.exception(self.log_msg('post title  not found in %s'% self.currenturi))
            page['title'] = ' '
        try:
            data_tag = post.find('ul','review')
            unwanted_tag = data_tag.find('span','label')
            if unwanted_tag:
                [each.extract()for each in unwanted_tag]
            page['data'] = stripHtml(data_tag.renderContents())
        except:
            log.exception(self.log_msg('Data not found for the url %s'%self.currenturi))
            page['data'] = ''  
        
        if not page['title'] and not page['data']: 
            log.info(self.log_msg("Data and title not found for %s,"\
                                    " discarding this review"%self.currenturi))
            return False           
        
        try:
            page['et_author_name'] = stripHtml(post.find('div','byline').find('span').\
                                    renderContents()).replace('-','')
        except:
            log.exception(self.log_msg('Author name not found'))
        try:
            page['et_rating_overall'] = int(post.find('div',id = 'rating_overall-preview').\
                                        span['style'].split(':')[-1].split('%')[0]) / 20
        except:
            log.debug(self.log_msg('overall  rating not available'))   
             
        try:
            page['et_rating_graphics'] = int(post.find('div',id = 'rating_graphics-preview').\
                                        span['style'].split(':')[-1].split('%')[0]) / 20
        except:
            log.debug(self.log_msg('graphics  rating not available'))
        try:
            page['et_rating_lasting_appeal'] = int(post.find('div',id = 'rating_appeal-preview').\
                                        span['style'].split(':')[-1].split('%')[0]) / 20
        except:
            log.debug(self.log_msg('appeal  rating not available')) 
        try:
            page['et_rating_sound'] = int(post.find('div',id = 'rating_sound-preview').\
                                        span['style'].split(':')[-1].split('%')[0]) / 20
        except:
            log.debug(self.log_msg('sound  rating not available')) 
        try:
            page['et_rating_easy_learning'] = int(post.find('div',id = 'rating_ease-preview').\
                                        span['style'].split(':')[-1].split('%')[0]) / 20
        except:
            log.debug(self.log_msg('easy  rating not available'))                                                
    
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