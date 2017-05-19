'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#MODIFIED BY PRERNA
#MOHIT RANKA
#Skumar

# from urllib2 import *
from cgi import parse_qsl
from urlparse import urlparse
from urllib import urlencode
from datetime import datetime,timedelta
import simplejson
import re
import feedparser
import logging
import copy
import time

from tgimport import *
from BeautifulSoup import BeautifulSoup
from baseconnector import BaseConnector
from urllib2 import urlopen
from utils.utils import stripHtml, get_hash
from utils.httpconnection import HTTPConnection
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import updateSessionInfo,checkSessionInfo

log = logging.getLogger('FaceBookConnector')

class FaceBookConnector(BaseConnector):  
    
    @logit(log,'fetch')
    def fetch(self):
        """
        Calls appropriate functions as per the type of facebook Url. If child function append something to self.pages, returns True, else returns False
        """
        '''
        The abstracted session_info logic makes it difficult to fetch wall information from pages page
        Hence commiting it for now, will get back to fetching pages' page information, while refactoring this
        connector.
        
        #elif re.match("http://www\.facebook\.com/pages/.*",self.currenturi):
        # return_value=self._fetchPages()
        '''
        try:
            self.fetch_next = True
            if self.currenturi.startswith('http://www.facebook.com/feeds'):
                return self.__processRSSFeeds()
            if re.match("http://www\.facebook\.com/board\.php\?uid=\d+$",self.currenturi):
                self.last_timestamp = datetime(1,1,1)
                self.genre='Search'
                self.entity = 'forum'
                self.__populateBoard()
                if self.linksOut:
                    updateSessionInfo(self.genre, self.session_info_out,self.last_timestamp , None,self.entity,self.task.instance_data.get('update'))
                return True
            elif re.match("http://www\.facebook\.com/topic\.php\?uid=\d+&topic=\d+$",self.currenturi):
                self.genre='Review'
                self.entity = 'topic'
                headers = {}
                headers['Accept-encoding'] = ''
                headers['Accept-Language'] = 'en-US,en;q=0.5'
                headers['Host'] = 'www.facebook.com'
                headers['Referer'] = self.task.instance_data['uri']
                conn = HTTPConnection()
                if self.__fetchTopic():
                    while self.fetch_next:
                        for post in self.soup.findAll('li',attrs={'id':re.compile('post\d+$')}):
                            self.__getPostInfo(post)
                        try:
                            next_page_uri = stripHtml(self.soup.find('a',text='Next').findPrevious()['href'])
                            uri_args=dict(parse_qsl(urlparse(next_page_uri)[4]))
                            data = {}
                            for each in ['perpage','post_id','post_index','start','topic','uid']:
                                data[each] = uri_args.get(each)
                            data['__a'] ='1'
                            data['fb_dtsg']=''
                            conn.createrequest('http://www.facebook.com/ajax/discussions/discussions_next_page.php',headers = headers,data=data)
                            res = conn.fetch().read()
                            self.rawpage = simplejson.loads(re.search('{.*}',res,re.DOTALL ).group())['payload']
                            self._setCurrentPage()
                            if not self.soup:
                                log.debug(self.log_msg("All posts fetched"))
                                self.fetch_next=False
                                break
                        except:
                            log.debug(self.log_msg("All posts fetched"))
                            
                            self.fetch_next=False
                            break
                return True
            else:
                #It is Review Page
                try:
                    self.genre='Review'
                    log.info(self.log_msg('Getting Review'))
                    headers = {}
                    headers['Accept-encoding'] = ''
                    headers['Accept-Language'] = 'en-US,en;q=0.8'
                    url_params = dict(parse_qsl(self.currenturi.split('?')[-1]))
                    start_page = self.task.instance_data.get('start_page','0')
                    url_params['x'] = start_page
                    if 'hash' in url_params.keys():
                        url_params.pop('hash')
                    self.currenturi = 'http://www.facebook.com/reviews/see_all.php?' + urlencode(url_params)
                    self.parent_uri = self.currenturi
                    res=self._getHTML(self.currenturi,headers=headers)
                    self.rawpage=res['result']
                    self._setCurrentPage()
                    self.__getParentPageForReviews()
                    #self.currenturi = 'http://www.facebook.com' + self.soup.find('div'\
                    #            ,'summary').find('a',text='See All').parent['href']
                    #res=self._getHTML(self.currenturi,headers=headers)
                    #self.rawpage=res['result']
                    #self._setCurrentPage()
                    while True:
                        next_page_soup_set = False
                        if not self.__getReviews():
                            log.info(self.log_msg('Get reviews returns False'))
                            break
                        try:
                            self.currenturi = 'http://www.facebook.com' +  self.\
                                        soup.find('a',text='Next').parent['href']
                            url_params = dict(parse_qsl(self.currenturi.split('?')[-1]))
                            if 'hash' in url_params.keys():
                                url_params.pop('hash')
                            post_count = url_params.get('x')
                            if post_count and int(post_count)%100==0:
                                time.sleep(30)
                            #self.currenturi = 'http://www.facebook.com/reviews/see_all.php?' + urlencode(url_params)
                            self.currenturi = self.task.instance_data['uri'] + '&x=%s'%post_count
                            no_of_fetch = 0
                            while no_of_fetch <= 3:
                                try:
                                    res=self._getHTML(self.currenturi,headers=headers)
                                    self.rawpage=res['result']
                                    self._setCurrentPage()
                                    next_page_soup_set = True
                                    break
                                except:
                                    time.sleep(30)
                                no_of_fetch += 1
                            if not next_page_soup_set:
                                log.info(self.log_msg('Next Page Soup Not Set'))
                                break
                        except:
                            log.exception(self.log_msg('next page not found'))
                            break
                        #if self.times_fetched >5:
                        #    break
                        log.info(self.log_msg('The Times Fetched %d'%self.times_fetched))
                    return True
                except:
                    log.exception(self.log_msg('error in fetch method'))
                    return False
        except:
            log.exception(self.log_msg("Exception occured in fetch()"))
            return False

    @logit(log,'__populateBoard')
    def __populateBoard(self):
        """
        Appends topic links to taskmaster
        """
        while self.fetch_next:
            headers = {}
            headers['Accept-encoding'] = ''
            headers['Accept-Language'] = 'en-US,en;q=0.8'
            res=self._getHTML(self.currenturi,headers)
            self.rawpage=res['result']
            self._setCurrentPage()
            for topic in self.soup.find('div',attrs={'class':'board_topics'}).\
                    findAll('div',attrs={'class':re.compile('board_topic*')}):
                try:
                    date_str = stripHtml(topic.find('span',attrs={'class':'timestamp'}).renderContents())
                    if re.search('(\d+) (hours?|minutes?|seconds?) ago',date_str):
                        topic_date=datetime.utcnow()
                    elif re.search(', \d{,4}',date_str):  
                        topic_date = datetime.strptime(date_str,"Created on %B %d, %Y at %H:%M%p")
                    else:
                        date_year = ' ' + str(datetime.utcnow().year)
                        date_str = re.sub('\sat', date_year,date_str)
                        topic_date =  datetime.strptime(date_str,"Created on %B %d %Y %H:%M%p")                                  
                except:
                    topic_date=datetime.utcnow()
                    log.exception(self.log_msg("Error occured while fetching review date"))
                if  not checkSessionInfo(self.genre,
                                         self.session_info_out, topic_date, 
                                         self.task.instance_data.get('update')):
                    try:
                        url = stripHtml(topic.find('div',attrs={'class':'topic_info'}).find('a')['href'])
                    except:
                        continue
                    self.last_timestamp = max(topic_date,self.last_timestamp)
                    temp_task=self.task.clone()
                    temp_task.instance_data['uri']= url
                    self.linksOut.append(temp_task)
                else:
                    self.fetch_next = False
                    break
            if self.fetch_next:
                try:
                    self.currenturi = stripHtml(self.soup.find('a',text='Next',attrs={'href':True}).findPrevious()['href'])
                    if not self.currenturi:
                        self.fetch_next=False
                        break
                except:
                    self.fetch_next = False
                    break
        log.debug(self.log_msg('.no of Tasks added %d'%(len(self.linksOut))))
        return True

    @logit(log,'__fetchTopic')
    def __fetchTopic(self):
        """
        """
        try:
            headers = {}
            headers['Accept-encoding'] = ''
            headers['Accept-Language'] = 'en-US,en;q=0.8'
            res=self._getHTML(self.currenturi,headers=headers)
            self.rawpage=res['result']
            self._setCurrentPage()
            try:
                post_hash= self.currenturi
            except:
                log.debug(self.log_msg("Error occured while creating the parent page hash"))
                return False
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    self.task.instance_data['uri'], self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug('got the connector instance first time, sending updatesessioninfo the id : %s' % str(id))
                    result=updateSessionInfo(self.genre, self.session_info_out, self.task.instance_data['uri'], post_hash, 
                                             'Post', self.task.instance_data.get('update'), Id=id)
            return True
        except:
            log.exception(self.log_msg("Error occured while processing %s"%(self.currenturi)))
            return False

    @logit(log,'__getPostInfo')
    def __getPostInfo(self,post):
        """
        """
        try:
            post_id = post['id']
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    post_id, self.task.instance_data.get('update'),
                                    parent_list=[self.task.instance_data['uri']]):
                page={}
                try:
                    page['data'] = stripHtml(post.find('h6',attrs={'class':'uiStreamMessage'}).renderContents())
                except:
                    log.info(self.log_msg("data not found for %s"%post_id))
                    #page['data']=''
                    return False
                    
                page['title']=page['data'][:50]
                try:
                    page['et_author_name'] = stripHtml(post.find('div','actorName actorDescription').renderContents())
                except:
                    log.exception(self.log_msg('author name not found %s'%self.currenturi))    
                try:
                    user_id_tag = post.find('a',attrs = {'class': re.compile('\W*UIImageBlock_Image UIImageBlock_MED_Image')})
                    if user_id_tag:
                        user_id = user_id_tag.find('img')['src'].split('/')[-1].split('_')[1]
                        user_profile = 'http://graph.facebook.com/' + user_id
                        page['et_author_gender'] = simplejson.loads(urlopen(user_profile).read().__str__())['gender']
                except:
                    log.exception(self.log_msg('gender not found')) 
                    
                #try:
                #    page['et_author_name']=stripHtml(post.find('span',attrs={'class':'author_header'}).find('strong').renderContents())
                #except:
                #    log.info(self.log_msg("Author name not found for %s"%post_id))
                
                try:
                    date_str = post.find('span', 'uiStreamSource').\
                                find('abbr','timestamp')['title'].strip()
                    page['posted_date']=datetime.strptime(date_str,'%A, %B %d, %Y at %I:%M%p').strftime("%Y-%m-%dT%H:%M:%SZ")
                except:
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    log.info(self.log_msg("posted date not found for %s"%post_id))

                try:
                    review_hash =  get_hash(page)
                except:
                    log.debug(self.log_msg("Error occured while creating the review hash %s" %post_id))
                    return False
                result=updateSessionInfo(self.genre, self.session_info_out, post_id, review_hash, 
                                         'review', self.task.instance_data.get('update'), parent_list=[self.task.instance_data['uri']])
                if result['updated']:
                    parent_list = [self.task.instance_data['uri']]
                    page['parent_path'] = copy.copy(parent_list)
                    parent_list.append(post_id)
                    page['path'] = parent_list
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id 
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['entity'] = 'post'
                    page['category'] = self.task.instance_data.get('category','')
                    page['task_log_id']=self.task.id
                    page['uri']=normalize(self.currenturi)
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    self.pages.append(page)
                    log.debug(self.log_msg("Post %s added to self.pages" %(post_id)))
                else:
                    log.debug(self.log_msg("Post %s NOT added to self.pages" %(post_id)))
            else:
                log.debug(self.log_msg("Post %s NOT added to self.pages" %(post_id)))
            return True
        except:
            log.exception(self.log_msg("Error occured while fetching post %s"%(post_id)))
            return False
        
    @logit(log,'__getParentPageForReviews')
    def __getParentPageForReviews(self):
        '''This will fetch the parent page info for Reviews
        '''
        page = {}
##        try:
##                pass
##        except:
##            pass
        try:
            self.parent_uri = 'http://www.facebook.com/reviews/see_all.php?of=' + dict(parse_qsl(self.currenturi.split('?')[-1]))['of']
            page['title'] = re.sub('^Reviews of ','',stripHtml(self.soup.find('h2','UIMediaHeader_Title').renderContents()))
            if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info return True, Already exists'))
                return False
            post_hash = get_hash( page )
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result = updateSessionInfo( self.genre, self.session_info_out, self.\
                   parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                return False
            page['path']=[self.parent_uri]
            page['parent_path']=[]
            page['uri'] = normalize( self.parent_uri )
            page['uri_domain'] = unicode(urlparse(page['uri'])[1])
            page['priority']=self.task.priority
            page['level']=self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            page['client_name'] = self.task.client_name
            page['last_updated_time'] = page['pickup_date']
            page['versioned'] = False
            page['data'] = ''
            page['task_log_id']=self.task.id
            page['entity'] = 'Post'
            page['category']=self.task.instance_data.get('category','')
            self.pages.append(page)
            log.info(self.log_msg('Parent page added'))
            return True
        except:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False

    @logit(log,'__getReviews')
    def __getReviews(self):
        '''This will fetch the reviews from facebook
        '''
        try:
            reviews = self.soup.find('div','reviews_tab').findAll('div',id=re.compile('review_\d+'))
            if not reviews:
                log.info(self.log_msg('No reviews found'))
                return False
        except:
            log.exception(self.log_msg('No reviews found'))
            return False
        for review in reviews:
            page={}
            try:
                unique_key = review['id']
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                        self.task.instance_data.get('update'),parent_list\
                            =[self.parent_uri]):
                        log.info(self.log_msg('Reached Last Crawled Date'))
                        #log.info(self.log_msg('session info return True'))
                        return False
            except:
                log.info(self.log_msg('cannot find the unique key'))
                return False
            try:
                #author_tag = review.find('div','review_header_subtitle_line')
                #author_name_tag = author_tag.find('span')
                page['et_author_name'] = stripHtml(review.find('span','Reviews_Name').renderContents())
            except:
                log.info(self.log_msg("Author name not found"))
            try:
                date_str = stripHtml(review.find('span','UIActionLinks UIActionLinks_bottom review_actions').\
                            renderContents()).strip()
                #cur_date = datetime.utcnow()
                #date_str = re.sub("(\d+)(st|nd|rd|th)",r"\1",stripHtml(review.find('span',attrs={'class':re.compile('review_actions')}).renderContents())).lower()
                #if 'yesterday' in date_str:
                #    page['posted_date'] = datetime.strftime(cur_date-timedelta(days=1),"%Y-%m-%dT%H:%M:%SZ")
                #page['posted_date'] = datetime.strftime(datetime.strptime(date_str,"%H:%M%p %B %d, %Y"),"%Y-%m-%dT%H:%M:%SZ")
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str,"%B %d, %Y at %I:%M%p"),"%Y-%m-%dT%H:%M:%SZ")
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                #log.info(self.log_msg("It Todays post"))
            #for gender    
            try:
                user_id_tag = review.find('a',attrs = {'class': re.compile('\W*UIImageBlock_Image UIImageBlock_MED_Image')})
                if user_id_tag:
                    user_id = user_id_tag.find('img')['src'].split('/')[-1].split('_')[1]
                    user_profile = 'http://graph.facebook.com/' + user_id
                    page['et_author_gender'] = simplejson.loads(urlopen(user_profile).read().__str__())['gender']
            except:
                log.exception(self.log_msg('gender not found'))                    
                                
            try:
                page['data'] = stripHtml(review.find('div','review_body').renderContents())
                if page['data']=='':
                    log.info(self.log_msg('Empty data found'))
                    continue
                page['uri'] = self.currenturi
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
            except:
                log.info(self.log_msg("Data not found"))
                continue
            try:
                page['ef_rating_overall'] = float(len(review.findAll('img','UIStarRating_Star spritemap_icons sx_icons_star_on')))
            except:
                log.info(self.log_msg("Data not found"))
            try:
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            get_hash( page ),'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    continue
                parent_list = [self.parent_uri]
                page['parent_path']=parent_list[:]
                parent_list.append(unique_key)
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
                page['uri_domain'] = urlparse(page['uri'])[1]
                self.pages.append( page )
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error in getReview'))
        return True


    def __processRSSFeeds(self):
        '''This will process the RSS Feeds of Facebook
        '''
        log.debug(self.log_msg("Entry Webpage: "+str(self.currenturi)))
        parser = feedparser.parse(self.currenturi)
        if len(parser.version) == 0 or not parser:
            log.info(self.log_msg('parser version not found , returning'))
            return False
        log.info('number of entries %s'%(len(parser.entries)))
        for entity in parser.entries:
            try:
                if checkSessionInfo('Review',self.session_info_out, entity['link'],
                                        self.task.instance_data.get('update')):
                    log.info(self.log_msg('Session info returns  True for uri %s'%entity['link']))
                    continue
                result = updateSessionInfo('Review', self.session_info_out, entity['link'], '',
                                          'Post', self.task.instance_data.get('update'))
                if not result['updated']:
                    log.info(self.log_msg('Result not updated for uri %s'%entity['link']))
                    continue
                temp_task = self.task.clone()
                temp_task.instance_data['uri'] = normalize(entity['link'])
                temp_task.pagedata['title'] = entity['title']
                temp_task.pagedata['source'] = 'facebook.com'
                temp_task.instance_data['connector_name'] = 'HTMLConnector'
                temp_task.pagedata['source_type'] = 'rss'
                self.linksOut.append(temp_task)
            except:
                log.exception(self.log_msg("exception in adding temptask to linksout"))
        return True
                
    