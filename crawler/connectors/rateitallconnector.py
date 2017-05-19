'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

import re
import logging
import urlparse
from datetime import datetime,timedelta
import copy

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
#from utils.httpconnection import HTTPConnection
from urllib2 import urlopen
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('RateItAllConnector')
class RateItAllConnector(BaseConnector):
    '''
    fetches the review from Rateitall.com
    sample uris http://www.rateitall.com/i-1066837-apple-iphone-3g.aspx,
    http://www.rateitall.com/i-945667-apple-iphone.aspx
    '''
    base_url = "http://www.rateitall.com/"

    @logit(log , 'fetch')
    def fetch(self):                                                                                         
        self.genre="Review"
        try:
            self.parent_url = self.currenturi
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
            self.__getParentPage()
            main_soup = copy.copy(self.soup)
            while True:
                try:
                    self.__addReviews()
                    self.currenturi = self.base_url + main_soup.find('a',text='Next &raquo;').parent['href']
                    self.rawpage = urlopen(self.currenturi).read()
                    self._setCurrentPage()      
                    main_soup = copy.copy(self.soup)
                except:
                    log.info(self.log_msg("fetched all reviews, exiting fetch"))
                    break                                     
            return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log, '_getParentpage')
    def __getParentPage(self):
        
        try:
            page={}
            #conn =  HTTPConnection()
            #headers = {'Host':'	www.rateitall.com'}
            #conn.createrequest(self.currenturi, headers=headers)            
            #conn.createrequest(self.currenturi)            
            self.rawpage = urlopen(self.currenturi).read()
            self._setCurrentPage()            
        except:
            log.exception(self.log_msg('Soup not set'))
            return 
        try:
            #page['title'] = stripHtml(self.soup.find('div','subHeader item itemReviewed').find('h1').renderContents())
            page['title'] = stripHtml(self.soup.find('div','subHeader').h1.renderContents())
        except:
            log.exception(self.log_msg('page title could not be parsed for '+ self.currenturi))
            page['title']=''
        try:
            page['ef_product_rating_overall'] = float(stripHtml(self.soup.find('span','itemRating right average').renderContents()))
        except:
            log.info(self.log_msg('Could not parse  Overall ratings for '+ self.currenturi))
        try:
            post_hash = get_hash(page)
            if checkSessionInfo(self.genre, self.session_info_out,
                                    self.parent_url ,self.task.instance_data.get('update')):
                self.log_msg('Parent page session info returns True')
                return False
            result=updateSessionInfo(self.genre, self.session_info_out, self.parent_url, post_hash, 'Post', self.task.instance_data.get('update'))
            if not result['updated']:
                self.log_msg('Parent page not updated')
                return False
            page['uri'] =self.currenturi
            page['path']=[self.parent_url]
            page['parent_path']=[]
            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['data'] = ''
            page['entity'] = 'Post'
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            log.info(self.log_msg('Paren page added'))
        except:
            log.exception(self.log_msg('Could not parse titl for the parent page'))
            
            
        return True

    @logit(log, 'addReviews')
    def __addReviews(self):
        '''It will add the reviews and comments
        '''
        review_url = self.currenturi
        for review in self.soup.findAll('div','reviewItem'):
            page={}
            unique_id = review.find('div','comments entity').find('a')['href']
            if not checkSessionInfo(self.genre, self.session_info_out,
                                    unique_id,  self.task.instance_data.get('update'),
                                    parent_list=[self.parent_url]):
                previous_uri = self.currenturi
                previous_soup = copy.copy(self.soup)
                try:
                    author_tag = review.find('a','reviewer')
                    page['et_author_profile'] = self.base_url + author_tag['href']
                    page['et_author_name'] = stripHtml(author_tag.renderContents())
                    if self.task.instance_data.get('pick_user_info'):
                        page= self.__getAuthorInfo(page)
                except:
                    log.info(self.log_msg('Author name not found'))
                self.currenturi = previous_uri
                self.soup = copy.copy(previous_soup)
                try:
                    rating_dict = {4:'Good',5:'Great',3:'Ok',2:'Bad',1:'Terrible'}
                    rating_value = int(stripHtml(review.find('span','rating').renderContents()))
                    page['ef_rating_overall'] = float(rating_value)
                    page['ef_rating_title'] = rating_dict[rating_value]
                except:
                    log.exception(self.log_msg('rating not found'))
                try:
                    page['data'] = '\n'.join([stripHtml(x.renderContents()) for x in review.find('div','reviewMid').findAll('p')[:-1]]).strip()
                except:
                    log.info(self.log_msg('Data not found'))
                    page['data'] =''
                try:
                    if len(page['data']) > 50:
                        page['title'] = page['data'][:50] + '...'
                    else:
                        page['title'] = page['data']
                except:
                    log.info(self.log_msg('Title not found'))
                    page['title'] = ''
                try:
                    date_str = stripHtml(review.find('span',attrs={'class':re.compile('.*dtreviewed$')}).renderContents())
                    page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%m/%d/%Y'),'%Y-%m-%dT%H:%M:%SZ')
                except:
                    page['posted_date']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    log.info(self.log_msg('Exception in Fetching Date from the review'))
                try:
                    votes_count = [ x.strip() for x in stripHtml(review.find('span','voteCounts').renderContents()).split('/')]
                    for x in votes_count:
                        try:
                            match_object = re.match('(\d+)\s*(\w+)',x)
                            page['ei_data_' + match_object.group(2).lower() + '_votes_count'] = int(match_object.group(1))
                        except:
                            log.info(self.log_msg('Votes not found'))
                except:
                    log.info(self.log_msg('Votes cannot be found'))
                try:
                    result=updateSessionInfo(self.genre, self.session_info_out,
                                             unique_id, get_hash(page), 'Review',
                                             self.task.instance_data.get('update'), parent_list=[self.parent_url])
                    if result['updated']:
                        page['path']=[ self.parent_url,unique_id ]
                        page['parent_path']=[self.parent_url]
                        page['uri'] = review_url
                        page['uri_domain'] = urlparse.urlparse(self.parent_url)[1]
                        page['entity'] = 'Review'
                        page.update(self.__task_elements_dict)
                        self.pages.append(page)
                        log.info(self.log_msg('Review Added'))
                except:
                    log.info(self.log_msg('Page not added'))
                try:
                    if self.task.instance_data.get('pick_comments'):
                        if int(re.search('\d+',stripHtml(review.find('div','comments entity').renderContents())).group())>0:
                            self.__addComments([self.parent_url,unique_id])
                except:
                    log.info(self.log_msg('Comments not added'))
                        
    @logit(log, '__getAuthorInfo')
    def __getAuthorInfo(self,page):
        try:
            self.currenturi = page['et_author_profile']
            self.rawpage = urlopen(self.currenturi).read()
            self._setCurrentPage()
        except:
            log.info(self.log_msg('Aurhor profile not found'))
            return page
        try:
            date_str = stripHtml(self.soup.find('div',id='membersince').renderContents()).replace('member since','').strip()
            page['edate_author_member_since'] = datetime.strftime(datetime.strptime(date_str,'%m/%d/%Y'),'%Y-%m-%dT%H:%M:%SZ')
        except:
            log.info(self.log_msg('Author info not found'))
        try:
            info_dict = {'et_author_title':'lblProfileTitle','et_author_about':'lblBioGraphy'}
            for each in info_dict:
                page[each] = stripHtml(self.soup.find('span',id=re.compile('.*' + info_dict[each] + '$')).renderContents())
        except:
            log.info(self.log_msg('Author title not found'))
        try:
            votes_count = [ x.strip() for x in stripHtml(self.soup.find('span',id=re.compile('VoteTally_TallySpan')).renderContents()).split('/')]
            for x in votes_count:
                try:
                    match_object = re.match('(\d+)\s*(\w+)',x)
                    page['ei_author_' + match_object.group(2).lower() + '_votes_count'] = int(match_object.group(1))
                except:
                    log.info(self.log_msg('Votes not found'))
        except:
            log.info(self.log_msg('Votes count not found'))
        try:
            ranking_list = self.soup.find('h5',text='By the Numbers').findParent('div').findAll('li')
            for each in ranking_list:
                try:
                    match_object = re.match('(\w+)\s*(\w+)',(stripHtml(each.find('a','byTheNumbers').renderContents())))
                    key = match_object.group(2).lower()
                    value = match_object.group(1).lower()
                    try:
                        page['ei_author_'+key+'_count'] = int(value)
                    except:
                        pass
                    page['ei_author_'+key+'_rank'] = int(stripHtml(each.find('span','bythenumRank').renderContents()))
                except:
                    log.info(self.log_msg('Author info not found'))
        except:
            log.info(self.log_msg('ranking not found'))
        return page
    
    @logit(log, '__addComments')
    def __addComments(self,parent_list):
        page={}
        try:
            self.currenturi = self.base_url + parent_list[-1]
            self.rawpage = urlopen(self.currenturi).read()
            self._setCurrentPage()
        except:
            log.info(self.log_msg('comment not found'))
        comments = self.soup.findAll('div','commentWrapper')
        for comment in comments:
            page={}
            try:
                page['et_author_name'] = stripHtml(comment.find('a',id=re.compile('.*UserProfileLink$')).renderContents())
            except:
                log.info(self.log_msg('author name not found'))
            try:
                comment_panel = comment.find('div',id=re.compile('.*CommentUpdatePanel$'))
                no_of_days = int(re.search('commented (\d+) days ago',stripHtml(comment_panel.renderContents())).group(1))
                page['posted_date'] = datetime.strftime(datetime.now() - timedelta(days=no_of_days),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.info(self.log_msg('posted date not found'))
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            try:
                page['data'] =  stripHtml(comment_panel.find('div','bottom').renderContents().replace('/>>', '/>'))
            except:
                log.info(self.log_msg('data not found'))
                page['data']=''
            try:
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
            except:
                log.info(self.log_msg('Title not found'))
                page['title'] = ''
            try:
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                if checkSessionInfo(self.genre, self.session_info_out,
                                            unique_key, self.task.instance_data.get('update'),
                                            parent_list=parent_list):
                    log.info(self.log_msg('session info returns true for comemnst'))
                    continue
                result=updateSessionInfo(self.genre, self.session_info_out,unique_key , get_hash(page),
                                                 'Comment', self.task.instance_data.get('update'),
                                                 parent_list=parent_list)
                if result['updated']:
                    temp_parent_list = parent_list[:]
                    page['parent_path'] = temp_parent_list[:]
                    temp_parent_list.append(unique_key)
                    page['path'] = temp_parent_list
                    page['uri'] = self.currenturi
                    page['uri_domain'] = urlparse.urlparse(self.currenturi)[1]
                    page['entity'] = 'Comment'
                    page.update(self.__task_elements_dict)
                    self.pages.append(page)
                    log.info(self.log_msg('Comment added'))
            except:
                log.info(self.log_msg('Comment not added'))
