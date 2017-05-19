'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

import re
from datetime import datetime
import logging
from urllib2 import urlparse,unquote
import copy

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('GamezeboConnector')
class GamezeboConnector(BaseConnector):
    '''
    sample uris
    http://www.gamezebo.com/games/everything-nice/review
    http://www.gamezebo.com/games/mystery-case-files-dire-grove/review
    http://www.gamezebo.com/games/escape-museum-2/review
    http://www.gamezebo.com/games/hidden-expedition-devils-triangle/review --40 
    http://www.gamezebo.com/games/murder-she-wrote/review --27
    '''
    @logit(log , 'fetch')
    def fetch(self):
        self.genre = 'Review'
        try:
            res=self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            self.__getParentPage()
            self.currenturi = self.currenturi.rsplit('/',1)[0]+'/user-reviews?post_per_page=50'
            res=self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            while True:
                try:
                    if not self.__addReviews():
                        break
                    self.currenturi = 'http://www.gamezebo.com' + self.soup.find('ul','nav_reviews').find('a',text='Next').parent['href']
                    res=self._getHTML()
                    self.rawpage=res['result']
                    self._setCurrentPage()
                except:
                    log.info(self.log_msg('Next Page is not found'))
                    break
            return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False
        
        
    @logit(log , '_getParentPage')
    def __getParentPage(self):
        '''
        This will fetch the parent info
        '''
        if checkSessionInfo(self.genre, self.session_info_out,
                                            self.task.instance_data['uri'], self.task.instance_data.get('update')):
            log.info(self.log_msg('Check session info returns True, Already post available'))
            return False
        page = {}
        try:
            hierarchy = stripHtml(self.soup.find('div',id='breadcrumbs').renderContents()).split('>')[:-1]
            page['data'] = page['title'] = hierarchy[-1]
            page['et_product_hierarchy'] = ' > '.join(hierarchy)
            page['uri'] = self.task.instance_data['uri']
        except Exception, e:
            log.exception(self.log_msg('could not parse title'))
            return False
        try:
            rating_str = stripHtml(self.soup.find('div',id='review-stats_user').find('p',attrs={'class':re.compile('sr-stars')}).renderContents()).lower()
            rating = re.search('\d+',rating_str).group()
            if 'half' in rating_str.lower():
                rating = rating + '.5'
            page['ef_product_rating_overall']=float(rating)
        except:
            log.exception(self.log_msg('Cannot find ef_product_rating_overall'))
        try:
            page['et_product_pros'] ,page['et_product_cons']=[stripHtml(x.renderContents()) for x in self.soup.find('div',id='pros-cons').findAll('cite')]
        except:
            log.info(self.log_msg('Cannot find pros and cons'))
        try:
            result=updateSessionInfo(self.genre, self.session_info_out,self.task.instance_data['uri'], get_hash(page), 
                                        'Post', self.task.instance_data.get('update'))
            if result['updated']:
                page['path'] = [ self.task.instance_data['uri'] ] 
                page['parent_path'] = []
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
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
                page['task_log_id']=self.task.id
                page['entity'] = 'Post'
                page['category']=self.task.instance_data.get('category','')
                self.pages.append(page)
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            raise e
        
    @logit(log , 'addreviews')
    def __addReviews(self):
        '''
        Doc String
        '''
        try:
            reviews = self.soup.find('ul',id='user-review-content').findAll('li',recursive=False)
            if not reviews:
                log.info(self.log_msg('Zero Reviews found'))
                return False
        except:
            log.info(self.log_msg('No Reviews are found'))
            return False
        for review in reviews:
            try:
                page = self.__getData(review)
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                if not checkSessionInfo(self.genre, self.session_info_out,unique_key, self.task.instance_data.get('update'),[ self.task.instance_data['uri'] ]):
                    
                    result=updateSessionInfo(self.genre, self.session_info_out, unique_key, get_hash(page), 
                                             'Review', self.task.instance_data.get('update'), parent_list=[ self.task.instance_data['uri'] ])
                    if result['updated']:
                        page['parent_path'] = [ self.task.instance_data['uri'] ]
                        page['path'] = [ self.task.instance_data['uri'],unique_key ]
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
                        page['entity'] = 'Review'
                        page['category'] = self.task.instance_data.get('category','')
                        page['task_log_id']=self.task.id
                        page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                        self.pages.append(page)                
                    else:
                        log.info(self.log_msg('Result not update'))
                else:
                    log.info(self.log_msg('reached already parsed review so returning'))
    
            except:
                log.exception(self.log_msg("exception in addreviews"))
        return True

    @logit(log,'__getData')
    def __getData(self,review):
        '''Doc String
        '''
        page = {}
        try:
            comment_head = review.find('div','comment-heading')
            rating_str = stripHtml(comment_head.find('p',attrs={'class':re.compile('lr-stars')}).renderContents()).lower()
            rating = re.search('\d+',rating_str).group()
            if 'half' in rating_str.lower():
                rating = rating + '.5'
            page['ef_rating_overall']=float(rating)
        except:
            log.info(self.log_msg('rating overall not found'))
        try:
            date_str = stripHtml(comment_head.find('p','comment-date').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime (date_str,'Posted on %b %d, %Y, %I:%M%p'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            log.info(self.log_msg('Posted date not found'))
        try:
            data_tag = review.find('p','comment')
            [x.extract() for x in data_tag.findAll('a')]
            page['data'] = stripHtml(data_tag.renderContents())
            page['title'] = page['data'][:50] + '...' 
            page['uri'] = self.currenturi
        except Exception, e:
            log.exception(self.log_msg('could not get data'))
        try:
            author_tag = review.find('div','review-user-info').a
            page['et_author_name']= stripHtml( author_tag.renderContents())
            page['et_author_profile'] = 'http://www.gamezebo.com' + author_tag['href']
        except:
            log.info(self.log_msg('Cannot find et_author_name'))
        parent_soup = copy.copy(self.soup)
        parent_uri = self.currenturi
        try:
            if self.task.instance_data.get('pick_user_info'):
                self.currenturi = page['et_author_profile']
                res=self._getHTML()
                self.rawpage=res['result']
                self._setCurrentPage()
                page.update(dict([('et_author_' + stripHtml(x.renderContents()).lower()[:-1].replace(' ','_'),stripHtml(x.findNext('dd').renderContents())) for x in self.soup.find('div',id='userpage_user-about-me').findAll('dt')]))
        except Exception, e:
            log.exception(self.log_msg('could not parse uri'))
        self.soup = copy.copy(parent_soup)
        self.currenturi = parent_uri
        return page
        