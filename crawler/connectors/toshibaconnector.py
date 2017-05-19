'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#prerna 


import re
import logging
from urllib2 import urlparse
from datetime import datetime
from cgi import parse_qsl

from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging. getLogger("ToshibaConnector")

class ToshibaConnector(BaseConnector):    
    """Connector for toshiba.com 
    """
    @logit(log,'fetch')
    def fetch(self):
        """
        It fetches the data from the url mentioned
        sample url :http://us.toshiba.com/tv/lcd/40e210u/read-reviews
        """
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
            self.__setSoupForCurrentUri()
            while True:
                if not self.__addReviews():
                    log.info(self.log_msg('fetched all posts')) 
                    break 
            return True
        except:
            log.exception(self.log_msg("Exception in fetch %s"%self.currenturi)) 
        return True
        
        
    @logit(log, '__addReviews')
    def __addReviews(self):
        '''
            It will fetch the the reviews from the given review uri
            and append it  to self.pages
        '''
        reviews = self.soup.findAll('div',id =  re.compile('review_\d+'))
        log.info(self.log_msg('no of reviews is %s' % len(reviews)))
        if not reviews:
            return False
        for review in reviews:
            page = {'uri':self.currenturi}
            try:
                #unique_key = dict(parse_qsl(review.find('a', 'BVSocialBookmarkingSharingLink')['href'].split('?')[-1]))['url']
                unique_key = review['id'].split('_')[-1]
                if checkSessionInfo(self.__genre, self.session_info_out, unique_key, \
                             self.task.instance_data.get('update'), parent_list\
                                                        =[self.task.instance_data['uri']]):
                    log.info(self.log_msg('session info return True'))
                    return False
            except:
                log.exception(self.log_msg('Unique key cannot be fetched for url %s'\
                                                                    %self.currenturi))
            try:
                data_str = review.find('q')
                if data_str:
                    page['data'] = stripHtml(data_str.renderContents())
            except:
                   log.exception(self.log_msg('data not fetched %s'%page['uri']))    
                #title_tag = review.find('span', 'BVRRValue BVRRReviewTitle')
                
            try:
                date_str = review.find('strong','date')
                if date_str:
                    date_str = stripHtml(date_str.renderContents()).strip()
                    page['posted_date'] = datetime.strftime(datetime.strptime(date_str\
                                                ,'%B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
                    title_tag = date_str.findParent('h4')
                    title_tag.find('strong','date').extract()
                    page['title'] = stripHtml(title_tag.renderContents())
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['title'] = ''
                log.exception(self.log_msg('Posted date cannot be fetched for url\
                                                                %s'%page['uri']))
                            
            #Check that both title and data are not absent, so baseconnector does not throw exception    
            if not page['title'] and not page['data']:
                log.info(self.log_msg("Data and title not found for %s,"\
                    " discarding this review"%(self.currenturi)))
                continue
            try:
                info = review.find('p','review-info').find('span',text = re.compile('POSTED BY:')).next.split(',')
                page['et_author_name'] = info[0].strip()
                page['et_author_location'] = info[1].strip()
                
            except:
                log.exception(self.log_msg('author name and loaction not found\
                                                                %s'%page['uri']))
            try:
                page['ef_rating_overall'] = float(stripHtml(review.find('p','review-info').find('span',text = re.compile('Overall rating:')).\
                                findParent('strong').renderContents()).\
                                split(':')[-1].split('out of')[0].strip())
            except:
                log.exception(self.log_msg('rating not found' ) )                                                            
                                                                                                                                                                                                
            
            try:
                result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                    get_hash(page), 'Review', self.task.instance_data.get('update'), \
                                                parent_list=[self.task.instance_data['uri']])
                if not result['updated']:
                    log.info(self.log_msg('result not updated'))
                    continue
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [self.task.instance_data['uri'], unique_key]
                page['uri'] = self.currenturi + '#' + unique_key
                page['entity'] = 'Review'
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                page.update(self.__task_elements_dict)
                self.pages.append(page)
                #log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Exception while adding page for the\
                                                            url %s'%page['uri']))
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
    #costco