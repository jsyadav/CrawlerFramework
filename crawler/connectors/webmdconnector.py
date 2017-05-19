'''
Copyright (c)2008-2010 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#SKumar

import re
import logging
from urllib2 import urlparse
from datetime import datetime

from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging. getLogger("WebmdConnector")

class WebmdConnector(BaseConnector):    
    
    @logit(log,'fetch')
    def fetch(self):
        """
        It fetches the data from the url mentioned
        sample url : http://www.webmd.com/drugs/drugreview-64439-Abilify+Oral.aspx?drugid=64439&drugname=Abilify+Oral
        """
        self.genre = "Review"
        try:
            self.parent_uri = self.currenturi = self.currenturi + '&conditionFilter=-1'
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
            self.__setParentPage()
            while True:
                if not self.__addReviews():
                    log.info(self.log_msg('fetched all posts for the url %s'%\
                                                                self.currenturi))
                    break 
                try:
                    self.currenturi = 'http://www.webmd.com' + self.soup.find('a', text='Next').parent['href']
                    self.__setSoupForCurrentUri()
                except:
                    log.info(self.log_msg('Next page not found for the url %s'\
                                                            %self.currenturi)) 
                    break 
            return True
        except:
            log.exception(self.log_msg("Exception in fetch %s"%self.currenturi)) 
            return False
        
    @logit(log,'getparentpage')    
    def __setParentPage(self):
        """It fetches the product information
        """
        page = {'uri': self.currenturi}
        try:
            self.__product_name = page['data'] = page['title'] = stripHtml(self.soup.find('span', id='titleBarTitle_fmt').renderContents()).split(' - ', 1)[-1].strip()
        except:
            log.exception(self.log_msg("Exception Occurred while fetching the title!!"))
            return False
        try:
            page['ei_product_reviews_count'] = int(re.sub('[^\d]+', '', stripHtml(self.soup.find('span', 'totalreviews').renderContents())))
        except:
            log.info(self.log_msg("Exception while getting the Rating Tag in url :%s"\
                                                                    %self.currenturi ))
        try: 
            self.updateParentExtractedEntities(page) 
            if checkSessionInfo(self.genre, self.session_info_out, self.currenturi, \
                                self.task.instance_data.get('update')):
                log.info( self.log_msg('Session info return True for the url %s\
                                                            '%self.currenturi) )
                return False
            result = updateSessionInfo(self.genre, self.session_info_out, self.currenturi, \
                    get_hash(page) ,'Post', self.task.instance_data.get('update'))
            if not result['updated']:
                return False
            page['path'] = [self.currenturi]
            page['parent_path'] = []
            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['entity'] = 'Post'
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            log.info(self.log_msg('Parent Page added'))
            return True
        except:
            log.exception(self.log_msg("parent post couldn't be parsed for url \
                                                        %s"%self.currenturi))
            return False
        
    @logit(log, '_addreviews')
    def __addReviews(self):
        '''
            It will fetch the the reviews from the given review uri
            and append it  to self.pages
        '''
        posts = self.soup.findAll('div', 'userPost')
        if not posts:
            return False
        log.info(self.log_msg('no of reviews is %s' % len(posts)))
        for post in posts:
            
            page = {'uri':self.currenturi}
            try:
                data_tag = post.findAll('p', 'comment')[-1]
                strong_tags = data_tag.findAll('strong')
                if strong_tags:
                    [x.extract() for x in strong_tags]
                a_tag = data_tag.find('a', href='#')
                if a_tag:
                    a_tag.extract()
                page['data'] =  stripHtml(data_tag.renderContents())
                page['title'] = self.__product_name
            except:
                log.exception(self.log_msg('Title cannot be fetched for url %s'\
                                                                    %self.currenturi))
                continue
            
            try:
                page.update(dict([('ef_data_rating_' + stripHtml(x.find('p', 'category').__str__()).lower().replace(' ', '_'), float(stripHtml(x.find('span').renderContents()).split(': ', 1)[-1].strip())) for x in post.find('div', id='ctnStars').findAll('div', recursive=False)]))                
            except:
                log.exception(self.log_msg('data cannot be fetched for url %s'\
                                                                    %self.currenturi))
            try:
                author_info = stripHtml(post.find('p', 'reviewerInfo').renderContents())
                if ',' in author_info:
                    page['et_author_name'] = author_info.split(',', 1)[0].split(':')[1].strip()
            except:
                log.exception(self.log_msg('uri cannot be fetched for url %s'\
                                                                    %self.currenturi))
                
            try:
                author_age_group = re.search('\d+\-\d+', author_info)
                if author_age_group:
                    page['et_author_age_group'] = author_age_group.group()
            except:
                log.exception(self.log_msg('Posted date cannot be fetched for url\
                                                                %s'%self.currenturi))
                
            try:
                date_str =  stripHtml(post.find('div','date').renderContents())
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str\
                                                ,'%m/%d/%Y %I:%M:%S %p'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                log.info(self.log_msg("posted date cannnot be fetched for \
                                                        the uri%"%self.currenturi))
            try:
                treatment_period = re.search('on Treatment for ((.+)) \(Patient\)', author_info)
                if treatment_period:
                    page['et_author_treatment_period'] = treatment_period.group(1)
                
            except:
                log.info(self.log_msg("Author Treatment period cannot be fetched for url\
                                                                %s"%self.currenturi))
            try:
                page['ei_data_helpful_count'] = int(re.search('^\d+', stripHtml(post.find('p', 'helpful').renderContents())).group())
            except:
                log.info(self.log_msg("Help ful count cannot be fetched for url\
                                                                %s"%self.currenturi))
            try:
                #it is a Review ID
                unique_key = re.search('reviewid=(.+?)&', post.find('a', 'reportAbuse')['onclick']).group(1)
                if checkSessionInfo(self.genre, self.session_info_out, unique_key, \
                             self.task.instance_data.get('update'), parent_list\
                                                        =[self.parent_uri]):
                    log.info(self.log_msg('session info return True'))
                    return False
                result = updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                    get_hash(page), 'Review', self.task.instance_data.get('update'), \
                                                parent_list=[self.parent_uri])
                if not result['updated']:
                    log.info(self.log_msg('result not updated'))
                    continue
                
                page['parent_path'] = [self.parent_uri]
                page['path'] = [self.parent_uri, unique_key]
                page['entity'] = 'Review'
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                page.update(self.__task_elements_dict)
                self.pages.append(page)
                log.info(self.log_msg('Review Added'))
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
            log.info(self.log_msg('HTML Content cannot be fetched for the url: \
                                                            %s'%self.currenturi))
            return False
        self._setCurrentPage()