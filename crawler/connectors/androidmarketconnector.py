'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#prerna 


import re
import logging
from urllib2 import urlparse,urlopen
from datetime import datetime
from cgi import parse_qsl
import simplejson
from BeautifulSoup import BeautifulSoup
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.httpconnection import HTTPConnection
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging. getLogger("AndroidMarketConnector")

class AndroidMarketConnector(BaseConnector):    
    """Connector for market.android.com 
    """
    @logit(log,'fetch')
    def fetch(self):
        """
        It fetches the data from the url mentioned
        sample url ##uri ='https://market.android.com/details?id=com.handson.h2o.nfl&feature=search_result#?t=W251bGwsMSwxLDEsImNvbS5oYW5kc29uLmgyby5uZmwiXQ'
                'https://market.android.com/getreviews?id=com.rovio.angrybirds&reviewSortOrder=2&reviewType=1'
        Note:'There is maximum 49 pages in each ,so maximum total reviews 490'
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
            self.parenturi = self.currenturi
            self.__setSoupForCurrentUri()
            self.currenturi ='https://market.android.com'+self.soup.find('input',id ='reviewAjaxUrl')['value']
            data_dict = dict(parse_qsl(self.currenturi.split('?')[-1]))
            if 'id' in data_dict.keys():
                review_id = data_dict.pop('id')
            c = -1
            conn = HTTPConnection()
            is_total_page_num_set = False
            total_pagenum = 0
            headers = {'Host':'market.android.com','Referer':self.parenturi.split('#')[0]}
            while True:
                try:
                    c+=1
                    if c >total_pagenum:
                        log.info(self.log_msg('no more pages found'))
                        break
                    data = {'id':review_id,'pageNum':c,'reviewSortOrder':0,'reviewType':1}
                    conn.createrequest(self.currenturi,headers = headers,data=data)
                    num_of_tries = 0
                    page_fetched = True
                    while num_of_tries<=3:
                        try:
                            res = conn.fetch().read()
                            self.rawpage = res
                            self._setCurrentPage()
                            data_str = simplejson.loads(re.findall('{.*}', self.soup.__str__())[0])
                            if not is_total_page_num_set:
                                total_pagenum = data_str['numPages']
                                is_total_page_num_set = True
                            self.rawpage= data_str['htmlContent']
                            self._setCurrentPage()
                            log.info(self.log_msg("now in page %s"%c)) 
                            break
                        except:
                            num_of_tries += 1
                            if num_of_tries>3:
                                page_fetched = False
                                break
                            continue
                        #num_of_tries += 1
                    if not page_fetched:
                        log.info(self.log_msg("Page cannot be fetched"))
                        continue
                    if not self.__addReviews():
                        log.info(self.log_msg('fetched all posts')) 
                        break
                except:
                    log.exception(self.log_msg('Next page not found for the url %s'\
                                                            %self.currenturi)) 
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
        reviews = self.soup.findAll('div','doc-review')
        log.info(self.log_msg('no of reviews is %s' % len(reviews)))
        if not reviews:
            return False
        for review in reviews:
            page = {'uri':self.parenturi}
            try:
                page['title'] = stripHtml(review.find('h4', 'review-title').renderContents())
            except:
                log.exception(self.log_msg('title cannot be fetched for url %s'\
                                                                    %self.currenturi))
                page['title'] = ''
            try:
                data_str = review.find('p','review-text').renderContents()
                if data_str:
                    page['data'] = stripHtml(data_str.replace('<br />>', ''))
                else:
                    page['data'] = page['title']
            except:
                log.exception(self.log_msg('data cannot be fetched for url %s'\
                                                                    %self.currenturi))
                page['data'] =''
                            
            #Check that both title and data are not absent, so baseconnector does not throw exception    
            if not page['title'] and not page['data']:
                log.info(self.log_msg("Data and title not found for %s,"\
                    " discarding this review"%(self.currenturi)))
                continue
            try:
                date_str = stripHtml(review.find('span', 'doc-review-date').\
                            renderContents()).split('on ')[-1].strip()
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str\
                                                ,'%B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
                
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                log.exception(self.log_msg('Posted date cannot be fetched for url\
                                                                %s'%page['uri']))
            try:
                rating_tag = review.find('div', 'ratings goog-inline-block')
                if rating_tag:
                    page['ef_rating_overall'] = float(rating_tag['title'].split(':')[-1].split('stars')[0].strip())
                else:
                    log.info('rating_tag not found')
            except:
                log.exception(self.log_msg('rating not found' ) )                                                            
            try:
                page['et_author_name'] = stripHtml(review.find('span', 'doc-review-author').renderContents())
                
            except:
                log.exception(self.log_msg("Author name cannot be fetched for url %s\
                                                                "%page['uri']))
            try:
                unique_key = get_hash({'title': page['title'],'data':page['data'],'posted_date':page['posted_date']})  
                if checkSessionInfo(self.__genre, self.session_info_out, unique_key, \
                             self.task.instance_data.get('update'), parent_list\
                                                        =[self.task.instance_data['uri']]):
                    log.info(self.log_msg('session info return True'))
                    return False
                result = updateSessionInfo(self.__genre, self.session_info_out, unique_key, \
                    get_hash(page), 'Review', self.task.instance_data.get('update'), \
                                                parent_list=[self.task.instance_data['uri']])
                if not result['updated']:
                    log.info(self.log_msg('result not updated'))
                    continue
                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = [self.task.instance_data['uri'], unique_key]
                page['uri'] = self.parenturi #self.currenturi 
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
            print type(self.rawpage)
        else:
            log.info(self.log_msg('Page Content cannot be fetched for the url: \
                                                            %s'%self.currenturi))
            raise Exception('Page content not fetched for th url %s'%self.currenturi)
        self._setCurrentPage()
