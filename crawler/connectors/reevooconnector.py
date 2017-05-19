
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#modified by prerna
#Modified by Skumar

import re
import logging
import urlparse
from datetime import datetime
from BeautifulSoup import BeautifulSoup

from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('Reevoo Connector')

class ReeVooConnector(BaseConnector):
    '''
    The ReeVooConnector fetches data from 'http://www.reevoo.com/browse/product_type/laptop/product_brand/(Advent.....)' for a given Brand name 
    '''
    uri_list=[]
    base_uri='http://www.reevoo.com'
    @logit(log, 'fetch')
    def fetch(self):
        '''fetch method of reevoo connector,which fetches the info in for the given uri
        '''
        self.genre="Review"
        try:
            log.info(self.currenturi)
            if re.match('http://www.reevoo.com/browse/product_type/laptop/product_brand/(.+)', self.currenturi):
                log.debug(self.currenturi)
                if self._getLinks():
                    log.info(self.log_msg('SETTING Current page'+self.currenturi))
            #elif re.match(r'http://www.reevoo.com/reviews/(.+)', self.currenturi):
            else:
                self.currenturi = self.currenturi.split('#')[0]+'/most-recent'
                self.parent_url=self.currenturi
                self._getParentPage()
                while True:
                    try:
                        self.addReviews()
                        next_page_tag = self.soup.find('a', 'next_page')
                        if not next_page_tag:
                            self.currenturi = self.base_uri + self.soup.find('div', 'pagination').find('a', text='Show more reviews').parent['href'].split('#')[0]  
                        else:
                            self.currenturi = self.base_uri + next_page_tag['href'].split('#')[0]
                        res=self._getHTML()
                        self.rawpage=res['result']
                        self._setCurrentPage()
                    except:
                        log.exception(self.log_msg('Reached last page, no more page to fetch'))
                        break
            return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log, '_getLinks')
    def _getLinks(self): 
        '''
        fetches links from parentpage assigns each url in task
        '''
        res=self._getHTML()
        self.rawpage=res['result']
        self._setCurrentPage()
        try:
            while True:
                #next= self.soup.find('div', {'class':'will_paginate'}).find(text='next')
                if self.soup.find('div', id='products').findAll('h2'):
                    parent_links=[each.find('a', href=True)['href'] for each in self.soup.find('div', id='products').findAll('h2')]
                    log.info(self.log_msg('total number of parent urls: %d' % len(parent_links)))
                    for link in parent_links:
                        if not link in self.uri_list:
                            link=self.base_uri+link
                            self.uri_list.append(link)
                            log.info(self.log_msg('parent' + str(link) ))
                            temp_task = self.task.clone()
                            temp_task.instance_data[ 'uri' ] = normalize( link )
                            self.linksOut.append(temp_task)
                if self.soup.find('div', {'class':'will_paginate'}):
                    if self.soup.find('div', {'class':'will_paginate'}).find(text='next'):
                        if self.soup.find('div', {'class':'will_paginate'}).find(text='next').parent.get('href'):
                            next_url=self.base_uri+ self.soup.find('div', {'class':'will_paginate'}).find(text='next').parent.get('href')
                            res=self._getHTML(next_url)
                            self.rawpage=res['result']
                            self._setCurrentPage()
                        else:
                            break
                else:
                    return False
            return True
        
        except Exception, e:
            log.exception(self.log_msg("parent links couldn't be parsed"))
            return False

    @logit(log, '_getParentPage')
    def _getParentPage(self):
        try:
            page={}
            res=self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
        except:
            log.info(self.log_msg('soup not set'))
            return False
        try:
            page['title'] = stripHtml(self.soup.find('h1', id = 'productName').renderContents())
        except:
            page['title']=''
            log.exception(self.log_msg('Title not found'))
        try:
            #page['ef_overall_ratings'] = float(self.soup.find('div', 'box picture').find('img', {'alt':re.compile('^Score.+')})['alt'].split(':')[1].split('/')[0])
            page['ef_product_rating_overall'] = float(self.soup.find('div','average-score').img['alt'].split(':')[-1].split('/')[0].strip())
        except:
            log.info(self.log_msg('Rating not found'))
        try:
            ratings_table = self.soup.find('table',id='scoreSummary').findAll('tr')
            for each_rating in ratings_table:
                try:
                    key = 'ef_product_rating_' + stripHtml(each_rating.find('th').renderContents()).lower().replace(' ','_')
                    page[key] = float(stripHtml(each_rating.find('td').renderContents()))
                except:
                    log.info(self.log_msg('one of rating is not found'))
        except:
            log.info(self.log_msg('other ratings are not found'))
        try:
            if checkSessionInfo(self.genre, self.session_info_out,self.parent_url, self.task.instance_data.get('update')):
                log.info(self.log_msg('Parent page session info return s True, cannot proceed'))
                return False
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            post_hash=get_hash( page )
            result=updateSessionInfo(self.genre, self.session_info_out, self.parent_url, post_hash,
                                     'Post', self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                return False
            page['uri'] =self.parent_url
            page['path']=[self.parent_url]
            page['parent_path']=[]
            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
            page['priority']=self.task.priority
            page['level']=self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            page['last_updated_time'] = page['pickup_date']
            page['versioned'] = False
            page['data'] = ''
            page['entity'] = 'Post'
            page['category']=self.task.instance_data.get('category','')
            page['task_log_id']=self.task.id
            page['client_name'] = self.task.client_name
            page['versioned']=self.task.instance_data.get('versioned',False)
            self.pages.append(page)
            log.info(self.log_msg('Parent page added'))
        except:
            log.info(self.log_msg('cannot add parent page'))
            
    @logit(log, 'addreviews')
    def addReviews(self):
        """ gets the reviewLinks and for each review links calls addreviews
        """
        try:
            reviews = self.soup.findAll('div',id=re.compile('review_.+'))
            log.info(self.log_msg('Total Reviews found is %d'%len(reviews)))
        except:
            log.info(self.log_msg('Cannot fetch the log reviews'))
            return False
        for each in reviews:
            page={}
            try:
                div_id =each['id']
            except:
                log.info(self.log_msg('Canot find the review id, continue'))
                continue
            if checkSessionInfo(self.genre, self.session_info_out,
                                    div_id,  self.task.instance_data.get('update'),
                                    parent_list=[self.parent_url]):
                log.info(self.log_msg('session info returns True for review for %s'%div_id))
                continue
            pros_cons = {'et_data_pros':'response pros','et_data_cons':'response cons'}
            for key in pros_cons.keys():
                page[key] = ''
                try:
                    tag = each.find('p',{ 'class':re.compile('^' +pros_cons[key])})
                    unwanted_tag = tag.find('span',{ 'class':re.compile('_point')})
                    if unwanted_tag:
                        unwanted_tag.extract()
                    #tag.find('span','icon').extract()
                    result_str = stripHtml(tag.renderContents())
                    if not result_str=='Reviewer left no comment':
                        page[key] = result_str
                except:
                    log.info(self.log_msg('Could not parse %s for the review page'%key))
            try:
                page['data'] = (page['et_data_pros'] + '\n' + page['et_data_cons']).strip()
            except:
                page['data']=''
                log.info(self.log_msg('Could not parse DATA for the review page'))
            if not page['data']:
                log.info(self.log_msg('No data found for this url %s'%self.currenturi))
                continue
            try:
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
            except:
                log.info(self.log_msg('Title not found'))
                page['title'] = ''
            try:
                date_str = stripHtml(each.find('p', attrs={'class':re.compile('reviewDate')}).renderContents())
                date_str = re.search('\d{1,2} \w{3} \d{4}$', date_str).group()
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%d %b %Y'),'%Y-%m-%dT%H:%M:%SZ')
            except:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                log.info(self.log_msg('Posted date not found'))
            try:
                page['ef_rating_overall'] = float(stripHtml(each.find('span', 'smallProductScore').renderContents()).replace('Score ','').split('/')[0])
            except:
                log.info(self.log_msg('Could not parse product rating for the Review page'))
            try:
                page['ei_data_helpful_count'] = int(re.search('^\d+',stripHtml(each.find('div','helpfulCount').renderContents())).group())
            except:
                log.info(self.log_msg('helpful count not found'))
            try:
                author_tag = each.find('h3')
                extralink = author_tag.find('a','moreLikeThisLink')
                if extralink:
                    extralink.extract()
                usage = author_tag.find('em')
                if usage:
                    usage.extract()
                    page['et_author_usages'] =stripHtml(usage.renderContents())
                author_info = [x.strip() for x in  stripHtml(author_tag.renderContents()).split(',') if not x.strip()=='']
                page['et_author_location'] = ', '.join(author_info[1:])
                page['et_author_name']= author_info[0]
            except:
                log.exception(self.log_msg('author name not found'))
            try:
                #log.info(page)
                result=updateSessionInfo(self.genre, self.session_info_out,
                                         div_id, get_hash( page ) , 'Review',
                                         self.task.instance_data.get('update'), parent_list=[self.parent_url])
                if not result['updated']:
                    continue
                page['parent_path'] = [self.parent_url]
                page['path']=[self.parent_url,div_id]
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['last_updated_time'] = page['pickup_date']
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                page['client_name'] = self.task.client_name
                page['versioned'] = False
                page['uri'] = self.currenturi
                page['uri_domain'] = urlparse.urlparse(self.parent_url)[1]
                page['task_log_id']=self.task.id
                page['entity'] = 'Review'
                page['category'] = self.task.instance_data.get('category' ,'')
                self.pages.append(page)
            except:
                log.info(self.log_msg('Cannot add the review'))