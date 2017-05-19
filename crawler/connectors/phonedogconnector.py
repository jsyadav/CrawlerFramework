'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#SKumar
#modified by harsh
import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging. getLogger("PhoneDogConnector")

class PhoneDogConnector(BaseConnector):
    '''Connector for Phonedog.com
    '''
    
    @logit(log,'fetch')
    def fetch(self):
        """
        It fetches the data from the url mentioned
        sample urls
        http://www.phonedog.com/cell-phone-research/motorola-droid_user-reviews.aspx
        http://www.phonedog.com/cell-phone-research/companies/u-s-cellular_user-reviews.aspx
        """
        self.genre = "Review"
        try:
            if not self.__setSoup():
                log.info(self.log_msg("Soup not set,returning false"))
                return False
            #if not self._getParentPage():
         #       log.info(self.log_msg("Parent page not found"))
            while True:
                parent_page_soup = copy.copy(self.soup)
               # log.info(self.log_msg('current uri%s'%parent_page_soup))
                if not self.__addReviews():
                    log.info(self.log_msg('fetched all reviews for the url %s'\
                                            %self.task.instance_data['uri']))
                
                log.info(self.log_msg('Next page%s'%self.currenturi))
                try:
                    
                   # self.currenturi = self.task.instance_data['uri'].rsplit\
                   #                 ('/', 1)[0] + '/' + self.soup.find('a', \
                   #                         title='Go to the next page')['href']
                    self.currenturi = 'http://www.phonedog.com' + parent_page_soup.find('a',title='Go to the next page')['href']
                    
                    if not self.__setSoup():
                        log.info(self.log_msg('soup not set for the uri %s'%\
                                                            self.currenturi))
                        break
                except:
                    log.info(self.log_msg('Next page not found for the uri %s'%\
                                                                self.currenturi))
                    break
            return True
        except:
            log.exception(self.log_msg("Exception in fetch"))
            return False
        
    @logit(log,'getparentpage')    
    def _getParentPage(self):
        """It fetches the product information
        """
        page = {}
        tag=[]
        
        data= self.soup.findAll('div','span8')
        for d in data:
            tag=d.findAll('div','pd-comment')
        
        try:
           # page['title'] = stripHtml(self.soup.find('div','breadcrumbs')\
            #                                .findAll('a')[-1].renderContents())
            for t in tag:
                    title=(t.find('h4'))
                    page['title'] = title
                    log.info(self.log_msg("title:%s"%page['title']))
        except:
            log.exception(self.log_msg("Title not fetched"))
            return False
        
        try:
#==============================================================================
#             rating_tag = self.soup.find('div','reviews-ratingcombined')
#             page['ef_product_rating_overall'] = float(rating_tag.b.renderContents())
#             for each in rating_tag.findParent('div').findAll('div','reviews-rating'):
#                 key = 'ef_product_rating_' + stripHtml(each.label.renderContents\
#                                         ()).lower().split('/')[0].replace(' ','_')
#                 page[key] = float(each.b.renderContents())
#==============================================================================
            for r in tag:
                  rating_tag=(r.find('div','badge pd-review-score')).replace('Overall','')
                  page['rating_tag'] = rating_tag
                  
        except:
            log.exception(self.log_msg("Specifications not found!!"))
            
        try:
            self.updateParentExtractedEntities(page) 
            if checkSessionInfo(self.genre, self.session_info_out, \
                self.task.instance_data['uri'],self.task.instance_data.get('update')):
                log.info(self.log_msg('Check Session info return True'))
                return False
            result = updateSessionInfo(self.genre, self.session_info_out,\
                        self.task.instance_data['uri'], get_hash(page) ,'Post',\
                                        self.task.instance_data.get('update'))
            if not result['updated']:
                return False
            page['uri'] = self.task.instance_data['uri']
            page['data'] = ''
            page['path'] = [self.task.instance_data['uri']]
            page['parent_path'] = []
            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
            page['priority'] = self.task.priority
            page['level'] = self.task.level
            page['last_updated_time'] = page['posted_date'] = page['pickup_date'] = \
                    datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            page['client_name'] = self.task.client_name
            page['versioned'] = False
            page['task_log_id'] = self.task.id
            page['entity'] = 'Post'
            page['category'] = self.task.instance_data.get('category','')
            self.pages.append(page)
            log.info(self.log_msg('Parent Page added'))
            return True
        except:
            log.exception(self.log_msg("Exception while adding parent Page info"))
            return False
        
    @logit(log, '_addreviews')
    def __addReviews(self):
        '''It will fetch the the reviews and append it  to self.pages
        '''
        
        try:
#==============================================================================
#             reviews = [each.findParent('div','reviews-item') for each in \
#                 self.soup.find('div','reviews').findAll('h3') if each.find('a')]
#             log.debug(self.log_msg('no of reviews is %s'%len(reviews))) 
#             if not reviews:
#                 return False
#==============================================================================
            review=[]
           
            page = {}
            data= self.soup.findAll('div','span8')
            for d in data:
                reviews=d.findAll('div','pd-comment')
            page['uri']= self.currenturi
        except:
            log.exception(self.log_msg('Reviews cannot be fetched for the url %s'\
                                                            %self.currenturi))
            return False
        
       
        
        for review in reviews:
            
            try:
              #  title_tag = review.find('h3')
              #  page['uri'] = 'http://www.phonedog.com' + title_tag.a['href']
              #  page['title'] =  stripHtml(title_tag.renderContents())
                title_tag=review.find('h4')
                page['title'] = title_tag.renderContents()             
              #  log.info(self.log_msg("title:%s"%page['title']))
            except:
                  log.exception(self.log_msg("Either title or uri is missing!!"))
                     
           
            try:    
       #         rating_info = [x.strip() for x in stripHtml(review.find('div',\
       #                     'reviews-item-header').renderContents()).split('\n')]
                tag = review.find('ul',attrs={'class':'unstyled pd-review-ratings'})
                value=tag.findAll('li')
                for v in value:
                    rating_tag=stripHtml(v.renderContents()).split()
                    page['ef_rating_display'] = rating_tag[0]
                    page['ef_rating_reception_&_call_quality'] = rating_tag[0]
                    page['ef_rating_battery_life'] = rating_tag[0]
                    page['ef_rating_design_form_factor'] = rating_tag[0]
                    page['ef_rating_apps_&_media_support'] = rating_tag[0]
                    
            except:
                log.info(self.log_msg("rating Info cannot be fetched"))

            try:
               # match_obj = re.search('^(.*?) out of 5 By:(.*?) on (.+)', rating_info[1])
               # page['ef_rating_overall'] = float(match_obj.group(1).strip())
                rating_tag=review.find('div','badge pd-review-score')
                page['ef_rating_overall'] = stripHtml(rating_tag.renderContents()).replace('Overall','')
               # page['ef_rating_overall'] = rating_tag.renderContents().replace('Overall','')
              #  log.info(self.log_msg("rating:%s"%page['ef_rating_overall']))
                             
            except:
                log.info(self.log_msg("Overall rating not found"))
                
            try:
               # page['et_author_name'] = match_obj.group(2).strip()
                author_tag=review.find('h6')
                page['et_author_name'] = stripHtml(author_tag.renderContents())
               # page['et_author_name'] = author_tag.renderContents()
               # log.info(self.log_msg("author:%s"%page['et_author_name']))
               
            except:
                log.info(self.log_msg("Author name cannot be fetched"))
                
            try:
                date_tag=review.find('div','pull-right').renderContents().strip()+ ' ' + str(datetime.now().year)                 
                page['posted_date'] = date_tag 
               # log.info(self.log_msg("posted_date:%s"%page['posted_date']))
                
                #page['posted_date'] = datetime.strftime(datetime.strptime( \
                #    match_obj.group(3).strip(),'%A, %B %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
            except:
               # page['posted_date'] = datetime.strftime(datetime.utcnow(),\
               #                                         "%Y-%m-%dT%H:%M:%SZ")
                log.info(self.log_msg("Posted date cannot be fetched "))
                
            try:
                data_link = review.find('a',attrs={'title':'Read the full review'}) 
               # log.info(self.log_msg("link: %s"%data_link))
                if data_link:
                    self.currenturi = 'http://www.phonedog.com' + data_link['href']
                    #self.currenturi = 'http://www.phonedog.com/products/htc-one-silver-32gb/user-reviews/12023/'
                  #  log.info(self.log_msg("current_uri: %s"%self.currenturi))
                    self.__setSoup()
                    try:
                        unwanted_tag = self.soup.find('div','pd-comment-txt clearfix').find('h4').extract()
                   #     log.info(self.log_msg("head: %s"%unwanted_tag))
                    except:
                        log.info(self.log_msg("cannot extract no heading found"))
                    try:
                        unwanted_tag = self.soup.find('div','pd-comment-txt clearfix').find('div','badge pd-review-score').extract()
                  #      log.info(self.log_msg("rating: %s"%unwanted_tag))
                    except:
                        log.info(self.log_msg("cannot extract no ratings found"))
                    data_tag = stripHtml(self.soup.find('div','pd-comment-txt clearfix').renderContents())
                    #log.info(self.log_msg("data: %s"%data_tag))
                    page['data'] = data_tag
                #    log.info(self.log_msg("Data:%s"%page['data']))
                else:
                    try:
                        data_tag = review.find('div','pd-comment-txt clearfix').find('h4').extract()
                    except:
                        log.info(self.log_msg("cannot extract no heading found"))
                    try:
                        data_tag = review.find('div','pd-comment-txt clearfix').find('div','badge pd-review-score').extract()
                    except:
                        log.info(self.log_msg("cannot extract no ratings found"))
                    data_tag = stripHtml(review.find('div','pd-comment-txt clearfix').renderContents())
                    page['data'] = data_tag
                 #   log.info(self.log_msg("Data:%s"%page['data']))
            except:  
                   log.info(self.log_msg("data cannot be fetched"))
                   
            try:
                 tag=review.find('div',attrs={'class':'pd-comment-actions clearfix'})
                 list_tag = tag.findAll('li')
                 for l in list_tag:
                     data_helpful = l.renderContents().split()[0]
                 page['ei_data_helpful_count'] = data_helpful
               #  log.info(self.log_msg("count:%s"%page['ei_data_helpful_count']))
            except:
                 log.info(self.log_msg("data helpful cannot be fetched"))
                 
                 
#==============================================================================
#             try:
#                 page['et_author_location'] = re.sub('^From:','',rating_info[2])\
#                                                                         .strip()
#             except:
#                 log.info(self.log_msg("Author Location cannot be fetched"))
#                 
#             try:
#                 page['et_author_experience_with_product'] = re.sub('Experience:',\
#                                                     '',rating_info[3]).strip()
#             except:
#                 log.info(self.log_msg("Author's product exp cannot be fetched"))
#                 
#             try:
#                 page['ei_data_helpful_count'] = int(re.search('^\d+', stripHtml\
#                     (review.find('div','reviews-item-voting').renderContents()))\
#                                                                         .group()) 
#             except:
#                 log.info(self.log_msg("data helpful cannot be fetched"))
#                 
#             try:
#                 data_tag = review.find('div', 'reviews-item-body')
#                 for each in ['Pros:', 'Cons:', 'Summary:']:
#                     if not each=='Summary:':
#                         key = 'et_data_' + each[:-1].lower()
#                     else:
#                         key = 'data'
#                     tag = data_tag.find('span', text=each)
#                     if tag:qqqqqq
#                         tag_str = stripHtml(tag.next.__str__())                    
#                         if tag_str:
#                             page[key] = tag_str
#                 if not page.get('data'):
#                     page['data'] = page.get('et_data_pros','') + '\n' + page.get('et_data_cons','')
#             except:
#                 log.info(self.log_msg("data not found"))
#                 page['data']=''
#                 
#             if not page['title'] and not page['data']:
#                 log.info(self.log_msg("Data and title not found for %s,"\
#==============================================================================
            #                        " discarding this review"%(page['uri'])))
                                    
            try:
                unique_key = get_hash({'data' : page['data'],'title' : page['title']})
                #if checkSessionInfo(self.genre, self.session_info_out, page['uri'],\
                 #            self.task.instance_data.get('update'),parent_list\
                  #                          =[ self.task.instance_data['uri'] ]):
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                            self.task.instance_data.get('update'),parent_list\
                                            =[ self.task.instance_data['uri'] ]):
                    log.info(self.log_msg('session info return True'))
                    return False
                result = updateSessionInfo(self.genre, self.session_info_out, unique_key,\
                    get_hash(page),'Review', self.task.instance_data.get('update'),\
                                    parent_list=[self.task.instance_data['uri']])
                if not result['updated']:
                    log.info(self.log_msg('result not updated'))
                    continue
                page['path'] = page['parent_path'] = [ self.task.instance_data['uri'] ]
                page['path'].append( page['uri'] )
                page['priority'] = self.task.priority
                page['level'] = self.task.level
                page['last_updated_time'] = page['pickup_date'] = datetime.\
                                strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                page['client_name'] = self.task.client_name
                page['versioned'] = False
                page['entity'] = 'Review'
                page['category'] = self.task.instance_data.get('category','')
                page['task_log_id'] = self.task.id
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append(page)
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))
                

    @logit(log,'__setSoup')
    def __setSoup(self, url=None, data=None, headers={}):
        """
            It will take the URL and change it to self.currenturi and set soup,
            if url is mentioned. If url is not given it will take the 
            self.current uri
        """
        if url:
            self.currenturi = url
        res = self._getHTML(data=data, headers=headers)
        if res:
            self.rawpage = res['result']
        else:
            log.info(self.log_msg('HTML Content cannot be fetched for the url: \
                                                            %s'%self.currenturi))
            return False
        self._setCurrentPage()
        return True

    
        
                                                
                    
                
                
    
    
        