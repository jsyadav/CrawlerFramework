
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''


#ASHISH YADAV
#JV
#prerna
#data recommended and pagination bug fixed


import re 
import md5
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import logging
import pickle
import time
from urllib2 import urlparse,unquote
from cgi import parse_qsl

from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash

from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('AmazonConnector')
class AmazonConnector(BaseConnector):
    '''
    Presently amazon gives the option to update comments ,so check each comment for updates ,
    while comments have a permalink , and can be idetified using that
    '''

    @logit(log,'_createSiteUrl')
    def _createSiteUrl(self,code=None):
        ''' will be of this form , which will give reviews on 
        http://www.amazon.com/review/product/B001418WF4/?_encoding=UTF8&sortBy=bySubmissionDateDescending 
        in order of newest posts first order
        '''
        if not code:
        #testing code
            code = 'B00142Q51G'
        #
        log.info(self.log_msg('seedurl :%s'%('http://www.amazon.com/review/product/' + code + '/?_encoding=UTF8&sortBy=bySubmissionDateDescending')))
        return 'http://www.amazon.com/review/product/' + code + '/?_encoding=UTF8&sortBy=bySubmissionDateDescending'

    @logit(log , 'fetch')
    def fetch(self):
        print "in fetch"
        self.genre="Review"
        try:
            #for sentiment extraction
            code = None
            parent_uri = self.currenturi
            if self.currenturi:
                print "in if"
                try:
                    code = re.search(re.compile(r'^http://www.amazon.com.*?/review/product/([^/]*)/.*$'),unquote(self.currenturi)).group(1)
                except:
                    log.info(self.log_msg("couldn't parse this url , doing 2nd pass"))
                try:
                    if not code:
                        code = re.search(re.compile(r'^http://www.amazon.com.*?/product-reviews/([^/]*)/.*$'),
                                     unquote(self.currenturi)).group(1)
                except:
                    log.exception(self.log_msg('could not parse this url , returning false'))
                    return False
                if not code:
                    self.task.status['fetch_status']=False
                    return False
            self.currenturi = self._createSiteUrl(code)
            # for sentiment extraction
            res=self._getHTML()
            review_next_page_list = []
            self.reviews_list=[]
            self.rawpage=res['result']
            self._setCurrentPage()
            self._getParentPage(parent_uri) #checks for parent page ,and appends a empty dictionay or a modified post.
            c = 0
            print "before while loop"
            while True:
                try:
                    next_page = self.soup.find('a',{'href':True}, text='Next &rsaquo;')
                    if self.addreviews(parent_uri) and next_page: #check if there's next_page  and we haven't reached the last crawled review
                        if next_page.parent['href'] not in review_next_page_list:
                            review_next_page_list.append(next_page.parent['href'])
                            self.currenturi = next_page.parent['href']
                            log.debug(self.log_msg("Fetching the review url %s" %(self.currenturi)))
                            page_fetched =True
                            num_of_tries = 0
                            while num_of_tries<=3:
                                try:
                                    res=self._getHTML()
                                    self.rawpage=res['result']
                                    self._setCurrentPage()
                                    log.info('now in next page')
                                    break
                                except:
                                    num_of_tries +=1
                                    time.sleep(30)
                                    if num_of_tries>3:
                                        page_fetched = False
                                        break
                                    continue
#                            c +=1
 #                           if c>20:
  
#                              break
                        else:
                            log.critical(self.log_msg("DUPLICATE REVIEW LINK %s, POTENTIAL INFINITE LOOP" %(next_page.parent['href'])))
                            break
                    else:
                        log.info(self.log_msg('Reached last page of reviews'))
                        break
                except Exception, e:
                    log.exception(self.log_msg('exception in iterating pages in fetch'))
            self.task.status['fetch_status']=True
            print "end fetch"
            return True
        except:
            import traceback
            print traceback.format_exc()
            self.task.status['fetch_status']=False
            log.exception(self.log_msg('Exception in fetch'))
            return False
    
    #addReviews
    @logit(log , 'addreviews')
    def addreviews(self, parenturi):
        try:
            reviews = self.soup.find('table', id = 'productReviews').find('td').\
                    findAll('div',recursive = False)
            log.info(self.log_msg('no. of reviews found on page %d'%(len(reviews[:]))))        
        except:
            log.exception(self.log_msg('no reviews found'))
            return False
        #for review in reviews[1:]:#why?? JV
        for review in reviews:
            try:
                page ={}
                url = review.find('a', text='Permalink')
                if not url:
                    log.info(self.log_msg('could not get permalink for this review , continuing from the next reviews'))
                    continue 
                #url = normalize([link for link in review.findAll('a') if link.string == 'Permalink'][0]['href'])
                url = re.sub('ref=.*$','',normalize(url.parent['href']))
                ##ODD JV
                #for cases in which review has been deleted , no permalink is present hence we skip for that review,same applies for comments
                page['uri'] = normalize(url)
                if page['uri'] in self.reviews_list:
                    log.info(self.log_msg('%s is already processed link'%page['uri']))
                    continue
                if (not checkSessionInfo(self.genre, self.session_info_out, 
                                        page['uri'], self.task.instance_data.get('update'),
                                        parent_list=[parenturi]) ) or self.task.instance_data.get('pick_comments'):
                
                    levels = review.findAll('div',{'style':'margin-bottom:0.5em;'})

                    try:
                        data = BeautifulSoup(str(review))
                        [junk.extract() for junk in data.findAll('div', {'style':True})[1:]]
                    except:
                        log.exception(self.log_msg("junk data couldn't be removed from review"))##ODD JV
                    try:
                        page['data'] = stripHtml(data.renderContents())
                    except:
                        page['data'] = ''
                        log.info(self.log_msg("review data couldn't be extracted"))
                    try:
                        page['title'] = stripHtml(review.b.renderContents())
                    except:
                        page['title'] = ''
                        log.info(self.log_msg("review title couldn't be extracted"))

                    try:
                        ##state the case - copy and paste the text from page JV
                        #page['et_author_name'] =  stripHtml(review.find('td' ,{'valign':'top'}).parent.find('a').renderContents())
                        page['et_author_name'] = stripHtml(review.find('span' ,{'style':'font-weight: bold;'}).\
                                                    renderContents()).split('"')[0]
                        if page['et_author_name'] and  page['et_author_name'].startswith('Help other customers'):
                            del(page['et_author_name'])

                    except:
                        log.info(self.log_msg("review author name couldn't be extracted"))
                    try:
                        auth_loc_tag = stripHtml(review.find('span' ,{'style':'font-weight: bold;'}).\
                                        findParent('div').renderContents()).split('-')[0]
                        auth_location = auth_loc_tag.split('(')
                        if len(auth_location) == 2:
                            page['et_author_location'] = auth_location[-1].replace(')','')
                        else:
                            log.info('author location not found')    
                    except:
                        log.exception(self.log_msg('author location not found for this page'))        
                    try:
                        page['ef_rating_overall']=float(self.soup.find('span',attrs = {'class':re.compile('swSprite s_star_\d+_\d+')})['title'].replace(' out of 5 stars',''))
                    except:
                        log.info(self.log_msg('overall rating could not be parsed'))
                    try:
                        log.info('now fetching data recommended')
                        helpful_comment=re.search(re.compile(r'([0-9,]*) of ([0-9,]*) people found the following review helpful:')\
                                                                                             ,levels[0].renderContents().strip())
                        if helpful_comment:
                            page['ei_data_recommended_yes'] = int(helpful_comment.group(1).replace(',',''))
                            page['ei_data_recommended_total'] = int(helpful_comment.group(2).replace(',',''))
                            log.info(self.log_msg('data_re:%s %s'%(page['ei_data_recommended_yes'], page['ei_data_recommended_total'])))
                    except:
                            log.info(self.log_msg('review helpfulness could not be parsed'))
                    try:
                        review_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                                                page.values()))).encode('utf-8','ignore')).hexdigest()
                    except:
                        log.exception(self.log_msg("exception in buidling review_hash , moving onto next comment"))
                        continue
                    parent_list=[parenturi]
                    result=updateSessionInfo(self.genre, self.session_info_out, page['uri'], review_hash, 
                                             'Review', self.task.instance_data.get('update'), parent_list=parent_list)
                    if result['updated']:
                        page['parent_path'] = parent_list[:]
                        parent_list.append(page['uri'])
                        page['path'] = parent_list
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id  # TODO: Get the client from the project 
                        page['client_name'] = self.task.client_name
                        page['last_updated_time'] = page['pickup_date']
                        page['versioned'] = False
                        page['entity'] = 'Review'
                        page['category'] = self.task.instance_data.get('category','')
                        try:
                            page['posted_date'] = datetime.strftime(datetime.strptime(review.nobr.renderContents(),'%B %d, %Y')\
                                                                        ,"%Y-%m-%dT%H:%M:%SZ")
                        except:
                            log.info(self.log_msg("couldn't parse posted_date"))
                            page['posted_date'] = page['pickup_date']

                        page['task_log_id']=self.task.id

                        page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                        self.pages.append(page)
                        log.info('Page added')
                        self.reviews_list.append(page['uri'])
                else:
                    log.info(self.log_msg('reached already parsed review so returning'))
                    return False
                try:
                    comment_url = filter(lambda x: x.renderContents().startswith('Comments ') , review.findAll('a',href=True))
                    if self.task.instance_data.get('pick_comments') and comment_url and re.search('Comments \([0-9]+\)' ,
                                                                                                  comment_url[0].renderContents()):
                        parent_soup = self.soup
                        self.add_comments(comment_url[0]['href'], [parenturi, page['uri']])##added page['uri'] - JV
                        self.soup = parent_soup#need to change this?? - JV
                    else:
                        log.info(self.log_msg('comments link could not be found / No comments found for this review'))
                except:
                    log.info(self.log_msg("exception in calling add_comments function"))
            except:
                log.exception(self.log_msg("exception in addreviews"))
                continue
        return True

    @logit(log , 'add_comments')
    def add_comments(self,comment_url, parent_list):
        ''' params : permalink of the review page , 
            extracts comments from the page given comment link
        '''    
        self.currenturi = comment_url
        while True:
            res=self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            comments = self.soup.findAll('div',{'class':'postBody'})
            log.debug('number of comments found: %d' % len(comments))
            i=0
            for comment in comments:
                i=i+1
                log.debug('going for comment: %d' % i)
                try:
                    page = {}
                    try:
                        page['data'] =  stripHtml(comment.find('div',{'class':'postContent','style':'display: block;'}).renderContents())
                    except:
                        log.info(self.log_msg('could not extract comment data'))#??review?? copy paste??
                        page['data'] = ''
                    #for cases in which comment has been deleted , no permalink is present so we skip that comment    
                    try:
                        uri = comment.find('a',href=True,text='Permalink').parent['href']
                    except:
                        log.info(self.log_msg('could not extract comment uri , so continue from the next comment'))
                        continue

                    if not checkSessionInfo(self.genre, self.session_info_out, 
                                            uri, self.task.instance_data.get('update'),
                                            parent_list=parent_list):
                        try:
                            page['et_author_name'] = stripHtml(comment.find('div',{'class':'postFrom'}).a.renderContents())
                        except:
                            log.info(self.log_msg('could not parse author name'))

                        hash = md5.md5(''.join(sorted(page.values())).encode('utf-8','ignore')).hexdigest()
                        ##CUT##
                        result=updateSessionInfo(self.genre, self.session_info_out, uri, hash, 
                                                 'Comment', self.task.instance_data.get('update'), 
                                                 parent_list=parent_list)
                        if result['updated']:
                            page['parent_path'] = parent_list[:]
                            parent_list.append(uri)
                            page['path'] = parent_list
                            page['priority']=self.task.priority
                            page['level']=self.task.level
                            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                            page['last_updated_time'] = page['pickup_date']
                            page['connector_instance_log_id'] = self.task.connector_instance_log_id
                            page['connector_instance_id'] = self.task.connector_instance_id
                            page['workspace_id'] = self.task.workspace_id
                            page['client_id'] = self.task.client_id  # TODO: Get the client from the project 
                            page['client_name'] = self.task.client_name
                            page['versioned'] = False
                            page['uri'] = uri
                            page['uri_domain'] = urlparse.urlparse(uri)[1]
                            page['task_log_id']=self.task.id
                            page['entity'] = 'Comment'
                            page['category'] = self.task.instance_data.get('category' ,'')
                            try:
                                posted_date = re.sub('.*\n' , '',stripHtml(comment.find('div',{'class':'postHeader'}).renderContents())).strip()
                                try:
                                    posted_date = datetime.strptime(' '.join(re.split('\s+|\n+',posted_date)[-6:-1]),'%B %d, %Y %I:%M %p')
                                except:
                                    date_str = ' '.join(re.split('\s+|\n+',posted_date)[-6:-1])
                                    month = date_str.split('.')[0][:3]
                                    date_str = date_str.replace(date_str.split(' ')[0],month)
                                    log.info(date_str)
                                    posted_date = datetime.strptime(date_str,'%b %d, %Y %I:%M %p')
                                page['posted_date'] = datetime.strftime(posted_date,"%Y-%m-%dT%H:%M:%SZ")
                            except:
                                log.exception(self.log_msg('could not parse post date'))
                                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")

                            try:
                                if len(page['data']) > 50: #title is set to first 50 characters or the post whichever is less
                                    page['title'] = page['data'][:50] + '...'
                                else:
                                    page['title'] = page['data']
                            except:
                                log.info(self.log_msg('could not parse title'))
                            self.pages.append(page)
                    else:
                        return
                except:
                    log.exception(self.log_msg('Exception in add_comments'))
                    continue
            comment_next_page_list=[]
            next = self.soup.find('a',href=True , text='Next &#8250;')
            if next:
                if next.parent['href'] not in comment_next_page_list:
                    log.info(self.log_msg('moving to next page in comments'))
                    self.currenturi = next.parent['href']
                    comment_next_page_list.append(next.parent['href'])
                else:
                    log.critical(self.log_msg("DUPLICATE COMMENT NEXT LINK %s FOUND, POTENTIAL INFINITE LOOP" %next.parent['href']))
                    break
            else:
                log.info(self.log_msg('reached last page in comments'))
                break


    @logit(log , '_getParentPage')
    def _getParentPage(self,parent_uri):#NAMING CONVENTION IS WRONG
            ##J- I think these needs to be in a try except- if th title fails or rating fails - coz the html changed---what crash?
            ## a try-except-raise
            try:
                page={}
                try:
                    page['title'] = stripHtml(self.soup.find('div',{'style':'font-size:80%;text-decoration:underline;'}).a.renderContents())
                except Exception, e:
                    try:
                        page['title'] = stripHtml(self.soup.find('h1').findParent('td').span.renderContents())
                    except:
                        log.exception(self.log_msg('could not parse page title'))
                        raise e
                try:
                    page['ef_product_rating_overall'] =float(self.soup.find('span',attrs = {'class':re.compile('swSprite s_star_\d+_\d+')})['title'].replace(' out of 5 stars',''))
                except:
                    log.exception(self.log_msg('could not parse rating overall'))    
                try:
                    page['et_product_price'] = stripHtml(self.soup.find('span',{'class':'price'}).renderContents())
                except:
                    log.info(self.log_msg('could not parse product price'))
                try:
                    post_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x ,\
                                                               page.values()))).encode('utf-8','ignore')).hexdigest()
                except Exception,e:
                    log.exception(self.log_msg('could not build post_hash'))
                    raise e
                log.debug(self.log_msg('checking session info'))

                #continue if returned true
                try:
                    self.updateParentExtractedEntities(page) #update parent extracted entities
                except:
                    log.info(self.log_msg('Cannot update extracted Entities'))
                if not checkSessionInfo(self.genre, self.session_info_out, 
                                        parent_uri, self.task.instance_data.get('update')):
                    id=None
                    if self.session_info_out=={}:
                        id=self.task.id
                    result=updateSessionInfo(self.genre, self.session_info_out,parent_uri, post_hash, 
                                             'Post', self.task.instance_data.get('update'), Id=id)
                    if result['updated']:
                        page['uri'] = normalize(self.currenturi)
                        page['path'] = [parent_uri]
                        page['parent_path'] = []
                        page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
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
                        page['data'] = ''
                        page['task_log_id']=self.task.id
                        page['entity'] = 'Post'
                        page['category']=self.task.instance_data.get('category','')                        
                        self.pages.append(page)
            except Exception,e:
                log.exception(self.log_msg("parent post couldn't be parsed"))
                raise e

#amazonconnector right now allows editing reviews/comments and the connector is picking up updates if found for those comments , if updates id enables , otherwise it falls back to regular behaviour.

