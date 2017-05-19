
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#ASHISH YADAV  

import re
import datetime
import md5
from urllib2 import urlparse,unquote
import urllib
import logging
from BeautifulSoup import BeautifulSoup
import pickle
import copy

from baseconnector import BaseConnector
from utils.utils import stripHtml
from utils.urlnorm import normalize
from utils.decorators import logit

from tgimport import *
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('MouthshutConnector')
from utils.decorators import logit

class MouthshutConnector(BaseConnector):
    @logit(log,'_createSiteUrl')
    def _createSiteUrl(self,code):
        try:
            return 'http://www.mouthshut.com/product-reviews/' + code + '-sort-MsDate-order-d.html'
        except Exception,e:
            log.exception(self.log_msg('Exception occured while creating URL'))
            raise e

    @logit(log,'fetch')
    def fetch(self):
        try:
            # TESTING SENTIMENT EXTRACTION 
            self.genre='Review'
            parent_uri = self.currenturi
            if self.currenturi:
                code = re.search(re.compile(r'http://www.mouthshut.com/product-reviews/(.*?)(?!-sort-.*).html'),self.currenturi).group(1)
                if not code:
                    return False ## will cj
            self.currenturi = self._createSiteUrl(code)
            #SENTI TESTING - self.currenturi is already the task.uri                                                                      
            log.debug(self.log_msg(':seed url :: %s'%(self.currenturi)))
            if not self.rawpage:
                res=self._getHTML()
                self.rawpage=res['result']
            self._setCurrentPage() 
            self._getparentpage(parent_uri) # to get the parent page new/updated
            reviews = []
            page_number = 1
            while True:
                parent_soup = copy.copy(self.soup)
                self.addreviews(parent_uri)
                try:
                    next_page = parent_soup.find('a',{'class':'Next','rel':'nofollow'})
                    if next_page :
                        page_number +=1
    #self.currenturi = 'http://www.mouthshut.com/product-reviews/' + str(code) +'-sort-MsDate-order-d-page-'+str(page_number)+'.html'
                        self.currenturi = 'http://www.mouthshut.com/product-reviews/' + str(code) +'-page-'+str(page_number)+'.html'
                        res=self._getHTML()
                        self.rawpage=res['result']
                        self._setCurrentPage()
                    else:
                        log.debug(self.log_msg('no next page found , returning'))
                        break
                except Exception,e:
                    log.info(self.log_msg('exception in page iteration loop in fetch'))
                    raise e
            return True
        except:
            log.exception(self.log_msg('exception in fetch'))
            return False

    @logit(log,'getparentpage')
    def _getparentpage(self,parent_uri):
        try:
            page= {}
            try:
                page['title'] = re.sub(r'Reviews$','',stripHtml(self.soup.find('div',{'class':'product_heading'}).renderContents())).strip()
            except Exception,e:
                log.exception(self.log_msg('could not parse page title'))
                raise e

            try:
                overall_rating = self.soup.find('span',{'id':re.compile('.*?_lblProductRating')})
                page['ef_product_rating_overall'] = float(len(filter(lambda img:img['src'].endswith('_full.gif'),overall_rating.findAll('img'))))
            except:
                log.info(self.log_msg('could not parse overall rating'))

            try:
                page['ei_data_recommended_yes'] = int(self.soup.find('span',{'id':re.compile('.*_lblRecommendation$')}).renderContents().replace('%',''))
                ratings = self.soup.find('table',{'class':'font','cellspacing':'1','cellpadding':'1','border':'0'}).findAll('tr')
                for rating in ratings:
                    try:
                        rating_feature =  stripHtml(rating.renderContents()).replace(':', '')
                        rating_value = str(len(filter(lambda img:img['src'].endswith('_full.gif'),rating.findAll('img'))))
                        page['ef_product_rating_' + str(rating_feature)] = float(rating_value)
                    except:
                        log.info(self.log_msg('exception in parsing review ratings'))
                    continue
            except:
                log.info(self.log_msg('could not parse review ratings'))

            try:
                post_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                           page.values()))).encode('utf-8','ignore')).hexdigest()
            except Exception,e:
                log.info(self.log_msg('could not build post_hash , so returning'))
                raise e

                #continue if returned true
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    parent_uri, self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out,parent_uri, post_hash, 
                                         'Post', self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    page['path'] = [parent_uri]
                    page['parent_path'] = []
                    page['uri'] = normalize(self.currenturi)
                    page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.datetime.strftime(datetime.datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['posted_date'] = datetime.datetime.strftime(datetime.datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['last_updated_time'] = page['pickup_date']
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project       
                    page['client_name'] = self.task.client_name
                    page['data'] = ''
                    page['task_log_id']=self.task.id
                    page['category'] = self.task.instance_data.get('category','')
                    page['entity'] = 'post'
                    page['versioned'] = False
                    self.pages.append(page)
                else:
                    log.info(self.log_msg('parent page is not updated'))
            else:
                log.info(self.log_msg('parent page is not added'))

        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            raise e


    @logit(log,'addreviews')
    def addreviews(self,parenturi):
        review_links = self.soup.findAll('a' , href=True , text='Read complete review')
        log.info(self.log_msg('reviews found on page = %d'%(len(review_links))))
        for review in review_links:
            try:
                review_link = review.parent['href']
                self.currenturi = normalize(review_link)
                if not self.currenturi:
                    log.info(self.log_msg('could not get permalink for this review , continuing from the next reviews'))
                    continue
                if not checkSessionInfo(self.genre, self.session_info_out, 
                                        review_link, self.task.instance_data.get('update'),
                                        parent_list=[parenturi]):
                
                    res=self._getHTML()
                    self.rawpage=res['result']
                    self._setCurrentPage()
                    page={}
                    try:
                        page['title'] =  stripHtml(self.soup.find('div',{'class':'heading_orange'}).renderContents())
                    except:
                        page['title'] = ''
                        log.info(self.log_msg('exception in extracting page title'))

                    try:
                        page['et_author_name'] = stripHtml(self.soup.find('div',{'id':'right_container'}).h4.a.renderContents())
                    except:
                        log.info(self.log_msg('exception in parsing author_name'))
                    #rating ... per feature 
                    try:
                        ratings = self.soup.find('div',{'class':'featuredrating'}).findAll('tr')
                        overall_rating = self.soup.find('div',{'id':'ctl00_ctl00_ctl00_ContentPlaceHolderHeader_ContentPlaceHolderFooter_ContentPlaceHolderBody_divMemRating'})
                        page['ef_rating_overall'] = float(len(filter(lambda img:img['src'].endswith('_full.gif'),overall_rating.findAll('img'))))
                        for rating in ratings:
                            try:
                                rating_feature =  stripHtml(rating.renderContents()).replace(':', '')
                                rating_value = float(len(filter(lambda img:img['src'].endswith('_full.gif'),rating.findAll('img'))))
                                page['ef_rating_' + str(rating_feature)] = rating_value
                            except:
                                log.info(self.log_msg('exception in parsing review ratings'))
                                continue
                    except:
                        log.info(self.log_msg('exception in parsing review ratings'))
                   #pros , cons from the review
                    try:
                        summary = re.search(re.compile(r'^Pros:(.*?)Cons:(.*?)$',re.DOTALL),stripHtml\
                                                (self.soup.find('div',{'class':'questions_proscons'}).parent.renderContents()))
                        if summary:
                            page['et_data_pros'] =  summary.group(1).strip()
                            page['et_data_cons'] =  summary.group(2).strip()
                    except:
                        log.info(self.log_msg('exception in parsing review pros/cons'))
                    try:
                        page['data']=stripHtml(BeautifulSoup(self.soup.prettify()).find('div',{'class':'review'}).renderContents())
                    except:
                        log.info(self.log_msg('exception in extracting review content'))
                        page['data'] = ''

                    try:
                        review_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                                     page.values()))).encode('utf-8','ignore')).hexdigest()
                    except:
                        log.exception(self.log_msg("exception in buidling review_hash , moving onto next comment"))
                        continue

                    result=updateSessionInfo(self.genre, self.session_info_out,review_link, review_hash, 
                                             'Review', self.task.instance_data.get('update'), parent_list=[parenturi])
                    if result['updated']:
                        try:
                            date = stripHtml(self.soup.find('div',{'class':'heading_orange'}).parent.find('span',{'class':'font'}).\
                                                 renderContents()).split('|')[-1].strip()
                            page['posted_date'] = datetime.datetime.strftime(datetime.datetime.strptime(date,"%b %d, %Y %H:%M %p")\
                                                                                 ,"%Y-%m-%dT%H:%M:%SZ")
                        except:
                            log.info(self.log_msg('exception in parsing posted_date'))

                        page['path'] = page['parent_path'] = [parenturi]
                        page['parent_path'].append(review_link)
                        if not page.get('posted_date') and date:
                            try:
                                date_expr = re.search(re.compile(r'([0-9]* hrs )?([0-9]* mins )?.*?') , date)
                                if date_expr.group(1):
                                    hrs = int(date_expr.group(1).replace(' hrs ',''))
                                else:
                                    hrs = 0
                                if date_expr.group(2):
                                    mins = int(date_expr.group(2).replace(' mins ',''))
                                else:
                                    mins = 0
                                secs = hrs*3600 + mins*60
                                date = datetime.datetime.utcnow() - datetime.timedelta(seconds = secs)
                                page['posted_date']=datetime.datetime.strftime(date ,"%Y-%m-%dT%H:%M:%SZ")
                            except:
                                 log.info(self.log_msg('exception in parsing posted_date'))

                            if not page.get('posted_date'):
                                page['posted_date'] = datetime.datetime.strftime(datetime.datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")

                        page['pickup_date'] = datetime.datetime.strftime(datetime.datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['last_updated_time'] = page['pickup_date']
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id  # TODO: Get the client from the projet
                        page['client_name'] = self.task.client_name
                        page['task_log_id']=self.task.id
                        page['category'] = self.task.instance_data.get('category','')
                        page['entity'] = 'review'
                        page['uri'] = review_link
                        page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                        page['versioned'] = False
                        if self.task.instance_data.get('pick_user_info'):     #get user extended info if pick_user_info present,
                            parent_soup = copy.copy(self.soup)                           #false by default
                            try:
                                profile_url = 'http://www.mouthshut.com'+self.soup.find('a',text='...view complete profile').\
                                                                                                              parent['href']
                                self.getuserinfo(page,profile_url)
                            except:
                                log.info(self.log_msg('exception in calling user_info'))
                            self.soup = parent_soup
                        self.pages.append(page)
                try:
                    comment_url = filter(lambda x: re.match('Comments \([1-9]+\)' ,x.renderContents().strip()),\
                                             review.parent.parent.findAll('a',href=True))
                    if self.task.instance_data.get('pick_comments') and comment_url:
                        parent_soup = self.soup
                        self.add_comments(comment_url[0]['href'],parent_list=[parenturi,review_link])#calling add_comments function for this review      
                        self.soup = parent_soup
                    else:
                        log.info(self.log_msg('comments link could not be found / No comments found for this review'))
                except:
                    log.exception(self.log_msg("exception in calling add_comments function"))
            
            except:
                log.exception(self.log_msg('exception in extracting review'))
                continue
        return True

    @logit(log , 'add_comments')
    def add_comments(self,comment_url,parent_list):
        #as of now epinions keeps a maximum of 30 comments, and on a single page.
#        self.currenturi = comment_url
        pattern_to_replace = re.compile('[\x7F-\xFF]+')
        res=self._getHTML(comment_url)
        self.rawpage=res['result']
        self._setCurrentPage()
        log.info(self.log_msg('comment_url :: %s'%(comment_url)))
        comments = self.soup.findAll('tr',{'id':True,'class':re.compile('comment[0-9]+') , 'valign':'top'})
        log.info(self.log_msg('no. of comments found :: ' + str(len(comments))))
        for comment in comments:
            try:
                page={}
                data =  BeautifulSoup(str(comment))
                try:
                    [junk.extract() for junk in data.findAll('a')]
                    [junk.extract() for junk in data.findAll('span')]
                    data = re.sub('^said:' ,'', stripHtml(data.renderContents())).strip()
                except:
                    log.info(self.log_msg('comment data could not be extracted'))
                    data = ''
                try:
                    if len(data) > 50: #title is set to first 50 characters or the post whichever is less                      
                        title = data[:50] + '...'
                    else:
                        title = data
                except:
                    log.exception(self.log_msg('could not parse title'))
                if not checkSessionInfo(self.genre, self.session_info_out, 
                                        re.sub(pattern_to_replace,'',title), self.task.instance_data.get('update'),
                                        parent_list=parent_list):
                    try:
                        author_name = filter(lambda x: stripHtml(x.renderContents()),comment.findAll('a',{'href':re.compile('/user/.*'),\
                                                                                                 'id':True}))[-1].renderContents()
                    except:
                        log.info(self.log_msg('comment author_name could not be extracted'))
                        author_name = ''
                    try:
                        posted_date =  comment.find('span',{'id':re.compile('.*lblmsdate$')}).renderContents()
                        page['posted_date']= datetime.datetime.strftime(datetime.datetime.strptime(posted_date,'%b %d, %Y %H:%M %p') , 
                                                                        "%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg('comment posted_date could not be extracted'))
                        page['posted_date'] = datetime.datetime.strftime(datetime.datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    hash_data = data + author_name 
                    comment_hash = md5.md5(hash_data.encode('utf-8','ignore')).hexdigest()
                    result=updateSessionInfo(self.genre, self.session_info_out,re.sub(pattern_to_replace,'',title), comment_hash, 
                                             'Comment', self.task.instance_data.get('update'), parent_list=parent_list)
                    if result['updated']:
                        page['data'] = data
                        if author_name:
                            page['et_author_name'] = author_name
                        page['path']=page['parent_path'] = parent_list
                        page['path'].append(re.sub(pattern_to_replace,'',title))
                        page['title'] = title
                        page['uri'] = self.currenturi
                        page['pickup_date'] = datetime.datetime.strftime(datetime.datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['last_updated_time'] = page['pickup_date']
                        page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id  # TODO: Get the client from the project                             
                        page['client_name'] = self.task.client_name
                        page['category'] = self.task.instance_data.get('category','')
                        page['entity'] = 'Comment'
                        page['task_log_id']=self.task.id
                        page['versioned'] = False
                        self.pages.append(page)
                else:
                    log.info(self.log_msg('found previously fetch comment so continue'))
            except:
                log.exception(self.log_msg('exception in add_comments'))
                continue

    @logit(log,'getuserinfo')
    def getuserinfo(self,page,profile_url):
        try:
#            self.currenturi = profile_url
            res=self._getHTML(profile_url)
            self.rawpage=res['result']
            self._setCurrentPage()
            page['et_author_profile'] = profile_url
            try:
                page['et_author_name'] = re.search('.*/(.*?).html' , profile_url).group(1)
            except:
                log.info("couldn't parse author profile name")
            try:
                page['et_author_real_name'] = stripHtml(self.soup.find('tr',{'id':re.compile('.*_trUsrName')}).findAll('td')[-1]\
                                                                                                             .renderContents())
            except:
                log.info("couldn't parse author name")
                
            try:
                author_age_info = stripHtml(self.soup.find('tr',{'id':re.compile('.*_trAgeProfile')}).findAll('td')[-1].renderContents())
                page['ei_author_age'] =  int(re.search('([0-9]+).*',author_age_info).group(1))
                page['et_author_gender'] = author_age_info.split(' ')[-1]
            except:
                log.info("couldn't parse author age/gender info")

            try:
                page['et_author_city'] = stripHtml(self.soup.find('td',text =re.compile('Hometown:')).parent.parent.find\
                                                                                               ('td' ,{'class':'font'}).renderContents())
            except:
                log.info("couldn't parse author hometown")
            try:
                page['et_author_country'] = stripHtml(self.soup.find('td',text =re.compile('Country:')).parent.parent.\
                                                                                           find('td' ,{'class':'font'}).renderContents())
            except:
                log.info("couldn't parse author country")

            try:
                page['et_author_education'] = stripHtml(self.soup.find('td',{'class':'fontgrey'},text=re.compile('Education:')).\
                                                            parent.parent.renderContents()).replace('Education:','').strip()
            except:
                log.info("couldn't parse author education")

            try:
                author_joined_date = stripHtml(self.soup.find('td',{'class':'fontgrey'},text=re.compile('Member since:')).\
                                                   parent.parent.renderContents()).replace('Member since:','').strip()
                page['edate_author_member_since'] = datetime.datetime.strftime(datetime.datetime.strptime(author_joined_date,"%b %d, %Y")\
                                                                                  , "%Y-%m-%dT%H:%M:%SZ")
            except:
                log.info("couldn't parse author education")

        except:
            log.info("couldn't parse author info")
            
