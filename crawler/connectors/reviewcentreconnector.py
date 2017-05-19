
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#!/usr/bin/env python
# reviewcentre.com connector, Sudharshan

from BeautifulSoup import BeautifulSoup
from baseconnector import BaseConnector
import re
import copy
import md5
from datetime import datetime
from urllib2 import urlparse

from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize

import logging

log = logging.getLogger('ReviewCentreConnector')
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

debug = True
base_url = "http://www.reviewcentre.com"

class ReviewCentreConnector (BaseConnector):
    '''
    Connector to http://www.reviewcentre.com
    '''

    @logit(log, 'fetch')
    def fetch(self):
        '''
        Fetch all links to the reviews and add them in review_links
        '''
        self.genre = "Review"
        try:
            #self.currenturi = base_url + '/reviews-all-1155.html'
            #self.session_info_out = copy.copy(self.task.session_info)
            #self.session_info_out['reviews'] = self.session_info_out.get('reviews', {})
            parenturi = self.currenturi
            res = self._getHTML()
            self.rawpage = res['result']
            self._setCurrentPage()            
            self._getParentPage()
            self.review_links = []
            # Get all links to the full reviews
            try:
                while True:
                    try:
                        self.review_links.extend(list(set([x.find('a',href=re.compile('^/review') ,text=re.compile('^Read')).parent['href'] for x in self.soup.find('ul','reviews').findAll('li')])))
                        self.currenturi = base_url + self.soup.find('a',text='Next&gt;').parent['href']
                        res = self._getHTML()
                        self.rawpage = res['result']
                        self._setCurrentPage()
                    except:
                        log.info(self.log_msg('Next page not found'))
                        break
            except:
                log.info(self.log_msg('Next page not found'))
            #self.update_links()
##            pagination = self.soup.find("dl", { "class" : "pagination" })
##            if pagination:
##                pages = pagination.findAll("dd")[-1].next
##                last_page = pages.attrMap['href']
##                links = self.__compute_all_links(last_page)
##                for link in links:
##                    self.currenturi = base_url + link
##                    log.info(self.log_msg(self.currenturi))
##                    res = self._getHTML()
##                    self.rawpage = res['result']
##                    self._setCurrentPage()
##                    self.update_links()
##            log.info(self.log_msg(self.review_links))
            self.addreviews(parenturi)
            #self.task.status['fetch_status'] = True
            #self.task.session_info = self.session_info_out ### copying session_out to session_info , 
            return True
        except:
            self.task.status['fetch_status'] = False
            log.exception(self.log_msg('Exception in fetch'))
            return False


    @logit(log, '__compute_all_links')
    def __compute_all_links(self, last_page):
        '''
        Returns: a list of all pages with the reviews
        Params : the link to the last page with the reviews
        Gets called only when there is pagination
        '''
        links = []
        __split = last_page.split('_')
        # There are _split[1] pages in total
        for i in xrange(1, int(__split[1])+1):
            # FIXME: maybe, this could be done better
            links.append(__split[0] + '_' + str(i) + "_" + "_".join(__split[2:]))
        return links
    

    @logit(log, '_update_links')
    def update_links(self):
        ahrefs = self.soup.findAll("a", { "href" : re.compile(r'/review[^s].*\.html')})
        for ahref in ahrefs:
            self.review_links.append(ahref.get("href"))        


    @logit(log, 'addreviews')
    def addreviews(self, parenturi):
        log.info(self.log_msg('No. of reviews found on page, %d' %(len(self.review_links))))
        for link in self.review_links:
            self.currenturi = base_url + link
            #self.currenturi = "http://www.reviewcentre.com/review241142.html"
            #self.currenturi = "http://www.reviewcentre.com/review351639.html"
            res = self._getHTML()
            self.rawpage = res['result']
            self._setCurrentPage()
            try:
                page = {}
                page['uri'] = self.currenturi
                log.info(self.currenturi)
                if not checkSessionInfo(self.genre, self.session_info_out, 
                                        page['uri'], self.task.instance_data.get('update'),
                                        parent_list=[parenturi]):
                
                    review = self.soup.find('div',id='wrap-full-review')
                    #posted_info = stripHtml(review.find("p").renderContents())
                    #posted_info = " ".join(posted_info.split()) # Normalize extra spaces
                    #log.debug(posted_info)
                    # User Name
                    try:
                        page['et_author_name'] = re.search('(.*)\'s Review',stripHtml(self.soup.find('h2').renderContents())).group(1).strip()
                    except:
                        log.info(self.log_msg('Author name not found'))
                    try:
                        date_str = stripHtml(self.soup.find('div','clearfix full-review-pagination').find('p').renderContents())
                        page['posted_date'] = datetime.strftime(datetime.strptime(re.sub("(\d+)(st|nd|rd|th)",r"\1",date_str).strip(),"%d %b %Y"),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg('posted date not found'))
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    try:
                        page['ef_rating_overall'] = float(self.soup.find('h3',text='Overall Rating').findNext('img')['alt'].replace('stars','').strip())
                    except:
                        log.info(self.log_msg('rating not found'))
                    try:
                        ratings =  self.soup.find('div',id='inner-wrap-overall-rating').findAll('li')
                        for rating in ratings:
                            try:
                                page['ef_rating_' + stripHtml(rating.renderContents()).lower().replace(' ','_')] = float(rating.find('img',alt=True)['alt'].replace('stars','').strip())
                            except:
                                log.info(self.log_msg('rating not found'))
                    except:
                        log.info(self.log_msg('Rating fnot found'))
                    review = self.soup.find('div',id='wrap-overall-rating')
                    pros_cons = {'pros':'Good Points','cons':'Bad Points'}
                    for each in pros_cons:
                        try:
                            page['et_data_' + each ] = stripHtml(review.find('strong',text=pros_cons[each]).findNext('p').renderContents())
                        except:
                            pass
                            log.info(self.log_msg('pros cons not found'))
                    try:
                        page['data'] = stripHtml(review.find('strong',text='General Comments').findNext('p').renderContents())
                    except:
                        try:
                            [x.extract() for x in review.findAll('div')]
                            page['data'] = ''.join([stripHtml(x.renderContents()) for x in review.findAll('p')])
                        except:
                            log.info(self.log_msg('data not found'))
                            page['data'] =''
                    try:
                        if len(page['data']) > 50:
                            page['title'] = page['data'][:50] + '...'
                        else:
                            page['title'] = page['data']
                    except:
                        log.exception(self.log_msg('title not found'))
                        page['title'] = ''
                        

##                    h3s = review.findAll("h3")
##                    # Good Points
##                    try:
##                        if h3s[0].has_key("class"): # <h3 class="lgrey"> No Good points
##                            page['et_data_pros'] = ''
##                        else:
##                            pros = copy.copy(h3s[0].nextSibling)
##                            page['et_data_pros'] = stripHtml(pros.renderContents())
##                    except:
##                        page['et_data_pros'] = ''
##                        log.exception(self.log_msg("Couldn't extract pros"))
##
##                    # Bad Points
##                    try:
##                        if h3s[1].has_key("class"): # <h3 class="lgrey"> No Bad points
##                            page['et_data_cons'] = ''
##                        else:
##                            cons = copy.copy(h3s[1].nextSibling)
##                            page['et_data_cons'] = stripHtml(cons.renderContents())
##                    except:
##                        page['et_data_cons'] = ''
##                        log.exception(self.log_msg("Couldn't extract cons"))
##
##                        # Ratings are within <table> </table>
##                    fields = review.find("table").findAll("th")
##
##                    # Extract true ratings of the form x/10. Ignore free form values
##                    for field in fields:
##                        if field.nextSibling.find("img"):
##                            text = field.next
##                            key  = "ef_rating_" + text.lower().replace(" ", "_")
##                            try:
##                                rating = field.nextSibling.find("span").next.split('/')[0]
##                                page[key] = float(rating)
##                            except:
##                                page[key] = ''
##
##                    # Review Data (General comment)
##                    try:
##                        data = h3s[2].nextSibling
##                        page['data'] = stripHtml(data.renderContents())
##                    except:
##                        page['data'] = ''
##                        log.exception(self.log_msg("Couldn't extract review data"))
##
##                    # Review Title
##                    try:
##                        title_soup = stripHtml(self.soup.find("h2", { "class" : "fl" }).renderContents())
##                        #if title_soup.find("span"):
##                        #    page['title'] = title_soup.find("span").next
##                        #else:
##                        page['title'] = title_soup
##                    except:
##                        page['title'] = ''
##                        log.exception(self.log_msg("Couldn't extract title"))
                    log.info(page)
                    result=updateSessionInfo(self.genre, self.session_info_out, page['uri'], get_hash(page),
                                             'Review', self.task.instance_data.get('update'), parent_list=[parenturi])
                    if result['updated']:
                        page['path'] =  page['parent_path'] = [parenturi]
                        page['path'].append(page['uri'])
                        page['priority'] = self.task.priority
                        page['level'] = self.task.level
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
                        page['uri'] = self.currenturi
                        page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                        page['task_log_id'] = self.task.id
                        self.pages.append(page)
                comments_section = self.soup.findAll('div',id='wrap-comments')
                if comments_section and self.task.instance_data.get('pick_comments'):
                    log.info(self.log_msg("This review has comments"))
                    self.add_comments(comments_section, [parenturi, page['uri']])
            except:
                log.exception(self.log_msg("exception in addreviews"))
                continue
        return True


    @logit(log, "add_comments")
    def add_comments(self, comments, parent_list):
        #comments = comments_section.findAll("li")
        for comment in comments:
            #raw_input("Comment..continue?")
            try:
                page = {}
                poster = comment.find("p")
                posted_info = stripHtml(poster.renderContents())
                posted_info = " ".join(posted_info.split()) # Normalize extra spaces 
                log.debug(posted_info)
                try:
                    if poster.findChild("a"):
                        log.info(self.log_msg("This user has a profile page"))
                        page['et_author_name'] = stripHtml(poster.findChild("a").next)
                    else:
                        page['et_author_name'] = "".join(poster.next.split()[0:-4])
                except:
                    log.exception(self.log_msg("Couldn't extract comment author"))
                try:
                    page['data'] = stripHtml(comment.find('div','member-comment').renderContents())
                    page['title'] = page['data'][0:50] if len(page['data']) > 50 else page['data']
                except:
                    log.exception(self.log_msg("Couldn't extract comment data"))
                    page['data'] = ''
                    page['title'] = ''
                try:
                    date_suffix = re.match(r".* on \d+(st|nd|rd|th).*", posted_info).groups()[0]
                    page['posted_date'] = datetime.strftime(datetime.strptime(' '.join(posted_info.split()[-3:]), '%d' + date_suffix + ' %b %Y')
                                                                ,"%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.exception(self.log_msg("couldn't parse posted_date"))
                    page['posted_date'] = page['pickup_date']
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                log.info(page)
                if not checkSessionInfo(self.genre, self.session_info_out, 
                                        unique_key, self.task.instance_data.get('update'),
                                        parent_list=parent_list):
                    
                    
                    result = updateSessionInfo(self.genre, self.session_info_out, self.currenturi, get_hash(page),
                                             'Comment', self.task.instance_data.get('update'), 
                                             parent_list=parent_list)
                    if result['updated']:
                        page['path'] = page['parent_path'] = parent_list
                        page['path'].append(unique_key)
                        page['priority'] = self.task.priority
                        page['level'] = self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['last_updated_time'] = page['pickup_date']
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id  # TODO: Get the client from the project 
                        page['client_name'] = self.task.client_name
                        page['versioned'] = False
                        page['uri'] = self.currenturi
                        page['uri_domain'] = urlparse.urlparse(self.currenturi)[1]
                        page['task_log_id'] = self.task.id
                        page['entity'] = 'Comment'
                        page['category'] = self.task.instance_data.get('category' ,'')
                        self.pages.append(page)
                        log.info(self.log_msg('Comemnt added'))
                #print self.pages
                                             
            except:
                log.exception(self.log_msg("exception in add_comments"))
                #raw_input("Caught Exception")
                continue


    #@logit(log, "_getParentPage")
    def _getParentPage(self):
        try:
            page = {}
            try:
                page['title'] = re.sub(' Reviews$','',stripHtml(self.soup.find('h1').renderContents()))
            except Exception, e:
                log.exception(self.log_msg("Could not parse title"))
                raise e
            try:
                page['ef_rating_overall'] = float(re.search('\d+',stripHtml(self.soup.find('span','review-page-summary').renderContents())).group())
            except:
                log.info(self.log_msg('Overall Rating not found'))
            try:
                recommended = self.soup.find('p',id='member-recommendation')
                page['ef_product_recommendation'] = float(stripHtml(recommended.find('span').renderContents()).replace('%',''))
                recommendation_str = recommended.find('img',alt=re.compile('.*Thumb.*'))['alt'].strip()
                if recommendation_str=='Thumb up':
                    page['et_product_recommended'] ='yes'
                else:
                    page['et_product_recommended'] ='no'
            except:
                log.info(self.log_msg('Cannot find the recommendation'))
            try:
                ratings = self.soup.find('div','inner-col-2').findAll('li')
                for rating in ratings:
                    try:
                        page['ef_rating_' + stripHtml(rating.renderContents()).lower().replace(' ','_')] = float(rating.find('img',alt=True)['alt'].replace('stars','').strip())
                    except:
                        log.info(self.log_msg('rating not found'))
                #fields = self.soup.find("table").findAll("th")
            
                # Extract true ratings of the form x/10. Ignore free form values
##                for field in fields:
##                    if field.nextSibling.find("img"):
##                        text = field.next
##                        key  = "ef_rating_" + text.lower().replace(" ", "_")
##                        try:
##                            rating = field.nextSibling.find("span").next.split('/')[0]
##                            page[key] = float(rating)
##                        except:
##                            log.info (self.log_msg("Couldn't get rating for " + key))
##                            page[key] = ''
            except Exception, e:
                log.exception(self.log_msg("Couldn't parse get at all"))
            
            # This review has the thumbs down
##            try:
##                recommendations = stripHtml(self.soup.find("div", { "class" : "rc_no" }).renderContents())
##                log.info(self.log_msg("%s has a negative recommendation" %page['title']))
##            except:
##                recommendations = stripHtml(self.soup.find("div", { "class" : "rc_yes" }).renderContents())
##                log.info(self.log_msg("%s has a positive recommendation" %page['title']))
##
##            log.debug(recommendations)
##            re_obj = re.compile (r"\d+% Recommended (\d+) out of (\d+)")
##            matched = re_obj.match(recommendations)
##            if matched:
##                try:
##                    page['ei_data_recommended_yes'] = int(matched.groups()[0])
##                except:
##                    page['ei_data_recommended_yes'] = ''
##                    log.exception (self.log_msg("Data recommended yes unsuccessful"))
##
##                try:
##                    page['ei_data_recommended_total'] = int(matched.groups()[1])
##                except:
##                    page['ei_data_recommended_total'] = ''
##                    log.exception (self.log_msg("Data recommended not unsuccessful"))
##
##                if page['ei_data_recommended_total'] and page['ei_data_recommended_yes']:
##                    page['ei_data_recommended_no'] = page['ei_data_recommended_total'] - page['ei_data_recommended_yes']
##                else:
##                    page['ei_data_recommended_no'] = ''
                        

##            post_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x ,\
##                                                       page.values()))).encode('utf-8','ignore')).hexdigest()
##            log.debug('Checking session info for Parent page')

            #continue if returned true
            log.info(page)
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    self.currenturi, self.task.instance_data.get('update')):# and self.session_info_out!={}:
                id = None
                if self.session_info_out == {}:
                    id = self.task.id
                    log.debug(id)
                result = updateSessionInfo(self.genre, self.session_info_out, self.currenturi, get_hash(page),
                                           'Post', self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    page['path'] = page['parent_path'] = []
                    page['path'].append(self.currenturi)
                    page['uri'] = normalize(self.currenturi)
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
                    page['data'] = ''
                    page['task_log_id'] = self.task.id
                    page['entity'] = 'Post'
                    page['category'] = self.task.instance_data.get('category','')
                    self.pages.append(page)
                    log.info(self.log_msg(''))
            match_obj = re.match(r'http://www.reviewcentre.com/reviews(\d+).html', self.currenturi)
            if not match_obj:
                log.debug("This shouldn't happen, The url seems to be borked " + self.currenturi)
                raise
            review_id = match_obj.groups()[0]
            # Now that we have the pages list, All other reviews are found in the url below
            self.currenturi = "http://www.reviewcentre.com/" + "reviews-all-" +  review_id + ".html"
            # By now we are in reviews-all-*.html
            res = self._getHTML()
            self.rawpage = res['result']
            self._setCurrentPage()
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            raise e

#        print self.pages
