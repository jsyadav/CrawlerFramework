
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

# Sathya
# SKumar

import re
import logging
import copy

from datetime import datetime
from urlparse import urlparse
from BeautifulSoup import BeautifulStoneSoup , BeautifulSoup

from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit

log = logging.getLogger('YelpConnector')

class YelpConnector( BaseConnector ):
    '''
    A Connector to find the data in www.yelp.com which is of the format     
    http://www.yelp.com/biz/city-rent-a-car-san-francisco-2?rpp=40&sort_by=date_desc
    http://www.yelp.com/biz/city-carshare-san-francisco-2#hrid:mqZyNWQnv4tTQquvUCOurA/query:car rental
    check uri http://www.yelp.com/biz/british-motor-car-distributors-ltd-san-francisco?rpp=40&sort_by=date_desc
    # Sample uris http://www.yelp.com/biz/city-rent-a-car-san-francisco-2?rpp=40&sort_by=date_desc
    # http://www.yelp.com/biz/city-carshare-san-francisco-2#hrid:mqZyNWQnv4tTQquvUCOurA/query:car rental
    '''
    
    @logit(log,"fetch")
    def fetch(self):
        """
        Starting of a connector, the following steps are taken
        change the uri so that it is sorted by Date by doing following step
        uri = uri.split('#')[0] + '?rpp=40&sort_by=date_desc'
        set the Soup of of the uri
        get the Parent Page Detatils and add it to self.pages
        get the Reviews Details and add it to self.pages
        sample data sent to arun, have the follwing urls
        http://www.yelp.com/biz/city-carshare-san-francisco-2#hrid:mqZyNWQnv4tTQquvUCOurA/query:car rental
        http://www.yelp.com/biz/enterprise-rent-a-car-phoenix-23#hrid:_jR99Vj1_2ZqsMa6PolPEw/query:car rental
        http://www.yelp.com/biz/city-rent-a-car-san-francisco-2#hrid:IXma7XoCBbJvfTFvac_WjA/query:car rental
        http://www.yelp.com/user_details?userid=zpXqbJQ2CT4PsUBTy30DbQ( for author  profile)
        """
        
        self.genre = 'Review'
        try:        
            self.parent_uri = self.currenturi = self.currenturi.split('#')[0] + \
                                                    '?rpp=40&sort_by=date_desc'
            if not self._setSoup():
                log.info(self.log_msg('Soup not set, cannot not proceed'))
                return False
            if not self.__getParentPage():
                log.info(self.log_msg('Parent page not added'))
            current_page_no = 1
            while True:
                self.__addReviews()
                try:
                    self.currenturi = 'http://www.yelp.com' + self.soup.find('div',id='paginationControls')\
                            .find('table').find('a',text=re.compile('\s*'+ str\
                            (current_page_no + 1) + '\s*')).findParent('a')['href']
                    if not self._setSoup():
                        break
                    current_page_no = current_page_no + 1
                except:
                    log.info(self.log_msg('Next Page not found'))
                    break
            return True
        except:
            log.exception(self.log_msg('Error in Fetch methor') )
            return False
        
    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will get general information about the product
        Title, Overall Rating,Contact info, Hours( for this type of product )
                
        this is for future reference in ipython
        ---------------------------------------        
        title =  stripHtml(soup.find('h1').renderContents())
        rating is of the following format
         <div class="rating stars_4">4 star rating</div>( 4 can be replaced by any \d
        rating = float(re.search('^\d+',soup.find('div',{'class':re.compile('rating stars_\d+')}).renderContents()).group())
        category = stripHtml(soup.find('span',id='cat_display').renderContents())
        address = stripHtml(soup.find('address').renderContents())
        phone no = stripHtml(soup.find('span',id='bizPhone').renderContents())
        url =  stripHtml(soup.find('div',id='bizUrl').renderContents())
        reviews_count = int(re.search('based on (\d+) reviews',stripHtml(soup.find('div',id='bizRating').find('em').renderContents())).group(1))
        """
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        page = {}
        try:
            page['title'] = stripHtml( self.soup.find('h1').renderContents() )
        except:
            log.info(self.log_msg(' Title cannot be found'))
        try:
            page['ef_product_rating_overall']= float(self.soup.find('div','rating').find('img')['alt'].replace('star rating','').strip())
        except:
            log.info( self.log_msg(' rating cannot be found') )
        try:
            page['et_product_category']= stripHtml(self.soup.find('span',\
                                            id='cat_display').renderContents())
        except:
            log.info(self.log_msg(' category cannot be found'))
        try:
            page['et_product_address'] = stripHtml(self.soup.find('address')\
                                                   .renderContents()) 
        except:
            log.info(self.log_msg(' Address cannot be found'))
        try:
            page['et_product_phone_no'] = stripHtml(self.soup.find('span',id=\
                                                   'bizPhone').renderContents())        
        except:
            log.info(self.log_msg(' phone no cannot be found'))
        try:
            page['et_product_url'] = stripHtml(self.soup.find('div',id='bizUrl')\
                                                  .renderContents())        
        except:
            log.info(self.log_msg(' url cannot be found'))
        try:
            page['ei_product_reviews_count'] = int(re.search('based on (\d+) reviews'\
                                    ,stripHtml(self.soup.find('div',id=\
                                    'bizRating').find('em').renderContents()))\
                                    .group(1))
        except:
            log.info(self.log_msg(' reviews count cannot be found'))
        try:
            post_hash = get_hash(page)
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo( self.genre, self.session_info_out, self.\
                   parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                return False
            page['path']=[self.parent_uri]
            page['parent_path']=[]
            page['uri'] = normalize( self.currenturi )
            page['uri_domain'] = unicode(urlparse(page['uri'])[1])
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
            page['task_log_id']=self.task.id
            page['entity'] = 'Post'
            page['category']=self.task.instance_data.get('category','')
            self.pages.append(page)
            log.info(self.log_msg('Parent Page added'))
            return True
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False
    
    @logit(log,'__getReviews')
    def __addReviews(self):
        """
        This will get all the reviews found for the product
        reviews = soup.find('div',id='bizReviewsInner').findAll('div',id=re.compile('^review.*'))
        for review in reviews:
            # Reviewer Info
            
            reviews_info_dict = {'ei_author_friends_count':'miniOrange friend_count ieSucks',
                                  'ei_author_review_count':'miniOrange review_count ieSucks'
                                }
            for each in reviews_info_dict:
                try:
                    page[each] = int(stripHtml(review.find('div','reviewer').find('p',reviews_info_dict[each]).renderContents()))
                except:
                    print 'Error'
            author_title = stripHtml(review.find('div','reviewer').find('p','miniOrange is_elite ieSucks').renderContents())
            name_loc = review.findAll('p','reviewer_info')
            author_name = stripHtml(name_loc[0].renderContents())
            author_loc = stripHtml(name_loc[1].renderContents())
            rating = float(re.search('^\d+', review.find('div',{'class':re.compile('rating stars.*')}).renderContents()).group())
            posted_date = datetime.strftime(datetime.strptime(stripHtml(review.find('div','ext_rating ieSucks').find('em').renderContents()) ,"%m/%d/%Y"),"%Y-%m-%dT%H:%M:%SZ")
            data = stripHtml(review.find('p','review_comment ieSucks').renderContents())
        """
        try:
            reviews = self.soup.find('div',id='bizReviewsInner').findAll('div',\
                                                    id=re.compile('^review_.*'))
        except:
            log.info(self.log_msg('Cannot proceed , No reviews are found'))
            return False
        for review in reviews:
            page = {}
            page['uri'] = self.currenturi
            reviews_info_dict = {'ei_author_friends_count':'miniOrange friend_count ieSucks',
                                  'ei_author_reviews_count':'miniOrange review_count ieSucks'
                                }
            for each in reviews_info_dict:
                try:
                    page[each] = int( stripHtml(review.find('div','reviewer').\
                                find('p',reviews_info_dict[each]).renderContents()))
                except:
                    log.info(self.log_msg('%s cannot be found'%each))
            try:
                aut_title = stripHtml(review.find('div','reviewer').find('p',\
                            'miniOrange is_elite ieSucks').renderContents())
                if not aut_title == '':
                    page['et_author_title'] = aut_title
            except:
                log.info(self.log_msg('Author Tile cannot be found') )
            try:
                name_loc = review.findAll('p','reviewer_info')
                page['et_author_name'] = stripHtml(name_loc[0].renderContents())
                page['et_author_location'] = stripHtml(name_loc[1].renderContents())
            except:
                log.info(self.log_msg('Author infocannot be found') )
            previous_soup = copy.copy(self.soup)
            previous_uri = self.currenturi
            try:
                self.currenturi =  'http://www.yelp.com' + review.find('p','reviewer_info').find('a')['href']
                if self._setSoup():
                    page['et_author_uri'] = self.currenturi
                    page = self.__getAuthorInfo( page )
            except:
                log.exception(self.log_msg('Author info cannot be retrieved'))
            self.soup = copy.copy(previous_soup)
            self.currenturi = previous_uri
            temp_page = copy.copy( page ) 
            try:
                date_str = stripHtml(review.find('div','ext_rating ieSucks').find\
                         ('em').renderContents()).replace('Updated -','').strip()                
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str \
                                      ,"%m/%d/%Y"),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.info(self.log_msg('Posted date cannot be found') )
                page['posted_date'] = datetime.strftime(datetime.utcnow(),\
                                                    "%Y-%m-%dT%H:%M:%SZ")
            try:                
                page['data'] = stripHtml(review.find('p','review_comment ieSucks')\
                                                            .renderContents())
            except:
                log.info(self.log_msg('Data cannot be found') )
                page['data'] = ''
            try:
                if len(page['data']) > 50:
                        page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
            except:
                log.exception(self.log_msg('title not found'))
                page['title'] = ''             
            page = self.__getCommanPageData(review, page )
            self.__addPages( page )
            # For Older Reviews
            old_review =  review.find('div',id=re.compile('review:.*'))
            if old_review:
                try:
                    # Just doing as it is in the java script
                    metadata = re.search('review:([A-Za-z0-9_-]+)::biz:([A-Za-z0-9_-]+)$'\
                                                                ,old_review['id'])                    
                    review_url = "http://www.yelp.com" + "/archive_snippet?review_id="\
                                +metadata.group(1)+"&biz_id=" +metadata.group(2)\
                                            + "&show_full=1&draw_edit_links=1"
                    raw_page = self._getHTML( uri = review_url )
                    if not raw_page:
                        continue
                    xml_soup = BeautifulStoneSoup( raw_page['result'] )
                    review_snippet = BeautifulSoup( xml_soup.find('snippet').contents[0] )
                    review_soups = review_snippet.findAll('li')
                    for review_soup in review_soups:
                        page = {}
                        page = copy.copy( temp_page )
                        try:                
                            page['posted_date'] = datetime.strftime(datetime.strptime\
                                                (stripHtml(review_soup.find('em').\
                                                renderContents()) ,"%m/%d/%Y"),\
                                                "%Y-%m-%dT%H:%M:%SZ")
                        except:
                            log.info(self.log_msg('Posted date cannot be found') )
                            page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                              ,"%Y-%m-%dT%H:%M:%SZ")
                        try:                
                            page['data'] = stripHtml(review_soup.find('p',\
                                                'review_comment').renderContents())
                        except:
                            log.info(self.log_msg('Data cannot be found') )
                            page['data'] = ''
                        try:
                            if len(page['data']) > 50:
                                page['title'] = page['data'][:50] + '...'
                            else:
                                page['title'] = page['data']
                        except:
                            log.exception(self.log_msg('title not found'))
                            page['title'] = ''
                        page = self.__getCommanPageData(review_soup, page)
                        self.__addPages( page )
                except:
                    log.info(self.log_msg('Cannot get the old reviews'))
                    
    @logit(log, "__getCommonPageInfo")
    def __getCommanPageData( self, review,page ):
        """This will fetch the page details which are distinct
        """
        thought_dict = {'ei_data_useful_count':'Useful',
                       'ei_data_funny_count':'Funny',
                       'ei_data_cool_count':'Cool'
                      }
        for each in thought_dict:
            regex_str = '\s*' + thought_dict[each] + '.*'
            try:
                thought_str  =  stripHtml(review.find('div',id=re.compile('^ufc.*'))\
                                    .find('p',text=re.compile(regex_str ) ) )
                page[ each ] = int(re.search('\((\d+)\)',thought_str).group(1))
            except:
                log.info(self.log_msg('%s is not found'%each))
        try:                
            page['ef_rating_overall'] = float(review.find('div','rating').find('img')['alt'].replace('star rating','').strip())
        except:
            log.info(self.log_msg('Rating cannot be found') )
        return page           
    
    @logit(log,'__addPages')
    def __addPages(self,page):
        """This will add the page passed to it
        """ 
        try:
            review_hash = get_hash(page)
            unique_key = get_hash( {'data':page['data'],'title':page['title']})
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                        =[ self.parent_uri ]):
                log.info(self.log_msg('Review cannot be added'))
                return False
            result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                        review_hash,'Review', self.task.instance_data.get('update'),\
                                                    parent_list=[self.parent_uri])
            if not result['updated']:
                return False
            parent_list = [ self.parent_uri ]
            page['parent_path'] = copy.copy(parent_list)
            parent_list.append( unique_key )
            page['path']=parent_list
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
            page['uri_domain'] = urlparse(page['uri'])[1]
            self.pages.append(page)
            log.info(self.log_msg('Review Added'))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False
        
    @logit(log, "_setSoup")
    def _setSoup( self, url = None, data = None, headers = {} ):
        """
            It will set the uri to current page, written in seperate
            method so as to avoid code redundancy
        """
        if url:
            self.currenturi = url
        try:
            log.info(self.log_msg( 'for uri %s'%(self.currenturi) ))
            res = self._getHTML( data = data, headers=headers  )
            if res:
                self.rawpage = res[ 'result' ]
            else:
                log.info(self.log_msg('self.rawpage not set.... so Sorry..'))
                return False
            self._setCurrentPage()
            return True
        except Exception, e:
            log.exception(self.log_msg('Page not for  :%s'%url))
            raise e
    @logit(log,"__getAuthorInfo")
    def __getAuthorInfo(self,page):
        """ This will fetch teh author info
        """
        try:
            author_stat_dict= {'first_review_count':'ftrCount',
                                   'fans_count':'fanCount',
                                   'photo_count':'localPhotoCount',
                                   'friends_list_count': 'flistCount',
                                   'update_reviews_count': 'updatesCount'
                                  }
            for each in author_stat_dict:
                try:
                    page['ei_author_'+ each ] = int(re.search('^\d+',stripHtml(self.soup.find('li',id=author_stat_dict[each]).renderContents())).group())
                except:
                    log.info(self.log_msg('%s not found'%each))
            try:
                review_votes_str = stripHtml(self.soup.find('p',id='review_votes').renderContents())
                match_object = re.search('(\d+) Useful, (\d+) Funny, and (\d+) Cool',review_votes_str)
                page['ei_author_useful_reviews_count'] = int ( match_object.group(1) )
                page['ei_author_funny_reviews_count'] = int ( match_object.group(2) )
                page['ei_author_cool_reviews_count'] = int ( match_object.group(3) )
            except:
                log.info(self.log_msg('reviews votes are not found'))
            author_info_dict = {'member_since':'Yelping Since',
                                'found_place':'Find Me In',
                                'home_town':'My Hometown',
                                'review_writing_reason':'Why You Should Read My Reviews',
                                'last_book_read':'The Last Great Book I Read',
                                'last_meal_eaten':'My Last Meal On Earth',
                                'profession':'When I&#39;m Not Yelping...',
                                'first_concert':'My First Concert',
                                'current_crush':'Current Crush',
                                'blog_or_website':'My Blog Or Website',
                                'favorite_second_website':'My Second Favorite Website',
                                'favorite_movie':'My Favorite Movie',
                                'recent_discovery':'Most Recent Discovery',
                                'secret':'Don&#39;t Tell Anyone Else But...'
                                }
            for each in author_info_dict:
                try:
                    value =  self.soup.find('span',text=author_info_dict[ each  ])
                    if value:
                        if each=='member_since':
                            try:
                                value = '01 ' + stripHtml(value.findNext('p').renderContents())
                                page['edate_author_' + each ] = datetime.strftime(datetime.strptime( value ,"%d %B %Y"),"%Y-%m-%dT%H:%M:%SZ")
                                continue
                            except:
                                log.info(self.log_msg('Author member since not found'))
                                continue            
                        page['et_author_' + each ] = stripHtml(value.findNext('p').renderContents())
                except:
                    log.info(self.log_msg('%s is not found '%each ) )               
        except:
            log.exception('error with author')
        return page
        
        
     
        


#===============================================================================
# from httpconnection import *
# from BeautifulSoup import *
# from cgi import unescape
# import re
# 
# http = HTTPConnection()
# http.createrequest('http://www.yelp.com/biz/enterprise-rent-a-car-phoenix-23#hrid:_jR99Vj1_2ZqsMa6PolPEw/query:car%20rental')
# page = http.fetch().read()
# f = open('/home/sathya/Desktop/crawl.txt','w')
# soup = BeautifulSoup(page)
# element = soup.find('div',{'id':'bizMain'})
# element1 = soup.find('div',{'class':'wrap'})
# h1 = element1.find('h1')
# f.write( unescape(h1.renderContents().replace('&nbsp',' '))+"\n" )
# 
# e1 = element1.find('div',{'class':re.compile('rating stars')})
# f.write("OverAllStar:"+ unescape(e1.renderContents().replace('&nbsp',' '))+"\n" )
# 
# element1.find('em').renderContents()
# category = soup.find('span',{'id':'cat_display'})
# catTypes = category.findAll('a')
# 
# f.write("Categories:")
# for e in catTypes:
#    type(e.renderContents())
#    f.write(unescape(e.renderContents().replace('&nbsp',' '))+"\n")
# 
# reviews = element.find('div',{'id':'bizReviews'})
# rs = reviews.findAll('div',{'id':re.compile('review_.*'),'class':'review externalReview clearfix nonfavoriteReview  '})
# 
# for i,review in enumerate(rs):
#    f.write("\n\nReview %s Begins:\n\n" %(str(i+1)) )
#    ri = review.findAll('p',{'class':'reviewer_info'})
#    type(ri[0].find('a').renderContents())
#    f.write("Name:"+unescape(ri[0].find('a').renderContents().replace('&nbsp',' '))+"\n")
#    type(ri[1].renderContents())
#    f.write("Address:"+unescape(ri[1].renderContents().replace('&nbsp',' '))+"\n")
#    ratings = review.findAll('div',{'class':'ext_rating ieSucks'})
#    type(ratings[0].find('div').renderContents())
#    f.write("Ratings:"+unescape(ratings[0].find('div').renderContents().replace('&nbsp',' '))+"\n")
#    type(ratings[0].find('em').renderContents())
#    f.write("Review Date:"+unescape(ratings[0].find('em').renderContents().replace('&nbsp',' '))+"\n")
#    rc = review.find('p',{'class':'review_comment ieSucks'})
#    type(rc.renderContents())
#    f.write("Review Comment:\n"+unescape(rc.renderContents().replace('&nbsp',' '))+"\n")
#    rre = review.find('div', {'class':'rateReview external'})
#    if rre != None:
#        re = rre.findAll('p', {'class':'smaller'})
#        f.write("People Thought it was:\n")
#    for rr in re:
#            type(rr.renderContents())
#            f.write(unescape(rr.renderContents().replace('&nbsp',' ').strip())+"\n")
#    
# f.close()
#===============================================================================
