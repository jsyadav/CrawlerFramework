
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

# Srini
# Skumar
# Modified , Dec 17, 2008

import re
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import logging
from urllib2 import urlparse

from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('AdmanyaConnector')

"""
Review list page
================
review_block = soup.find(id='R_col')
reviews_list = review_block.findAll('div', {'style':'border-bottom:1px solid #dedede; padding-bottom:5px; margin-top:15px;'})
review_links = [review.find('a') for review in reviews_list]
next_url = review_block.find('img', alt='Next').parent['href']

Review page
===========
review_block = soup.find(id='R_col')
title = review_block.find('span', {'style':'font-size:13pt; font-weight:bold;'}).string
meta = review_block.find('div', {'style':'position:absolute; left:107px; bottom:0px;'})
metadata = review_block.find('span',{'style':'text-transform:uppercase;font-family:Arial, Helvetica, sans-serif;'})

# Positive Negative
pos_neg = review_block.find('div', {'style':'margin-top:15px; width:550px; padding-bottom:10px'})
pos_neg = pos_neg.findAll('span')
pos = pos_neg[0].string
neg = pos_neg[1].string

content = review_block.find(id='tabcontent')
helpful = review_block.findAll('span', {'style':'font-size:8pt; padding-left:15px; '}) # Note the space at the end
Skumar
-----
Start with testing the page http://consumer.admanya.com/review_ICICI_Bank_Banks-3038.html

"""

class AdmanyaConnector(BaseConnector):
    @logit(log , 'fetch')
    def fetch(self):
        """Main function to be called. Fetch parent page and get links to individual reviews and goto next page till end
        """
        try:
##            self.session_info_out = copy.copy(self.task.session_info)
            #self.currenturi ='http://consumer.admanya.com/reviews_ICICI_Bank_Banks-29-0.html'
            # Fetch the contents of the parent page
            self.genre = 'Review'
            self.parenturi = self.currenturi
            res=self._getHTML()
            if not res:
                return False
            self.rawpage=res['result']
            self._setCurrentPage()
            self.__getParentPage() #checks for parent page ,and appends a empty dictionary or a modified post.
            while True:
                try:
                    review_block = self.soup.find(id='R_col') # Entire review is inside this div only
                    next_page = review_block.find( 'img', alt = 'Next' )
                    log.debug(self.log_msg( "next_page: " + str(next_page) ) )
                    picked_reviews = self.__addReviews()
                    if picked_reviews and next_page: #check if there's next_page  and we haven't reached the last crawled review
                        next_page = next_page.parent
                        self.currenturi = 'http://consumer.admanya.com/' + next_page['href']
                        log.debug("Next Page: " + self.currenturi)
                        res = self._getHTML()
                        self.rawpage = res['result']
                        self._setCurrentPage()
                    else:
                        log.info(self.log_msg('Reached last page of reviews'))
                        break
                except Exception, e:
                    log.exception(self.log_msg ('exception occure in Fetchin reviews'))
                    break
                    #raise e
            self.task.status['fetch_status'] = True
            return True
        except:
            self.task.status['fetch_status'] = False
            log.exception(self.log_msg('Exception in fetch'))
            return False


    @logit(log , '_getParentPage')
    def __getParentPage(self):
        """Extract info from the main reviews listing page like average rating, ARscore, etc.
        """
        try:
            page={}
            review_block = self.soup.find(id='R_col')
            try:
                page['title'] = review_block.find('span', {'style':re.compile\
                                ('font-size:\s?13pt;\s?font-weight:\s?bold;')})\
                                                            .renderContents()
            except:
                log.info(self.log_msg('could not parse page title'))
            try:
                metatext = stripHtml( review_block.find('div', {'style':re.compile\
                    ('position:\s?absolute;\s?left:\s?107px;\s?bottom:\s?0px;')})\
                                                            .renderContents() )
                page['ef_rating_overall'] =  re.search('Average Rating:\s+\(([\d.]+)\)',\
                                                                     metatext).group(1)
            except:
                log.info(self.log_msg('could not parse overall_rating'))
            try:
                # AR Score
                arscore = unicode ( review_block.find('img', width=50, height=40)['src'] )
                page['ei_arscore'] = int ( re.search('(\d+)-\d+.png',arscore).group(1) )
            except:
                log.info(self.log_msg('could not parse ARScore'))
            try:
                post_hash = get_hash( page )
                if checkSessionInfo(self.genre, self.session_info_out, self.parenturi \
                                            , self.task.instance_data.get('update')):
                    return False
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out,self.parenturi,\
                             post_hash,'Post', self.task.instance_data.get('update'), Id=id)
                if not result[ 'updated' ]:
                    return False
                page['path'] = [self.parenturi]
                page['parent_path'] = []
                page['uri'] = normalize(self.currenturi)
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                page['priority'] = self.task.priority
                page['level'] = self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                            ,"%Y-%m-%dT%H:%M:%SZ")
                page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                            ,"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                page['client_name'] = self.task.client_name
                page['last_updated_time'] = page['pickup_date']
                page['versioned'] = False
                #page['first_version_id']=result['first_version_id']
                page['data'] = ''
                #page['id'] = result['id']
                page['task_log_id'] = self.task.id
                page['entity'] = 'Post'
                page['category'] = self.task.instance_data.get('category','')
                self.pages.append(page)
                log.info(page)
                log.info(self.log_msg('Review added'))
            except Exception,e:
                log.exception(self.log_msg('Parent Page could not be added'))
                raise e
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            raise e

    @logit(log , 'addReviews')
    def __addReviews(self):
        """Add the Reviews found in the current page, i.e 10 reviews
        """
        review_block = self.soup.find(id='R_col')
        review_links = [ each.find('a')['href'] for each in  review_block.findAll('div',{'style':re.compile('border-bottom:\s?1px\s?solid\s?#dedede;\s?padding-bottom:\s?5px;\s?margin-top:\s?15px;')})]
        log.info( self.log_msg( 'no. of reviews found on page %d'%( len(review_links) ) ) )
        for link in review_links:
            page = {}
            try:
                page['uri'] = 'http://consumer.admanya.com/' + link
                if checkSessionInfo(self.genre, self.session_info_out,page['uri'], self.task.instance_data.get('update'),parent_list=[ self.parenturi ]):
                    continue
                page = self.__extractReview( page )
                if not page:
                    continue
                review_hash = get_hash( page )
                result=updateSessionInfo(self.genre, self.session_info_out, page['uri'], review_hash,'Review', self.task.instance_data.get('update'), parent_list=[ self.parenturi ])
                if not result[ 'updated' ]:
                    continue
                #page[ 'id' ] = result[ 'id' ]
                #page[ 'first_version_id' ] = result[ 'first_version_id' ]
                #page[ 'parent_id' ] = '-'.join(result['id'].split('-')[:-1])
                parent_list = [ self.parenturi ]
                page['parent_path'] =  parent_list
                parent_list.append(page['uri'])
                page['path'] = parent_list
                page[ 'priority' ] = self.task.priority
                page[ 'level' ] = self.task.level
                page[ 'connector_instance_log_id' ] = self.task.connector_instance_log_id
                page[ 'connector_instance_id' ] = self.task.connector_instance_id
                page[ 'workspace_id' ] = self.task.workspace_id
                page[ 'client_id' ] = self.task.client_id  # TODO: Get the client from the project
                page[ 'client_name' ] = self.task.client_name
                page[ 'last_updated_time' ] = page['pickup_date']
                page[ 'versioned' ] = False
                page[ 'entity' ] = 'Review'
                page[ 'category' ] = self.task.instance_data.get('category','')
                page[ 'task_log_id' ] = self.task.id
                page[ 'uri_domain' ] = urlparse.urlparse(page['uri'])[1]
                self.pages.append( page )
                log.info(page)
                log.info(self.log_msg('Review Added'))
                log.debug(self.log_msg("Trying to extract comments ..."))
                self.__extractComments( page[ 'uri' ] )
                log.debug(self.log_msg("Extracted comments ..."))
            except:
                log.exception(self.log_msg("exception in addreviews"))

        return True

    def __extractReview( self, page ):
        """Extract all the info about the review from the url in the page dict and return back the page dict
        """
        """
        review_block = soup.find(id='R_col')
        title = review_block.find('span', {'style':'font-size:13pt; font-weight:bold;'}).string
        meta = review_block.find('div', {'style':'position: absolute; left: 107px; bottom: 5px;'})
        metadata = review_block.find('span',{'style':'text-transform:uppercase;font-family:Arial, Helvetica, sans-serif;'})

        # Positive Negative
        pos_neg = review_block.find('div', {'style':'margin-top:15px; width:550px; padding-bottom:10px'})
        pos_neg = pos_neg.findAll('span')
        pos = pos_neg[0].string
        neg = pos_neg[1].string

        content = review_block.find(id='tabcontent')
        helpful = review_block.findAll('span', {'style':'font-size:8pt; padding-left:15px; '}) # Note the space at the end
        """
        self.currenturi = normalize( page['uri'].split(';')[0] )
        log.debug( self.log_msg( "Fetching URL: " + page[ 'uri' ] ) )
        resp = self._getHTML()
        if not resp:
            return False
        self.rawpage = resp[ 'result' ]
        self._setCurrentPage()
        review_block = self.soup.find(id='R_col')
        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")

        # Title
        try:
            page['title'] = review_block.find('span', {'style':re.compile('font-size:\s?13pt;\s?font-weight:\s?bold;')}).string
        except:
            page['title'] = ''
            log.exception( self.log_msg( "review title couldn't be extracted" ) )

        # Date and author info
        try:
            metadata = review_block.find('span',{'style':re.compile('text-transform:\s?uppercase;\s?font-family:\s?Arial,\s?Helvetica,\s?sans-serif;')})
            try:
                posted_date = re.search(r'(\d+/\d+/\d+)', metadata.renderContents()).group(1)
                page[ 'posted_date' ] = datetime.strftime( datetime.strptime( posted_date,'%d/%m/%y'),"%Y-%m-%dT%H:%M:%SZ" )
            except:
                log.exception(self.log_msg ( 'posted date is not found' ))
                page['posted_date'] = page['pickup_date']
            try:
                page['et_author_name'] = unicode(metadata.find('u').string )
            except:
                log.info(self.log_msg('author name couldnt be extracted'))
            try:
                author_link = 'http://consumer.admanya.com' + metadata.find('a')['href']
                page = self.__extractAuthorInfo( page, author_link )
            except:
                log.info(self.log_msg('author details not extracted..') )
        except:
            log.info( self.log_msg('posted date or author details couldnt be extracted' ) )

        # Rating
        try:
            rating_imgs= review_block.find('div', {'style':re.compile('position:\s?absolute;\s?left:\s?107px;\s?bottom:\s?(5|0)px;')}).findAll('img')
            page['ef_rating_overall'] = self.__getRating(rating_imgs)
        except:
            page['ef_rating_overall'] = 0.0
            log.info( self.log_msg('Overall rating couldnt be extracted') )

        # Extra rating, info and pos or neg about product
        try:
            extras = review_block.find('div', {'style':re.compile(r'margin-top:\s?15px;\s?width:\s?550px;\s?padding-bottom:\s?10px;?')})
            other_ratings = extras.findAll('div', {'style':re.compile('width:\s?400px')})
            # other_ratings contains extra fields like speed of response, atm network, etc and rating (out of 5)
            for r in other_ratings:
                rating_head = unicode(r.find('span').string)
                rating_head = 'ef_rating:_' + rating_head.lower().replace(' ', '_')
                rating_imgs = r.findAll('img')
                page[ rating_head ] = self.__getRating( rating_imgs )

            # imporant_params -> most important parameter and least important parameter
            try:
                important_params = extras.find('div', {'style':re.compile('width:\s?550px;')}).findAll('div') # Note the ';' in style

                # Most important params is in a span tag whereas least important params is not in one - WTF

                ## for param in important_params:
                ##     log.debug("Param: " + str(param))
                ##     param_name = unicode(param.findAll('span')[0].string)
                ##     param_name = 'et_' + param_name.lower().replace(' ', '_')
                ##     param_text = param.findAll('span')[1].string
                ##     page[param_name] = unicode(param_text)
                ##     log.debug("Param saved: " + param_name + " Param text: " + param_text)

                # Most important param
                param = important_params[0]
                log.debug("Param: " + str(param))
                param_name = unicode(param.findAll('span')[0].string)
                param_name = 'et_' + param_name.lower().replace(' ', '_')
                param_text = param.findAll('span')[1].string
                page[param_name] = unicode(param_text)
                log.debug("Param saved: " + param_name + " Param text: " + param_text)

                # Least important param
                param = important_params[1]
                param_name = unicode(param.contents[2]).strip()
                param_name = 'et_' + param_name.lower().replace(' ', '_')
                param_text = param.findAll('span')[0].string
                page[param_name] = unicode(param_text)
                log.debug("Param saved: " + param_name + " Param text: " + param_text)
            except:
                log.info(self.log_msg ('error with Important Param') )

            # End of crappy most important params and least important params code
            # Positive and Negative
            # http://consumer.admanya.com/oldreview.jsp?id=2397
            #log.debug("*"*30)
            try:
                pos_neg = {'et_data_pros':'positives','et_data_cons':'negatives'}
                for key in pos_neg.keys():
                    value =  stripHtml( extras.find('img',src='imgs/' + pos_neg[key] +'_hd.png').parent.renderContents() )
                    if not value=='':
                        page[key] =value
            except:
                log.info(self.log_msg("Positive Negative couldn't be extracted"))
        except:
            log.info(self.log_msg('Extra ratings/info/pos/neg couldnt be extracted'))

        # Review data
        try:
            content = review_block.find(id='tabcontent')
            page['data'] = stripHtml(content.renderContents())
        except:
            log.info(self.log_msg('Could not extract data'))
        try:
            helpful = stripHtml ( review_block.find('span', {'style':re.compile('font-size:\s?8pt;\s?padding-left:\s?15px;\s?')}).renderContents() )
            helpful_match = re.search(r'\((\d+)-(\d+)\)',helpful)
            page['ei_data_recommended_yes'] = int(helpful_match.group(1))
            page['ei_data_recommended_no'] = int(helpful_match.group(2))
        except:
            page['ei_data_recommended_yes'] = page['ei_data_recommended_no'] = 0
            log.info(self.log_msg('could not extract helpful info'))
        return page

    def __extractComments( self, uri ):
        """Extract the comments from the page and return list of page dicts
        """
        comments = self.soup.findAll('div', {'style':re.compile('margin-top:\s?10px;')})[-2]
        comments = comments.findAll( 'div', {'class':'KonaBody'} )
        for comment in comments:
            page = {}
            page['uri'] = uri
            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            # Data
            try:
                page['data'] = stripHtml(comment.find('p').renderContents())
            except:
                log.info(self.log_msg('could not extract comment data'))
                page['data'] = ''
            try:
                if len(page['data']) > 100: #title is set to first 50 characters or the post whichever is less
                    page['title'] = page['data'][:100] + '...'
                else:
                    page['title'] = page['data']
            except:
                log.info(self.log_msg('could not parse title'))
                page['title'] = ''
            # Author
            try:
                page['et_author_name'] = unicode( comment.find('u').string )
            except:
                log.info(self.log_msg('author name couldnt be extracted'))
            try:
                author_link = 'http://consumer.admanya.com/' + comment.find('a')['href']
                page = self.__extractAuthorInfo( page, author_link )
            except:
                log.info(self.log_msg("Author doesn't have a profile link"))
            # Date
            try:
                posted_date = re.search(r'(\d+/\d+/\d+)', comment.find('span').renderContents()).group(1)
                page['posted_date'] = datetime.strftime(datetime.strptime(posted_date,'%d/%m/%y')\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception( self.log_msg( 'posted date data not found') )
                page['posted_date'] = page['pickup_date']
            try:
                comment_hash = get_hash( page )
                if checkSessionInfo(self.genre, self.session_info_out,comment_hash, self.task.instance_data.get('update'),parent_list=[self.parenturi, page['uri'] ] ):
                    log.info(self.log_msg ('Session info returned True, cannot proceed' ) )
                    continue
                result=updateSessionInfo(self.genre, self.session_info_out, comment_hash , comment_hash ,'Comment', self.task.instance_data.get('update'),parent_list=[self.parenturi, page['uri'] ])
                if not result['updated']:
                    log.info(self.log_msg('result[update] return false, cannot proceed'))
                #page['id']=result['id']
                #page['first_version_id'] = result['first_version_id']
                #page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                parent_list = [self.parenturi , page['uri'] ]
                page['parent_path'] = parent_list
                parent_list.append( comment_hash )
                page['path'] = parent_list
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['last_updated_time'] = page['pickup_date']
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                page['client_name'] = self.task.client_name
                page['versioned'] = False
                page['uri_domain'] = urlparse.urlparse(uri)[1]
                page['task_log_id']=self.task.id
                page['entity'] = 'Comment'
                page['category'] = self.task.instance_data.get('category' ,'')
                self.pages.append(page)
                log.info(page)
                log.info(self.log_msg ( ' Comment added'))
            except:
                log.exception(self.log_msg('Error with exception') )

    def __extractAuthorInfo( self, page, author_link ):
        """Goto the author profile link and pick the author details and return page dict
        """
        log.info(self.log_msg ("Author Link: " + author_link) )
        resp = self._getHTML( uri=author_link )
        data = resp['result']
        soup = BeautifulSoup( data )
        author_info = soup.find(id= 'bk-div1' ).findAll('span')
        try:
            page['et_author_name'] = stripHtml( author_info[0].renderContents() )
        except:
            log.info( self.log_msg ( 'author name not found' ) )
        try:
            info_match = re.search(r'(?P<age>\d+),&nbsp;(?P<gender>(fe)?male)<br />\r\n(?P<occupation>[A-z0-9_ ]+)<br />\r\n(?P<location>[A-z0-9_ ]+)\r\n',author_info[1].renderContents()).groupdict()
            page['ei_author_age'] = int(info_match['age'])
            page['et_author_gender'] = info_match['gender']
            page['et_author_location'] = info_match['location']
            page['et_author_occupation'] = info_match['occupation']
        except:
            log.info( self.log_msg ('author other info not found' ) )
        return page

    def __getRating( self, images ):
        """Given a list of image tags, find out the rating and return an integer
        """
        return len([i for i in images if 'rate_star_1' in i['src']])
