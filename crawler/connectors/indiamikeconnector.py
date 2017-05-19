
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Read python code written by big pythoniasts from net
#- concise code

#SKumar
# modified log messages and removing unnecessay log exceptions and verified
#Modified Solr fields , suggested by ashish

#Modified After the code review(Dec 03, 2008 )on 4 Dec, 2008
"""
Modification
--Unused import,_getReviewUris,_getStateReviewUris,_getHotelUris methods removed
-- doc string modified, _setParentPage to _getParentPage
-- Rating has been done, Dynamically
-- Author info has been done dynamically , except instant_messagin, homepage,
    join data , post count ( bcoz, they are differnt format)
--  removed unused codes and redundant variable

"""
import re
import logging
import copy
from urllib2 import urlparse
from datetime import datetime
from BeautifulSoup import BeautifulSoup

from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.urlnorm import normalize
from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log=logging.getLogger('IndiaMikeConnector')

class IndiaMikeConnector(BaseConnector):
    """A Connector for www.inidamike.com
    """

    @logit( log, "fetch" )
    def fetch(self):
        """same fetch method, nothing to write as doc string
        """
        try:
            self.currenturi ='http://www.indiamike.com/india-hotels/'
            self.baseuri='http://www.indiamike.com'
            self.genre="Review"
            if re.compile( r'http://www.indiamike.com/india-hotels/.+?-h\d+/').\
                                                       match( self.currenturi ):
                self.parenturi = self.currenturi
                self._setSoup()
                self._getParentPage()
                hrefs_res = list( set([self.baseuri + each['href'] for each in \
                           self.soup.findAll('a', { 'class': 'redlink', 'href':\
                              re.compile( r'/india-hotels/reviews/.*?') } )] ) )
                for href in hrefs_res:
                    self.currenturi = href
                    self._setSoup()
                    self._addReview()
                return True
            elif self.currenturi == 'http://www.indiamike.com/india-hotels/':
                if not self._setSoup():
                    return False
                state_hrefs = [ self.baseuri + each['href'] for each in self.\
                                soup.find('div','citystatehotels').findAll('a')]
                task_hrefs = [ ]
                for state_href in state_hrefs:
                    self.currenturi = state_href + '?sort=rating'
                    if not self._setSoup():
                        continue
                    while True:
                        task_hrefs = task_hrefs + [ self.baseuri + div.findNext\
                                 ( 'a' )[ 'href' ] for div in self.soup.findAll\
                                                             ('div','hoteldiv')]
                        try:
                            next_page_href = self.soup.find('a',\
                                            text = 'Next&raquo;').parent['href']
                        except:
                            log.info( self.log_msg( 'Next not found uri' ) )
                            break
                        self.currenturi = state_href + next_page_href
                        if not self._setSoup():
                            break
                for href in task_hrefs:
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] = normalize( href )
                    self.linksOut.append( temp_task )
                log.info(self.log_msg ('Total task are %d'%(len( task_hrefs ))))
                return True
        except:
            log.exception(self.log_msg('There is some problem with fetch'))
            return False

    @logit( log, "addReview" )
    def _addReview(self):
        """
        This is Same as the other connector has, which will add the review of
        the current page
        """
        page = {}
        try:
            try:
                page ['posted_date'] = datetime.strftime(datetime.utcnow(),\
                                                           "%Y-%m-%dT%H:%M:%SZ")
                aut_str = stripHtml( self.soup.find( 'div','byline' )\
                                                             .renderContents() )
                mat = re.search( 'by\s*(.*?)\s*on\s*(.*?)\.(.*)', aut_str, \
                                                                     re.DOTALL )
                if mat:
                    author_name = page ['et_author_name']  =mat.group(1).strip()
                    try:
                        page [ 'posted_date' ] = datetime.strftime(datetime.\
                                    strptime(mat.group(2).strip(),"%b %d, %Y"),\
                                                           "%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.exception('pblm in posted_date')

                    if re.match( author_name + '\s*recommends this hotel.', \
                                                        mat.group(3).strip() ):
                        page[ 'et_product_recommended_yes' ] = 'yes'
                else:
                    log.info('author name,posted date and info not found')
            except:
                log.info(self.log_msg( 'Pblm with parsing author author info') )
            for each in ['Overall','Location','Staff','Cleanliness']:
                try:
                    page[ 'ef_rating_' + each.lower() ] = float (self.soup.find\
                              ('div','hoteldiv').find('b',text= each ).findNext\
                                            ('img')['src'].split('/')[-1][:-6] )
                except:
                    log.info('rating is not found')
            positives = ''
            negatives = ''
            try:
                positives = page[ 'et_data_pros' ] = self.soup.find( 'div', \
                         'hoteldiv').find('b',text='Positives').next[1:].strip()
            except:
                log.info(self.log_msg ('No Positives are found') )
            try:
                negatives = page[ 'et_data_cons' ] = self.soup.find( 'div', \
                         'hoteldiv').find('b',text='Negatives').next[1:].strip()
            except:
                log.info(self.log_msg( 'No Negatives  are found') )
            try:
                div_str = self.soup.find('div','hoteldiv').renderContents()
                replace_list = ['<b>Overall</b>:','<b>Location</b>:','<b>Staff</b>:'\
                                          ,'<b>Cleanliness</b>:','<b>Positives</b>:'\
                                                            ,'<b>Negatives</b>:']
                for each in replace_list:
                    div_str = div_str.replace(each,'',1)
                    
##                div_str= stripHtml( re.sub('(<b>Overall</b>:)|<b>Location</b>:|\
##                           <b>Staff</b>:|<b>Cleanliness</b>:|<b>Positives</b>:|\
##                                   <b>Negatives</b>:','', self.soup.find('div',\
##                                                 'hoteldiv').renderContents()) )
                div_str = div_str.replace(positives.strip(),'',1).strip()
                page['data'] = stripHtml( div_str.replace(negatives.strip(),'',1)\
                                                                        .strip() )
            except:
                log.exception('There is some problem with getting review data')
                page[ 'data' ] = ''
            try:
                page['title'] = stripHtml( self.soup.find( 'h1' )\
                                                              .renderContents())
            except:
                log.exception('title not found')
                page['title'] =''
##            if page['title']=='':
##                if len (page['data']) > 100:
##                    page['title'] = page['data'][:100]
            try:
                page['uri'] = self.currenturi
                self.currenturi = self.baseuri +self.soup.find( 'div','byline')\
                                                          .findNext('a')['href']
                if self._setSoup():
                    page.update( self._getAuthorInfo() )
            except:
                log.info ( self.log_msg( 'pblm in getting  author info' ) )
            if not checkSessionInfo( self.genre, self.session_info_out, page\
                                ['uri'], self.task.instance_data.get('update'),\
                                                  parent_list=[self.parenturi]):
                review_hash = get_hash(page)
                result=updateSessionInfo(self.genre, self.session_info_out,page\
                         ['uri'], review_hash,'Review',self.task.instance_data.\
                                    get('update'), parent_list=[self.parenturi])
                if result['updated']:
                    parent_list = [self.parenturi]
                    page['parent_path'] = copy.copy(parent_list)
                    parent_list.append(page['uri'])
                    page['path'] = parent_list
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),\
                                                           "%Y-%m-%dT%H:%M:%SZ")
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
                    page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                    self.pages.append(page)
                    log.info(self.log_msg('review page added'))
                    return True
            else:
                log.info(self.log_msg('Check session info return True'))
                return False
        except:
            log.exception(self.log_msg( 'Some problem in the addReview method') )
            return False
        

    @logit( log, "_getParentPage" )
    def _getParentPage(self):
        """ This function sets up the values for the parent page
        """
        page = {}
        try:
            head_soup = self.soup.find( 'h1' )
            page['title'] = stripHtml( head_soup.renderContents())
        except:
            log.exception(' problem with finding head of the page')
            page['title'] = '' # data too

        try:
            page ['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                          ,"%Y-%m-%dT%H:%M:%SZ")
            site_details = stripHtml( head_soup.findNext().renderContents() )
            date_mat = re.search ('by(.*?)on(.*?)\. Last updated (.*)$', \
                                                                  site_details )

            if date_mat:
                page [ 'et_author_name' ] = date_mat.group(1).strip()
                date_str = date_mat.group(2).strip()
                try:
                    page ['posted_date'] = datetime.strftime(datetime.strptime\
                                    (date_str,"%b %d, %Y"),"%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.exception('posted data not found')
                date_str = date_mat.group(2).strip()
                page [ 'edate_product_info_last_updated_on' ] = datetime.strftime\
                   (datetime.strptime(date_str,"%b %d, %Y"), "%Y-%m-%dT%H:%M:%SZ")
                log.info( self.log_msg ( 'et_product_info_last_updated_on:  %s'%\
                            (page [ 'edate_product_info_last_updated_on' ] ) ) )
            else:
                log.info ( self.log_msg( 'date is not found as expected, Sorry') )
        except:
            log.exception(self.log_msg('hotel details not found'))
        try:
            hotel_address_str = ''
            t_str = ''
            tag_line_soup = self.soup.find( 'div', 'byline' )
            tag_line_str = stripHtml(tag_line_soup.renderContents() )
            while getattr(tag_line_soup, 'name', None) != 'img':
                if not tag_line_soup.nextSibling == None:
                    hotel_address_str = hotel_address_str +  tag_line_soup.\
                                                           nextSibling.__str__()
                tag_line_soup = tag_line_soup.next
            #reconsider
            hotel_address_soup = BeautifulSoup ( hotel_address_str )
            address_alone = stripHtml( hotel_address_soup.find( 'b' ).\
                                                      renderContents() ).strip()
            hotel_address_str = stripHtml( hotel_address_str ).strip()
            hotel_address_str = hotel_address_str.replace( tag_line_str , '').strip()
            hotel_other_contact_options = hotel_address_str.replace\
                                                   ( address_alone, '' ).strip()
            temp_list = hotel_other_contact_options.split('\n')
            phone = ''
            e_mail = ''
            website = ''
            for temp in temp_list:
                if temp.find('phone:') != -1:
                    matc = re.search('phone:(.*$)', temp)
                    if matc:
                        phone = matc.group(1).strip()
                if temp.find('e-mail:') != -1:
                    matc = re.search('e-mail:(.*$)', temp)
                    if matc:
                        e_mail = matc.group(1).strip()
                if temp.find('website:') != -1:
                    matc = re.search('website:(.*$)', temp)
                    if matc:
                        website = matc.group(1).strip()
            if not phone == '':
                page[ 'et_product_phone' ] = phone.strip()
            if not e_mail == '':
                page[ 'et_product_e-mail' ] =e_mail.strip()
            if not website == '':
                page [ 'et_product_website' ] = website.strip()
            if not address_alone == '':
                page [ 'et_product_address' ] = address_alone
        except:
            log.info( self.log_msg( 'exception in parsing hotel address string' ) )
        try:
            page['et_product_desc'] = stripHtml(self.soup.find('img','thumbnail')\
                                                            .findNext().previous )
        except:
            log.info( self.log_msg( 'There may not be found hotel desc') )

        try:
            hotel_rooms_rate_info_str = ''
            hotel_rooms_rate_info_tag = self.soup.find('h2').findNext()
            while getattr(hotel_rooms_rate_info_tag, 'name', None) != 'h2' :
                if not hotel_rooms_rate_info_tag ==None:
                    hotel_rooms_rate_info_str = hotel_rooms_rate_info_str + \
                                            hotel_rooms_rate_info_tag.__str__()
                hotel_rooms_rate_info_tag  = hotel_rooms_rate_info_tag.next
            if not hotel_rooms_rate_info_str == '':
                page[ 'et_product_rooms_rate_info' ] = stripHtml( hotel_rooms_rate_info_str )
        except:
            log.info(self.log_msg( 'may some error, in getting hotel room info') )

        try:
            post_hash = get_hash(page)
            if not checkSessionInfo(self.genre, self.session_info_out, \
                        self.currenturi, self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result = updateSessionInfo( self.genre, self.session_info_out, \
                                        self.currenturi, post_hash,'Post',self.\
                                        task.instance_data.get('update'), Id=id)
                if result['updated']:
                    page['path']=[self.currenturi]
                    page['parent_path']=[]
                    page['uri'] = normalize(self.currenturi)
                    page['uri_domain'] = 'www.indiamike.com'
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                                ,"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.\
                        task.connector_instance_log_id
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
                    log.info(self.log_msg( 'parent page added ') )
        except:
            log.exception(self.log_msg('Some problem with posting the parent page'))

    @logit(log, "setSoup")
    def _setSoup( self ):
        """
            It will set the uri to current page, written in seperate
            method so as to avoid code redundancy
        """
        try:
            log.info( 'for uri %s'%(self.currenturi) )
            res = self._getHTML()
            if res:
                self.rawpage = res[ 'result' ]
            else:
                log.info('self.rawpage not set.... so Sorry..')
                return False
            self._setCurrentPage()
            return True
        except Exception, e:
            log.exception(self.log_msg('can not set the current page :%s'%uri))
            raise e

    @logit(log,"getAuthorInfo")
    def _getAuthorInfo(self):
        """
        This will return the author info found on the current uri
        Splitted into 4 catagries
        1-- Join data and Total post
        2 -Addintional Info
        3 - Home page and Instant messaging
        4 - Forum details
        """
        author_page={}
        try:
            date_str = stripHtml(self.soup.find('div','fieldset').renderContents())
            date_str = date_str[date_str.find(':')+1:].strip()
            date_str = re.sub("(\d+)(st|nd|rd|th)",r'\1',date_str)
            author_page [ 'edate_author_member_since' ] = datetime.strftime(datetime.\
                           strptime(date_str,"%b %d, %Y"), "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg( 'join date not found' ) )
        try:
            post_str =stripHtml(self.soup.find('fieldset','fieldset').findNext\
                                                        ('td').renderContents())
            post_match= re.match('Total Posts:\s*(\d+)\s*\((.*?)\s*posts per day\)'\
                                                                        ,post_str)
            if post_match:
                author_page[ 'ei_author_reviews_count' ] = post_match.group(1)
                author_page[ 'ef_author_reviews_per_day'] = float( post_match.group(2) )
            else:
                log.info(self.log_msg( 'no match is found for total post') )
        except:
            log.exception(self.log_msg( 'post info  is not found') )

        add_info = { 'et_author_location': 'Location',
                     'et_author_dob':'Date of Birth',
                     'et_author_interests': 'Interests',
                     'et_author_occupation': 'Occupation',
                     'et_author_favorite_quote':'Favorite Quote',
                     'et_author_guilty_pleasure':'Guilty Pleasure',
                     'et_author_favorite_music':'Favorite Music',
                     'et_author_favorite_books':'Favorite Books',
                     'et_author_favorite_television_programs':'Favorite Television Programs',
                     'et_author_gender':'Gender',
                     'et_author_about' :'About Me',
                     'et_author_favorite_movies':'Favorite Movies'
                    }
        for each in add_info.keys():
            try:
                temp_str  = stripHtml(self.soup.find('strong',text = add_info\
                                    [each]).findPrevious('td').renderContents())
                author_page [ each ] = temp_str[temp_str.find(':')+1:].strip()
            except:
                log.info('info not found for %s'%each)
        try:
            temp_str = stripHtml( self.soup.find('td',text='Contact Info').parent.\
                  findNext('td','panelsurround').findNext('td','panelsurround')\
                                                  .find('td').renderContents() )
            author_page [ 'et_author_homepage' ] = temp_str[temp_str.rfind(':')+1:].strip()
        except:
            log.info(self.log_msg('Author Home page is not found'))
        try:
            author_page [ 'et_author_instant_messaging' ] = stripHtml( self.soup\
                        .find('legend',text='Instant Messaging').findNext('tr')\
                                                            .renderContents() )
        except:
            log.info( self.log_msg( 'Author instant message is not found' ) )
        return author_page
