'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

# SKumar

import logging
import re
import copy
from datetime import datetime,timedelta
from urllib2 import urlparse
import cgi

from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.urlnorm import normalize
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log=logging.getLogger('MyThreeCentsConnector')
class MyThreeCentsConnector(BaseConnector):
    """A Connector for my3cents connector
    """

    @logit(log,'fetch')
    def fetch( self ):
        """
        It fetches the Title,Autho info(If found), Review and comments,which are
        found in the same page
        """
        try:
            self.genre = "Review"
            self.base_uri = 'http://www.my3cents.com'
            self.parenturi = self.currenturi
            self.product_review = False
            if self.currenturi.startswith('http://www.my3cents.com/productReview.cgi'):
                self.product_review = True
            if not self._setSoup():
                return False
            self.__getParentPage()
            while True:
                #reviews_links = list(set([ self.base_uri + href['href'] for href in  self.soup.findAll('a',href = re.compile(r'(/showReview.cgi\?id=.+?)'))]))
                if self.product_review:
                    reviews_links = list(set([ self.base_uri + href['href'] for href in  self.soup.find('div','KonaBody').findAll('a',href = re.compile(r'(/showReview.cgi\?id=.+?)'))]))
                else:
                    reviews_links = list(set([ self.base_uri + href['href'] for href in  self.soup.findAll('a',href = re.compile(r'(/showReview.cgi\?id=.+?)'))]))
                #reviews_links = ['http://www.my3cents.com/showReview.cgi?id=52763']
                #reviews_links = ['http://www.my3cents.com/showReview.cgi?id=58094']# To do Remove
                parent_soup = copy.copy(self.soup)
                for review_link in reviews_links[:]:
                    self.currenturi = review_link
                    if not self._setSoup():
                        continue
                    self.__addReview(review_link)
                    if self.task.instance_data.get('pick_comments'):
                        self.__addComments(review_link)
                try:
                    """count +=1
                    if count>2:
                        break"""
                    self.currenturi =  'http://www.my3cents.com' + parent_soup.find('a','medbld',text=re.compile('Next[ ]*\d+')).parent['href']
                    if not self._setSoup():
                        break
                except:
                    log.exception(self.log_msg('Next Page link not found'))
                    break
            return True
        except:
            log.exception(self.log_msg('Erro in fetch'))
            return False

    @logit(log,'_getParentPage')
    def __getParentPage(self):
        """
        It appends the information about the parent page, excecuted once at
        beginning of fetch method
        """
        if checkSessionInfo( self.genre, self.session_info_out, self.parenturi, \
                                                self.task.instance_data.get('update') ):
            return False
        page= {'title':''}
        try:
            if not self.product_review:
                page['title'] = stripHtml( self.soup.find('h3').find('i').renderContents() )
            else:
                title_str = stripHtml(self.soup.find('h2').renderContents()).strip()
                title_str = re.sub( 'Reviews$', '', title_str ).strip()
                if title_str.endswith('-'):
                    title_str = title_str[:-1]
                page['title'] = title_str
        except:
            log.exception( self.log_msg('could not parse page title') )
        try:
            post_hash = get_hash( page )
            id = None
            if self.session_info_out == {}:
                id = self.task.id
            result = updateSessionInfo( self.genre, self.session_info_out, self.parenturi,\
                             post_hash,'Post', self.task.instance_data.get('update'), Id=id )
            if not result[ 'updated' ]:
                return False
            page['path']=[self.parenturi]
            page['parent_path']=[]
            #page['id'] = result['id']
            #page['first_version_id']=result['first_version_id']
            page[ 'uri' ] = normalize(self.currenturi)
            page[ 'uri_domain' ] = unicode( urlparse.urlparse( page[ 'uri' ] )[1] )
            page[ 'priority' ] = self.task.priority
            page[ 'level' ] = self.task.level
            page[ 'pickup_date' ] = datetime.strftime( datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ" )
            page[ 'posted_date' ] = datetime.strftime( datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ" )
            page[ 'connector_instance_log_id' ] = self.task.connector_instance_log_id
            page[ 'connector_instance_id' ] = self.task.connector_instance_id
            page[ 'workspace_id' ] = self.task.workspace_id
            page[ 'client_id' ] = self.task.client_id
            page[ 'client_name' ] = self.task.client_name
            page[ 'last_updated_time' ] = page[ 'pickup_date' ]
            page[ 'versioned' ] = False
            page[ 'data' ] = ''
            page[ 'task_log_id' ] = self.task.id
            page[ 'entity' ] = 'Post'
            page[ 'category' ] = self.task.instance_data.get( 'category', '' )
            self.pages.append( page )
            log.info('Parent page added')
        except:
            log.exception(self.log_msg("Parent page could not be posted "))
            return False

    @logit(log,'_addReview')
    def __addReview(self, review_link):
        """
        It will add the Review and its info in the current uri
        """
        if checkSessionInfo(self.genre, self.session_info_out, review_link, \
                        self.task.instance_data.get('update'), parent_list=[self.parenturi]):
            log.info(self.log_msg('session info return True'))
            return False
        page = {}
        page['uri'] = normalize( review_link )
        try:
            title_str  = stripHtml(self.soup.find('h3').renderContents().strip())
            #page['title'] = stripHtml(self.soup.find('h3').renderContents().strip())
            try:
                title_str = re.sub('([A-Za-z]&[A-Za-z])(;)',r'\1',title_str)
            except:
                log.info(self.log_msg('cannot replace & and ;'))
            page['title'] = title_str
        except:
            page['title'] = ''
            log.exception(self.log_msg("Could not initialized Title"))
        try:
            page['ei_product_recommended_yes'] = int(re.search('^\d+\s*',stripHtml(self.soup.find(True,text='Helpful Votes').findParent('table').renderContents())).group().strip())
        except:
            log.info(self.log_msg('Help ful votes not found'))
        try:
            loc_str = stripHtml( self.soup.find('i').__str__())
            try:
                page[ 'et_author_name' ] = stripHtml( self.soup.find('i').findNext('a')\
                                                                    .renderContents() )
            except:
                log.exception('author name not found')
            try:
                if not loc_str.rfind('Location:') == -1:
                    page['et_author_location' ] = (loc_str[loc_str.rfind('Location:')+\
                                                            len('Location:'):]).strip()
            except:
                log.exception('page[et_author_location] is not initialized')
            try:
                page[ 'posted_date' ] = datetime.strftime( datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ" )
                date_pattern = re.compile( '(on\s*)(\d+/\d+/\d+)', re.DOTALL)
                mat = re.search( date_pattern, loc_str )
                if mat:
                    try:
                        date_str=mat.group(2).strip()
                        page['posted_date'] = datetime.strftime(datetime.strptime(date_str,\
                                                        '%m/%d/%Y'),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.exception('pblm in getting posted date posteddate')
                else:
                    log.info('Error in parsing the post information(user name,Posted Date,\
                                                                                Location)')
            except:
                log.exception('error in posting posted details')
        except:
            log.exception('Author not found or some un expected exception ')

        parent_uri = self.currenturi
        parent_soup = copy.copy( self.soup )
        try:
            self.currenturi = self.base_uri + self.soup.find( 'td', { 'id':'authorTable' } ).\
                                                                    findNext('a')['href']
            self._setSoup()
            author_page = self._getAuthorInfo()
            log.info('fetched complete author info ')
            page.update( author_page )
        except:
            log.exception('not fetched complete author info ')
        self.currenturi = parent_uri
        self.soup = copy.copy ( parent_soup )
        next_page_found=False
        review_str=''
        # Start
        review_div_str = ''
        data_str = ''
        parent_soup = copy.copy(self.soup)
        review_uri = self.currenturi
        while True:
            try:
                review_div = self.soup.find('div','KonaBody')
                review_div_str = review_div_str + review_div.__str__()
                tables = review_div.findAll('table')
                for table in tables:
                    review_div_str = review_div_str.replace(table.__str__(),'')
                try:
                    next_page_div = review_div.find( 'center' )
                    next_page_div_str = next_page_div.__str__()
                    review_div_str = review_div_str.replace(next_page_div_str, '')
                    self.currenturi = self.base_uri + next_page_div.find('a', text = 'Next Page >').findPrevious('a')['href']
                    if not self._setSoup():
                        break
                except:
                    break
            except:
                log.exception(self.log_msg('Data cannot be found'))
                break
        try:
            review_div_str = re.sub('([A-Za-z]&[A-Za-z])(;)',r'\1',review_div_str)
        except:
            log.info(self.log_msg('cannot replace & and ;'))
        page['data'] = stripHtml(review_div_str)
        # End
        #page['data'] = self.__getCompleteReview()
        self.soup = copy.copy( parent_soup )
        self.currenturi = parent_uri
        try:
            review_hash = get_hash( page )
            result = updateSessionInfo(self.genre, self.session_info_out, review_link , review_hash, "Review", \
                            self.task.instance_data.get('update'), parent_list=[self.parenturi])
            if not result['updated']:
                log.info(self.log_msg('Result not updated'))
                return False
            #page['id'] = result['id']
            #page['first_version_id']=result['first_version_id']
            #page['parent_id']= '-'.join(result['id'].split('-')[:-1])
            parent_list = [ self.parenturi ]
            page['parent_path'] = copy.copy(parent_list)
            parent_list.append( review_link )
            page['path']=parent_list
            page['priority']=self.task.priority
            page['level']=self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            page[ 'task_log_id' ] = self.task.id
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            page['client_name'] = self.task.client_name
            page['last_updated_time'] = page['pickup_date']
            page['versioned'] = False
            page['entity'] = "Review"
            page['category'] = self.task.instance_data.get('category','')
            page['uri_domain'] = urlparse.urlparse( page['uri'] )[1]
            self.pages.append(page)
            log.info('Review Added')
            return True
        except:
            log.exception(self.log_msg("Exception in add session info"))

    def __addComments(self, review_link):
        try:
            comment_table = self.soup.find('h3',text = 'User Discussion - ').findNext('table')
            comment_sec = comment_table.findAll('table')
            for comment in comment_sec:
                page = {}
                page['uri'] = self.currenturi
                page['title'] = ''
                complete_comment = comment.parent
                comment_str = stripHtml( comment.parent.renderContents() )
                info_pattern = re.compile('^(.*)(\(\d+/\d+/\d+\))')
                info_match = info_pattern.match( comment_str )
                if info_match and not comment_str.strip() == '':
                    page['et_author_name'] = info_match.group(1)
                    date_str = info_match.group(2)
                    date_str = date_str.replace('(','')
                    date_str = date_str.replace(')','')
                    page['posted_date'] =  datetime.strftime(datetime.strptime(date_str,\
                                                        '%m/%d/%Y'),"%Y-%m-%dT%H:%M:%SZ")
                    user_comment = comment_str.replace( info_match.group(), '').strip()
                    if not user_comment.endswith('more >>'):
                        page['data'] = user_comment
                    else:
                        try:
                            comment_previous_soup = copy.copy(self.soup)
                            comment_previous_uri = self.currenturi
                            self.currenturi = ( self.base_uri + complete_comment.find('a',\
                                            {'href':re.compile(r'/showComment.cgi\?cid.*')})['href'])
                            self._setSoup()
                            full_comment = self.soup.find('a',href=re.compile('/userBlog.cgi\?id.*?')\
                                                                                    ).findNext('td')
                            if full_comment:
                                full_comment_str = stripHtml(full_comment.renderContents())
                                page['data'] = full_comment_str
                            self.soup = copy.copy(comment_previous_soup)
                            self.currenturi = comment_previous_uri
                        except:
                            log.exception('Problem in fetching comments in the other page')
                    try:
                        comment_hash = get_hash( page )
                        commnet_unique_key = get_hash( {'data':page['data'],'title':page['title']})
                        if not checkSessionInfo(self.genre, self.session_info_out, commnet_unique_key, \
                                self.task.instance_data.get('update'), parent_list=[ self.parenturi, \
                                                                                 review_link ]):
                            result = updateSessionInfo(self.genre, self.session_info_out, commnet_unique_key ,comment_hash,\
                                "Comment",self.task.instance_data.get('update'), parent_list=\
                                                        [  self.parenturi ,review_link ] )
                            if result['updated']:
                                """page['id'] = result['id']
                                page['first_version_id']=result['first_version_id']
                                page['parent_id']= '-'.join(result['id'].split('-')[:-1])"""
                                parent_list = [  self.parenturi ,review_link ]
                                page['parent_path'] = copy.copy(parent_list)
                                parent_list.append( commnet_unique_key )
                                page['path']=parent_list
                                page[ 'task_log_id' ] = self.task.id
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
                                page['entity'] = "Comment"
                                page['category'] = self.task.instance_data.get('category','')
                                page['uri_domain'] = urlparse.urlparse( page['uri'] )[1]
                                self.pages.append(page)
                                log.info(self.log_msg('Comment Added'))
                    except:
                        log.exception(self.log_msg('Commment Cannot be added'))
        except:
            log.info(self.log_msg('Comments not found'))

    @logit(log,"getAuthorInfo")
    def _getAuthorInfo(self):
        """This is the method which retrive the info abt the author in the
        author info"""
        log.info("Inside getAuthorInfo ().... go.....")
        page = { }
        author_name =''
        try:
            author_name = stripHtml( self.soup.find('h1').renderContents() )
        except:
            log.exception('author name not found')

        try:

            author_member_since = stripHtml( self.soup.find('td',text='User Rating:').parent.\
                parent.findPrevious('tr').findNext('td').findNext('td').renderContents()).strip()

            page[ 'edate_author_member_since' ] = datetime.strftime(datetime.strptime\
                                (author_member_since, '%m/%d/%Y'), "%Y-%m-%dT%H:%M:%SZ")

        except:
            log.exception('member since data is not found')

        try:
             aut_rat = float(len(filter(lambda img:img['src'].endswith('star.gif'),\
                            self.soup.find('td',text='User Rating:').parent.parent.findAll('img'))))
             if not aut_rat == 0.0:
                page[ 'ef_author_rating' ] = aut_rat
        except:
            log.exception('Could not find author rating')
        try:
            trusted_by_td = self.soup.find('td', text = re.compile( author_name + ' is trusted by ') )
            if trusted_by_td:
                trusted_by = trusted_by_td.parent.findNext( 'b' ).renderContents()
                trusted_by_user = re.search( '\d+', trusted_by ).group()
                page[ 'ei_author_recommendation' ] = int( trusted_by_user )
            else:
                log.info('No ei_author_recommendation is found ')
        except:
            log.exception("Could not be retrieved,'author_name' is trusted by 'n' users")

        try:
            trusts_td = self.soup.find('td', text = re.compile( author_name + ' trusts ' ) )
            if trusts_td:
                trusts = trusts_td.parent.findNext( 'b' ).renderContents()
                trusts_user = re.search( '\d+', trusts ).group()
                page[ 'ei_author_recommends' ] = int ( trusted_by_user )
            else:
                trusts_user = 'No one'
        except:
            log.exception("Could not be retrieved,'author_name' is trusts 'n' users")

        try:
            author_total_reviews_td = stripHtml( self.soup.find( 'h4', text = re.compile\
                                                        ("Read all my reviews \(\d+\)") ) )

            author_total_reviews = re.search( '\d+', author_total_reviews_td ).group()
            page[ 'ei_author_reviews_count' ] = int ( author_total_reviews )
        except:
            log.info('author_total_reviews is not found ')
        try:
            author_total_comments_td = stripHtml( self.soup.find( 'h4', text = re.compile\
                                                        ("Read all my comments \(\d+\)") ) )
            author_total_comments = re.search( '\d+', author_total_comments_td ).group()
            page[ 'ei_author_comments_count' ] = int ( author_total_comments )
        except:
            log.info('author_total_comments is not found ')
        return page

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