
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar
# This site has blocked out ip, when we did the first crawling
#prerna


import re
import md5
import copy
import logging
from urlparse import urlparse
from datetime import datetime
from BeautifulSoup import BeautifulSoup
from utils.utils import stripHtml
from utils.decorators import logit
from utils.urlnorm import normalize
from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log=logging.getLogger('ViewPointsConnector')
class ViewPointsConnector(BaseConnector):
    
    @logit (log, "fetch")
    def fetch(self):
        """
        same fetch method, I need to write something for doc string
        So I m writing this doc string
        """
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
        self.genre = "Review"
        self.parenturi = self.currenturi
        self._setSoup()
        hrefs = self._getReviewUris()
        log.info('Total # of links returned is %d'%len(hrefs))
        for href in hrefs[:]:
            self.currenturi = href
            self._setSoup()
            self._addReview()
            #self._addComments()
        return True

    @logit( log, "_addReview")
    def _addReview(self):
        """
        This will add the Reviews and comments found on this page
        """
        try:
            page = {}
            page['uri'] = self.currenturi
            try:
                page['title'] = stripHtml( self.soup.find('div','quote float_left padding_5_bottom_IE').renderContents())
                log.info(page['title'])
            except:
                log.exception('pblm in getting title')
                page['title'] = ''
            try:
                date_str = stripHtml(self.soup.find('em','date_plain').renderContents()).split('Posted on')[-1].strip()
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str,"%b %d, %Y"),"%Y-%m-%dT%H:%M:%SZ")
                log.info(page['posted_date'])
            except:
                log.exception('posted date not found')
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            try:
                page['et_data_pros'] = stripHtml( self.soup.find('strong',text='Pros').findNext('ul').renderContents())
            except:
                log.exception('data Pros is not found')
            try:
                page['et_data_cons'] = stripHtml( self.soup.find('strong',text='Cons').findNext('ul').renderContents())
            except:
                log.exception('data Cons is not found')
            try:
                page['ef_product_rating_overall'] = float(self.soup.find('div','rating_container rating_large').div['class'].rsplit('_',1)[1])
            except:
                log.exception('Product overall rating not found')
            try:
                page[ 'ei_data_vote'] = int(stripHtml(self.soup.find('span','helpful_count').renderContents() ).split('(')[-1].split(')')[0])
            except:
                log.exception('vote is not found')
            try:
                div_str = self.soup.find('div','KonaBody').__str__()
                div_sub_str = self.soup.find('div','date').__str__()
                page['data'] =  stripHtml( div_str.replace(div_sub_str , '') )
            except:
                log.exception('data is not found')
                page['data']=''
            try:
                page['et_author_name'] =  stripHtml( self.soup.find('a','user_info_name').renderContents() )
            except:
                log.exception('author name is not found')
            try:
                page['et_author_location'] =  stripHtml( self.soup.find('span','user_info_location').renderContents() )
            except:
                log.exception('author location is not found')
            if not checkSessionInfo(self.genre, self.session_info_out, self.currenturi ,self.task.instance_data.get('update'), parent_list=[ self.parenturi ]):
                try:
                    log.info(page.values())
                    review_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , page.values()))).encode('utf-8','ignore')).hexdigest()
                except UnicodeDecodeError,e:
                    
                    review_hash = md5.md5(''.join(sorted(self.cleanUnicode(map(lambda x: str(x) if isinstance(x,(int,float)) else x , page.values() )))).encode('utf-8','ignore')).hexdigest()
                except:
                    log.exception("Error with review hash")
                    return False
                result=updateSessionInfo(self.genre, self.session_info_out, self.currenturi , review_hash,'Review', self.task.instance_data.get('update'), parent_list=[self.parenturi])
                if result['updated']:
                    page['parent_path'] = [self.task.instance_data['uri']]
                    page['path'] =  [self.task.instance_data['uri']]
                    page['uri'] = self.currenturi 
                    page['entity'] = 'review'
                    page['uri_domain']  = urlparse(page['uri'])[1]
                    page.update(self.__task_elements_dict)
                    #log.info(page)
                    self.pages.append(page)
                    log.info('review page added')
                    return True
            else:
                log.info('Check session info return True')
        except:
            log.exception('error in addreview')

    @logit( log, "getReviewUris")
    def _getReviewUris(self):
        """
        It will return the Review uris
        """
        review_hrefs = [ ]
        
        while True:
            try:
                review_hrefs = review_hrefs + [ ahref.find('a')['href'] for ahref in  self.soup.findAll('div','quote')]
                self.currenturi = 'http://www.viewpoints.com' +  re.search("'(.*?)'", BeautifulSoup(self.soup.find('input',value='Next').__str__()).input['onclick'].split('=',1)[1].strip().split('return false')[0]).group(1)                    
                self._setSoup()
            except:
                log.exception('Got all retview urls')
                break
    
        log.info('Total no of uris found is break %d'%(len(review_hrefs)))
        return list(set(review_hrefs))
        
    @logit(log, "_checkSiteUri")
    def _checkSiteUri(self):
        """
        This will return the uri which is sorted by date,
        if it is not sorted by date
        """
        try:
            site_uri = self.currenturi
            uri_pattern1 = 'http://www.viewpoints.com/.+?-reviews'
            if re.compile( uri_pattern1 ).match( self.currenturi ):
                return True
            else:
                return False
        except:
            log.exception('Pblm with check site uri ')
            return False
##        uri_pattern1 = r'http://www.viewpoints.com/.+?-reviews?sort=date-1'
##        elif re.compile( uri_patterrn2 ).match( self.currenturi ):
##            return site_uri + '?sort=date-1'
##        else:
##            return False

    @logit (log, '')
    def cleanUnicode(self,text_list=[]):
        """
        Clean the given text of common unicode character which arent convertable by
        pyExcelerator
        """
        result_list = []
        repl_dict = {'\xe2\x80\xa6': '...',
            '\xe2\x80\x98': "'",
            '\xe2\x80\x99': "'",
            '\xe2\x80\x9c': '"',
            '\xe2\x80\x9d': '"',
            '\xe2\x80\xb3': '"',
            '\xe2\x80\x93': '-',
            }
        for text in text_list:
            regex = re.compile("(%s)" % "|".join(map(re.escape, repl_dict.keys())))
            replaced_text = regex.sub(lambda mo: repl_dict[mo.string[mo.start():mo.end()]], text)
            replaced_text = re.sub('[\x00-\x1F\x7F-\xFF]+', ' ', replaced_text)
            result_list.append(replaced_text)
        return result_list
    
    @logit(log, '_setSoup')
    def _setSoup( self ):
        """
            It will set the uri to current page, written in seperate
            method so as to avoid code redundancy
        """
        try:
            log.info( 'for uri %s'%( self.currenturi ) )
            res = self._getHTML()
            if res:
                self.rawpage = res[ 'result' ]
            else:
                log.info('self.rawpage not set.... so Sorry..')
                return False
            self._setCurrentPage()
            return True
        except Exception, e:
            log.exception(self.log_msg('could not set the page as current page :%s'%uri))
            raise e
    
    ##    @logit( log, "_setParentPage")
##    def _setParentPage(self):
##        """
##        This will set the parent page details and add it to session info,
##        Same thing, need to be written for all connectors
##        """
##        page = {}
##        page [ 'uri' ] = self.parenturi
##        try:
##            product_title = stripHtml( self.soup.find('h1').renderContents() )
##            page [ 'title' ] = re.sub( 'Reviews$', '', product_title ).strip()
##        except:
##            log.exception('title not found, bad desing')
##            page [ 'title' ] = ''
##        try:
##             page[ 'ei_prodcut_review_count'] = int ( self.soup.find('span',\
##                                            'text_black text_bold').renderContents() )
##        except:
##            log.exception(' total reviews are not found')
##            
##        try:
##             page [ 'ef_product_rating_overall' ] =  float ( self.soup.find('span',\
##                                            'text_bold text_black').renderContents() )
##        except:
##            log.exception(' ef_product_rating_overall is not found')
##        try:
##             page[ 'data' ] =stripHtml ( self.soup.find('div',{'id':'product_seo'}).renderContents() )
##        except:
##            page['data'] = ''
##            log.exception(' data are not found')
##        num_text = {'1':'one', '2':'two','3':'three','4':'four','5':'five'}
##        soup.find('img',{'alt':re.compile('as 5 stars%$') }).findNext('td')
##        for number in num_text:
##            try:
##                star_pattern = 'as ' + number + ' star(s)?%$'
##                page_key = 'ei_product_rated_' + num_text[ number ] + '_star_percentage'
##                page[ page_key ] =  int ( self.soup.find('img',{'alt':re.compile(star_pattern)}).findNext('td').renderContents().replace('%','') )
##                log.info( page[ page_key ] )
##            except:
##                log.exception(' ratings starts are not found')
##        try:
##            page['ei_product_recommended_yes_percentage'] = int (  stripHtml( self.soup.find('span',{'title':re.compile('^(\d+% of people|Everyone) recommend')}).renderContents()).replace('%','') )
##            if page['ei_product_recommended_yes_percentage'] == 100:
##                page['ei_product_recommended_no_percentage'] =0
##                
##        except:
##            log.exception(' total ei_product_recommended_yes_percentage is not found')
##        try:
##            page['ei_product_recommended_no'] = int (stripHtml(self.soup.find('span',{'title':re.compile('^\d+% of people don\'t recommend')}).renderContents()).replace('%','') )
##        except:
##            log.exception(' total ei_product_recommended_yes is not found')
##        try:
##            page['et_product_favorable_review'] = self.soup.find('span',text='Favorable Review').findNext('a')['href']
##        except:
##            log.exception(' total et_product_favorable_review is not found')
##        try:
##            page['et_product_critical_review'] = self.soup.find('span',text='Critical Review').findNext('a')['href']
##        except:
##            log.exception(' total et_product_favorable_review is not found')
##        self.currenturi = self.currenturi + '?sort=date-1'
##        self._setSoup()
##        pros_cons={'product_Pros_tags_div':'et_product_pros','product_Cons_tags_div':'et_product_cons'}
##        for pro_con in pros_cons:
##            page_key = pros_cons[ pro_con ]
##            try:
##                pros_div = self.soup.find('div',{'id':pro_con}).findAll('a')
##                pros_str = ''
##                for pro in pros_div:
##                    pros_str = pros_str + re.sub('\(\d+\)','', stripHtml ( pro.renderContents() ) ).strip() + '\n'
##                if not pros_str == '':
##                    page[ page_key ] = pros_str
##            except:
##                log.exception('pblm wiht pros')
##        log.info( page )
##        try:
##            post_hash=  md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
##                                                                                page.values()))).encode('utf-8','ignore')).hexdigest()
##        except:
##            log.debug("Error Occured while making parent post hash, Not fetching the parent page data")
##            return False
##        try:
##            if not checkSessionInfo(self.genre, self.session_info_out, self.parenturi, self.task.\
##                                                                        instance_data.get('update')):
##                id = None
##                if self.session_info_out=={}:
##                    id = self.task.id
##                    log.debug(id)
##                result = updateSessionInfo( self.genre, self.session_info_out, self.parenturi, \
##                                post_hash,'Post', self.task.instance_data.get('update'), Id=id )
##                if result['updated']:
##                    page['id'] = result['id']
##                    page['first_version_id'] = result['first_version_id']
##                    page['task_log_id'] = self.task.id
##                    page['versioned'] = self.task.instance_data.get('versioned',False)
##                    page['category'] = self.task.instance_data.get('category','generic')
##                    page['last_updated_time'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
##                    page['client_name'] = self.task.client_name
##                    page['entity'] = 'post'
##                    page['uri'] = normalize(self.parenturi)
##                    page['uri_domain'] = unicode(urlparse(self.parenturi)[1])
##                    page['priority'] = self.task.priority
##                    page['level'] = self.task.level
##                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
##                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
##                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
##                    page['connector_instance_id'] = self.task.connector_instance_id
##                    page['workspace_id'] = self.task.workspace_id
##                    page['client_id'] = self.task.client_id
##                    self.pages.append(page)
##                    log.info(page)
##                    log.info( "Parent Page added" )
##                    return True
##                else:
##                    log.info("Parent page not added")
##                    return False
##            else:
##                log.info(("product review main page details NOT stored"))
##                return False
##        except:
##            log.exception('error with session info')
    
        
