
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar
#Ashish
# modified March 23, Due to site changes ( major changes)
# Remove unused import, copy, tgimport, Dec 5, 2008 and getTaskUris()

import re
import logging
from urlparse import urlparse
from datetime import datetime
import cgi
import copy

from utils.utils import stripHtml,get_hash
from utils.decorators import logit
from utils.urlnorm import normalize
from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log=logging.getLogger('NotebookReviewConnector')
class NotebookReviewConnector(BaseConnector):
    """
    for www.notebookreview.com
    url segment is : notebookreview.com, source_type =review
    """
    
    @logit (log, "fetch")
    def fetch(self):
        """
        same fetch method, I need to write something for doc string
        So I m writing this doc string
        """
        try:
##            self.currenturi = 'http://www.notebookreview.com/price/default.asp?productID=0&productFamilyID=592&display=opinionDetail'
##            self.currenturi = 'http://www.notebookreview.com/price/default.asp?productID=0&productFamilyID=448&display=opinionDetail'

            self.parenturi = self.currenturi
            self.genre = "Review"
            self.calling_page_no = 1
            if self.currenturi == 'http://www.notebookreview.com/price/':
                if not self._setSoup():
                    return False
                task_uris = [ ]
                try:
                    brand_list = ['http://www.notebookreview.com/price/'+li_tag.\
                        find('a')['href'] for li_tag in self.soup.findAll('li')]
                    for brand_uri in brand_list:
                        self.currenturi = brand_uri
                        if not self._setSoup():
                            continue
                        model_list = [ 'http://www.notebookreview.com/price/' + \
                                            ahref.find('a')['href'] for ahref in\
                                                         self.soup.findAll('li')]
                        for model_uri in model_list:
                            self.currenturi = model_uri
                            if not self._setSoup():
                                continue
                            ahref_tag = self.soup.find(text='User Opinions')
                            if ahref_tag:
                                task_uris.append ('http://www.notebookreview.com/price/'+ \
                                                                ahref_tag.parent['href'] )
                except:
                    log.exception(self.log_msg('exception in getting task uris'))
                
                log.info(self.log_msg('Total no of task found is %d'%hrefs))
                for href in task_uris:
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] = normalize( href )
                    self.linksOut.append( temp_task )
                return True
            else:
                if not self._setSoup():
                    return False
                self.parenturi = self.currenturi
                self.__getParentPage()
                while True:
                    self.__addReviews()
                    if not self._setNextPage():
                        break
                return True
        except:
            log.exception(self.log_msg('error in fetch '))
            return False

    @logit( log, "__addReviews")
    def __addReviews(self):
        """
        This will add the Reviews and comments found on this page
        """
        reviews = self.soup.find('ul','userRatings').findAll('li')
        for review in reviews:
            page = {}
            try:
                page['title'] = stripHtml( review.find('h6').renderContents() )
            except:
                log.exception(self.log_msg('title not found'))
                page['title'] =''
            para_tags = review.findAll('p')
            par_tag = [None,None,None,None]
            try:
                for p_tag in para_tags:
                    tag_str = stripHtml(p_tag.renderContents())
                    if tag_str.startswith('Submitted by'):
                        par_tag[0] = p_tag
                    if tag_str.startswith('Pros:'):
                        par_tag[1] = p_tag
                    if tag_str.startswith('Cons:'):
                        par_tag[2] = p_tag
                    if tag_str.startswith('Overall Score:'):
                        par_tag[3] = p_tag
            except:
                log.exception(self.log_msg('Error with assiging p_tags'))
                continue
            try:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                post_str =  stripHtml(par_tag[0].renderContents())
                post_mat = re.compile('Submitted by (.+?) on (.+$)').match(post_str)
                if post_mat:
                    page['et_author_name'] = post_mat.group(1)
                    page['posted_date'] = datetime.strftime(datetime.strptime(post_mat.group(2).\
                                            strip(),"%m/%d/%Y %I:%M:%S %p"),"%Y-%m-%dT%H:%M:%SZ")
                else:
                    log.info(self.log_msg('posted date not found'))
            except:
                log.info(self.log_msg('posted date not found'))
            try:
                recommend_str = review['class']
                recommend_str = recommend_str.replace(' ','').lower()
                if recommend_str == 'thumbsup':
                    page['et_product_recommended'] ='yes'
                if recommend_str == 'thumbsdown':
                    page['et_product_recommended'] ='no'
            except:
                log.info(self.log_msg('product recommended not found'))
            try:
                rating_str = stripHtml(par_tag[3].renderContents())
                page['ei_product_rating'] = int(re.search('Overall Score:  (\d+)/10', rating_str).group(1).strip())
            except:
                log.info( self.log_msg('product rating not found') )
            try:
                page['data'] =  re.sub('^Pros:','',stripHtml(par_tag[1].renderContents())).strip() + '\n ' +\
                        re.sub('^Cons:','',stripHtml(par_tag[2].renderContents())).strip()
            except:
                log.exception(self.log_msg('data not found'))
                page['data'] =''
            try:
                review_hash = get_hash( page )
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                if not checkSessionInfo(self.genre, self.session_info_out, unique_key,self.\
                                task.instance_data.get('update'),parent_list=[ self.parenturi ]):
                    id=None
                    if self.session_info_out=={}:
                        id=self.task.id
                    result=updateSessionInfo(self.genre, self.session_info_out, unique_key \
                                    , review_hash,'Review', self.task.instance_data.get('update'), \
                                                                        parent_list=[ self.parenturi ])


                    if result['updated']:
                        parent_list = [ self.parenturi ]
                        page['parent_path'] = copy.copy(parent_list)
                        parent_list.append( unique_key )
                        page['path']=parent_list
                        page['task_log_id'] = self.task.id
                        page['versioned'] = self.task.instance_data.get('versioned',False)
                        page['category'] = self.task.instance_data.get('category','generic')
                        page['last_updated_time'] = datetime.strftime(datetime.utcnow()\
                                                                ,"%Y-%m-%dT%H:%M:%SZ")
                        page['client_name'] = self.task.client_name
                        page['entity'] = 'Review'
                        page['uri'] = normalize( self.parenturi )
                        page['uri_domain'] = urlparse(page['uri'])[1]
                        page['priority'] = self.task.priority
                        page['level'] = self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id
                        self.pages.append( page )
                        log.debug(self.log_msg("Review added"))
                    else:
                        log.debug(self.log_msg("product review main page details not saved"))
            except:
                log.exception(self.log_msg('Error with session info'))

    @logit( log, "__getParentPage")
    def __getParentPage(self):
        """
        This will set the parent page details and add it to session info,
        Same thing, need to be written for all connectors
        """
        page = {}
        try:
            page['title'] =  re.sub(' User Opinions$','',stripHtml(self.soup.find('title').renderContents())).strip()
        except:
            log.exception(self.log_msg('title could not be found'))
            page['title'] =''
        try:
            total_rating_count_str = self.soup.find('small',text=re.compile('TOTAL USER RATINGS: \d+'))
            page['ei_product_rating_count'] = int(re.search('TOTAL USER RATINGS:\s*(\d+)',total_rating_count_str).group(1))
             
        except:
            log.info(self.log_msg('toatal rating count is not found'))
        try:
            recommend_str = stripHtml(self.soup.find('div',id='contentContainer').find('p').findNext('p').renderContents())
            page['ef_product_recommended_no'] =  float(re.search('(.*?)%\s*of Users Would Not Recommend this Laptop', recommend_str).group(1).strip())
        except:
            log.info( self.log_msg( 'recommend no not found' ) )
        try:
            recommend_str =  stripHtml(self.soup.find('div',id='contentContainer').find('p').renderContents())
            page['ef_product_recommended_yes'] =  float(re.search('(.*?)%\s*of Users Would Recommend this Laptop', recommend_str).group(1).strip())
        except:
            log.info(self.log_msg('recommender Yes not found') )
        try:
            post_hash = get_hash( page )
            if not checkSessionInfo(self.genre, self.session_info_out,self.parenturi,\
                                                            self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out, self.parenturi, post_hash\
                                                ,'Post', self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    page['path']=[self.parenturi]
                    page['parent_path']=[]
                    page['task_log_id'] = self.task.id
                    page['versioned'] = self.task.instance_data.get('versioned',False)
                    page['category'] = self.task.instance_data.get('category','generic')
                    page['last_updated_time'] = datetime.strftime(datetime.utcnow()\
                                                            ,"%Y-%m-%dT%H:%M:%SZ")
                    page['client_name'] = self.task.client_name
                    page['entity'] = 'post'
                    page['data'] = ''
                    page['uri'] = normalize(self.parenturi)
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    page['priority'] = self.task.priority
                    page['level'] = self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")

                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    self.pages.append( page )
                    log.debug(self.log_msg("Parent page added"))
                    return True
                else:
                    log.debug(self.log_msg("product review main page details NOT stored"))
                    return False
        except:
            log.exception(self.log_msg('There is some problem with Session info'))
            return False

    @logit(log, "setSoup")
    def _setSoup( self ):
        """
            It will set the uri to current page, written in seperate
            method so as to avoid code redundancy
        """
        try:
            log.info(self.log_msg( 'for uri %s'%(self.currenturi) ))
            res = self._getHTML()
            if res:
                self.rawpage = res[ 'result' ]
            else:
                log.info(self.log_msg('self.rawpage not set.... so Sorry..'))
                return False
            self._setCurrentPage()
            return True
        except Exception, e:
            log.exception(self.log_msg('could not set the page as current page :%s'%uri))
            raise e


    @logit(log, "_setNextPage")
    def _setNextPage(self):
        """
        This will set find the next link and set the soup
        if nextLink not found, it will return a False
        """
        try:
            next_page_found = None
            next_page_found = self.soup.find('a',text='next >').parent['href']
            if not next_page_found:
                return False
            params = self.parenturi.split('?')[-1]
            data = dict(cgi.parse_qsl(params))
            headers = {'Referer':self.currenturi}
            post_url = 'http://www.notebookreview.com/price/opinions.asp?cmd=next&callingPage=' + \
                                                                        str( self.calling_page_no )
            res=self._getHTML(post_url,data=data,headers=headers)
            log.info(self.log_msg('for url : %s'%self.posturl))
            self.rawpage=res['result']
            self._setCurrentPage()
            self.calling_page_no = self.calling_page_no + 1
            return True
        except:
            log.info(self.log_msg('error  with setting next page'))
            return False

    @logit(log,"_getTaskUris")
    def _getTaskUris(self):
        """
        This will fetch the list of uris
        if the uri is http://www.notebookreview.com/price/
        """
        task_uris = []
        try:
            brand_list = ['http://www.notebookreview.com/price/'+li_tag.find('a')['href'] \
                                                            for li_tag in self.soup.findAll('li')]
            for brand_uri in brand_list:
                self.currenturi = brand_uri
                self._setSoup()
                model_list = [ 'http://www.notebookreview.com/price/' + ahref.find('a')['href'] \
                                                                for ahref in self.soup.findAll('li')]
                for model_uri in model_list:
                    self.currenturi = model_uri
                    self._setSoup()
                    ahref_tag = self.soup.find(text='User Opinions')
                    if ahref_tag:
                        task_uris.append ('http://www.notebookreview.com/price/'+ \
                                                                        ahref_tag.parent['href'] )
        except:
            log.exception(self.log_msg('exception in getting task uris'))
        return task_uris
    
            
            
            
