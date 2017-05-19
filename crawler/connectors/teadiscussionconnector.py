
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
from datetime import datetime
import logging
from urllib2 import urlparse
import copy

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('TeaViewsConnector')
class TeaDiscussionConnector( BaseConnector ):
    '''
    A Connector for www.http://www.teadiscussion.com
    The sample uri is :
    1) http://www.teadiscussion.com/reviews/flavored-tea/adagio-almond-tea.php    
    
    Solr Fields to capture are other than, title,posted_data,uri,data :
    et_product_manufacturer
    et_product_type
    ef_product_rating
    et_product_instructions
    '''
     
    @logit(log , 'fetch')
    def fetch(self):
        """
        This will fetch the post of a tea review
        and add all info to the base class
        """
        try:                                                                                 
            self.genre ="Review"
            self.parent_uri = self.currenturi
            if not self._setSoup():
                log.info(self.log_msg('Task uri not set, cannot proceed') )
                return False
            if self.currenturi == 'http://www.teadiscussion.com/categories/index.php':
                for each in ['http://www.teadiscussion.com/categories/' + each['href'] for each in self.soup.find('p',text='Reviews of tea by types of tea:').parent.findNext('ul').findAll('a')]:
                    self.currenturi = each
                    if self._setSoup():
                        for href in [ahref['href'] for ahref in self.soup.findAll('a','categoryTitle')]:
                            temp_task=self.task.clone()
                            temp_task.instance_data[ 'uri' ] = normalize( href )
                            self.linksOut.append( temp_task )
                return True
            if not self.__getParentPage():
                log.info(self.log_msg('Parent page not posted '))
            self.__addReview()
            return True
        except:
            log.exception(self.log_msg('Error in Fetch'))
            return False                
    
    @logit(log,"getParentPage")
    def __getParentPage(self):
        """
        This will add the Parent Page info 
        Only Title and uri can be found,
        No data are found
        """
        page = {}
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri \
                                                , self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info return True'))
                return False
        tag_dict = {'Tea Reviewed':'title','Manufacturer':'et_product_manufacturer','Available from':'et_product_available','Type of Tea':'et_product_type','Brewing Instructions':'et_product_instructions'}
        page['title'] = ''            
        for each in tag_dict.keys():
            try:
                tag = self.soup.find('strong',text=each)
                page[tag_dict[each]] = stripHtml(tag.findParent('li').__str__().replace(tag.__str__(),''))[1:].strip()
            except:
                log.info(self.log_msg('%s is not found'%tag_dict[each]))
        try:    
            log.info(page)
            post_hash = get_hash(page)
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo(self.genre, self.session_info_out,self.parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                return False
            page['path']=[self.parent_uri]
            page['parent_path']=[]
            page['uri'] = normalize(self.parent_uri)
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
            page['task_log_id']=self.task.id
            page['entity'] = 'Post'
            page['category']=self.task.instance_data.get('category','')
            self.pages.append(page)
            log.info(self.log_msg('Parent Page added'))
            return True
        except:
            log.exception(self.log_msg("parent post couldn't be posted"))
            return False
    @logit(log, "addReview")
    def __addReview(self):
        """
        It will add the review which has
        et_data_snapshot, et_author_name, et_author_info
        """
        page = {}
        try:
            tag = self.soup.find('strong',text='Date of Review')
            date_str = stripHtml(tag.findParent('li').__str__().replace(tag.__str__(),''))[1:].strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(re.sub\
                                  ("(\d+)(st|nd|rd|th)",r"\1",date_str).strip()\
                                                ,"%B %d, %Y"),"%Y-%m-%dT%H:%M:%SZ")
            
        except:
            log.exception(self.log_msg('posted_date not found '))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            # src is like '../../site-graphics/stars-3-0.gif'
            rating_str = self.soup.find('strong',text='Tea Rating').findParent('li').find('img')['src'].split('/')[-1].replace('stars-','')[:-4]
            page['ef_product_rating'] =  float(re.sub('^(\d)-',r'\1.',rating_str))
        except:
            log.info(self.log_msg('Ratings are not found'))
        try:
            page['title'] = stripHtml(self.soup.find('td','mainContent').find('p').find('strong').renderContents())
        except:
            page['title'] = ''
            log.exception(self.log_msg('Title cannot be retrived'))
        try:
            tag = self.soup.find('strong',text='Summary')
            page['et_data_summary'] = stripHtml(tag.findParent('p').__str__().replace(tag.__str__(),''))[1:].strip()
        except:
            log.info(self.log_msg('Data summary not found') )
                
                #if  tag.__str__().startswith('<p><strong>Review</strong>'):
                #   break    
        try:
            data_str = ''
            review_tag_found = False
            for tag in self.soup.find('td','mainContent'):
                if  tag.__str__().startswith('<p><strong>Review</strong>'):                    
                    review_tag_found = True
                if  tag.__str__().startswith('<p><strong>Recommendation</strong>'):
                    break
                if  tag.__str__().startswith('<table width="100%" border="0" cellpadding="0" cellspacing="0" class="brewingTable">'):
                    break
                if review_tag_found:
                    data_str = data_str + tag.__str__().strip()
            page['data'] = re.sub('^Review','',stripHtml(data_str)).strip()[1:].strip()
        except:
            log.exception(self.log_msg('data cannot be retrieved'))
        try:
            tag = self.soup.find('strong',text='Recommendation')
            page['et_data_recommendation_info'] = stripHtml(tag.findParent('p').__str__().replace(tag.__str__(),''))[1:].strip()           
                          
        except:
            log.info(self.log_msg('Recommendation string not found'))
        try:
            if page['title'] =='':
                if len(page['data']) > 50:
                        page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
        try:
            review_hash = get_hash( page )
            unique_key = get_hash( {'data':page['data'],'title':page['title']})
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                        =[self.parent_uri]):
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
            page['uri'] = normalize(self.parent_uri)
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
            page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
            self.pages.append(page)
            log.info(self.log_msg('Review Added'))
        except:
            log.exception(self.log_msg('Error while adding session info'))
            
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

