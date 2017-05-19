
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar
# Author Name : Catalan

import re
import copy
from datetime import datetime
import logging
from urllib2 import urlparse

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('HudInConnector')
class HudInConnector( BaseConnector ):
    '''
    A Connector for www.teaviews.com
    The sample uri is :
    http://www.hudin.com/tea_reviews/tazo/awake/
    http://www.hudin.com/tea_reviews/fortnum_mason/assam_tippy_golden/
    http://www.hudin.com/tea_reviews/peets/lapsang_souchong/
    http://www.hudin.com/tea_reviews/ineeka/himalayan_black/
    
    Solr Fields to capture are other than, title,posted_data,uri,data :
    et_author_name
    ef_product_rating
    et_product_manufacturer
    Other's Ratings are to be ignored    
    '''
     
    @logit(log , 'fetch')
    def fetch(self): 
        """
        it will add all the posts for the given url and
        return True, return False, if post is not added
        """        
        self.genre = "Review"
        if not self._setSoup():
            log.info(self.log_msg('Task uri not set, cannot proceed'))
            return False
        if self.currenturi =='http://www.hudin.com/tea_reviews':
            try:
                for href in ['http://www.hudin.com' + each['href'] for each in self.soup.find('ul',id='teaNav').findAll('a')]:
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] = normalize( href )
                    self.linksOut.append( temp_task )
                return True
            except:
                log.exception(self.log_msg('Task Cannot be added'))
                return False
            
        page = {}
        if checkSessionInfo(self.genre , self.session_info_out, self.currenturi \
                                                , self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info return True'))
                return False        
        try:
            page['title'] =stripHtml(self.soup.find('span','size11').findNext('h1').renderContents())                
        except:
            log.exception(self.log_msg('Title could not be found'))
            page['title'] =''
        try:
            page['et_product_manufacturer'] =  re.sub("Tea's$",'', stripHtml(self.soup.find('span','size11').renderContents())).strip()
        except:
            log.info(self.log_msg('Manufacturer cannot be found'))
        try:
            data_str = ''
            review_tag_found = False
            for tag in self.soup.find('span','size11').findParent('div'):
                tag_str = tag.__str__().strip() 
                if  tag_str == '<b>Review:</b>':
                    review_tag_found = True
                    continue
                if tag_str == '<b>Pairings:</b>' or tag_str == '<b>Rating:</b>':                    
                    break
                if review_tag_found:
                    data_str = data_str + tag_str
            page['data'] = stripHtml(data_str)
        except:
            log.exception(self.log_msg('data cannot be retired'))
            page['data'] =''
        try:
            page['et_product_parings'] = self.soup.find('b',text='Pairings:').next
        except:
            log.info(self.log_msg('Parings not found'))
        try:
            page['ef_product_rating'] =  float(re.search('(^\d+) of',self.soup.find('b',text='Rating:').next.strip()).group(1))
        except:
            log.info(self.log_msg('Rating cannot be retrieved'))       
        try:    
            log.info(page)
            post_hash = get_hash(page)
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo("Review", self.session_info_out,self.currenturi, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                return False
            page['path']=[self.currenturi]
            page['parent_path']=[]
            page['et_author_name'] ='Catalan'
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
            page['task_log_id']=self.task.id
            page['entity'] = 'Post'
            page['category']=self.task.instance_data.get('category','')
            self.pages.append(page)
            log.info(self.log_msg('Page added'))
            return True
        except:
            log.exception(self.log_msg("parent post couldn't be posted"))
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
