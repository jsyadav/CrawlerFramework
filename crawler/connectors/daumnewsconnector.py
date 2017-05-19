
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''


#ASHISH YADAV


import re
import md5
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import logging
from urllib2 import urlparse,unquote

from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash

from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('DaumNewsConnector')
class DaumNewsConnector(BaseConnector):

    @logit(log , 'fetch')
    def fetch(self):                                                                                         
        self.genre="Generic"
        try:
            #for sentiment extraction
            code = None
            parent_uri = self.currenturi
            # for sentiment extraction
            res=self._getHTML()
            review_next_page_list = []
            self.rawpage=res['result']
            self._setCurrentPage()
            res=self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            page = {}
            if (not checkSessionInfo(self.genre, self.session_info_out, 
                                     parent_uri, self.task.instance_data.get('update'),
                                     parent_list=[])):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    
                page['uri'] = self.currenturi
                try:
                    page['et_data_source'] = stripHtml(self.soup.find('p',{'class':'date'}).renderContents()).split('|')[0]
                except:
                    log.info('could not parse source Name')

                try:
                    page['data'] = stripHtml(self.soup.find('div',{'id':'news_content'}).renderContents())
                except:
                    log.exception('could not parse page data')
                    
                try:
                    page['title'] = stripHtml(self.soup.find('h3',{'id':'GS_con_tit'}).renderContents())
                except:
                    log.info('could not parse number of page views')
                    page['title'] = page['data'][:100]
                    
                try:
                    posted_date = self.soup.find('p',{'class':'date'}).findAll('span')[-1].em.renderContents()
                    posted_date = datetime.strptime(posted_date,'%Y.%m.%d %H:%M')
                    page['posted_date'] = datetime.strftime(posted_date,"%Y-%m-%dT%H:%M:%SZ")
                except:
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    log.exception('could not parse  posted_date')

                news_hash = get_hash(page)
                result=updateSessionInfo(self.genre, self.session_info_out, parent_uri, news_hash,
                                         'News', self.task.instance_data.get('update'),Id=id )
                if result['updated']:
                    page['uri'] = normalize(self.currenturi)
                    page['path'] = [parent_uri]
                    page['parent_path'] = []
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project       
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['entity'] = 'post'
                    page['category']=self.task.instance_data.get('category','')                        
                    self.pages.append(page)
                else:
                    log.info(self.log_msg('no updates found'))
            else:
                log.info(self.log_msg('already fetched before and updates false so not updating again'))
            return True
            
        except:
            log.exception(self.log_msg('problem in daumnewsconnector'))
            return False
