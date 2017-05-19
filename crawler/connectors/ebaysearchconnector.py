
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#ASHISH YADAV

from BeautifulSoup import BeautifulSoup
import traceback
import logging
import urllib
import traceback
import urlparse
import pickle

from tgimport import *
from baseconnector import BaseConnector
from htmlconnector import HTMLConnector
from ebayreviewconnector import EbayReviewConnector
from utils.task import Task
from utils.utils import stripHtml
from utils.urlnorm import normalize
from utils.decorators import logit
from turbogears.database import session
from knowledgemate import model
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('EbaySearchConnector')
class EbaySearchConnector(BaseConnector):
    
#    @logit(log , 'Processtask')
#     def processTask(self, token):
#         try:
#             self.rawpage = self._getHTML()['result']
#             if not self.rawpage:
#                 return
#         except:
#             log.exception(log.info('exception in processtask'))
#             return

#         # check it it is a RSS/Atom feed
# #        if self.mimeType != 'text/html':
# #            return -100
#         try:
#             if self.related_uris and urlparse.urlparse(self.related_uris[0])[1] == 'catalog.ebay.com':
#                 log.info(self.log_msg("taking up EbayreviewConnector"))
#                 temp_task = self.task.clone()
#                 temp_task.instance_data['uri'] = normalize(self.related_uris[0])
#                 self.linksOut.append(temp_task)
#                 self.sendBackLinks()
# #                log.info(self.log_msg('uri changes in task , new uri :' + self.related_uris[0]))
# #                cInstance=EbayReviewConnector(self.task, self.rawpage)
# #                cInstance.processTask(token)
#             elif urlparse.urlparse(self.currenturi)[1] == 'search.reviews.ebay.com':
#                 log.info(self.log_msg("continue with Ebaysearchconnector"))
#                 BaseConnector.processTask(self,token)
# #             else:
# #                 log.debug("taking up htmlconnector") #may be from a ebay domain , we haven't written connector for
# #                 cInstance=HTMLConnector(self.task, self.rawpage)
# #                cInstance.processTask(token)
#             return
#         except:
#             log.exception(self.log_msg('exception in process token'))
#             return


    @logit(log , '_createsiteurl')
    def _createsiteurl(self):
        #testing
        seedurl = '"http://search.reviews.ebay.com/ws/UCSearch?satitle=nokia+N95+8gb&ucc=r"'
        #testing
        log.info(self.log_msg('seedurl :%s'%(seedurl)))
        return seedurl
        
    @logit(log , 'fetch')
    def fetch(self):
        try:

            if not self.rawpage:
            #Testing sentiment extraction                                                                      
                if not self.currenturi:
                    log.info(self.log_msg('currenturi not provided , so returning back'))
                    self.task.status['fetch_status']=False
                    return False
            # Testing sentiment extraction                                                                    
                res=self._getHTML()
                self.rawpage=res['result']


            #check if this gets redirected to ebay reviews , if it is append a task for this review
            if self.related_uris and urlparse.urlparse(self.related_uris[0])[1] == 'catalog.ebay.com':
                log.info(self.log_msg("taking up EbayreviewConnector"))
                temp_task = self.task.clone()
                temp_task.instance_data['uri'] = normalize(self.related_uris[0])
                self.linksOut.append(temp_task)
                self.task.status['fetch_status']=True
                return True

            self._setCurrentPage()
            while True:
                try:
                    self.addreviewpages()
                    next = self.soup.find('span',{'class':'next'})
                    if next and next.find('a',href=True):
                        next_page = 'http://search.reviews.ebay.com' + next.a['href']
                        self.currenturi = next_page
                        res=self._getHTML()
                        self.rawpage=res['result']
                        self._setCurrentPage()
                    else:
                        break
                except Exception, e:
                    raise e
            log.info(self.log_msg("appended %d jobs"%(len(self.linksOut))))
            self.task.status['fetch_status']=True
            return True
        except:
            self.task.status['fetch_status']=False
            log.exception(self.log_msg('exception in fetch'))
            return False

    @logit(log , 'addreviewpages')
    def addreviewpages(self):
        posts = self.soup.findAll('tr',{'class':'highlight reviewRow'}) + self.soup.findAll('tr',{'class':' reviewRow'})
        for post in posts:
            try:
                review_details = post.find('td',{'class':'reviewDtls'})
                if review_details and review_details.find('span',{'class':'revCount'}):
                    temp_task=self.task.clone()
                    temp_task.instance_data['uri']=normalize('http://search.reviews.ebay.com'+review_details.find('a',href=True)['href'])
                    self.linksOut.append(temp_task)
            except:
                log.info(self.log_msg('exception in addreviews'))
        return True
 
    @logit(log , 'savetosolr')
    def saveToSolr(self):
        return True
