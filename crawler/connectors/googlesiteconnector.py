
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#JV
#Ashish

from googleconnector import GoogleConnector
#Added by Mohit - 4/09/2008
from tgimport import *
import logging
log = logging.getLogger('GoogleSiteConnector')
from utils.decorators import *
import urllib2

class GoogleSiteConnector(GoogleConnector):

    @logit(log, '_createUrl')
    def _createUrl(self,code):
#        if self.task.level > self.max_recursion_level:
#            log.info(self.log_msg('recursion level greater then MAX, returning'))
#            return []
        if not code:
            code = self.task.instance_data.get('queryterm') or ''
            code = '%s+%s'%(code,'site:'+self.currenturi)
        numresults = tg.config.get(path='Connector', key='google_search_numresults')
        url_template = "http://www.google.com/search?hl=en&btnG=Google+Search&num=%s&q=%s"
        query_terms = []
        if self.task.keywords and self.task.instance_data.get('apply_keywords'):
            query_terms = ['%s %s'%(q.keyword.decode('utf-8','ignore'),code.decode('utf-8','ignore'))  for q in \
                               self.task.keywords if q.filter]
        if not query_terms and code:
            query_terms = [code]
        return [url_template %(numresults , urllib2.quote(urllib2.unquote(query_term.strip().encode('utf-8')))) for query_term in query_terms]


#     @logit(log, '_createUrl')
#     def _createUrl(self,code=None):
#         numresults = tg.config.get(path='Connector', key='google_search_numresults')#TODO - make this entry in config
#         url_template = "http://www.google.com/search?hl=en&btnG=Google+Search&num=%s&q=%s"
#         siteterm = '+site:'+self.currenturi
#         if self.task.instance_data.get('queryterm'):
#             url = url_template % (numresults,urllib2.quote((self.task.instance_data['queryterm']+siteterm).encode('utf-8')))
#             log.info(self.log_msg('seedurl :%s'%(url)))
#             return url
#         elif self.task.keywords and self.task.instance_data.get('apply_keywords'):
#             urls = []
#             for keyword in self.task.keywords:
#                url = url_template% (numresults,urllib2.quote((keyword+siteterm).encode('utf-8')))
#                urls.append(url)
#                log.info(self.log_msg('seed_url from keyword : %s'%url))
#             return urls
#         elif code:
#             url = url_template % (numresults,(code+siteterm).encode('utf-8'))
#             log.info(self.log_msg('seedurl :%s'%(url)))
#             return url
#         else:
#             log.info(self.log_msg('could not parse code from url or get queryterm so returning none'))
#             return None
