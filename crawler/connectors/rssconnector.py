
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Ashish
from baseconnector import BaseConnector
import urllib2
import urlparse
import feedparser
from tgimport import *
import datetime
from utils.task import Task
from utils.urlnorm import normalize
from utils.utils import stripHtml
import urlparse
import logging
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('RSSConnector')
class RSSConnector(BaseConnector):
    
    def fetch(self):
        """
        specific needs: feedparser
        """
        try:
#            if self.task.level > self.max_recursion_level:
            self.task.instance_data['source_type'] = self.task.instance_data.get('source_type','rss')
            if self.task.level > 1:
                log.debug(self.log_msg('recursion level greater then MAX, returning'))
                return True
            if self.parser:
                parser = self.parser
            else:
                parser = feedparser.parse(self.currenturi)

            if len(parser.version) == 0:
                log.info(self.log_msg('parser version not found , returning'))
                return False
            unique_urls = []
#             if self.session_info_out:
#                 genre = self.session_info_out.values()[0]['entity']

            if parser is not None:
                max_time_stamp = datetime.datetime(1,1,1)
                log.info('number of entries %s'%(len(parser.entries)))
                for entity in parser.entries:
                    if entity.has_key('updated_parsed'):
#                        log.info('using last posted_date as a criteria to create new posts')
                        key = datetime.datetime(*entity.updated_parsed[:7])
                        genre = 'Search'
                        max_time_stamp = max(key,max_time_stamp)
                    else:
#                        log.info("Feed doesn't provides article created date as a way to get new posts i will keep urls so crawled in" \
#                                     "session_info to check againt the new urls")
                        genre = 'Generic'
                        key = entity['link']
                    try:
                        if not checkSessionInfo(genre,self.session_info_out, key,
                                                self.task.instance_data.get('update')) \
                                                and not entity['link'] in unique_urls:
                            unique_urls.append(entity['link'])
                            temp_task=self.task.clone()
                            temp_task.instance_data['uri']= normalize(entity['link'])
                            temp_task.pagedata['title']=entity['title']
                            temp_task.pagedata['source'] = urlparse.urlparse(self.currenturi)[1]
                            temp_task.pagedata['source_type'] = self.task.instance_data.get('source_type','rss')
                            if genre == 'Search':
                                temp_task.pagedata['posted_date'] = datetime.datetime.strftime(key, "%Y-%m-%dT%H:%M:%SZ")
                            elif genre == 'Generic':
                                updateSessionInfo(genre, self.session_info_out, key, '',
                                                  'Post', self.task.instance_data.get('update'))
                                
                            self.linksOut.append(temp_task)
                    except:
                        log.exception(self.log_msg("exception in adding temptask to linksout"))
                        continue
                if genre == 'Search' and self.linksOut:
                    updateSessionInfo(genre, self.session_info_out,max_time_stamp , None,
                                      'Post',self.task.instance_data.get('update'))
            log.info(self.log_msg("no. of new links added %d" %(len(unique_urls))))
            return True
        except Exception, e:
            log.exception(log.info('exception in fetch'))
            return False
        
#    def __checkSessionInfo(self,genre,id


    def saveToSolr(self):
        return True

