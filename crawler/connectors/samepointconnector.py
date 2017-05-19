
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

from baseconnector import BaseConnector
from urllib import quote_plus
from datetime import datetime
from urlparse import urlparse
import feedparser
import copy
from tgimport import *
from utils.task import Task
from utils.urlnorm import normalize
from utils.utils import stripHtml,get_hash
from utils.sessioninfomanager import *
from utils.decorators import logit
import logging

log = logging.getLogger('SamePointConnector')

class SamePointConnector(BaseConnector):

    @logit(log,'fetch')
    def fetch(self):
        """
        Fetches all the posts for a given self.currenturi and returns Fetched 
        staus depending on the success and faliure of the task
        """
        try:
            if self.task.instance_data.get('queryterm'):
                feed_url = 'http://www.samepoint.com/rss_socialmention.php?q=%s&searchb=Create+RSS+Feed'\
                    %(quote_plus(self.task.instance_data['queryterm']))
            elif 'http://www.samepoint.com/rss_socialmention.php?q=' in self.task.instance_data['uri']:
                # A atom feed url is provided
                feed_url = self.task.instance_data['uri']
            else:
                log.info(self.log_msg("Unexpected feed url, quitting"))
                return False
            parser = feedparser.parse(feed_url)
            if len(parser.version) == 0:
                log.info(self.log_msg('parser version not found , returning'))
                return False
            if not checkSessionInfo('review', self.session_info_out, 
                                    self.task.instance_data['uri'], self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug('got the connector instance first time, sending updatesessioninfo the id : %s' % str(id))
                    result=updateSessionInfo('review', self.session_info_out, self.task.instance_data['uri'],self.task.instance_data['uri'], 
                                             'post', self.task.instance_data.get('update'), Id=id)
            for entry in parser['entries']:
                page={}
                try:
                    page['title']=stripHtml(entry['title'])
                except:
                    log.info(self.log_msg("Error occured while fetching the data/title from SamePoint"))
                    page['title']=''
                try:
                    page['data']=stripHtml(entry['summary'])
                except:
                    log.info(self.log_msg("Error occured while fetching the data/title from SamePoint"))
                    page['data']=''
                try:
                    message_hash = get_hash(page)
                except:
                    log.info(self.log_msg("Error occured while creating hash for %s"%entry['link']))
                    continue
                if not checkSessionInfo('review', self.session_info_out, 
                                        message_hash, self.task.instance_data.get('update')):
                    result=updateSessionInfo('review', self.session_info_out, message_hash, message_hash, 
                                             'post', self.task.instance_data.get('update'), parent_list=[self.task.instance_data['uri']])
                    if result['updated']:
                        parent_list = [self.task.instance_data['uri']]
                        page['parent_path']=copy.copy(parent_list)
                        parent_list.append(message_hash)
                        page['path']=parent_list
                        page['uri'] = normalize(entry['link'])
                        page['uri_domain'] = unicode(urlparse(page['uri'])[1])
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        try:
                            page['posted_date'] = datetime.strftime( datetime.strptime(" ".join(entry['updated'].split(' ')[:-1])\
                                                                                           ,"%a, %d %b %Y %H:%M:%S"),"%Y-%m-%dT%H:%M:%SZ")
                        except:
                            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id 
                        page['client_name'] = self.task.client_name
                        page['last_updated_time'] = page['pickup_date']
                        page['versioned'] = False
                        page['task_log_id']=self.task.id
                        page['entity'] = 'post'
                        page['category']=self.task.instance_data.get('category','')
                        self.pages.append(page)
                        log.debug(self.log_msg("Message info added to self.pages"))
                    else:
                        # As far as my observation, SamePoint results are newest first
                        log.info(self.log_msg("Already picked the link"))
                        if not self.task.instance_data.get('update'):
                            break
                        else:
                            continue
                else:
                    log.info(self.log_msg("Already picked the link"))
                    if not self.task.instance_data.get('update'):
                        break
                    else:
                        continue
            return True
        except Exception,e:
            log.exception(self.log_msg("Exception occured in fetch()"))
            return False
