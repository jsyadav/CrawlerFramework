
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
import simplejson
import logging
from baseconnector import BaseConnector
from urllib import quote,urlopen
from datetime import datetime
from urlparse import urlparse
from tgimport import *
from utils.task import Task
from utils.urlnorm import normalize
from utils.utils import stripHtml,get_hash
from utils.sessioninfomanager import *
from utils.decorators import logit
import copy
log = logging.getLogger('BackTypeConnector')

class BackTypeConnector(BaseConnector):

    @logit(log,'fetch')
    def fetch(self):
        """
        Fetched all the comments for a given self.currenturi and returns Fetched 
        staus depending on the success and faliure of the task
        """
        try:
            num_results = tg.config.get(path='Connector',key='backtype_search_numresults')
            api_key = tg.config.get(path='Connector',key='backtype_api_key')
            if self.task.instance_data.get('queryterm'):
                api_url = 'http://api.backtype.com/comments/search.json?q=%s&key=%s&itemsperpage=%s'\
                    %(quote(self.task.instance_data['queryterm']),api_key,num_results)
            elif 'api.backtype.com/comments/search.json' in self.task.instance_data['uri']:
                api_url = self.task.instance_data['uri']+'&key=%s&itemsperpage=%s' %(api_key,num_results)
            else:
                log.info(self.log_msg("Unexpected feed url, quitting"))
                return False
            comments = simplejson.loads(urlopen(api_url).read())['comments']
            sorted_comments = sorted(comments,key = lambda x:x['comment']['id'])
            sorted_comments.reverse()
            if not checkSessionInfo('review', self.session_info_out, 
                                    self.task.instance_data['uri'], self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug('got the connector instance first time, sending updatesessioninfo the id : %s' % str(id))
                    result=updateSessionInfo('review', self.session_info_out, self.task.instance_data['uri'],self.task.instance_data['uri'], 
                                             'Post', self.task.instance_data.get('update'), Id=id)
            for comment in sorted_comments:
                if not checkSessionInfo('review', self.session_info_out, 
                                        comment['comment']['id'], self.task.instance_data.get('update')):
                    page={}
                    try:
                        page['data']=comment['comment']['content']
                    except:
                        page['data']=''
                        log.info(self.log_msg("Error occured while fetching the data from BackType"))
                    try:
                        page['title']=comment['post']['title']
                    except:
                        page['title']=''
                        log.info(self.log_msg("Error occured while fetching the title from BackType"))
                    try:
                        page['et_author_name']=comment['author']['name']
                    except:
                        log.info(self.log_msg("Error occured while fetching author name"))
                    try:
                        page['et_author_profile']=comment['author']['url']
                    except:
                        log.info(self.log_msg("Error occured while fetching author profile link"))
                    try:
                        comment_hash = get_hash(page)
                    except:
                        log.info(self.log_msg("Error occured while creating hash for %s"%comment['comment']['id']))
                        continue
                    result=updateSessionInfo('review', self.session_info_out, comment['comment']['id'], comment_hash, 
                                             'Comment', self.task.instance_data.get('update'), parent_list=[self.task.instance_data['uri']])
                    if result['updated']:
                        parent_list = [self.task.instance_data['uri']]
                        page['parent_path'] = copy.copy(parent_list)
                        parent_list.append(str(comment['comment']['id']))
                        page['path'] = parent_list
                        page['uri'] = normalize(comment['comment']['url'])
                        page['uri_domain'] = unicode(urlparse(page['uri'])[1])
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        try:
                            page['posted_date'] = datetime.strftime(datetime.strptime(comment['comment']['date'],\
                                                                                          "%Y-%m-%d %H:%M:%S"),"%Y-%m-%dT%H:%M:%SZ")
                        except:
                            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id  # TODO: Get the client from the project       
                        page['client_name'] = self.task.client_name
                        page['last_updated_time'] = page['pickup_date']
                        page['versioned'] = False
                        page['task_log_id']=self.task.id
                        page['entity'] = 'Post'
                        page['category']=self.task.instance_data.get('category','')
                        self.pages.append(page)
                        log.debug(self.log_msg("comment %s info added to self.pages" %(comment['comment']['id'])))
                    else:
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
