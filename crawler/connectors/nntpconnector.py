
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#MOHIT RANKA
import re
from tgimport import *
import logging
from nntplib import NNTP
from urlparse import urlparse
from datetime import datetime
from baseconnector import BaseConnector
from utils.sessioninfomanager import *
from utils.decorators import *
from utils.urlnorm import normalize
from utils.utils import get_hash

log = logging.getLogger('NNTPConnector')

class NNTPConnector(BaseConnector):  
    @logit(log,'fetch')
    def fetch(self):
        """
        Fetches all the messages for a given news group uri and return Fetched staus depending 
        on the success and faliure of the task
        """
        try:
            #eg. self.currenturi = nntp://msnews.microsoft.com/microsoft.public.exchange.setup
            #nntp_server = 'msnews.microsoft.com'
            #nntp_group = 'microsoft.public.exchange.setup'
            self.genre = 'review'
            try:
                nntp_server = urlparse(self.currenturi)[1]
            except:
                log.exception(self.log_msg("Exception occured while connecting to NNTP server %s"%self.currenturi))
                return False
            nntp_group =  urlparse(self.currenturi)[2][1:]
            self.server = NNTP(nntp_server)
            try:
                self.__updateParentSessionInfo()
                resp, count, first, last, name = self.server.group(nntp_group)
                last_id = int(last)
                first_id = self.__getMaxCrawledId(last_id)+1
                log.debug("first_id is %d:"%first_id)
                log.debug("last_id is %d:"%last_id)
                if last_id >= first_id:
                    resp, items = self.server.xover(str(first_id), str(last_id))
                    log.debug(self.log_msg("length of items:%s"%str(len(items))))
                    for self.id, self.subject, self.author, self.date, self.message_id,\
                            self.references, size, lines in items:
                        self.__getMessages(self.task.instance_data['uri'])
                self.server.quit()
                return True
            except:
                log.exception(self.log_msg("Exception occured in fetch()"))
                self.server.quit()
                return False
        except Exception,e:
            log.exception(self.log_msg("Exception occured in fetch()"))
            return False

    @logit(log,'__getMessages')
    def __getMessages(self,parent_uri):
        """
        Retirves information from a newsgroup message
        """
        try:           
            resp, id, message_id, text = self.server.article(self.id)
            log.debug("id=%s"%str(id))
            log.debug("message_id=%s"%str(message_id))
            ### To remove article head
            try:
                text = text[len(self.server.head(str(self.id))[3]):]
            except:
                log.exception("Error occured while removing the head")
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    id, self.task.instance_data.get('update'),
                                    parent_list=[parent_uri]):
                page = {}
                try:
                    page['title']=self.subject
                except:
                    log.info(self.log_msg('Title for the message not found'))
                    page['title']=''
                try:
                    page['data']="\n".join(text)
                except:
                    log.info(self.log_msg('Message content not found'))
                    page['data']=page['title'][:50]
                try:
                    try:
                        page['et_author_name']= re.findall("=\?Utf-8\?B\?(.*?)\s",self.author)[0].decode('base64').decode('utf-8')+\
                            " "+self.author.split(' ')[-1]
                        log.debug(self.log_msg("base64 encoded author name found."))
                    except:
                        page['et_author_name']=self.author
                except:
                    log.info(self.log_msg('Author Name not available for the post'))

                try:
                    page['et_data_message_id']=message_id
                except:
                    log.info(self.log_msg('unable to pick message id for NNTP message'))
                try:
                    page['et_data_reply_to']=self.references[0]
                except:
                    log.info(self.log_msg('References not available for the post'))
        
                try:
                    post_date = datetime.strptime(re.findall("\w+,\s\d+\s"\
                                                                 + "\w+\s\d{4}"\
                                                                 + "\s\d{1,2}:\d{1,2}:\d{1,2}",self.date)[0],\
                                                      "%a, %d %b %Y %H:%M:%S")
                except:
                    log.info(self.log_msg('Exception occured while processing date'))
                    post_date = datetime.utcnow() 

                review_hash = get_hash(page)
                result=updateSessionInfo(self.genre, self.session_info_out, id, review_hash, 
                                         'NewsGroupMessage', self.task.instance_data.get('update'), parent_list=[parent_uri])
                if result['updated']:
                    page['path'] = page['parent_path'] = [parent_uri]
                    page['path'].append(id)
                    page['task_log_id']=self.task.id
                    page['versioned']=self.task.instance_data.get('versioned',False)
                    page['category']=self.task.instance_data.get('category','generic')
                    page['last_updated_time']= datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['client_name']=self.task.client_name
                    page['entity']='post'
                    page['uri'] = normalize(self.currenturi)
                    page['uri_domain'] = unicode(urlparse(self.currenturi)[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['posted_date'] = datetime.strftime(post_date,"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  
                    self.pages.append(page)
                    return True
                else:
                    return False
            else:
                return False
        except:
            log.exception("Exception occured in __getMessages")
            return False

    @logit(log,'__updateParentSessionInfo')
    def __updateParentSessionInfo(self):
        """
        updates the session information for a parent uri
        """
        if not checkSessionInfo(self.genre, self.session_info_out, 
                                self.task.instance_data['uri'], self.task.instance_data.get('update')):
            id=None
            if self.session_info_out=={}:
                id=self.task.id
                log.debug('got the connector instance first time, sending updatesessioninfo the id : %s' % str(id))
            result=updateSessionInfo(self.genre, self.session_info_out, self.task.instance_data['uri'], self.task.instance_data['uri'], 
                                     'NewsGroupPost', self.task.instance_data.get('update'), Id=id)

    @logit(log,'__getMaxCrawledId')
    def __getMaxCrawledId(self,last_id):
        """
        Returns the max id from the session info 
        
        @todo: Read the first crawl count from app.cfg 
        """
        try:
            if not  self.task.instance_data.get('update'):
                #Update is False ---- > Return the maximum id
                return max([int(id) for id in list(set(self.session_info_out[self.currenturi].keys())-set(['attrs']))])
            else:
                #Update is True ---- > Return the minimum id -1 (-1 to incorporated with + 1 later)
                return min([int(id) for id in list(set(self.session_info_out[self.currenturi].keys())-set(['attrs']))])-1 
        except:
            #First Crawl
            #return last_id - 100 #For now
            return last_id - int(tg.config.get(path='Connector',key='nntp_numresults'))
