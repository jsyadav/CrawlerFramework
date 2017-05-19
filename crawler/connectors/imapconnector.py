import imaplib
import StringIO
import rfc822
import re
import copy
import logging
from urlparse import urlparse
from datetime import datetime
from baseconnector import BaseConnector
from tgimport import *
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.sessioninfomanager import *
from utils.decorators import logit

log = logging.getLogger('IMAPConnector')
class IMAPConnector(BaseConnector):
    
    @logit(log,'fetch')
    def fetch(self):
        """
        """
        try:
            ### Override source to show up correctly in the UI
            self.task.instance_data['source'] = "imap %s".strip()%(self.task.instance_data['name'])
            self.genre = 'review'
            imap_user,imap_password,imap_host,imap_port = self.task.instance_data['uri'].split("//")[1].split(':')
            imap_port = int(imap_port)
            try:
                self.imap_server = imaplib.IMAP4_SSL(imap_host,imap_port)
            except:
                self.imap_server = imaplib.IMAP4(imap_host,imap_port)
            self.imap_server.login(imap_user, imap_password)
            self.imap_server.select()
            resp, items = self.imap_server.search(None, "ALL")
            ids = [int(id) for id in items[0].split(' ')]
            ids.reverse()
            self.fetch_next_message = True
            self.__updateParentSessionInfo()
            mail_count = int(tg.config.get(path='Connector',key='imap_num_results'))
            self.count = 0
            for id in ids:
                if self.fetch_next_message and mail_count>=self.count:
                    self.__getData(id)
                else:
                    break
            self.imap_server.logout()
            return True
        except:
            log.exception("Exception occured in fetch")
            try:
                self.imap_server.logout()
            except:
                log.info(self.log_msg("Error while logging out from self.imap_server"))
            return False
        
    @logit(log,'__getData')    
    def __getData(self,id):
        """
        Gets information from an email.
        """
        self.count = self.count + 1
        try:
            resp, data = self.imap_server.fetch(id, "(RFC822)")
            text = data[0][1]
            file = StringIO.StringIO(text)
            message = rfc822.Message(file)
            try:
                message_id = message['message-id']
            except:
                log.exception(self.log_msg("Unable to fetch message_id, returning"))
                return False
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    message_id, self.task.instance_data.get('update'),
                                    parent_list=[self.task.instance_data['uri']]):
                page = {}
                try:
                    page['title']=stripHtml(message['subject'])
                except:
                    page['title']=''
                    log.info("Error occured while fetching subject for message_id:%s"%(message_id))
                try:
                    page['data'] = stripHtml(message.fp.read())
                except:
                    page['data']=''
                    log.info(self.log_msg("Exception in fetching message body for message_id:%s"%(message_id)))
                try:
                    page['et_data_to_address']=message['to']
                except:
                    log.info(self.log_msg("Exception in fetching to address for message_id:%s"%(message_id)))
                try:
                    page['et_data_from_address']=message['from']
                except:
                    log.info(self.log_msg("Exception in fetching from address for message_id:%s"%(message_id)))
                try:
                    mail_hash = get_hash(page)
                except:
                    log.exception(self.log_msg("Exception occured while creating email data hash, returning"))
                    return False
                result=updateSessionInfo(self.genre, self.session_info_out, message_id, mail_hash, 
                                         'Post', self.task.instance_data.get('update'), parent_list=[self.task.instance_data['uri']])
                if result['updated']:
                    parent_list = [self.task.instance_data['uri']]
                    page['parent_path']=copy.copy(parent_list)
                    parent_list.append(message_id)
                    page['path']=parent_list
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    try:
                        page['posted_date'] =  datetime.strftime(datetime.strptime(message['date'].split('-')[0].split('+')[0].strip(),\
                                                                                       "%a, %d %b %Y %H:%M:%S"),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project 
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['entity'] = 'post'
                    page['category'] = self.task.instance_data.get('category','')
                    page['task_log_id']=self.task.id
                    page['uri']=normalize(self.currenturi)
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    self.pages.append(page)
                    log.debug(self.log_msg("Message %s added to self.pages" %(message_id)))
                else:
                    if not self.task.instance_data.get('update'):
                        self.fetch_next_message = False
                    log.debug(self.log_msg("Message %s NOT added to self.pages" %(message_id)))
            else:
                if not self.task.instance_data.get('update'):
                    self.fetch_next_message = False
                log.debug(self.log_msg("Message %s NOT added to self.pages" %(message_id)))
            return True
        except:
            log.exception(self.log_msg("Exception occured in __getData function"))
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
                                     'Email', self.task.instance_data.get('update'), Id=id)
