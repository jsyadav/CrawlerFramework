import poplib
import re
import logging
from urlparse import urlparse
from datetime import datetime
from baseconnector import BaseConnector
from tgimport import *
import copy
from email.parser import Parser
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.sessioninfomanager import *
from utils.decorators import logit

log = logging.getLogger('POPConnector')
class POPConnector(BaseConnector):
    
    @logit(log,'fetch')
    def fetch(self):
        """
        """
        try:
            ### Override source to show up correctly in the UI
            self.task.instance_data['source'] = "pop %s".strip()%(self.task.instance_data['name'])
            self.genre = 'review'
            self.pop_user,pop_password,pop_host,pop_port = self.task.instance_data['uri'].split("//")[1].split(':')
            pop_port = int(pop_port)
            try:
                self.pop_server = poplib.POP3_SSL(pop_host,pop_port)
            except:
                self.pop_server = poplib.POP3(pop_host,pop_port)
            self.__updateParentSessionInfo()
            mail_count = int(tg.config.get(path='Connector',key='pop_num_results'))
            self.count = 0
            if self.pop_server.user(self.pop_user) == '+OK send PASS':
                if self.pop_server.pass_(pop_password) == '+OK Welcome.':
                    self.fetch_next_message = True
                    for id in xrange(int(self.pop_server.stat()[0]),0,-1):
                        if self.fetch_next_message and mail_count>=self.count:
                            try:
                                self.__getData(id)
                            except:
                                continue
                        else:
                            break
                else:
                    log.info(self.log_msg("Could not authenticate the user"))
                    return False
            else:
                log.info(self.log_msg("User not found"))
                return False
            self.pop_server.quit()
            return True
        except:
            log.exception("Exception occured in fetch")
            try:
                self.pop_server.quit()
            except:
                log.info(self.log_msg("Error while logging out from self.pop_server"))
            return False
        
    @logit(log,'__getData')    
    def __getData(self,id):
        """
        Gets information from an email.
        """
        self.count = self.count + 1
        try:
            text = "\n".join(self.pop_server.top(id,10000)[1])
            message = Parser.parsestr(Parser(),text)
            if self.pop_user not in message['to']:
                log.info(self.log_msg("This email is not from inbox"))
                return True
            try:
                message_id =  message['message-id']
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
                    page['data'] = stripHtml("\n".join(text.splitlines()[len(message.values()):]))
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
                                         'post', self.task.instance_data.get('update'), parent_list=[self.task.instance_data['uri']])
                if result['updated']:
                    parent_list = [self.task.instance_data['uri']]
                    page['parent_path'] = copy.copy(parent_list)
                    parent_list.append(message_id)
                    page['path'] = parent_list
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
                    page['client_id'] = self.task.client_id  
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
