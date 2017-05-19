'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#SKumar


import re
import logging
import smtplib
import copy
from cgi import parse_qsl
from datetime import datetime,timedelta
from urllib2 import urlparse
from urllib import urlencode
from tgimport import tg


from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText

log = logging.getLogger('AskVilleConnector')
class AskVilleConnector(BaseConnector):
    '''
    This will fetch the info for ask ville Question answers
    http://askville.amazon.com/SearchRequests.do?search=%22windows+7%22&x=0&y=0&start=0&max=10&open=true&closed=true&bonus=false
    Needs to be picked up only Question and Answers
    Discussions also available, but answers are needed now
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of ask ville connector
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            self.total_posts_count = 0
            self.email_message = []
            try:
                self.max_posts_count = int(tg.config.get(path='Connector',key='askville_numresults'))
            except:
                self.email_message.append(self.log_msg('Config file not updated for askville connector,taken first 50'))
                self.max_posts_count = 50
            if not self.currenturi.startswith('http://askville.amazon.com/SearchRequests.do'):
                if not  self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                self.__addQuestionAndAnswers()
                self.__sendEmail()
                return True
            else:
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                while True:
                    if not self.__getThreadPage():
                        break
                    try:
                        code = re.search('\((\d+)\)',self.soup.find('a',text='Next &gt;').parent['href']).group(1)
                        params = dict(parse_qsl(self.currenturi.split('?')[-1]))
                        params['start'] = code
                        self.currenturi =  'http://askville.amazon.com/SearchRequests.do?' + urlencode(params)
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                self.__sendEmail()
                return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False
        

    @logit(log , '__getThreadPage')
    def __getThreadPage( self ):
            """
            It will fetch each thread and its associate infomarmation
            and add the tasks
            """
            question_urls = ['http://askville.amazon.com'+ x['href'] for x in \
                        self.soup.find('div',id='ad_contained_1').findAll('a')]
            if len(question_urls)==0:
                self.email_message.append('No Question link found in this url :%s '%self.currenturi)
                return False
            for question_url in list(set(question_urls)):
                self.total_posts_count = self.total_posts_count + 1
                if self.total_posts_count >= self.max_posts_count:
                    log.info(self.log_msg('Reached max posts count'))
                    return False
                try:
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] =  question_url
                    self.linksOut.append( temp_task )
                    log.info(self.log_msg('Task Added'))
                except:
                    log.info(self.log_msg('Cannot add the Task'))
            return True    

    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will get the parent info
        """
        
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        page = {}
        try:
            page['title']= stripHtml(self.soup.find('h1','label').renderContents())
        except:
            self.email_message.append(self.log_msg('Title Not found'))
            log.exception(self.log_msg('Title not found'))
            page['title'] = ''
        try:
            post_hash = get_hash( page )
            id = None
            if self.session_info_out=={}:
                id=self.task.id
            result = updateSessionInfo( self.genre, self.session_info_out, self.\
                   parent_uri, post_hash,'Forum',self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                return False
            page['path']=[self.parent_uri]
            page['parent_path']=[]
            page['uri'] = normalize( self.currenturi )
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
            page['data'] = ''
            page['task_log_id']=self.task.id
            page['entity'] = 'Post'
            page['category']=self.task.instance_data.get('category','')
            self.pages.append(page)
            log.info(page)
            log.info(self.log_msg('Parent Page added'))
            return True
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False
    
    @logit(log, "__addQuestionAndAnswers")
    def __addQuestionAndAnswers(self):
        """It will add the Quesion and Answer
        """
        try:
            unique_key, page = self.__getQuestionData()
            if not self.__addPage(unique_key,page):
                log.info(self.log_msg('Question not added'))
            while True:
                if not self.__addAnswers():
                    break
                try:
                    code = re.search('\((\d+)\)',self.soup.find('a',text='Next &gt;').parent['href']).group(1)
                    params = dict(parse_qsl(self.currenturi.split('?')[-1]))
                    params['start'] = code
                    self.currenturi = self.currenturi.split('?')[0] +'?' + urlencode(params)
                    if not self.__setSoup():
                        break
                except:
                    log.info(self.log_msg('Next Page link not found'))
                    break
        except:
            log.info(self.log_msg('Cannot add question'))
            return False
        return True
    
    @logit(log, "__getQuestionData")
    def __getQuestionData(self):
        """It will add the Quesion and its details
        """
        try:
            question_tag = self.soup.find('table','request_details_table')
            if not question_tag:
                log.info(self.log_msg('No Question Is Found'))
                self.email_message.append(self.log_msg('No Question Details is found'))
                return False
        except:
            log.info(self.log_msg("No Question info is found"))
            return False
        page = {'entity':'question'}
        try:
            page['et_data_categories'] = ','.join([stripHtml(x.renderContents()) for x in self.soup.find('div',id=re.compile('tags_\d*')).findAll('a')])
        except:
            log.info(self.log_msg("No Category is found info is found"))
        try:
            page['posted_date'] =  datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            aut_tag = question_tag.find('span','mininav',text=re.compile('Asked by')).parent
            page['et_author_profile'] = 'http://askville.amazon.com'+ aut_tag.find('a',href=re.compile('ViewUserProfile'))['href']
            page['et_author_name'],date_str = [x.strip() for x in stripHtml(aut_tag.renderContents()).split('\n') if not x.strip()==''][1:3]                              
            code = int(re.search('\d+',date_str).group())
            date_obj =  datetime.utcnow()
            if 'months' in date_str:
                date_obj = date_obj-timedelta(days=code*30)
            elif 'days' in date_str:
                date_obj = date_obj-timedelta(days=code)
            elif 'hours' in date_str:
                date_obj = date_obj-timedelta(hours=code)
            elif 'mins' in date_str:
                date_obj = date_obj-timedelta(minutes=code)
            page['posted_date'] =  datetime.strftime(date_obj,"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg("posted_date is found"))
        parent_soup = copy.copy(self.soup)
        parent_uri = self.currenturi
        try:
            if self.task.instance_data.get('pick_user_info'):
                self.currenturi = page['et_author_profile']
                if self.__setSoup():
                    page.update(dict([('et_author_'+k.strip().lower().replace(' ','_'),\
                        v.strip()) for k,v in dict([stripHtml(x.renderContents()).split(':')  \
                        for x in self.soup.find('div',id='profile_detail').findAll('tr')]).iteritems()]))
                    page.update(dict([('et_author_' + k.strip().lower().replace(' ','_'),v.strip()) \
                        for k,v in dict([stripHtml(x.renderContents()).split(':') for x in \
                                    self.soup.findAll('div','stats_lineitem')]).iteritems()]))
        except:
            log.info(self.log_msg("Cannot fetch author info"))
        self.soup = copy.copy(parent_soup)
        self.currenturi = parent_uri
        try:
            page['data'] = re.sub('^Details:','',stripHtml(self.soup.find('div','details_text').renderContents())).strip()            
        except:
            page['data']=''            
            self.email_message.append(str(log.exception(self.log_msg(self.log_msg("Cannot fetch data")))))
            return False
        try:
            page['title'] = stripHtml(question_tag.find('h1','label').renderContents())
        except:
            log.info(self.log_msg("Cannot fetch Title"))
            self.email_message.append(self.log_msg('Title  not found'))
            page['title'] = ''
        try:
            if page['title'] == '':
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
        try:
            unique_key = question_tag.find('div','request_details_in_list')['id']
        except:
            log.exception(self.log_msg('Cannot find the unique Key'))
            self.email_message.append(self.log_msg('No Unique Key found for this page'))
            return False
        return unique_key,page
    
    
    @logit(log, "__addAnswers")
    def __addAnswers(self):
        '''This will get the Answers
        '''
        try:
            answers = self.soup.findAll('table','answer')
            if not answers:
                log.info(self.log_msg('No Answer is found'))
                return False
        except:
            log.exception(self.log_msg('Cannot find the unique Key'))
            return False
        for answer in answers:
            try:
                unique_key,page = self.__getAnswerData(answer)
                if not self.__addPage(unique_key,page):
                    log.info(self.log_msg('Question not added'))
            except:
                log.info(self.log_msg('Cannot add the Answer Data'))
        return True
                
    @logit(log, "__getAnswerData")
    def __getAnswerData(self,answer):
        page = {'entity':'answer','title':''}
        try:
            title_tag= answer.find('h2','answer_summary')
            page['title'] = stripHtml(title_tag.renderContents())
            tag_info = title_tag.findParent('table').findAll('tr')[-1]
            page['et_author_name'],temp,date_str = [x.strip() for x in stripHtml(tag_info.renderContents()).split('\n') if not x.strip()==''][1:4]
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str, '%b %d %Y'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg("title and author info"))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        parent_soup = copy.copy(self.soup)
        parent_uri = self.currenturi
        try:
            page['et_author_profile'] ='http://askville.amazon.com'+ tag_info.find('a',href=re.compile('ViewUserProfile'))['href']            
            if self.task.instance_data.get('pick_user_info'):
                self.currenturi = page['et_author_profile']
                if self.__setSoup():
                    page.update(dict([('et_author_'+k.strip().lower().replace(' ','_'),\
                        v.strip()) for k,v in dict([stripHtml(x.renderContents()).split(':')  \
                        for x in self.soup.find('div',id='profile_detail').findAll('tr')]).iteritems()]))
                    page.update(dict([('et_author_' + k.strip().lower().replace(' ','_'),v.strip()) \
                        for k,v in dict([stripHtml(x.renderContents()).split(':') for x in \
                                    self.soup.findAll('div','stats_lineitem')]).iteritems()]))
        except:
            log.info(self.log_msg("Cannot fetch author info"))
        self.soup = copy.copy(parent_soup)
        self.currenturi = parent_uri
        try:
            page['ef_rating_overall'] = float(len(answer.findAll('img',src=re.compile('rating_star_small_gold-v0002.gif'))))
        except:
            log.info(self.log_msg("Cannot fetch rating"))
        try:
            if answer.find('span','bestanswer'):
                page['et_data_best_answer'] = 'True'
            else:
                page['et_data_best_answer'] = 'False'
        except:
            log.info(self.log_msg("Cannot fetch Best answer"))
        try:
            answer_segments =  answer.findAll('table','answer_segment')
            answer_segment = answer_segments[0]
            data_tags = answer_segment.findAll('tr',recursive=False)
            page['data'] = stripHtml(data_tags[0].renderContents())
            if len(data_tags)>1:
                try:
                    page['et_data_sources'] = re.sub('^Sources:','',stripHtml(data_tags[1].renderContents())).strip()
                except:
                    log.info(self.log_msg("Cannot fetch data author recommendation"))
            if len(answer_segments)>1:
                try:
                    page['et_author_recommended'] = stripHtml(answer_segments[1].findAll('tr',recursive=False)[1].renderContents())
                except:
                    log.info(self.log_msg("Cannot fetch data author recommendation"))
        except:
            page['data']=''
            log.info(self.log_msg("Cannot fetch data"))
            self.email_message.append('data not found')
        try:
            page['ei_data_recommended'] = int(stripHtml(answer.find('span','thumbs_up_count').renderContents()))
        except:
            log.info(self.log_msg("Cannot fetch recommended"))
        try:
            if page['title'] == '':
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
        except:
            log.exception(self.log_msg('title not found'))
            page['title'] = ''
        try:
            unique_key = answer['id']
        except:
            log.exception(self.log_msg('Cannot find the unique Key'))
            self.email_message.append('No Unique Key found for this page')
            return False
        return unique_key,page
    
    @logit(log, "__addPage")
    def __addPage(self,unique_key,page):
        '''This will add the page
        '''
        try:
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                         self.task.instance_data.get('update'),parent_list\
                                                        =[self.parent_uri]):
                log.info(self.log_msg('Session info returns True'))
                return False
            result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                        get_hash( page ),page['entity'], self.task.instance_data.get('update'),\
                                                    parent_list=[self.parent_uri])
            if not result['updated']:
                log.info(self.log_msg('Result not updated'))
            parent_list = [ self.parent_uri ]
            page['parent_path'] = copy.copy(parent_list)
            parent_list.append( unique_key )
            page['path']=parent_list
            page['priority']=self.task.priority
            page['level']=self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                ,"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            page['client_name'] = self.task.client_name
            page['last_updated_time'] = page['pickup_date']
            page['versioned'] = False
            page['category'] = self.task.instance_data.get('category','')
            page['task_log_id']=self.task.id
            page['uri'] = self.currenturi
            page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
            self.pages.append( page )
            log.info(self.log_msg('Post Added'))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False
            
    @logit(log, "_setSoup")
    def __setSoup(self, url = None, data = None, headers = {}):
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
        
    @logit(log, "__sendEmail")
    def __sendEmail(self):
        '''This will send a email to Saravana kumar abt the site changes and Exceptions
        '''
        try:
            if len(self.email_message) == 0:
                log.info(self.log_msg('no exceptions found'))
                return False
            subject = 'List of Exceptions in AskVille Connector'
            self.email_message(self.parent_uri)
            body = '\n'.join(self.email_message)
            from_addr = 'services@serendio.com'
            to_addr = ['saravanan@serendio.com']
            smtp_host = 'smtp.gmail.com'
            smtp_port = 587
            smtp_username = 'services@serendio.com'
            smtp_pw = 'kmsystemmailer'
            server = smtplib.SMTP(smtp_host,smtp_port)
            server.ehlo()
            server.starttls()
            server.ehlo()
            msg = MIMEMultipart()
            msg.attach(MIMEText (body))
            msg['Subject'] = subject
            msg['From'] = from_addr
            msg['To'] = ','.join(to_addr)
            server.login(smtp_username, smtp_pw)
            response = server.sendmail(from_addr, to_addr, msg.as_string())
            server.quit()
            log.info(self.log_msg('email sent'))
        except:
            log.exception(self.log_msg('cannot send email'))