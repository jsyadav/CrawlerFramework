'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#SKumar


import re
import logging
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

log = logging.getLogger('FixyaConnector')
class FixyaConnector(BaseConnector):
    '''
    This will fetch the info for Fixya.com Question answers
    http://www.fixya.com/search.aspx?cstm=0&_s=window
    Needs to be picked up only Question and Answers
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of Fix ya Connector
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            self.total_posts_count = 0
            try:
                self.max_posts_count = int(tg.config.get(path='Connector',key='fixya_numresults'))
            except:
                self.max_posts_count = 25
            #if not self.currenturi.startswith('http://www.fixya.com/search.aspx'):
            #log.info(self.task.instance_data)
            if self.task.pagedata.get('already_parsed'):
                if not  self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                self.__addQuestionAndAnswers()
                return True
            else:
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                while True:
                    if not self.__getThreadPage():
                        break
                    try:
                        self.currenturi =  self.soup.find('a',text='Next').parent['href']
                        if not self.currenturi.startswith('http://www.fixya.com'):
                            self.currenturi = 'http://www.fixya.com' + self.currenturi
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
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
            question_urls = ['http://www.fixya.com' + x.a['href'] for x in self.\
                                            soup.findAll('div','c_gen_item_title')]

            if len(question_urls)==0:
                log.info(self.log_msg('Trying another Tags'))
                question_urls = ['http://www.fixya.com' + x.a['href'] for x in self.\
                                            soup.findAll('span','c_uni_cp_title')]
                if len(question_urls)==0:
                    log.info(self.log_msg('No Question link found in this url :%s '%self.currenturi))
                    return False
            for question_url in list(set(question_urls)):
                self.total_posts_count = self.total_posts_count + 1
                if self.total_posts_count >= self.max_posts_count:
                    log.info(self.log_msg('Reached max posts count'))
                    return False
                try:
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] =  question_url
                    temp_task.pagedata['already_parsed'] = True
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
            page['title']= stripHtml(self.soup.find('h1').renderContents())
        except:
            self.email_message.append(self.log_msg('Title Not found'))
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
            if self.task.instance_data.get('pick_comments'):
                self.__addComments(unique_key)
            while True:
                parent_soup = copy.copy(self.soup)
                parent_uri = self.currenturi
                if not self.__addAnswers():
                    break
                try:
                    self.currenturi =  parent_soup.find('a',text='Next').parent['href']
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
            question_tag = self.soup.find('div','g_thread_box g_thread_regular g_thread_regular_n')
            if not question_tag:
                log.info(self.log_msg('No Question Is Found'))
                return False
        except:
            log.info(self.log_msg("No Question info is found"))
            return False
        page = {'entity':'question'}
        try:
            page['et_data_categories'] = '>'.join([x.strip() for x in ('Home |'+\
                re.split('Home  \|',stripHtml(self.soup.find('div','c_global_header')\
                .renderContents()))[-1]).split('|')][:-1])
        except:
            log.info(self.log_msg("No Category is found info is found"))
        try:
            page['posted_date'] =  datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")            
            author_tag = question_tag.find('h3',id='thread_postedBy').find('a')
            page['et_author_name'] = stripHtml(author_tag.renderContents())            
            page['et_author_profile'] = 'http://www.fixya.com' + author_tag['href']
            date_str  = author_tag.next.next.__str__().strip()
            page['posted_date'] =  datetime.strftime(datetime.strptime(date_str,'on %b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg("posted_date is not found or author name not found"))
        parent_uri = self.currenturi
        parent_soup = copy.copy(self.soup)
        try:
            if self.task.instance_data.get('pick_user_info'):
                page.update(self.__getAuthorInfo(page['et_author_profile']))
        except:
            log.info(self.log_msg("Cannot fetch author info"))
        self.currenturi = parent_uri
        self.soup = copy.copy(parent_soup)
        try:
            page['data'] = stripHtml(question_tag.find('div','fintl KonaBody').renderContents())
        except:
            log.exception(self.log_msg('Data not found'))
            page['data']=''
        try:
            page['title']= stripHtml(self.soup.find('h1').renderContents())
        except:
            log.info(self.log_msg("Cannot fetch Title"))
            page['title']=''
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
            unique_key = question_tag.find('a')['name']
            page['uri'] = self.currenturi +'#' + unique_key
        except:
            log.exception(self.log_msg('Cannot find the unique Key'))
            return False
        return unique_key,page
    
    @logit(log, "__addAnswers")
    def __addAnswers(self):
        '''This will get the Answers
        '''
        try:
            answers = self.soup.findAll('div','g_thread_answerbox g_thread_answer_regular')
            if not answers:
                log.info(self.log_msg('No Answer is found'))
                return False
        except:
            log.exception(self.log_msg('Cannot find the Answers'))
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
            title_info = stripHtml(answer.find('h2').renderContents())
            if title_info.lower()=='best solution':
                page['et_data_best_answer'] ='True'
        except:
            log.info(self.log_msg("Cannot find the best answer"))
        try:
            page['posted_date'] = datetime.strftime(datetime.strptime(stripHtml\
                (answer.find('div','posted').renderContents()),'posted on %b %d, %Y')\
                                                            ,"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg("title and author info"))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        
        parent_uri = self.currenturi
        try:
            aut_tag =  answer.find('div','userlink').find('a')
            page['et_author_name'] = stripHtml(aut_tag.renderContents())
            page['et_author_profile'] = 'http://www.fixya.com' + aut_tag['href']
            if self.task.instance_data.get('pick_user_info'):
                page.update(self.__getAuthorInfo(page['et_author_profile']))
        except:
            log.info(self.log_msg("Cannot fetch author info"))
        self.currenturi = parent_uri
        try:
            rating = re.search('\d+',answer.find('div','star').find('img')['src'].split('/')[-1]).group()
            if not rating=='0':
                page['ef_rating_overall'] = float(rating)
        except:
            log.info(self.log_msg("Cannot fetch rating"))
        try:
            page['data'] = stripHtml(answer.find('div','p_fintl').renderContents())
            page['title']=page['data'][:50] + '...'
        except:
            log.info(self.log_msg('data not found'))
            page['title'] = page['data']=''            
        try:
            page['ei_data_recommended'] = int(re.search('\d+',stripHtml(answer.find('span','yes_ctr').renderContents())).group())
        except:
            log.info(self.log_msg("Cannot fetch recommended"))
        try:
            unique_key = answer.find('a')['name']
            page['uri'] = self.currenturi +'#' + unique_key
        except:
            log.exception(self.log_msg('Cannot find the unique Key'))
            return False
        return unique_key,page
    
    @logit(log, "__addPage")
    def __addPage(self,unique_key,page,parent_list=[]):
        '''This will add the page
        '''
        try:
            if not parent_list:
                parent_list = [self.parent_uri]
            log.info(page['posted_date'])
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                         self.task.instance_data.get('update'),parent_list\
                                                        =parent_list):
                log.info(self.log_msg('Session info returns True'))
                return False
            result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                        get_hash( page ),page['entity'], self.task.instance_data.get('update'),\
                                                    parent_list=parent_list)
            if not result['updated']:
                log.info(self.log_msg('Result not updated'))
                return False
            my_parent_list = parent_list
            page['parent_path'] = copy.copy(my_parent_list)
            my_parent_list.append( unique_key )
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
            if not 'uri' in page.keys():
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
    @logit(log,'__getAuthorInfo')
    def __getAuthorInfo(self,author_url):
        '''This will fetch the author info
        '''
        aut_page = {}
        try:
            self.currenturi = author_url
            if not self.__setSoup():
                return aut_page
        except:
            log.info(self.log_msg('Author page soup found'))
            return aut_page
        try:
            div_tag = self.soup.find('div','secRow')
            aut_page['et_author_title'] = stripHtml(div_tag.findAll('div',recursive=False)[1].renderContents())
        except:
            log.info(self.log_msg('cannot find the author info'))
        try:
            aut_page['edate_author_member_since'] =  datetime.strftime(datetime.strptime('01 ' + stripHtml(div_tag.find('div','memsince').renderContents()).split(':')[-1].strip(),'%d %b %Y'),"%Y-%m-%dT%H:%M:%SZ")
            message = stripHtml(div_tag.find('div','moto').renderContents())
            if not message=='':
                aut_page['et_author_message'] = message
        except:
            log.info(self.log_msg('author member since or author message not found'))
        try:
            info_tag = self.soup.find('div','bgContainer')
            aut_page['ei_author_solved_problems_count'] = int(stripHtml(info_tag.find('div','leftNum').renderContents()))
            aut_page['ef_author_rating'] = float(stripHtml(info_tag.find('div','rightNum').renderContents())[:-1])            
        except:
            log.info(self.log_msg('Problems count not found'))
        try:
            aut_page.update(dict([('ei_author_'+re.sub('[^\w]+','_',k).lower(),int(v)) for k,v in dict([[y.strip() for  y in stripHtml(x.renderContents()).split(':')] for x in self.soup.find('div','rightText').findAll('div','dotBG')]).iteritems()]))
        except:
            log.info(self.log_msg('Author stat not found'))
        return aut_page
    
    @logit(log,'__addComments')
    def __addComments(self,question_key):
        '''This will add the comments for the Question or clarifications
        '''
        try:
            comments = self.soup.find('div','g_thread_box g_thread_regular g_thread_regular_n').findAll('div','cmmnt')        
            parent_uri = self.currenturi
        except:
            log.info(self.log_msg('No comments found'))
            return False
        for comment in comments:
            page={'entity':'comment','uri':parent_uri}
            try:
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                post_info = comment.find('h3').extract()
                class_name = dict(post_info.attrs).get('class')
                if class_name and class_name=='p_cmmnt_date':
                    page['posted_date'] =  datetime.strftime(datetime.strptime(stripHtml(post_info.renderContents()),'%b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
                else:
                    try:
                        aut_tag = post_info.find('a')
                        page['et_author_name'] = stripHtml(aut_tag.renderContents())
                        page['et_author_profile']= aut_tag['href']
                        if self.task.instance_data.get('pick_user_info'):
                            page.update(self.__getAuthorInfo(page['et_author_profile']))
                        date_str  = aut_tag.next.next.__str__().strip()
                        page['posted_date'] =  datetime.strftime(datetime.strptime(date_str,'on %b %d, %Y'),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg('Author info not found'))
            except:
                log.info(self.log_msg('Author info and posted date not found'))
            try:
                page['data'] = stripHtml(comment.renderContents())
                page['title']=page['data'][:50]+'...'
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                self.__addPage( unique_key,page,[self.parent_uri,question_key ] )
            except:
                page['title'] = page['data'] = ''
        return True
                
        
        
    
    
    