
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''


#ASHISH YADAV


import re
import md5
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import logging
from urllib2 import urlparse,unquote
from urllib import urlencode
import cgi

from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash

from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('YahooAnswersConnector')
class YahooAnswersConnector(BaseConnector):

    @logit(log , 'fetch')
    def fetch(self):

        self.genre="Review"
        try:
            self.__base_uri = 'http://answers.yahoo.com/'
            code = None
            parent_uri = self.currenturi
            res=self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            self.POSTS_ITERATIONS = tg.config.get(path='Connector',key='yahooanswers_numposts')
            self.__max_date_submission_date = tg.config.get(path='Connector',key='yahooanswers_max_date_submission')
            self.curiter = 0
            if '/question/index' not in self.currenturi:
                self.__createSiteUrl()
                next_page = self.soup.find('li',{'class':'next'})
                while self.addQuestionUrls(parent_uri) and next_page:
                    try:
                        self.currenturi = normalize(self.__base_uri + next_page.a['href'])
                        log.debug(self.log_msg("Fetching url %s" %(self.currenturi)))
                        res=self._getHTML()
                        self.rawpage=res['result']
                        self._setCurrentPage()
                        next_page = self.soup.find('li',{'class':'next'})
                    except Exception, e:
                        log.exception(self.log_msg('exception in iterating pages in fetch'))
                        break
            else:
                self._getParentPage(parent_uri)
                self._getAnswers(parent_uri)
            return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False
    
    @logit(log, '__createSiteUrl')
    def __createSiteUrl(self):
        '''This will change the url so that it will pickup the answers that are
            posted within the specified date range
        '''
        params = {}
        default_values = { 'category':'',            
                    'exclude_terms':'',                        
                    'filter_search':'Apply',
                    'fltr':'_en',
                    'keywords_filter':'all',
                    'question_status':'all',
                    'scope':'all',
                    'date_submitted':self.__max_date_submission_date
                }
        url_parts = self.currenturi.split('?')
        old_params = dict(cgi.parse_qsl(url_parts[1]))
        params['p'] = old_params['p']
        for each in default_values.keys():
            params[each] = old_params.get(each, default_values[each])
        crumb_value = self.soup.find('input', attrs={'name':'crumb','value':True})
        if crumb_value and old_params.has_key('crumb'):
            params['crumb'] =  crumb_value['value']
        self.currenturi = url_parts[0] + '?' + urlencode(params)
        res = self._getHTML()
        self.rawpage = res['result']
        self._setCurrentPage()
            
    @logit(log , 'addQuestionsUrls')
    def addQuestionUrls(self, parenturi):
        question_urls = self.soup.findAll('a',{'href' :re.compile('/question/index;.*')})
        for url in question_urls:
            self.curiter+=1
            if self.curiter > self.POSTS_ITERATIONS:
                return False
            url = normalize(self.__base_uri + url['href'])
            url_segments = [each for each in urlparse.urlparse(url)]
            url_segments[3] = ';'.join([re.sub('=.*?$','=0',url_s) for url_s in url_segments[3].split(';') ])
            url = urlparse.urlunparse(url_segments)
            temp_task = self.task.clone()
            temp_task.instance_data['uri'] = url
            temp_task.instance_data['isquestion'] = True
            self.linksOut.append(temp_task)
        log.info(self.log_msg("No. of tasks appened %s"%len(self.linksOut)))
        return True

    @logit(log , '_getParentPage')
    def _getParentPage(self,url):
            try:
                #continue if returned true
                qid =  cgi.parse_qs(url.split('?')[-1]).get('qid')
                if not qid:
                    log.info(self.log_msg("Question id not found so returning"))
                    raise e

                qid = qid[0]
                page={}
                try:
                    page['title'] = stripHtml(self.soup.find('h1',{'class':'subject'}).renderContents())
                except Exception, e:
                    log.exception(self.log_msg('could not parse page title'))
                    raise e

                try:
                    page['data'] = stripHtml(self.soup.find('div',{'class':'content'}).renderContents())
                except Exception, e:
                    log.exception(self.log_msg('could not parse post data'))
                    raise e

                try:
                    page['et_author_name'] = stripHtml(self.soup.find('span',{'class':'user'}).span['title'])
                except:
                    log.info(self.log_msg('could not parse author name'))
                try:
                    self.hierachy = [x.strip() for x in stripHtml(self.soup.find('ol',id='yan-breadcrumbs').renderContents()).split('>')]
                    page['et_data_hierarchy'] =  '>'.join(self.hierachy[1:-1])
                    page['et_data_question_type'] = self.hierachy[-1]
                except:
                    log.info(self.log_msg('Data hierachy not found'))
                try:
                    page['et_author_profile'] = normalize(self.__base_uri + self.soup.find('a',{'class':'url'})['href'])
                except:
                    log.info(self.log_msg('could not parse author profile link'))

                try:
                    author_joined_date = stripHtml(self.soup.find('div',{'id':'yan-question'}).find('dd',
                                                                                         {'class':'member'}).renderContents())
                    author_joined_date = datetime.strptime(author_joined_date , '%B %d, %Y')
                    page['edate_author_member_since'] = datetime.strftime(author_joined_date,"%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.info(self.log_msg('could not parse author joined date'))

                try:
                    page['et_author_points'] = stripHtml(self.soup.find('div',{'id':'yan-question'}).find('dd',
                                                                                         {'class':'total'}).renderContents())
                except:
                    log.info(self.log_msg('could not parse author points'))

                self.updateParentExtractedEntities(page) #update parent extracted entities

                if not checkSessionInfo(self.genre, self.session_info_out,
                                        qid, self.task.instance_data.get('update')):
                    id=None
                    if self.session_info_out=={}:
                            id=self.task.id
                    log.debug(self.log_msg('checking session info'))

                    try:
                        post_hash = get_hash(page)
                    except Exception,e:
                        log.exception(self.log_msg('could not build post_hash'))
                        raise e

                    result=updateSessionInfo(self.genre,self.session_info_out,qid, post_hash,
                                             'Question', self.task.instance_data.get('update'), Id=id)
                    if result['updated']:
                        page['path'] = page['parent_path'] = []
                        page['path'].append(qid)
                        page['uri'] = normalize(url)
                        page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        try:
                            posted_date = self.soup.find('ul',{'class':'meta'}).li.abbr['title']
                            posted_date = datetime.strptime(posted_date, '%Y-%m-%d %H:%M:%S')
                            page['posted_date'] = datetime.strftime(posted_date,"%Y-%m-%dT%H:%M:%SZ")
                        except:
                            log.info('could not parse posted_date')
                            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id  # TODO: Get the client from the project
                        page['client_name'] = self.task.client_name
                        page['last_updated_time'] = page['pickup_date']
                        page['versioned'] = False
                        page['task_log_id']=self.task.id
                        page['entity'] = 'post'
                        page['category']=self.task.instance_data.get('category','')
                        self.pages.append(page)
            except Exception,e:
                log.exception(self.log_msg("parent post couldn't be parsed"))
                raise e

    @logit(log , '_getAnswers')
    def _getAnswers(self,parent_url):
            try:
                #continue if returned true
                qid =  cgi.parse_qs(parent_url.split('?')[-1]).get('qid')
                if not qid:
                    log.info(self.log_msg("Question id not found so returning"))
                    raise e
                qid = qid[0]
                answers = self.soup.findAll('div',{'class':['answer','answer best']})

                for answer in answers:
                    kid =  cgi.parse_qs(answer.find('a',title='Report this answer')['href']).get('kid')
                    if not kid:
                        log.info(self.log_msg('Could not find comment id , so continuing from the next comment'))
                        continue
                    kid = kid[0]
                    if not checkSessionInfo(self.genre, self.session_info_out,
                                            kid, self.task.instance_data.get('update'),parent_list=[qid]):
                        log.debug(self.log_msg('checking session info'))
                        page={}
                        try:
                            class_name = answer.get('class')
                            if class_name=='answer best':
                                page['et_data_best_answer'] =  'True'
                                log.info(page['et_data_best_answer'] )
                        except:
                            log.info(self.log_msg('not a best answer'))
                        try:
                            page['et_data_hierarchy'] =  '>'.join(self.hierachy[1:-1])
                            page['et_data_question_type'] = self.hierachy[-1]
                        except:
                            log.info(self.log_msg('Data hierachy not found'))

                        try:
                            page['data'] = stripHtml(answer.find('div',{'class':'content'}).renderContents())
                        except Exception, e:
                            log.exception(self.log_msg('could not parse post data'))
                            page['data'] = ''

                        try:
                            page['title'] = re.sub('\n','',page['data'])[:50].capitalize() + '... '
                        except Exception, e:
                            log.exception(self.log_msg('could not parse page title'))
                            page['title'] = ''


                        try:
                            page['et_author_name'] = stripHtml(answer.find('span',{'class':'user'}).find('span',
                                                                                     title=True)['title'])
                        except:
                            log.info(self.log_msg('could not parse author name'))

                        try:
                            page['et_author_profile'] = normalize(self.__base_uri + answer.find('a',{'class':'url'})['href'])
                        except:
                            log.info(self.log_msg('could not parse author profile link'))


                        try:
                            rating = answers.find('dl',{'class':'answer-rating'})
                            if rating:
                                answer_rating =  rating.dd.img['alt'].split()[0]
                                page['ei_data_rating'] = int(answer_rating)
                                page['et_data_feedback'] = stripHtml(answer.find('dl',{'class':'answer-rating'}).find('dd',
                                                                                             {'class':'desc'}).renderContents())
                        except:
                            log.info(self.log_msg('could not parse author feedback / rating on the best answer'))

                        try:
                            author_joined_date = stripHtml(answer.find('dd',{'class':'member'}).renderContents())
                            author_joined_date = datetime.strptime(author_joined_date , '%B %d, %Y')
                            page['edate_author_member_since'] = datetime.strftime(author_joined_date,"%Y-%m-%dT%H:%M:%SZ")
                        except:
                            log.info(self.log_msg('could not parse author joined date'))

                        try:
                            page['et_author_points'] = stripHtml(answer.find('dd',{'class':'total'}).renderContents())
                        except:
                            log.info(self.log_msg('could not parse author points'))

                        try:
                            vote_count = stripHtml(answer.find('div',{'class':'vote-count'}).renderContents())
                            vote_per,vote_count = re.findall('^([0-9]*)%[ ]+([0-9]*).*?$','21%   3  Votes')[0]
                            page['et_data_votes_percentage'] = vote_per + '%'
                            page['ei_data_votes_number'] = int(vote_count)
                        except:
                            log.info(self.log_msg('could not parse answer vote count'))

                        try:
                            page['ei_data_votes_up'] = int(stripHtml(answer.find('li',{'class':'rate-up'}).renderContents()).split()[0])
                        except:
                            log.info(self.log_msg('could not parse answer vote up count'))

                        try:
                            page['ei_data_votes_down'] = int(stripHtml(answer.find('li',
                                                                                {'class':'rate-down'}).renderContents()).split()[0])
                        except:
                            log.info(self.log_msg('could not parse answer vote down count'))

                        try:
                            post_hash = get_hash(page)
                        except Exception,e:
                            log.exception(self.log_msg('could not build post_hash'))
                            raise e

                        result=updateSessionInfo(self.genre,self.session_info_out,kid, post_hash,
                                                 'Answer', self.task.instance_data.get('update'), parent_list =[qid])
                        if result['updated']:
                            page['path'] = page['parent_path'] = [qid]
                            page['path'].append(kid)
                            page['uri'] = normalize(parent_url)
                            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                            page['priority']=self.task.priority
                            page['level']=self.task.level
                            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                            try:
                                posted_date = answer.find('abbr',{'title':True})['title']
                                posted_date = datetime.strptime(posted_date, '%Y-%m-%d %H:%M:%S')
                                page['posted_date'] = datetime.strftime(posted_date,"%Y-%m-%dT%H:%M:%SZ")
                            except:
                                log.info('could not parse posted_date')
                                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                            page['connector_instance_log_id'] = self.task.connector_instance_log_id
                            page['connector_instance_id'] = self.task.connector_instance_id
                            page['workspace_id'] = self.task.workspace_id
                            page['client_id'] = self.task.client_id  # TODO: Get the client from the project
                            page['client_name'] = self.task.client_name
                            page['last_updated_time'] = page['pickup_date']
                            page['versioned'] = False
                            page['task_log_id']=self.task.id
                            page['entity'] = 'comment'
                            page['category']=self.task.instance_data.get('category','')
                            self.pages.append(page)
            except Exception,e:
                log.exception(self.log_msg("parent post couldn't be parsed"))
                raise e
