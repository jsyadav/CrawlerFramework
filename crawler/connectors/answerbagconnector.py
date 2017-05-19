import re
import copy
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import logging
from urllib2 import urlparse,unquote
import cgi

from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash

from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('AnswerBagConnector')
class AnswerBagConnector(BaseConnector):
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of answer bag connector
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            self.total_posts_count = 0
            try:
                self.max_posts_count = int(tg.config.get(path='Connector',key='answerbag_numresults'))
            except:
                self.max_posts_count = 25
                log.exception(self.log_msg("Max counts not obtained"))
            if not self.currenturi.startswith('http://www.answerbag.com/c_view'):
                if not self._setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self._getParentPage()
                while True:
                    if not self._getAnswers():
                        break
                    try:
                        self.currenturi = 'http://www.answerbag.com'+self.soup.find('div','pagination').find('a',text='next').parent['href']
                        if not self._setSoup():
                            break
                    except:
                        log.exception(self.log_msg("Next page not found"))                    
                        break
                
                return True
            else:
                if not self._setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                while True:
                    if not self._getThreadPage():
                        break
                    try:
                        self.currenturi = 'http://www.answerbag.com'+self.soup.find('div','pagination').find('a',text='next').parent['href']
                        if not self._setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False
            
    @logit(log , 'getThreadPage')
    def _getThreadPage( self ):
            """
            It will fetch each thread and its associate infomarmation
            and add the tasks
            """
            
            questions=self.soup.findAll('div','qa')
            for question in questions:                
                self.total_posts_count = self.total_posts_count + 1
                if self.total_posts_count > self.max_posts_count:
                    log.info(self.log_msg('Reached max Posts count'))
                    return False
                page={}
                try:
                    page['et_answerd_data']= stripHtml(str(question.find('div','primary b').find('a')))
                except:
                    log.exception(self.log_msg("Answered or not is not specified"))             
                try:
                   page['et_data_questions'] = stripHtml(str(question.find('span','question').find('a')))
                except:
                    log.exception(self.log_msg("No questions found!!"))
                try:    
                    page['et_author_name']= stripHtml(str(question.find('div','question_info').find('a')))
                except:
                    log.exception(self.log_msg("No author name found"))
                try:
                    page['et_question_rating'] = stripHtml(str(question.find('div','points')))
                except:
                    log.exception(self.log_msg("Question ratings are not found"))
                try:
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] =  'http://www.answerbag.com'+stripHtml(str(question.find('span','question').find('a')['href']))
                    temp_task.pagedata.update(page)
                    log.info(temp_task.pagedata)
                    self.linksOut.append( temp_task )
                    log.info(self.log_msg('Task Added'))
                except:
                    log.info(self.log_msg('Cannot add the Task'))
            return True 
        
    @logit(log,'getParentPage')
    def _getParentPage(self):
        page = {}
        page['uri'] = self.currenturi
        try:
            page['et_data_hierarchy'] = stripHtml(str(self.soup.find('div','breadcrumb'))).replace(u'\xbb','>>')
        except:
                     
            log.exception(self.log_msg("No category found"))    
        try:
            page['title'] = stripHtml(str(self.soup.find('div',id='question').find('a')))
        except:
            log.exception(self.log_msg("No title found!!"))
        for each in ['et_answerd_data','et_data_questions','et_author_name','et_question_rating']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.exception(self.log_msg('page data cannot be extracted for %s'%each))
        try:
            page['data'] = page['et_data_questions']
        except:
            page['data'] =''
            log.info(self.log_msg('Data not found'))
        main_page_soup = copy.copy(self.soup)
        main_page_uri = self.currenturi    
        try:
            self.currenturi = 'http://www.answerbag.com'+stripHtml(str(self.soup.find('span','asker').find('a')['href']))
            if self._setSoup():
                try:
                    s = stripHtml(str(self.soup.find('div',id='user_info_container2')))
                except:
                    log.exception(self.log_msg("No author details is mentioned"))
                try:
                    page['et_author_member_since'] = re.search('Member Since:(.*?)\n\n',s,re.DOTALL).group(1).strip()[1:].strip()
                except:
                    log.exception(self.log_msg("Member since when is not mentioned")) 
                try:
                    page['et_author_last_seen'] = re.search('Last Seen:(.*?)\n\n',s,re.DOTALL).group(1).strip()[1:].strip()
                except:
                    log.exception(self.log_msg("No proper information is mentioned")) 
                try:
                    page['et_author_gender'] = re.search('Gender:(.*?)\n>\n',s,re.DOTALL).group(1).strip().strip()
                except:
                    log.exception(self.log_msg("Gender is not specified")) 
                try:
                    page['et_author_location'] = re.search('Location:(.*?)\n>\n',s,re.DOTALL).group(1).strip().strip()
                except:
                    log.exception(self.log_msg("Location of the author is not mentioned")) 
        except:
            log.exception(self.log_msg("No Author information found!"))               
        self.soup = copy.copy(main_page_soup)
        self.currenturi = main_page_uri
        try:
            if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri, \
                                self.task.instance_data.get('update')):
                log.info(self.log_msg('Session infor return True'))
                return False
            post_hash = get_hash(page)
            id=None
            if self.session_info_out=={}:
                id = self.task.id
            result = updateSessionInfo( self.genre, self.session_info_out, self.\
                   parent_uri, post_hash,'question',self.task.instance_data.get('update'), Id=id)
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
            #page['data'] = ''
            page['task_log_id']=self.task.id
            page['entity'] = 'question'
            page['category']=self.task.instance_data.get('category','')
            self.pages.append(page)
            log.info(page)
            log.info(self.log_msg('Parent Page added'))
            return True
        except :
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False
           
    @logit(log,'getAnswers')
    def _getAnswers(self):
        try:
            answers=self.soup.findAll('div','question_answer')
        except:
            log.exception(self.log_msg("No answers found!"))    
        for answer in answers:
            page={}
            try:
                page['et_data_bestanswer']=re.sub('\d','',stripHtml(str(answer.find('strong','primary large'))).replace('out of',''))
            except:
                log.exception(self.log_msg("Top answer or not is not mentioned")) 
            try:       
                page['et_answers_data_author_name']=stripHtml(str(answer.find('div','block').find('a')))
            except:
                log.exception(self.log_msg("Author name is not specified for each answer")) 
            try:
                page['posted_date']=datetime.strftime(datetime.strptime( stripHtml(answer.find('span','light').renderContents()),'on %b %d, %Y at %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception(self.log_msg("Posted date not found"))
            try:
                page['data'] = stripHtml(str(answer.find('div','KonaBody')))
                page['title'] = page['data'][:50]
            except:    
                log.exception(self.log_msg("Data not found"))
                continue
            try:    
                page['et_individual_rating']=stripHtml(str(answer.find('div','points')))
            except:
                log.exception(self.log_msg("Individual ratings"))
            try:
                page['uri'] = 'http://www.answewrbag.com'+ answer.findAll('a')[2]['href']
            except:
                log.exception(self.log_msg("No permalink found!!"))
                page['uri'] = self.currenturi
            try:
                log.info(page)
                review_hash = get_hash(page)
                unique_key = get_hash({'data':page['data'],'title':page['title']})
                if not checkSessionInfo(self.genre, self.session_info_out, review_hash,\
                             self.task.instance_data.get('update'),parent_list\
                                                        =[self.parent_uri]):
                    #log.info(self.log_msg('session info return True'))
                    
                    result = updateSessionInfo(self.genre, self.session_info_out, review_hash, \
                        review_hash,'answer', self.task.instance_data.get('update'),\
                                                    parent_list=[self.parent_uri])

                    if result['updated']:
                        parent_list = [self.parent_uri]
                        page['parent_path'] = copy.copy(parent_list)
                        parent_list.append(unique_key)
                        page['path'] = parent_list
                        page['priority'] = self.task.priority
                        page['level'] = self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id
                        page['client_name'] = self.task.client_name
                        page['last_updated_time'] = page['pickup_date']
                        page['versioned'] = False
                        page['entity'] = 'answer'
                        page['category'] = self.task.instance_data.get('category','')
                        page['task_log_id'] = self.task.id
                        page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                        self.pages.append(page)
                        log.info(page) # To do, remove this
                        log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))
                return False
            if self.task.instance_data.get('pick_comments'):
                    self.__getComments(answer,review_hash)
                
                
    @logit(log,'_getComments')
    def __getComments(self,answer,review_hash):
        """
        This function will fetch all the comments given 
        for each answers.
        """            
        page = {}
        try:
            comments = answer.findAll('span',id=re.compile('^cid'))
        except:
            log.exception(self.log_msg("No multiple comments found!!"))
        for comment in comments:        
            try:        
                #comments_author_name = comment.find('div','comment_list').findAll('a')
                author_tag = comment.findAll('a')[1]
                page['et_author_name']=stripHtml(author_tag.renderContents())
                page['et_author_profile'] = 'http://www.answerbag.com' +  author_tag['href']
            except:
                log.exception(self.log_msg("Author name in the comments is not found"))
            try:        
                page['posted_date']=datetime.strftime(datetime.strptime( stripHtml(comment.find('span','small light').renderContents()),'%b, %d %Y at %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception(self.log_msg("Posted date is not mentioned"))
            try:        
                page['data'] = stripHtml(str(comment.find('div','comment_txt')))
                page['title'] = page['data'][:30]
            except:
                log.exception(self.log_msg("No comment data is specified!!"))           
            main_page_soup = copy.copy(self.soup)
            main_page_uri = self.currenturi
            try:
                self.currenturi = page['et_author_profile']
                if self._setSoup():
                    try:
                        s = stripHtml(str(self.soup.find('div',id='user_info_container2')))
                    except:
                        log.exception(self.log_msg("No author details is mentioned"))
                    try:
                        page['et_author_member_since'] = re.search('Member Since:(.*?)\n\n',s,re.DOTALL).group(1).strip()[1:].strip()
                    except:
                        log.exception(self.log_msg("Member since when is not mentioned")) 
                    try:
                        page['et_author_last_seen'] = re.search('Last Seen:(.*?)\n\n',s,re.DOTALL).group(1).strip()[1:].strip()
                    except:
                        log.exception(self.log_msg("No proper information is mentioned")) 
                    try:
                        page['et_author_gender'] = re.search('Gender:(.*?)\n>\n',s,re.DOTALL).group(1).strip().strip()
                    except:
                        log.exception(self.log_msg("Gender is not specified")) 
                    try:
                        page['et_author_location'] = re.search('Location:(.*?)\n>\n',s,re.DOTALL).group(1).strip().strip()
                    except:
                        log.exception(self.log_msg("Location of the author is not mentioned")) 
            except:
                log.exception(self.log_msg("No information about the author of the comment"))            
            self.soup = copy.copy(main_page_soup)
            self.currenturi = main_page_uri
            try:
                log.info(page)
                review_hash = get_hash(page)
                unique_key = get_hash({'data':page['data'],'title':page['title']})
                if not checkSessionInfo(self.genre, self.session_info_out, review_hash,\
                             self.task.instance_data.get('update'),parent_list\
                                                        =[self.parent_uri,review_hash]):
                    #log.info(self.log_msg('session info return True'))
                    
                    result = updateSessionInfo(self.genre, self.session_info_out, review_hash, \
                        review_hash,'comment', self.task.instance_data.get('update'),\
                                                    parent_list=[self.parent_uri,review_hash])

                    if result['updated']:
                        #log.info(self.log_msg('result not updated'))
                        parent_list = [self.parent_uri,review_hash]
                        page['parent_path'] = copy.copy(parent_list)
                        parent_list.append(unique_key)
                        page['path'] = parent_list
                        page['priority'] = self.task.priority
                        page['level'] = self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id
                        page['client_name'] = self.task.client_name
                        page['last_updated_time'] = page['pickup_date']
                        page['versioned'] = False
                        page['entity'] = 'comment'
                        page['category'] = self.task.instance_data.get('category','')
                        page['task_log_id'] = self.task.id
                        page['uri'] = self.currenturi
                        page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                        self.pages.append(page)
                        log.info(page) # To do, remove this
                        log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))
                return False
            
                    
    @logit(log,'setSoup')
    def _setSoup(self, url=None, data=None, headers={}):
        """
            It will set the uri to current page, written in seperate
            method so as to avoid code redundancy
        """
        if url:
            self.currenturi = url
        try:
            log.info(self.log_msg( 'for uri %s' %(self.currenturi) ))
            res = self._getHTML(data=data, headers=headers)
            if res:
                self.rawpage = res['result']
            else:
                log.info(self.log_msg('self.rawpage not set.... so Sorry..'))
                return False
            self._setCurrentPage()
            return True
        except Exception, e:
            log.exception(self.log_msg('Page not for  :%s' %uri))
            raise e
                          