import re
from BeautifulSoup import BeautifulSoup
from datetime import datetime
import logging
from urllib2 import urlparse,unquote
import copy

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging. getLogger("stuvuConnector")

class StuvuConnector(BaseConnector):
    @logit(log,'fetch')
    def fetch(self):
        self.genre = "Review"
        try:
            self.parent_uri = self.currenturi
            if not self._setSoup():
                log.info(self.log_msg("Soup not set,returning false"))
                return False
            if not self._getParentPage():
                log.info(self.log_msg("Parent page not found"))
                
            self._addReviews()
            return True
        except:
            log.exception(self.log_msg("Exception in fetch"))
            return False
    @logit(log,'getParentPage')
    def _getParentPage(self):
        page = {}
        page['uri'] = self.currenturi
        page['title'] = stripHtml(str(self.soup.find('div','header_text_left').find('h1').find('a').renderContents()))           
        try:
            if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri, \
                                self.task.instance_data.get('update')):
                log.info(self.log_msg('Session infor return True'))
                return False
            post_hash = get_hash(page)
            id=None
            if self.session_info_out=={}:
                id = self.task.id
            result = updateSessionInfo(self.genre, self.session_info_out,self.parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                return False
            page['path'] = [self.parent_uri]
            page['parent_path'] = []
            page['uri'] = normalize(self.parent_uri)
            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
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
            page['data'] = ''
            page['task_log_id'] = self.task.id
            page['entity'] = 'Post'
            page['category'] = self.task.instance_data.get('category','')
            self.updateParentExtractedEntities(page)
            log.info(self.parent_extracted_entites)
            self.pages.append(page)
            log.info(self.log_msg('Parent Page added'))
            log.info(page)
            return True
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False
        
        
    @logit(log,'addReviews')    
    def _addReviews(self):
        try:
            reviews = self.soup.findAll('div','picbyrow')
            log.info(self.log_msg('no of reviews is %s'%len(reviews)))
            if not reviews:
                return False
        except:
            log.exception(self.log_msg('No Reviews are found'))
            return False
        
        for review in reviews:
            page={}
            
            try:
                page['title'] = stripHtml(review.find('div','pictext reviewtext').find('p').find('a').renderContents())
            except:
                page['title'] = ''    
                log.exception(self.log_msg("Title not found!!"))
            try:
                page['et_author_name'] = stripHtml(str(review.find('div','picinfo').find('p').find('a').renderContents()))

            except:
                log.exception(self.log_msg("Author not mentioned!!"))
            try:
                main_page_soup = copy.copy(self.soup)
                main_page_uri = self.currenturi
                self.currenturi = 'http://www.stuvu.com'+ review.find('div','pictext reviewtext').find('a')['href']
                if self._setSoup():
                    page['data'] = stripHtml(str(self.soup.find('div','review_content').findAll('p')))
                    page['uri']=self.currenturi

            except:
                log.exception(self.log_msg("Next page couldn be parsed!!"))
            try:
                self.soup = copy.copy(main_page_soup)
                self.currenturi = main_page_uri
                self.currenturi = 'http://www.stuvu.com'+ review.find('div','picinfo').find('a')['href']
                if self._setSoup():
                    type = self.soup.findAll('div','profilecontent')
                    page['et_Student_type'] = stripHtml(str(type[0]))
                    page['et_class_of'] = stripHtml(str(type[1]))
            except:
                log.exception(self.log_msg("No author details found!!"))  
            
            try:
                log.info(page)
                review_hash = get_hash(page)
                unique_key = get_hash({'data':page['data'],'title':page['title']})
                if checkSessionInfo(self.genre, self.session_info_out, review_hash,\
                             self.task.instance_data.get('update'),parent_list\
                                                        =[self.parent_uri]):
                    log.info(self.log_msg('session info return True'))
                    
                result = updateSessionInfo(self.genre, self.session_info_out, review_hash, \
                    review_hash,'Review', self.task.instance_data.get('update'),\
                                                parent_list=[self.parent_uri])

                if not result['updated']:
                    log.info(self.log_msg('result not updated'))
                    
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
                page['entity'] = 'Review'
                page['category'] = self.task.instance_data.get('category','')
                page['task_log_id'] = self.task.id
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append(page)
                log.info(page) # To do, remove this
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))
            
                     
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

    
            