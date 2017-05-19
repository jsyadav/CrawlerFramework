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
log = logging. getLogger("ProjectWeddingConnector")

class ProjectWeddingConnector(BaseConnector):
    @logit(log,'fetch')
    def fetch(self):
        self.genre = "Review"
        try:
            self.parent_uri = self.currenturi
            if not self._setSoup():
                log.info(self.log_msg("Soup not set,returning false"))
                return False
            self._getParentPage()
            self._addReviews()
        except:
            log.exception(self.log_msg("Exception in fetch"))
        return True
        
    @logit(log,'getparentpage')    
    def _getParentPage(self):
        page={}
        page['uri'] = self.currenturi
        try:
            page['title'] = stripHtml(str(self.soup.find('h1','vendor_title')))
        except:
            log.exception(self.log_msg("No title found!"))
        try:
            page.update(dict(zip(['et_product_'+ stripHtml(each.renderContents()).lower().replace(' ','_')[:-1] for each in self.soup.find('div','vsh_categories').findAll('span','minor_detail_type')],[stripHtml(each.renderContents()) for each in self.soup.find('div','vsh_categories').findAll('span','minor_detail_info')])))
            if page.has_key ('et_product_price'):
                del page['et_product_price']
        except:
            log.exception(self.log_msg("No product details given"))
        try:
            page['ef_rating_overall'] = float(len(self.soup.find('div','vendor_review_stars').findAll('img',src=re.compile('star_full'))))    
        except:
            log.exception(self.log_msg("Rating not given"))    
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
            
            
            return True
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False
    @logit(log, '_addreviews')
    def _addReviews(self):
        try:
            reviews = self.soup.findAll('div','vendor_show_review')
##            f = open('wed.html','w')
##            f.write(self.soup.prettify())
##            f.close()
            log.info(self.log_msg('no of reviews is %s'%len(reviews)))
            if not reviews:
                log.info(self.log_msg('No Reviews found'))
                return False
        except:
            log.exception(self.log_msg('No Reviews are found'))
            return False         
        for review in reviews:
            page = {}
            page['uri'] = self.currenturi
            try:
                page['et_author_name'] = stripHtml(str(review.find('div','profile_user_name')))
            except:
                log.exception(self.log_msg("Author name not given!!"))
            try:
                page['ef_product_rating_overall'] = float(len(review.find('div','vendor_review_stars').findAll('img',src=re.compile('star_full'))))    
            except:
                log.info(self.log_msg("No rating is found!"))
            try:
                page['posted_date'] = datetime.strftime(datetime.strptime(stripHtml(str(review.find('div','vendor_review_date'))),'%m/%d/%Y'),"%Y-%m-%dT%H:%M:%SZ")        
            except:
                log.exception(self.log_msg("Popsted date not mentione"))
            try:
                page['data'] = stripHtml(review.find('div','vendor_review_text').find('p').renderContents()) 
                page['title'] = page['data'][0:20]       
            except:
                log.exception(self.log_msg("Date not found"))
            try:
                page['et_vendor_name'] = stripHtml(str(review.find('div','vendor_review_leftcol').find('a')))
            except:
                log.exception(self.log_msg("Vendor name not given"))
            author_info = [stripHtml(x.renderContents()) for x in self.soup.find('div','profile_caption_right').findAll('a')]
            try:
                page['ei_author_reviews_count'] = author_info[0]
            except:
                log.exception(self.log_msg("The number of reviews written by author is not mentioned"))
            try:
                page['ei_author_photos_count'] = author_info[1]
            except:
                log.exception(self.log_msg("The count of authors photo is not given"))
            try:
                page['ei_author_friends_count'] = author_info[2]
            except:
                log.exception(self.log_msg("The count of authors friends is not given"))                    
    
            try:
                log.info(page)
                review_hash = get_hash(page)
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
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

    
        
                                                        
