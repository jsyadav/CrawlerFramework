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
log = logging. getLogger("YoutellmeConnector")

class YouTellMeConnector(BaseConnector):
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
                return False
            self._addReviews()
            return True
        except:
            log.exception(self.log_msg("Exception in fetch"))
            return False
        
    @logit(log,'getparentpage')    
    def _getParentPage(self):
        page={}
        page['uri'] = self.currenturi
        parent_rating = self.soup.find('div', id='product_summary')
        parent_rating_div = self.soup.find('div', id='product_summary').find('div', {'class':'product_widgets'}) 
        try:
            page['title'] = stripHtml(self.soup.find('div', id='main').find('h1').find('a').renderContents())
        except:
            page['title'] = None
            log.exception(self.log_msg("Title could not be fetched"))
        try:
            x = stripHtml(parent_rating.find('div','price').find('a').renderContents()).split('-')
            page['et_product_price'] = u'\u20ac '  + unicode(sum([float(each.replace(u'\u20ac','').strip())  for each in x])/2)
            #page['et_product_price'] = unicode(sum([float(each.replace(u'\u20ac','').strip())  for each in x])/2)
        except:
            log.exception(self.log_msg("Price not found!!"))
           
        try:
            release_data = parent_rating_div.findAll('div','widget') 
            page['et_' + (release_data[1].find('b').string).lower().replace(' ','_')] = stripHtml(release_data[1].find('b').next.next.next.strip())   
        except:
            log.exception(self.log_msg("Released date not found"))

        try:
            gscore = parent_rating_div.find('div', {"class":"widget first"})
            page['ef_rating_' + (gscore.find('b').string).lower()] = float(str(gscore.find('div').string).strip())
        except:
            log.exception(self.log_msg("Rating not specified"))

        try:
            page['ef_rating_' + (parent_rating_div.find('div','widget').find('b').string).lower()] = float(str(parent_rating_div.find('div','widget').find('div').string).strip())
        except:
            log.exception(self.log_msg("Expert Score not found"))

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
        
    @logit(log, '_addreviews')
    def _addReviews(self):
        try:
            #reviews = self.soup.findAll('div',id='user_reviews')
            reviews = self.soup.findAll('div','block reviewcontainer')
            log.info(self.log_msg('no of reviews is %s'%len(reviews)))
            if not reviews:
                return False
        except:
            log.exception(self.log_msg('No Reviews are found'))
            return False
        for review in reviews:
            page = {}
            page['uri'] = self.currenturi
            try:
                page['title'] = stripHtml(review.find('div','review_header').find('h3').find('a').renderContents())
            except:
                page['title'] = None
                log.exception(self.log_msg("Title Not Found"))

            try:
                date_str = ' '.join(review.find('div','review_header').contents[4].strip().split(' ')[1:-1])
                page['posted_date'] = datetime.strftime(datetime.strptime(date_str, '%d %m %y %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.exception(self.log_msg("Posted date not specified"))

            try:
                page['et_author_name'] = stripHtml(review.find('div','review_header').find('b').find('a').renderContents())
            except:
                log.exception(self.log_msg("Author_info not found"))

            try:
                pros = review.find('div','pros_list').find('ul').findAll('li')
                page['et_data_pros'] = stripHtml(','.join([i.string for i in pros]))                
            except:
                log.exception(self.log_msg("No benifits found"))
            
            try:
                cons = review.find('div','cons_list').find('ul').findAll('li')
                page['et_data_cons'] = stripHtml(','.join([i.string for i in cons]))
                
            except:
                log.exception(self.log_msg("Disadvantages not mentioned"))

            try:
                uses = review.find('div','uses_list').find('ul').findAll('li')
                page['et_data_uses'] = stripHtml(','.join(i.string for i in uses))
            except:
                log.exception(self.log_msg("Uses not found!!"))
            
            try:
                page['ef_rating_overall'] = float(stripHtml(review.find('div','score_container').renderContents()))
            except:
                log.exception(self.log-msg("Rating is not specified"))

            try:
                page.update(dict([('ef_rating_'+x.next.next.string.strip().lower(),float(stripHtml(x.renderContents()))) for x in review.findAll('div','review_ratings')[1].findAll('div')]))
            except:
                log.exception(self.log_msg("Individual ratings not specified"))

            try:
                page['data'] = '\n'.join([i.string for i in review.find('div','review_content').findAll('p')])
            except:
                log.exception(self.log_msg("No Contents!!"))

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

    
        
                                                
                    
                
                
    
    
