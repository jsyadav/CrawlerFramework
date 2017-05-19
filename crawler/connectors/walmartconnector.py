#modified by prerna for new url on 19th july
#prerna

import re
import copy
import logging
from urllib2 import urlparse
from datetime import datetime, timedelta

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.decorators import logit
from utils.httpconnection import HTTPConnection
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from BeautifulSoup import BeautifulSoup

log = logging.getLogger('WalmartConnector')
class WalmartConnector(BaseConnector):
    
    @logit(log , 'fetch')
    def fetch(self):
        '''This is a fetch method which  fetches the data 
        sample url: http://www.walmart.com/catalog/allReviews.do?product_id=16004961
        '''
        try:
            self.genre = "Review"
            self.__task_elements_dict = {
                            'priority':self.task.priority,
                            'level': self.task.level,
                            'last_updated_time':datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ"),
                            'pickup_date':datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ"),
                            'connector_instance_log_id': self.task.connector_instance_log_id,
                            'connector_instance_id':self.task.connector_instance_id,
                            'workspace_id':self.task.workspace_id,
                            'client_id':self.task.client_id,
                            'client_name':self.task.client_name,
                            'versioned':False,
                            'category':self.task.instance_data.get('category',''),
                            'task_log_id':self.task.id }
            self.parent_uri = self.currenturi
            product_id = '%20'+self.currenturi.split('product_id=')[-1]
            log.info(product_id)
            self.currenturi = 'http://walmart.ugc.bazaarvoice.com/1336a/%s/reviews.djs?format=embeddedhtml&dir=desc&sort=helpfulness'%product_id
            self.__setPage()
            #count = 0
            while self.__iteratePosts():
                self.currenturi = self.soup.find('span','BVRRPageLink BVRRNextPage').\
                                    find('a',title= 'Next')['data-bvjsref']
                self.__setPage()
##                count+=1
##                if count >= 2: #for pagination
##                    break
        except:
            log.exception(self.log_msg('Exception while add the theread posts \
                                            for the url %s'%self.currenturi))
        return True
    @logit(log, '__setPage')
    def __setPage(self):  
        conn = HTTPConnection()
        conn.createrequest(self.currenturi)
        res = conn.fetch().read()
        result= [x for x in res.splitlines() if x.strip().startswith('var materials')][0]
        result1 = result.replace('var materials={"BVRRSecondaryRatingSummarySourceID":"','')
        self.rawpage = result1.replace(result1[-3:],'').replace('\\','')
        self._setCurrentPage()
        
    @logit(log, '__iteratePosts')
    def __iteratePosts(self):   
        try:
            posts = self.soup.findAll('div',id = re.compile('BVRRDisplayContentReviewID_\d+'))
            log.info(len(posts))
            if not posts:
                log.info(self.log_msg('No posts found %s'%self.currenturi))
                return False
            log.debug(self.log_msg('Total No of Posts found is %d'%len(posts)))
            for post in posts:#use some range for few data  
                if not self.__addPost(post):
                    return False
            return True    
        except:
            log.exception(self.log_msg('can not  find the data %s'%self.currenturi))
            return False  
    
    
    @logit(log, '__addPost')    
    def __addPost(self, post): 
        """
        This will take the post tag , and fetch data and meta data and add it to 
        self.pages
        """
        try:
            unique_key = post['id']
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                         self.task.instance_data.get('update'),parent_list\
                                            = [self.task.instance_data['uri']]):
                log.info(self.log_msg('Session info returns True'))
                return False
            page = self.__getData(post)
            if not page:
                return True 
        except:
            log.exception(self.log_msg('Cannot add the post for the url %s'%\
                                                            self.currenturi))
            return False
        try:
            result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                get_hash( page ),'review', self.task.instance_data.get('update'),\
                                parent_list=[self.task.instance_data['uri']])
            if not result['updated']:
                log.exception(self.log_msg('Update session info returns False'))
                return True
            page['parent_path'] = [self.task.instance_data['uri']]
            page['path'] =  [self.task.instance_data['uri'], unique_key]
            page['uri'] = self.parent_uri + '#' + unique_key
            page['entity'] = 'review'
            page['uri_domain']  = urlparse.urlparse(page['uri'])[1]
            page.update(self.__task_elements_dict)
            self.pages.append(page)
            #log.info(page)
            log.info(self.log_msg('page added  %s'%self.currenturi))
            return True
        except:
            log.exception(self.log_msg('Error while adding session info'))
            return False         
    
    
            
    @logit(log, '__getData')
    def __getData(self, post):
        page = {}
        try:
            page['title'] = stripHtml(post.find('span','BVRRValue BVRRReviewTitle').\
                            renderContents())
        except:
            log.exception(self.log_msg('title and data not found'))
            page['title'] = ''
        try:
            page['data'] = stripHtml(post.find('span','BVRRReviewText').\
                            renderContents())
        except:
            log.exception(self.log_msg('title and data not found'))
            page['data'] = ''
        
        if not page['data'] and not page['title']:
            log.info(self.log_msg("data and title not found for %s,""discarding the review"%self.currenturi))
            return False    

        try:
            date_str = stripHtml(post.find('span','BVRRValue BVRRReviewDate').\
                        renderContents()).strip()
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%m/%d/%Y'),\
                                    "%Y-%m-%dT%H:%M:%SZ")
        except:
            log.exception(self.log_msg('posted_date nt found %s'%self.currenturi))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                                           
        try:
            page['ei_data_found_helpful'] = int(stripHtml(post.\
                    find('span',attrs={'class':re.compile('BVDI_FVVote BVDI_FVPositive\w*')}).\
                    find('span','BVDINumber').renderContents()))
        except:
            log.exception(self.log_msg('data found helpful not found ')) 
        try:
            page['et_author_location'] = stripHtml(post.find('span','BVRRValue BVRRUserLocation').\
                                            renderContents())                               
        except:
            log.exception(self.log_msg('author location not found'))                                  
        # author info 
        try:
            page['et_author_name'] = stripHtml(post.find('span','BVRRNickname reviewer').\
                                        renderContents())
        except:
            log.exception(self.log_msg('author_name not found %s'%self.currenturi))
        try:
            page['ef_rating_overall'] =  float(post.find('div','BVRRRatingNormalImage').\
                                            find('img')['title'].split('out')[0])
        except:
            log.exception(self.log_msg('rating overall not found'))    
        try:
            page['et_author_gender']= stripHtml(post.find('span','BVRRValue BVRRContextDataValue BVRRContextDataValueGender').\
                                        renderContents())
        except:
            log.exception(self.log_msg('author gender not found')) 
        try:     
            page['et_author_age_range'] = stripHtml(post.find('span','BVRRValue BVRRContextDataValue BVRRContextDataValueAge').\
                                            renderContents())
        except:
            log.exception(self.log_msg('age range not found'))
        try:
            page['et_data_recommended'] = stripHtml(post.find('span','BVRRValue BVRRRecommended').\
                                            renderContents()) 
        except:
            log.exception(self.log_msg('data recommended not found'))  
        try:
            prod_list =[each.findParent('div')for each in post.findAll('div','BVRRLabel BVRRRatingNormalLabel')]
            for each in prod_list[1:]:
                data_key =stripHtml( each.find('div','BVRRLabel BVRRRatingNormalLabel').renderContents())
                page['ef_rating_'+ data_key.lower().replace(' ','_')] = float(each.find('img')['title'].split('out')[0])
        except:
            log.exception(self.log_msg('product list not found'))                     
                                                                   
                                 
        return page             
        
    
            
            
                                 
                     
           
          
        
    