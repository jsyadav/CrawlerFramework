from baseconnector import BaseConnector
from datetime import datetime
from urlparse import urlparse
from tgimport import *
from utils.urlnorm import normalize
from utils.utils import stripHtml,get_hash
from utils.sessioninfomanager import *
from utils.decorators import logit
import logging

log = logging.getLogger('IMDBConnector')
class IMDBConnector(BaseConnector):
    """
    Connector for imdb.com reviews
    """
    base_url = "http://www.imdb.com"
    def fetch(self):
        """
        Overrides BaseConnector's fetch Function.
        """
        try:
            self.genre = 'review'
            if not self.rawpage:
                res=self._getHTML(self.currenturi)
                self.rawpage=res['result']
            self._setCurrentPage()
            self.__getParentPage()
            self.done = False
            self.currenturi =  self.base_url + [each['href'] for each in self.soup.find('div',attrs={'class':'comment'}).findAllNext('a')\
                                                    if each.renderContents()=='more'][0]+'?filter=chrono'

            while not self.done:
                self.__iterateReviews()
            return True
        except:
            log.exception(self.log_msg("Exception occured in fetch()"))
            return False

    def __getParentPage(self):
        """
        Gets the information regarding the movie
        """
        try:
            page={}
            try:
                self.soup.find('div',attrs={'id':'tn15title'}).find('h1').find('span').extract()
                page['title'] = stripHtml(self.soup.find('div',attrs={'id':'tn15title'}).find('h1').renderContents())
            except:
                page['title'] = ''
                log.info(self.log_msg("Exception in getting product page title"))
            page['data']=page['title']
            log.debug(self.log_msg('got the content of the product main page'))
            try:
                post_hash=get_hash(page)
            except:
                log.debug(self.log_msg("Error occured while creating the parent page hash"))
                return False
            if not checkSessionInfo(self.genre, self.session_info_out,
                                    self.task.instance_data['uri'], self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug('got the connector instance first time, sending updatesessioninfo the id : %s' % str(id))
                result=updateSessionInfo(self.genre, self.session_info_out, self.task.instance_data['uri'], post_hash,
                                         'Post', self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    try:
                        page['ef_rating_overall']=stripHtml(self.soup.find('div',attrs={'id':'tn15rating'}).\
                                                                find('div',attrs={'class':'meta'}).find('b').\
                                                                renderContents()).split('/')[0]
                    except:
                        log.info(self.log_msg("Exception in getting overall rating for movie"))
                    try:
                        page['et_data_director_name']= stripHtml(self.soup.find('div',attrs={'id':'director-info'}).find('a').renderContents())
                    except:
                        log.info(self.log_msg("Exception in getting director's name"))
                    page['path'] = [self.task.instance_data['uri']]
                    page['parent_path'] = []
                    page['uri'] = normalize(self.currenturi)
                    page['uri_domain'] = unicode(urlparse(page['uri'])[1])
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
                    page['versioned'] = self.task.instance_data.get('versioned',False)
                    page['task_log_id']=self.task.id
                    page['entity'] = 'post'
                    page['category']=self.task.instance_data.get('category','')
                    self.pages.append(page)
                    log.debug(self.log_msg("Parent page %s info added to self.pages" %(self.currenturi)))
                else:
                    log.debug(self.log_msg("Parent page %s info NOT added to self.pages" %(self.currenturi)))
            else:
                log.debug(self.log_msg("Parent page %s info NOT added to self.pages" %(self.currenturi)))
            return True
        except:
            log.exception(self.log_msg("Exception occured in __getParentPage()"))
            return False

    def __iterateReviews(self):
        """
        Iterates over the review pages
        """
        try:
            res=self._getHTML(self.currenturi)
            self.rawpage=res['result']
            self._setCurrentPage()
            reviews = self.soup.findAll('div',attrs={'class':'yn'})
            for review in reviews:
                if not self.done:
                    self.current_review = review
                    self.__getReview(review['id'])
                else:
                    break
            try:
                self.currenturi = self.task.instance_data['uri'].strip("/")+"/"+self.soup.find('img',attrs={'alt':'[Next]'}).findPrevious('a')['href']
                log.debug(self.log_msg("Next comments page url %s"%(self.currenturi)))
            except:
                self.done = True
                log.info(self.log_msg("Could not fetch the review page link"))
            return True
        except:
            self.done = True #precautionary exit, to avoid infinite loop
            return False

    def __getReview(self,review_id):
        """
        Gets a particular user comment (movie review)
        """
        page={}
        try:
            if not checkSessionInfo(self.genre, self.session_info_out,
                                    review_id, self.task.instance_data.get('update'),
                                    parent_list=[self.task.instance_data['uri']]):
                try:
                    if self.current_review.findPrevious('p').findPrevious('p').findPrevious('p').findAll('small'):
                        metainfo = self.current_review.findPrevious('p').findPrevious('p').findPrevious('p')
                    else:
                        metainfo = self.current_review.findPrevious('p').findPrevious('p')
                except:
                        metainfo = self.current_review.findPrevious('p').findPrevious('p')
                try:
                    page['data']= stripHtml(self.current_review.findPrevious('p').renderContents())
                except:
                    page['data']=''
                    log.info(self.log_msg("Exception occured while fetching review data"))

                try:
                    page['title'] = stripHtml(metainfo.find('b').renderContents())
                except:
                    page['title'] = page['data'][:50]
                    log.info(self.log_msg("Exception occured while fetching review title"))
                try:
                    page['ef_rating_overall'] = stripHtml(metainfo.find('img')['alt'].split('/')[0])
                except:
                    log.info(self.log_msg("Exception occured while fetching overall rating"))
                try:
                    page['et_author_profile'] = self.base_url+ stripHtml(metainfo.find('a')['href'])
                except:
                    log.info(self.log_msg("Exception occured while fetching author's profile link"))

                try:
                    page['et_author_name'] = stripHtml(metainfo.find('a').renderContents())
                except:
                    log.info(self.log_msg("Exception occured while fetching author name from the review"))
                try:
                    page['et_author_location'] = stripHtml(metainfo.\
                                                               findAll('small')[-1].renderContents().\
                                                               strip('from '))

                except:
                    log.info(self.log_msg("Exception occured while fetching overall rating"))
                try:
                    review_hash =  get_hash(page)
                except:
                    log.debug(self.log_msg("Error occured while creating the review hash %s" %self.self.current_review))
                    return False
                result=updateSessionInfo(self.genre, self.session_info_out, review_id, review_hash,
                                         'review', self.task.instance_data.get('update'), parent_list=[self.task.instance_data['uri']])
                if result['updated']:
                    try:
                        page['posted_date'] = datetime.strftime(datetime.strptime(\
                                metainfo.findAll('small')[1].renderContents()\
                                    ,"%d %B %Y"),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        page['posted_date']=datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        log.exception(self.log_msg("Exception occured while fetching post date from review %s" %review_id))

                    parent_list = [self.task.instance_data['uri']]
                    page['parent_path'] = copy.copy(parent_list)
                    parent_list.append(review_id)
                    page['path'] = parent_list
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['entity'] = 'review'
                    page['category'] = self.task.instance_data.get('category','')
                    page['task_log_id']=self.task.id
                    page['uri']=normalize(self.currenturi)
                    page['uri_domain'] = urlparse(page['uri'])[1]
                    self.pages.append(page)
                    log.debug(self.log_msg("Review %s added to self.pages" %(review_id)))
                else:
                    log.debug(self.log_msg("Review %s NOT added to self.pages" %(review_id)))
            else:
                if not self.task.instance_data.get('update'):
                    self.done = True
                    log.debug(self.log_msg("Review %s NOT added to self.pages" %(review_id)))
            return True
        except:
            log.exception(self.log_msg("Exception occured while iterating over review pages" %(review_id)))
            return False
