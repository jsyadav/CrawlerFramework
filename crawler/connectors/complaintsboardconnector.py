
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

####
#1. decorators missing
#2. 

#LATHA
import re
import md5
from datetime import datetime,timedelta#J
from BeautifulSoup import BeautifulSoup
from tgimport import *
import logging
import copy
from urllib2 import urlparse
from baseconnector import BaseConnector
from utils.task import Task
from utils.utils import stripHtml
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from utils.utils import get_hash
#logging.config.fileConfig('logging.cfg')
log = logging.getLogger('ComplaintsBoardConnector')

class ComplaintsBoardConnector (BaseConnector):
    '''
    currently complaintsboard fetches complaints and comments.
    checks review_hash and comment_hash if its new , it will get added
    to self.pages otherwise not, comment_hash depend on div_id which
    is unique same as consumercomplaints.in
    '''
     
    @logit(log, '_createurl')
    def _createurl(self):
        ''' 
        this func replaces  ' ' +  from self.task.instance_data['queryterm'] if self.currenturi is not available
        '''
        try:
            url= 'http://www.complaintsboard.com/?search=%s' % (self.task.instance_data['queryterm'].replace(' ', '+'))
            log.debug(self.log_msg("seed url : %s" %(url)))
            return url
        except:
            log.exception(self.log_msg("Exception occured while creating url"))
            
    @logit(log, 'fetch')
    def fetch(self):
        self.genre="Review"
        #why made a part of self?? not used beyong the scope of this method
        RESULTS_ITERATIONS = tg.config.get(path='Connector',key='complaints_board_numresults')
        self.COMMENTS_ITERATIONS= tg.config.get(path='Connector',
                                                key='complaints_board_commentresults')
        self.iterator_count = 0
        #why made a part of self?? not used beyong the scope of this method
        done = False
        try:
            if self.currenturi:
                self.currenturi='http://www.complaintsboard.com/?search=%s' % re.search(re.compile(r'^http://www.complaintsboard.com/\?search=(.+)'), self.currenturi).group(1).replace(' ', '+')
                #come up with explanation
                self.currenturi='http://www.complaintsboard.com/?search=%s' % re.search(re.compile(r'^http://www.complaintsboard.com/\?search=(.+)'), self.currenturi).group(1).replace('/\s+?', '%2F')                
            if not self.currenturi:
                self.currenturi = self._createUrl()
                if not self.currenturi:
                    log.debug(self.log_msg("Not a consumer complaints url and No search term provided, Quitting"))
                    return False
            res=self._getHTML()
            if res:
            #check for res None here
                self.rawpage=res['result']
                self._setCurrentPage()
            else:
                return False
            #find out what normalize does
            self.parent_uri=self.currenturi
            self._getparentPage()
            while self.iterator_count < RESULTS_ITERATIONS and not done:
                try:
                    next_page = self.soup.find('td', {'class':'categories'}).find(\
                        'a', href=True, text='Next')
                    ##Please care about the return of method calls
                    if self.addReviews() and next_page:
                        log.info(self.log_msg('Entering into Next page from main page reviews'))
                        self.currenturi = 'http://www.complaintsboard.com'+next_page.parent['href']
                        res=self._getHTML()
                        self.rawpage=res['result']
                        self._setCurrentPage()
                    else:
                        log.info(self.log_msg('Reached last page of reviews'))
                        done= True
                        break
                except Exception, e:
                    raise e
            ##not supposed to be here
            
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False
        return True
    @logit(log, '_getparentPage')
    def _getparentPage(self):
         try:
            page ={}
            page['uri']=self.currenturi
            tem=self.soup.h1.renderContents().split('matching')
            page['title']= tem[1].strip().strip('"')
            try:
                post_hash=get_hash(page)
                
            except:
                log.exception(self.log_msg('exception in building post_hash  moving onto next review'))
            log.debug('checking session info')
            if not checkSessionInfo(self.genre, self.session_info_out,
                                    page['uri'], self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    log.debug(id)
                result=updateSessionInfo(self.genre, self.session_info_out, page['uri'], post_hash, 'Post', self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    page['path']=[page['uri']]
                    page['parent_path']=[]
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id                                                        
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['data'] = ''
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')
                    page['task_log_id']=self.task.id
                    page['client_name'] = self.task.client_name
                    page['versioned']=self.task.instance_data.get('versioned',False)
                    self.pages.append(page)
         except Exception,e:
             log.exception(self.log_msg("error in getParentPage"))
             raise e
         


    @logit(log, 'addRevies')
    def addReviews(self):
        ##outer try -except missing
        ##nothing is raised if there is an error???
        try:
            links=self.soup.findAll('td', {'class':'complaint'})
            review_links=[x.find('a', href=True)['href']  for x in links]
            pattern=re.compile('/img/rating/rating_(\d+).gif')
            for review_link in review_links:
                self.iterator_count = self.iterator_count + 1
                page={}
                try:
                    review_link = 'http://www.complaintsboard.com' + review_link
                    self.currenturi = review_link
                    parent_review_url=self.currenturi
                    res=self._getHTML()
                    if res:
                        self.rawpage=res['result']
                        self._setCurrentPage()
                    else:
                        continue
                ##if the connector gives/allows the results to be arranged by date desc
                ##there should be a else: return -
                ##meaning - the moment we hit an existing review and update is turned off -
                ##we save the expense of looking for all other reviews
                
                ##otherwise
                ##else is not required
                    if not checkSessionInfo(self.genre, self.session_info_out, 
                                            self.currenturi,  self.task.instance_data.get('update'), 
                                            parent_list=[self.parent_uri]):
                   
                        try:
                            page['title']=stripHtml(self.soup.find('td', {'class':'complaint'}).renderContents()).strip()
                        except:
                            log.info(self.log_msg(' title could not be parsed'))
                        ##put the first 100 charachters of the data or compelete data 
                        ##which ever is less - remove '\n'before u put it in place
                            page['title'] =''

                        try:
                            temp=self.soup.find('td', {'class':'small'}).renderContents().split('by')
                            try:
                            ##incorrect naming - temp_
                                temp_p=temp[0].split('Posted:')
                                page['posted_date']= datetime.strftime(\
                                    datetime.strptime(temp_p[1].strip(),
                                                      '%Y-%m-%d'),'%Y-%m-%dT%H:%M:%SZ')
                            except:
                                log.info(self.log_msg('date could not be parsed'))
                                page['posted_date'] =datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                            try:
                                tem=temp[1].split('[')
                                page['et_author_name']=tem[0].strip()
                            except:
                                log.info(self.log_msg('autor name could not be parsed'))
                                page['et_author_name']  =''
                        except:
                            log.info(self.log_msg('soup object for DATE and AUTHOR NAME could not be parsed'))
                        try:
                            page['data']=stripHtml(self.soup.find('td', {'class': 'compl-text'}).renderContents()).strip()
                        except:
                            log.info(self.log_msg('Review Data could not be parsed'))
                            page['data']=''
                        try:
                            r=soup.find('img', {'src':re.compile("/img/rating/rating_\d+")})['src']
                            page['ef_rating_complaint']=float(pattern.match(r).group(1))
                        except:
                            page['ef_rating_complaint'] =0.0
                            
                        try:
                            review_hash=get_hash(page)
                            
                        except:
                            log.exception(self.log_msg('exception in building review_hash  moving onto next review'))
                            continue  
                        ##CONTINUE??
                            
                        result=updateSessionInfo(self.genre, self.session_info_out, 
                                                 self.currenturi, review_hash, 
                                                 'Review', self.task.instance_data.get('update'), 
                                                 parent_list=[self.parent_uri])

                        if result['updated']:
                            parent_list = [self.parent_uri]
                            page['parent_path'] = copy.copy(parent_list)
                            parent_list.append(self.currenturi)
                            page['path'] = parent_list
                            page['uri']=normalize(self.currenturi)
                            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                            page['priority']=self.task.priority
                            page['level']=self.task.level
                            page['connector_instance_log_id'] = self.task.connector_instance_log_id
                            page['connector_instance_id'] = self.task.connector_instance_id
                            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                            page['last_updated_time'] = page['pickup_date']
                            page['versioned'] = False
                            page['workspace_id'] = self.task.workspace_id
                            page['client_id'] = self.task.client_id          
                            page['client_name'] = self.task.client_name
                            page['entity'] = 'Review'
                            page['category'] = self.task.instance_data.get('category','')
                            page['task_log_id']=self.task.id
                            page['versioned']=self.task.instance_data.get('versioned',False)
                        ####do all this befor the 'if', assign to local variables 
                            self.pages.append(page)
                        #parenturi = [parent_uri, self.currenturi]
                            while True:            
                                
                                try:
                            ##raise exception from children methods and chk before proceeding
                                    if self.addComments(self.currenturi, [self.parent_uri, parent_review_url]):
                                        try:
                                            next=self.soup.find('a', href=True, text='Next')
                                            if next:
                                                self.currenturi='http://www.complaintsboard.com'+ next.parent['href']
                                                log.info(self.log_msg('setting Next page as self.currenturi   '  +  self.currenturi))
                                            else:
                                                break
                                        except Exception, e:
                                            log.exception(self.log_msg("No next page for the review links"))
                                            break
                                except Exception, e:
                                    log.exception(self.log_msg('exception in add_comments'))
                                    raise e
                except:
                    log.exception(self.log_msg("Exception occured while fetching reviewlinks"))
                    continue
        except Exception, e:
            log.exception(self.log_msg("Exception occured while fetching reviewlinks"))
            return False
        return True

    @logit(log, 'addComments')
    def addComments(self, link, parent_list ):
        self.currenturi=link
        try:
            res=self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            ##give a sample url and explanation
            fixdata=re.sub(r'<a name="([A-z0-9]+)">', r'<a name="\1"></a>', self.rawpage)
            ##join??
            fixsoup=BeautifulSoup(fixdata)
            ##wrong variable_ names
            commentss=fixsoup.find('table', {'class':'grey-normal'})

            ##why (self)??
            self.comment_count = 0
            if commentss:
                comments=commentss.findAll('div')
                for comment in comments:
                    self.comment_count= self.comment_count+1
                    page={}
                    try:
                        div_id =comment['id']
                    except:
                        log.exception(self.log_msg("could not parse div_id"))
                        #give example link and id
                        div_id=''
                    if not checkSessionInfo(self.genre, self.session_info_out,
                                            div_id, self.task.instance_data.get('update'),
                                            parent_list=parent_list):

                        try:
                            page['et_author_name']=comment.find('td', {'class':'comments'}).contents[1].split ('by')[1].strip("[")
                        except:
                            log.exception(self.log_msg("could not parse author name"))
                        try:
                            page['data']=stripHtml(comment.find('td', {'class':'compl-text'}).renderContents())
                        except:
                            log.exception(self.log_msg("could not parse comment data"))
                            page['data'] =''
                        try:
                            page['et_author_name']=comment.find('td', {'class':'comments'}).contents[1].split ('by')[1].strip("[")
                        except:
                            log.exception(self.log_msg("could not parse author name"))
                        try:
                            hash =get_hash(page)
                        except Exception, e:
                            log.info(self.log_msg('could not generare comment hash'))
                            continue
                        try:
                            temp=stripHtml(comment.find('td', {'class':'comments'}).contents[1].split ('days')[0]).strip()
                            date=datetime.strftime(datetime.now()-timedelta(int(temp)), "%Y-%m-%dT%H:%M:%SZ")
                            page['posted_date']=date
                        except:
                            log.info(self.log_msg('could not parse date'))
                            page['posted_date']=  datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        try:
                            if len(page['data']) > 50:
                                page['title'] = page['data'][:50] + '...'
                            else:
                                page['title'] = page['data']
                        except:
                            log.info(self.log_msg('could not parse title'))

                        result=updateSessionInfo(self.genre, self.session_info_out, div_id, hash,
                                                 'Comment', self.task.instance_data.get('update'),
                                                 parent_list=parent_list)
                        if result['updated']:
                            copied_parent_list = copy.copy(parent_list)
                            page['parent_path']=copy.copy(copied_parent_list)
                            copied_parent_list.append(div_id)
                            page['path']=copy.copy(copied_parent_list)
                            page['priority']=self.task.priority
                            page['level']=self.task.level
                            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                            page['last_updated_time'] = page['pickup_date']
                            page['connector_instance_log_id'] = self.task.connector_instance_log_id
                            page['connector_instance_id'] = self.task.connector_instance_id
                            page['workspace_id'] = self.task.workspace_id
                            page['client_id'] = self.task.client_id                                                           
                            page['client_name'] = self.task.client_name
                            page['versioned'] = False
                            page['uri'] = self.currenturi
                            page['uri_domain'] = urlparse.urlparse(self.currenturi)[1]
                            page['task_log_id']=self.task.id
                            page['entity'] = 'Comment'
                            page['category'] = self.task.instance_data.get('category' ,'')
                            self.pages.append(page)
                            if self.comment_count >= self.COMMENTS_ITERATIONS:
                                break

        except Exception, e:
            log.exception(self.log_msg("Exception occured while fetching comments"))
            raise e
        return True
