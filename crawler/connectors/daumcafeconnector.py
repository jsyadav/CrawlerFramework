
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
from utils.httpconnection import HTTPConnection
import StringIO
import gzip
import md5


from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash

from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('DaumCafeConnector')
class DaumCafeConnector(BaseConnector):

    @logit(log , 'fetch')
    def fetch(self):                                                                                         
        self.conn = HTTPConnection()
        self.genre="Review"
        try:
            #for sentiment extraction
            code = None
            parent_uri = self.currenturi
            # for sentiment extraction
            review_next_page_list = []

            res=self._getHTML()
            self.rawpage=res['result']
            self.soup = BeautifulSoup(self.rawpage)

            self.currenturi = self.soup.find('frame',{'name':'down','id':'down'})['src'].strip()

            res=self._getHTML()
            self.rawpage=res['result']
            self.soup = BeautifulSoup(self.rawpage)

            page = {}
            if (not checkSessionInfo(self.genre, self.session_info_out, 
                                     parent_uri, self.task.instance_data.get('update'),
                                     parent_list=[])):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                    
                page['uri'] = self.currenturi
                try:
                    page['et_author_name'] = stripHtml(self.soup.find('div',{'class':'writer'}).find('a').renderContents())
                except:
                    log.info('could not parse Author Name')

                try:
                    page['ei_num_page_views'] = int(stripHtml(self.soup.find('div',{'class':'writer'}).find('span',{'class':'skinTxt'})))
                except:
                    log.info('could not parse number of page views')
        
                try:
                    page['data'] = stripHtml(self.soup.find('td',{'id':'user_contents'}).renderContents())
                except:
                    log.exception('could not parse page data')
                    
                try:
                    page['title'] = stripHtml(self.soup.find('table',{'class':'subject_table'}).find('th')\
                                                  .renderContents().decode('utf-8'))
                except:
                    log.info('could not parse number of page views')
                    page['title'] = page['data'][:100]
                    
                try:
                    page['posted_date'] = datetime.strptime(self.soup.find('li',{'class':'skinTxt'}).\
                                                          renderContents().strip(),'%y.%m.%d %H:%M')
                    page['posted_date'] = datetime.strftime(page['posted_date'],"%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.info('could not parse number of posted_date')

                review_hash = get_hash(page)
                result=updateSessionInfo(self.genre, self.session_info_out, parent_uri, review_hash,
                                         'Review', self.task.instance_data.get('update'),Id=id )
                if result['updated']:
                    page['uri'] = normalize(self.currenturi)
                    page['path'] = [parent_uri]
                    page['parent_path'] = []
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project       
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['task_log_id']=self.task.id
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')
                    self.pages.append(page)
            self.addcomments(parent_uri)
        except:
            log.exception(self.log_msg('error in fetch'))
            return False
        
    #addReviews
    @logit(log , 'addreviews')
    def addcomments(self, parenturi):
        comments = self.soup.findAll('div',{'id':re.compile('_cmt[0-9]+'),'class':re.compile('.*comment_pos')})
        log.info(self.log_msg('no. of comments found on page %d'%(len(comments))))
        for comment in comments:
            try:
                page ={}
                url = parenturi
                ##ODD JV
                #for cases in which review has been deleted , no permalink is present hence we skip for 
                 #that review,same applies for comments
                page['uri'] = normalize(url)
                comment_id = stripHtml(comment.find('span',{'id':'_cmt_datestr[0-9]+'}).renderContents())
                if (not checkSessionInfo(self.genre, self.session_info_out, 
                                         comment_id, self.task.instance_data.get('update'),
                                         parent_list=[parenturi]) ):

                    if self.session_info_out=={}:
                        id=self.task.id
                    try:
                        page['data'] = stripHtml(comment.find('span',{'id':re.compile('cmt_contents[0-9]+')}).renderContents())
                        page['title'] = page['data'][:80]                    
                    except:
                        log.info(self.log_msg("could not get data of the comment "))
                        continue

                    try:
                        ##state the case - copy and paste the text from page JV
                        page['et_author_name'] =  stripHtml(comment.find('div',{'class':'id_admin'}).a.renderContents())
                    except:
                        log.info(self.log_msg("review author name couldn't be extracted"))

                    try:
                        comment_hash = get_hash(page)
                    except:
                        log.exception(self.log_msg("exception in buidling review_hash , moving onto next comment"))
                        continue
                    parent_list=[parenturi]
                    result=updateSessionInfo(self.genre, self.session_info_out, page['uri'], review_hash, 
                                             'Review', self.task.instance_data.get('update'), parent_list=parent_list)
                    if result['updated']:
                        page['parent_path'] = parent_list[:]
                        parent_list.append(comment_id)
                        page['path'] = parent_list
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id  # TODO: Get the client from the project 
                        page['client_name'] = self.task.client_name
                        page['last_updated_time'] = page['pickup_date']
                        page['versioned'] = False
                        page['entity'] = 'Comment'
                        page['category'] = self.task.instance_data.get('category','')
                        try:
                            page['posted_date'] = datetime.strftime(datetime.strptime(comment_id,'%y.%m.%d %H:%M')\
                                                                        ,"%Y-%m-%dT%H:%M:%SZ")
                        except:
                            log.info(self.log_msg("couldn't parse posted_date"))
                            page['posted_date'] = page['pickup_date']
                        page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                        self.pages.append(page)
                
                else:
                    log.info(self.log_msg('reached already parsed review so returning'))
                    return False
            except:
                log.exception(self.log_msg("exception in addreviews"))
                continue
        return True


    @logit(log, '_getHTML') #copied the function in thi
    def _getHTML(self, uri=None,data=None,headers={}):
        """
        urlopen and data acquire
        408- request timeout
        500- internal server error
        502- bad gateway
        503- service unavailable
        504- gateway timeout
        104- partial content
        """
        try:
            if not uri:
                uri=self.currenturi
            if uri.strip() == '':          # No url (due to #named anchors)
                self.task.status['fetch_message']='URL is empty'
                return ''
            log.info(self.log_msg('Fetching uri :::: %s'%(uri)))
            self.conn.createrequest(uri)
            response = self.conn.fetch()
            result = response.read()
            return dict(result=result, fetch_message=response.code)
        except:
            log.exception('TaskID:%s::Client:%s::httpconnection fetch failed' % (self.task.id, self.task.client_name))
            return None
