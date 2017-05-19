'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.
Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''


# Sudharshan S
#Pooja
import re
import md5
import logging
import time
import traceback

from urllib2 import urlparse,unquote
from datetime import datetime
from BeautifulSoup import BeautifulSoup

from tgimport import *
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash

from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo



log = logging.getLogger("CollegeProwlerConnector")
class CollegeProwlerConnector( BaseConnector ):
    '''Connector for collegeprowler.com'''
    
    @logit(log, "fetch")
    def fetch(self):
        self.genre = 'Review'
        try:
            self.parenturi = self.currenturi
            if not self._setSoup():
                return False
            self._getParentPage()
            self.addcomments([self.parenturi])
            self.task.status['fetch_status'] = True
            return True
        except:
            self.task.status['fetch_status'] = False
            log.exception(self.log_msg('Exception in fetch'))
            return False


    @logit(log, '_getParentPage')
    def _getParentPage(self):
        try:
            page = {}
            try:
                reportcard = self.soup.find("div", {"class":"schoolTL"}).findAll("li")
                for r in reportcard:
                    grade = r.div.renderContents() # B+
                    area = r.a.next # Greek life
                    page['et_%s' % area.lower().replace(' ', '_').replace('-','_')] = grade.strip()
            except:
                log.info("Exception while getting the report card")
                log.exception(traceback.format_exc())
                return False

            try:
                school_stats = self.soup.find("table", {"class":"featureStats"}).findAll('tr')
                for s in school_stats:
                    key = s.td.next.strip(":") # Location
                    value = s.span.renderContents()
                    page['et_%s' %key.lower().replace(' ', '_').replace('-', '_')] = value
            except:
                log.info("Exception while getting the feature statistics")
                log.exception(traceback.format_exc())
                return False

            page['title'] = self.soup.title.next.strip()
            data = ""
            try:
                content_div = self.soup.find("div", {"class":"sectionSchool", "id":"editorial"})
                data = " ".join([x.next for x in content_div.findAll("p")])
            except:
                log.info("Excepting while getting the overall review")
                log.exception(traceback.format_exc())
            page['data'] = data
            
            try:
                post_hash = get_hash(page)
            except Exception,e:
                log.exception(self.log_msg('could not build post_hash'))
                raise e
            log.debug(self.log_msg('checking session info'))
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    self.parenturi, self.task.instance_data.get('update')):
                id = None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo(self.genre, self.session_info_out, self.parenturi, post_hash, 
                                         'Post', self.task.instance_data.get('update'), Id=id)
            if result['updated']:
                page['uri'] = normalize(self.currenturi)
                page['path'] = [self.parenturi]
                page['parent_path'] = []
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id  # TODO: Get the client from the project       
                page['client_name'] = self.task.client_name
                page['last_updated_time'] = page['pickup_date']
                #page['first_version_id'] = result['first_version_id']
                #page['id'] = result['id']
                page['versioned'] = False
                page['data'] = ''
                page['task_log_id']=self.task.id
                page['entity'] = 'Post'
                page['category']=self.task.instance_data.get('category','')                        
                self.pages.append(page)
                    
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed. Reason: %s" % traceback.format_exc()) )
            raise e


    @logit(log, "addcomments")
    def addcomments( self, parent_list ):
        try:
            no_of_pages = int(self.soup.find("div", {"class":"pageOFpage"}).next.split()[-1])
            f = self.soup.find("div", {"class":"feedWrapper"}).find("div", {"class":"paginate"})
            school_id = re.search(r"school=(\d+)", f.parent.nextSibling.renderContents())
            school_id = school_id.groups()[0]
            log.info("School ID:%s ; No of Pages:%s" %(school_id, no_of_pages))
            self.currenturi = "http://collegeprowler.com/_ajax/Feed.aspx?&sections=overall-experience&PageSize=%(no_of_comments)s&AllSectionChoiceAvailable=True&UseSmallComment=False&availablesections=overall-experience&page=1&direction=date&section=all&school=%(school_id)s" % { \
                'no_of_comments':no_of_pages,
                'school_id':school_id
                }
            if not self._setSoup():
                raise
            
            comments = self.soup.findAll("table", { "class" : "userReview" })
            for i, comment in enumerate(comments):
                page = {}
                try:
                    # Right side of the review page
                    log.info("Fetching comment: %d" %i)
                    try:
                        page['et_author_name'] = stripHtml(comment.find("p").find("a").renderContents())
                    except:
                        log.info(self.log_msg("Couldn't extract comment author. Reason:%s" %traceback.format_exc()))
                        page['et_author_name'] = ''

                    review_content = comment.find("td", {"class":"reviewRight"}).find("div", {"class":"theReview"})
                    try:
                        grade = stripHtml(review_content.find("div", {"class":"theGrade"}).next)
                        page['et_grade'] = grade
                    except:
                        log.info(self.log_msg("Couldn't extract et_grade: %s" %traceback.format_exc()))
                        page['et_grade'] = ''

                    try:
                        data = stripHtml(review_content.findAll("p")[-1].renderContents())
                        page['data'] = data
                    except:
                        log.info(self.log_msg("Couldn't extract comment data: %s" %traceback.format_exc()))
                        page['data'] = ''

                    try:
                        title = review_content.find("p", {"class":"theTitle"}).renderContents()
                        log.info("Title :%s" %title)
                        page['title'] = title
                    except:
                        log.info(self.log_msg("Couldn't extract title: %s" %traceback.format_exc()))
                        page['title'] = page['data'][0:50] + "..."

                    review_content_left = comment.find("td", {"class":"reviewLeft"})
                    try:
                        _class = review_content_left.find("p", {"class":"reviewInfo"}).renderContents()
                        page['et_class'] = _class
                    except:
                        page['et_class'] = ''
                        log.info(self.log_msg("Couldn't extract Class: %s" %traceback.format_exc()))

                    try:
                        major = review_content_left.find("p", {"id":re.compile("majorP"), "class":"reviewInfo"}).renderContents()
                        page['et_major'] = major
                    except:
                        page['et_major'] = ''
                        log.info(self.log_msg("Couldn't extract major: %s" %traceback.format_exc()))

                    try:
                        date = review_content_left.find("p", {"id":re.compile("timeP"), "class":"reviewInfo"}).renderContents()
                        page['posted_date'] = datetime.strftime(datetime.strptime(date,"%b %d, %Y"),
                                                        "%Y-%m-%dT%H:%M:%SZ")
                    except:
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        log.info(self.log_msg("Couldn't extract date: %s" %traceback.format_exc()))

                    try:
                        review_category = comment.find("div", {"class":"belowReview"}).find("div", {"class":"reviewInfo"}).renderContents()
                        page['et_review_category'] = review_category
                    except:
                        page['et_review_category'] = ''
                        log.info(self.log_msg("Couldn't extract Review category: %s" %traceback.format_exc()))

                    hash = md5.md5(repr(page)+str(i)).hexdigest()
                    if not checkSessionInfo(self.genre, self.session_info_out, 
                                            hash, self.task.instance_data.get('update'),
                                            parent_list=parent_list):
                        #hash = md5.md5(''.join(sorted(page.values())).encode('utf-8','ignore')).hexdigest()
                        result = updateSessionInfo(self.genre, self.session_info_out, self.currenturi, hash, 
                                                 'Comment', self.task.instance_data.get('update'), 
                                                 parent_list=parent_list)

                        if result['updated']:
                            _parent = parent_list + [hash]
                            page['parent_path'] = parent_list
                            page['path'] = _parent[:]
                            page['priority'] = self.task.priority
                            page['level'] = self.task.level
                            page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                            page['last_updated_time'] = page['pickup_date']
                            page['connector_instance_log_id'] = self.task.connector_instance_log_id
                            page['connector_instance_id'] = self.task.connector_instance_id
                            page['workspace_id'] = self.task.workspace_id
                            page['client_id'] = self.task.client_id  # TODO: Get the client from the project 
                            page['client_name'] = self.task.client_name
                            page['versioned'] = False
                            page['uri'] = self.currenturi
                            page['uri_domain'] = urlparse.urlparse(self.currenturi)[1]
                            page['task_log_id'] = self.task.id
                            page['entity'] = 'Review'
                            page['category'] = self.task.instance_data.get('category' ,'')
                            self.pages.append(page)
                except:
                    log.exception(self.log_msg("exception in add_comments: %s" %traceback.format_exc()))
                    #raw_input("Caught Exception")
                    continue # End Loop
        except:
            log.exception(self.log_msg("exception in add_comments, Reason:%s" %traceback.format_exc())) # End function block


                
    

    @logit(log, "_setSoup")
    def _setSoup( self, url = None, data = None, headers = {} ):
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
        
