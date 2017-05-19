
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

import re
import copy
from datetime import datetime
import logging
import traceback
from urllib2 import urlparse,unquote

from baseconnector import BaseConnector
from utils.utils import stripHtml, get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

BASEURI = "http://usa.hotcoursesabroad.com"

log = logging.getLogger('HotCoursesAbroadConnector')
class HotCoursesAbroadConnector(BaseConnector):

    @logit(log , 'fetch')
    def fetch(self):
        """Create the url, fetch the page and extract reviews
        """
        try:
            self.genre='Review'
            if not self.__setSoup():
                log.info(self.log_msg('returning false from fetch, setting soup fails'))
                return False
            self.parent_uri = self.currenturi
##            if not self.__setSoup():
##                log.info(self.log_msg('returning false from fetch, setting soup fails'))
##                return False
##            self.currenturi = self.soup.find('div',id='reviews').find('a',text=re.compile('See more reviews')).parent['href']
            self.__getParentPage()
            pgtn = self.soup.find("div", {"class":"pgntn"})
            # Top level review page of the form
            # http://usa.hotcoursesabroad.com/study/student-reviews-ratings/university-of-maryland/115356/1/rankings.html
            review_pages = list(set([BASEURI + x['href'] for x in pgtn.find("span", {"class":'num'}).findAll("a")]))
            review_pages.append(self.currenturi)
            review_pages.sort()
            
            for review_page in review_pages:
                try:
                    self.currenturi = review_page
                    if not self.__setSoup():
                        break
               
                    if not self.__addReviews():
                        break
                except:
                    log.info(self.log_msg('next page not found'))
                    break
            return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log , '_getParentPage')
    def __getParentPage(self):
        """Get the average rating from the parent page
        """
        page={}
        try:
            if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info return True, Already exists'))
                return False
            page['title'] = stripHtml(self.soup.find("div", {"id":"studrank"}).find("span", {"class":"nline"}).renderContents())                   
        except Exception, e:
            log.exception(self.log_msg('could not parse page title'))
            raise e
        rating_info = {'Overall experience':'overall',
                        'Selection process and gaining admission':'processing_and_admission',
                        'Funding and scholarships':'funding_and_scholarships',
                        'Study experience':'study_experience',
                        'Life experience':'life_experience',
                        'Job prospects':'job_prospects',
                        }
        rating = self.soup.find('div',id='studrank')
        for each in rating_info.keys():
            try:
                _rating = float(stripHtml(rating.find("a", text=each).parent.parent.parent.find("span", {"class":"basedon"}).renderContents().strip('()')))
                page['ef_rating_' + rating_info[each]] = _rating
            except:
                log.info(self.log_msg(traceback.format_exc()))
                log.info(self.log_msg('could not parse %s rating'%each))
        try:
            post_hash = get_hash( page )
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo( self.genre, self.session_info_out, self.\
                   parent_uri, post_hash,'Forum',self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                return False
            page['path']=[self.parent_uri]
            page['parent_path']=[]
            page['uri'] = self.currenturi
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
            page['data'] = ''
            page['task_log_id']=self.task.id
            page['entity'] = 'Post'
            page['category']=self.task.instance_data.get('category','')
            self.pages.append(page)
            log.info(page)
            log.info(self.log_msg('Parent Page added'))
            return True
        except:
            log.exception(self.log_msg("parent post couldn't be parsed"))


    @logit(log , '__addReviews')
    def __addReviews(self):
        try:
            review_links =  self.soup.find("div", {"id":"prvrev", "class":"mrevs"}).findAll("dl", {"class":re.compile("(left)|(right)")})
            review_links = [x.find("a")['href'] for x in review_links]
            log.info("Review for page in review_pages: links are: %s: len: %d" %(review_links, len(review_links)))
        except:
            log.exception(self.log_msg('Reviews are not found for uri:%s' %self.currenturi))
            return False

                    
        for review in review_links:
            try:
                unique_key = self.currenturi =  review
                if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    continue
                if not self.__setSoup():
                    continue
                page = self.__getData()
                log.info(page)
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            get_hash( page ),'Review', self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    continue
                parent_list = [self.parent_uri]
                page['parent_path'] = parent_list[:]
                parent_list.append(unique_key)
                page['path'] = parent_list
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                page['client_name'] = self.task.client_name
                page['last_updated_time'] = page['pickup_date']
                page['versioned'] = False
                page['entity'] = 'Review'
                page['category'] = self.task.instance_data.get('category','')
                page['task_log_id']=self.task.id
                page['uri'] = self.currenturi
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append( page )
                #log.info(page)
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))
        return True

    def __getData(self):
        """ This will return the page dictionry
        """
        page = {'title':''}
        content = self.soup.find('div', {'id':'contentcolumn'})
        data_tag = self.soup.find('div',id='flrev')
        rating_info = {'Overall experience':'overall',
                    'Selection process and gaining admission':'processing_and_admission',
                    'Funding and scholarships':'funding_and_scholarships',
                    'Your study experience':'study_experience',
                    'Your life experience':'life_experience',
                    'Job prospects':'job_prospects',
                    }
        for each in rating_info.keys():
            try:
                page['ef_rating_' + rating_info[each]] = float(content.find('label', text=each).parent.previousSibling['class'][6:])
            except:
                log.info(self.log_msg('could not parse %s rating'%each))
        try:
            author_tag = data_tag.find('dl')
            avatar_block = author_tag.find("dt", {'class':'dtimg'})
            page['et_author_name'] = stripHtml(avatar_block.find("strong", {"class":"block"}).renderContents())
            if not page['et_author_name'] =='Anonymous':
                page['et_author_location'] =  stripHtml(avatar_block.find("span", {"class":"block"}).renderContents())
                page['et_author_study'] = author_tag.findAll("dd")[1].renderContents()
        except:
            log.info(self.log_msg('Author name or loc or study not found for uri:%s' %self.currenturi))
        try:
            page['et_author_graduation_year'] = stripHtml(author_tag.find('dd',text=re.compile('Year of graduation:')).next)
            page['et_author_study_level'] = stripHtml(author_tag.find('dd',text=re.compile('Level of study:')).next )
        except:
            log.info(self.log_msg('Author info not found for uri:%s' %self.currenturi))

        data_str = ''
        try:
            for i in content.findAll('p', {'class':re.compile('(rev)|(reviewalt)')}):
                data_str += str(i.next)
            try:
                page['data'] = data_str.decode('utf-8')
            except:
                page['data'] = data_str
            
        except:
            log.exception(self.log_msg('data not found for uri:%s' %self.currenturi))
            return False
        try:
            title = stripHtml(content.find("h3").renderContents())
            page['title'] = title
        except:
            log.info(self.log_msg('Cannot find the title'))
            page['title'] = ''
        try:
            if page['title']=='':
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
        except:
            log.exception(self.log_msg('title not found for uri:%s' %self.currenturi))
            page['title'] = ''

        return page

    @logit(log, "_setSoup")
    def __setSoup( self, url = None, data = None, headers = {} ):
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
