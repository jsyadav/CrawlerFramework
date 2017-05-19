
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Ashish (removed extract function)
import re
import md5
import logging
import urlparse
from datetime import datetime
from BeautifulSoup import BeautifulSoup

from tgimport import *
from baseconnector import BaseConnector
from utils.task import Task
from utils.utils import stripHtml
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('NoteBookReviewForumConnector')

class NoteBookReviewForumConnector(BaseConnector):

    base_url = "http://forum.notebookreview.com/"

    @logit(log , 'fetch')
    def fetch(self):                                                                                         
        self.genre="Review"
        try:
            if re.match("^http://forum\.notebookreview\.com/showthread\.php\?t=\d+?$",self.currenturi):
                self._getParentPage(self.currenturi)
                self.parent_page_url = self.currenturi
                self._iteratePosts()
                return True
            else:
                log.info(self.log_msg("NOT a thread url %s" %self.currenturi))
                return False
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log , '_getParentPage')
    def _getParentPage(self,parent_uri):
        try:
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    parent_uri, self.task.instance_data.get('update')):
                post_hash = parent_uri
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out,parent_uri, post_hash, 
                                         'Search_url', self.task.instance_data.get('update'), Id=id)
            return True
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            return False

    @logit (log,'_iteratePosts')
    def _iteratePosts(self):
        """
        iterates for all the posts, adds them to list and fetch data for those posts in list
        """
        '''
        1-10 - page1 - date1
        11-20 - page2 - date2
        21-30 - page3 - date3

        date3 > date2 > date 1
        
        Hence we fetch all the posts in the ascending order of date, as page shows up and reverse the list containing the posts,
        and iterate to this list which is ordered in the descending order of the date to fetch the latest data first. 
        '''
        try:
            post_soup_list = []
            while True:
                log.debug(self.log_msg("Iterating url %s"%self.currenturi))
                this_page_post_soup = []
               
                res=self._getHTML(self.currenturi)
                self.rawpage=res['result']
                self._setCurrentPage()

                for each_post in self.soup.findAll('div',attrs={'id':re.compile('^edit\d+$')}):
                    post_dict = dict({'post':each_post,'uri':self.currenturi})
                    this_page_post_soup.append(post_dict)                
                post_soup_list.extend(this_page_post_soup)
                try:
                    self.currenturi = self.base_url + self.soup.find('a',attrs={'title':re.compile('^Next.*$')}).get('href')
                except:
                    log.debug(self.log_msg("Reached on the last page"))
                    break
            post_soup_list.reverse()
            for each_post in post_soup_list:
                self.current_post = each_post['post']
                self.currenturi = each_post['uri']
                self._getPosts()

            return True
        except:
            log.exception(self.log_msg("Exception occured in _iteratePost"))
            return False

    @logit(log , '_getPosts')
    def _getPosts(self):
        try:
            try:
                post_id = self.current_post['id']
                log.info(self.log_msg('post_identifier : %s '%(post_id)))
            except:
                log.exception(self.log_msg('could not extract post identifier, so continuing'))
                return False
            if not checkSessionInfo(self.genre, self.session_info_out,
                                post_id, self.task.instance_data.get('update'),
                                parent_list=[self.parent_page_url]):
                page = {}
                try:
                    page['data'] = stripHtml(self.current_post.find('div',attrs={'id':re.compile('^post_message_\d+$')}).renderContents())
                except:
                    log.info(self.log_msg('could not parse post data'))
                    page['data'] = ''
                page['title'] = page['data'][:50]
                try:
                    page['et_author_name'] = stripHtml(self.current_post.find('a',attrs={'class':'bigusername'}).renderContents())
                except:
                    log.info(self.log_msg('could not parse author name'))
                try:
                    page['et_author_type'] =  stripHtml(self.current_post.find('a',attrs={'class':'bigusername'}).findNext('div').renderContents())
                except:
                    log.info(self.log_msg('could not determine author type'))
                try:
                    author_metadata = self.current_post.find('a',attrs={'class':'bigusername'}).findNext('div').findNext('div')
                    try:
                        page['edate_author_member_since'] = datetime.strftime(datetime.strptime(author_metadata.find('div',text=re.compile('Join Date:')).replace('Join Date:','').strip(),'%b %Y'),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg('could not determine author joined date'))
                    try:
                        page['ei_author_posts_count'] = str(int(re.sub('Posts:|,','',author_metadata.find('div',text=re.compile('Posts:'))).strip()))
                    except:
                        log.info(self.log_msg('could not parse author posts count'))
                    try:
                        page['et_author_location'] = author_metadata.find('div',text=re.compile('Location:')).replace('Location:','').strip()
                    except:
                        log.info(self.log_msg('could not parse author location'))
                except:
                    log.info(self.log_msg('could not parse author information'))       
                try:
                    post_hash = md5.md5(''.join(sorted(map(lambda x: str(x) if isinstance(x,(int,float)) else x , \
                                                                                        page.values()))).encode('utf-8','ignore')).hexdigest()
                except:
                    log.exception(self.log_msg("exception in buidling post_hash , moving onto next post"))
                    return False

                result=updateSessionInfo(self.genre, self.session_info_out,post_id, post_hash, 
                                         'Post', self.task.instance_data.get('update'), 
                                         parent_list=[self.parent_page_url])
                if result['updated']:
                    try:
                        page['posted_date'] = datetime.strftime(datetime.strptime(re.match('([^ ]+)(.*?)$',stripHtml(self.current_post.find('a',attrs={'name':True}).next.next)).group(1),"%m-%d-%Y,"),"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg('posted_date could not be parsed'))
                        page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")

                    page['id']=result['id']
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['last_updated_time'] = page['pickup_date']
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project 
                    page['client_name'] = self.task.client_name
                    page['versioned'] = False
                    page['uri'] = normalize(self.currenturi).replace('&goto=newpost','')
                    page['uri_domain'] = urlparse.urlparse(self.currenturi)[1]
                    page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                    page['first_version_id'] = result['first_version_id']
                    page['task_log_id']=self.task.id
                    page['entity'] = 'Post'
                    page['category'] = self.task.instance_data.get('category' ,'')
                    self.pages.append(page)
                else:
                    log.info(self.log_msg('NOT Appending to self.pages'))
            else:
                log.info(self.log_msg('NOT Appending to self.pages'))
        except:
            log.exception(self.log_msg('Exception in _getPosts'))
            return False
