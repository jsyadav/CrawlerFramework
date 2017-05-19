'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#Pratik


import re
import copy
from datetime import datetime,timedelta
import logging
from urllib2 import urlparse
from urllib2 import urlopen
from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from BeautifulSoup import BeautifulSoup
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('able2knowConnector')

class able2knowConnector(BaseConnector):
    '''
    Sample uris is
    http://able2know.org/tag/finance/
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """

        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            if self.currenturi.startswith('http://able2know.org/topic/'):
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                self.post_type= True
                while True:
                    self.__addPosts()
                    try:
                        self.currenturi = self.soup.find('a',title = 'Next Page')['href']
                    except:
                        log.info(self.log_msg('Next page not set'))
                        break
                    if not self.__setSoup():
                        log.info(self.log_msg('cannot continue'))
                        break
                return True
            elif self.currenturi.startswith('http://able2know.org/tag/'):
                self.total_posts_count = 0
                self.last_timestamp = datetime( 1980,1,1 )
                self.max_posts_count = int(tg.config.get(path='Connector',key='know_forum_numresults'))
                self.currenturi = self.currenturi 
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                count = 2
                while True:
                    if not self.__getThreads():
                        break
                    try:
                        self.currenturi = self.currenturi + self.soup.find('a',accesskey='n')['href'].lstrip('.')
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                    count = count+1
                if self.linksOut:
                    updateSessionInfo('Search', self.session_info_out,self.last_timestamp , None,'ForumThreadsPage', self.task.instance_data.get('update'))
                return True
            else:
                log.info(self.log_msg('Url format is not recognized, Please verify the url'))
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False
        
    @logit(log , '__getThreads')
    def __getThreads(self):
            try:
                threads = [each.findParent('div') for each in self.soup.findAll('div',id=re.compile('^topic-.*$'))]
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False
            for thread in threads:
                if self.max_posts_count <= self.total_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    try:
                        temp_s = thread.find('span',attrs={'class':'topicMeta '})
                        temp_v = [x.strip() for x in stripHtml(temp_s.renderContents()).split('\n') if not x.strip()==0][-1]
                        post_date = temp_v.split('on')[-1].strip()
                        last_author_name = temp_v.split('on')[0].split('by')[-1].strip()
                        thread_time = datetime.strptime (post_date,'%m/%d/%y %I:%M %p')
                    except:
                        log.info(self.log_msg('posted_date not found here.'))
                        continue
                    if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        log.info(self.log_msg('Session info return True or Reaches max count'))
                        continue
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] =thread.find('div',attrs={'style':'overflow:hidden;'}).find('a')['href']
                    temp_task.pagedata['et_author_name'] = thread.find('span',attrs={'class':'topicMeta '}).find('a').renderContents()
                    temp_task.pagedata['title'] = stripHtml(thread.find('div',attrs={'style':'overflow:hidden;'}).find('a').renderContents())
                    try:
                        temp_str = stripHtml(thread.find('span',attrs={'class':'topicMeta '}).renderContents())
                        temp_str = temp_str.replace(',','')
                        temp_task.pagedata['ei_thread_replies_count'] = int(re.compile(r'Replies: \d+',re.I + re.U).findall(temp_str)[0].split(':')[1])
                        temp_task.pagedata['ei_thread_views_count'] = int(re.compile(r'Views: \d+',re.I + re.U).findall(temp_str)[0].split(':')[1])
                        temp_task.pagedata['et_last_post_author_name'] = last_author_name
                        temp_task.pagedata['edate_last_post_date']= datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.exception(self.log_msg('Post details may not be found'))
                    self.linksOut.append( temp_task )
                    log.info(self.log_msg('task added'))
                    log.info(temp_task.pagedata)
                except:
                    log.exception( self.log_msg('Posted date not found') )
                    continue
            return True
    @logit(log , '__addPosts')
    def __addPosts(self):
            ''
            try:
                reviews =[each.findParent('div') for each in self.soup.findAll('div',attrs={'class':'header'})]
            except:
                log.exception(self.log_msg('Reviews are not found'))
                return False
            for i, review in enumerate(reviews):
                post_type = "Question"
                if i==0 and self.post_type:
                    post_type = "Question"
                    self.post_type = False
                else:
                    post_type = "Suggestion"
                page = self.__getData( review,post_type )
                if post_type=='Question':
                    try:
                        title_data = stripHtml(self.soup.find('div',id = re.compile('^topicHeader-.*$')).find('h1').renderContents())
                        page['title'] = title_data
                    except:
                        log.info(self.log_msg('Question post title not found'))
                if page['data']=='':
                   continue
                try:
                    review_hash = get_hash( page )
                    unique_key = get_hash( {'data':page['data'],'title':page['title']})
                    if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                                 self.task.instance_data.get('update'),parent_list\
                                                                =[self.parent_uri]):
                        continue
                    result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                                review_hash,'Review', self.task.instance_data.get('update'),\
                                                            parent_list=[self.parent_uri])
                    if not result['updated']:
                        continue
                    #page['id'] = result['id']
                    #page['first_version_id']=result['first_version_id']
                    #page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                    parent_list = [ self.parent_uri ]
                    page['parent_path'] = copy.copy(parent_list)
                    parent_list.append( unique_key )
                    page['path']=parent_list
                    page['priority']=self.task.priority
                    page['level']=self.task.level
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
                    log.info(page)
                    log.info(self.log_msg('Review Added'))
                except:
                    log.exception(self.log_msg('Error while adding session info'))
                    
    @logit(log , '__getAuthorInfo')
    def __getAuthorInfo(self,link):
            temp_page = urlopen(link)
            tempSoup = BeautifulSoup(temp_page)
            t_page = {}
            temp_lis = tempSoup.find('div',style=re.compile('^height:.*$')).findAll('li')
            if len(temp_lis) > 0:
                try:
                    t_page['ei_author_answered_quoestions'] = int(stripHtml(temp_lis[0].renderContents()).split(':')[1].strip().replace(',',''))
                    t_page['ei_author_posts_count'] = int(stripHtml(temp_lis[1].renderContents()).split(':')[1].strip().replace(',',''))
                    t_page['ei_author_location'] = stripHtml(temp_lis[2].renderContents()).split(':')[1].strip()
                except:
                    log.info(self.log_msg('author info can not be extracted'))
                    log.exception('author fail')
            log.info(self.log_msg('author info fetched'))
            log.info(t_page)
            return t_page

    @logit(log , '__getData')
    def __getData(self,review,post_type):
            ''
            page = {'title':''}
            
            try:
                page['et_author_name'] = review.find('a',attrs ={'class':'user'}).renderContents()
            except:
                log.info(self.log_msg('author name not found'))
            try:
                page['ef_data_post_score'] = float(stripHtml(review.find('span',attrs ={'class':'postScore'}).renderContents()))
            except:
                ''
            try:
                date_str = stripHtml(review.find('span',attrs ={'class':'date smalltxt'}).renderContents())
                page['posted_date']= datetime.strftime( datetime.strptime(date_str,"%a %d %b, %Y %I:%M %p"),"%Y-%m-%dT%H:%M:%SZ")
            except:
                log.info(self.log_msg('Posted date not found for this post'))
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                log.info(page['posted_date'])
            try:
                post_reply_count = int(stripHtml(review.find('div',attrs={'class':'replyBox'}).renderContents()).split( )[0])
                page['ei_post_replies_count'] = post_reply_count
            except:
                log.info(self.log_msg('post reply count not found'))
            try:
                author_info_link = review.find('a',attrs={'class':'user'})['href']
                log.info(self.log_msg('author link'))
                log.info(author_info_link)
                res =self.__getAuthorInfo(author_info_link)
                log.info(self.log_msg('author info'))
                page.update(res)
            except:
                log.exception(self.log_msg('author info added'))

            try:
                data_tag = review.find('div',attrs ={'class':'expandedPostBody'})
                try:
                    data_tag.find('div','quote').extract()
                except:
                    log.info(self.log_msg('data does not contain the previous posts'))
                page['data']  =  stripHtml(data_tag.renderContents())
            except:
                page['data'] = ''
                log.info(self.log_msg('Data not found for this post'))
            try:
                if page['title']=='':
                    if len(page['data']) > 50:
                        page['title'] = page['data'][:50] + '...'
                    else:
                        page['title'] = page['data']
            except:
                log.exception(self.log_msg('title not found'))
                page['title'] = ''
            try:
                page['et_data_post_type'] = post_type
            except:
                log.info(self.log_msg('Page info is missing'))
            return page

    @logit(log , '__getParentPage')
    def __getParentPage(self):
            if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
                log.info(self.log_msg('Session info return True, Already exists'))
                return False
            page = {}
            try:
                page['title']= stripHtml(self.soup.find('div',id = re.compile('^topicHeader-.*$')).find('h1').renderContents())
            except:
                log.info(self.log_msg('Thread title not found'))
                page['title']=''
                
            for each in ['title','et_last_post_author_name','edate_last_post_date','ei_thread_replies_count','ei_thread_views_count']:
                try:
                    page[each] = self.task.pagedata[each]
                except:
                    #log.info(self.log_msg('page data cannot be extracted'))
                    log.exception('page data can not be found')

            try:
                post_hash = get_hash( page )
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo( self.genre, self.session_info_out, self.\
                       parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
                if not result['updated']:
                    return False
                page['path']=[self.parent_uri]
                page['parent_path']=[]
                page['uri'] = normalize( self.currenturi )
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
                #page['first_version_id']=result['first_version_id']
                page['data'] = ''
                #page['id'] = result['id']
                page['task_log_id']=self.task.id
                page['entity'] = 'Post'
                page['category']=self.task.instance_data.get('category','')
                self.pages.append(page)
                log.info(page)
                log.info(self.log_msg('Parent Page added'))
                return True
            except :
                log.exception(self.log_msg("parent post couldn't be parsed"))
                return False

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
            ''