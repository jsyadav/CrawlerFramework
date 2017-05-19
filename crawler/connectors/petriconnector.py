'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
import copy
from datetime import datetime,timedelta
import logging
from urllib2 import urlparse
from tgimport import tg

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('PetriConnector')
class PetriConnector(BaseConnector):
    '''
    This will fetch the info for petri connector
    Sample uris is 
    http://www.petri.co.il/forums/forumdisplay.php?f=43
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """www.petri.co.il
        """
        self.genre="Review"
        try:
            self.parent_uri = self.currenturi
            if self.currenturi.startswith('http://www.petri.co.il/forums/showthread'):
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.__getParentPage()
                self.post_type= True
                while True:
                    self.__addPosts()
                    try:
                        self.currenturi = 'http://www.petri.co.il/forums/' + self.soup.find('a',text='&gt;').parent['href']
                    except:
                        log.info(self.log_msg('Next page not set'))
                        break
                    if not self.__setSoup():
                        log.info(self.log_msg('cannot continue'))
                        break
                return True
            else:
                self.total_posts_count = 0
                self.last_timestamp = datetime( 1980,1,1 )
                try:
                    self.max_posts_count = int(tg.config.get(path='Connector',key='petri_max_threads_count'))
                except:
                    log.info(self.log_msg('max therads count not set'))
                    self.max_posts_count=5
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                while True:
                    if not self.__getThreads():
                        break
                    try:
                        self.currenturi = 'http://www.petri.co.il/forums/' + self.soup.find('a',text='&gt;').parent['href']
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                #self.linksOut=None
                if self.linksOut:
                    updateSessionInfo('Search', self.session_info_out,self.last_timestamp , None,'ForumThreadsPage', self.task.instance_data.get('update'))
                return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log , '__addPosts')
    def __addPosts(self):
        """ It will add Post for a particular thread
        reviews =soup.findAll('div',id=re.compile('^edit.*?'))
        for review in reviews:
            author_name = stripHtml(review.find('a','bigusername').renderContents())
            author_type = stripHtml(review.find('a','bigusername').findNext('div','smallfont').renderContents())
            date_str  = stripHtml(review.find('td','thead').renderContents()).split('\n')[-1].strip()
            date_str = 
            posted_date = datetime.strftime(datetime.strptime(date_str,'%d-%m-%Y,  %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")            
            date_str = '01 '+ review.find('td','thead').findParent('table').find('div',text=re.compile('Join Date:.*')).split('Join Date:')[-1].strip()
            join_date = datetime.strftime(datetime.strptime(date_str,'%d %b %Y'),"%Y-%m-%dT%H:%M:%SZ")
            post_count = int(re.sub('[^\d]','',review.find('td','thead').findParent('table').find('div',text=re.compile('Posts:.*')).split('Posts:')[-1].strip()))
            last_edited = review.find('div',id=re.compile('post_message_.*')).findParent('td').find('em')
            date_str= stripHtml(last_edited.renderContents()).split(';')[-1].strip()
            last_edited_date= datetime.strftime(datetime.strptime(date_str,'%d-%m-%Y at  %I:%M %p ..'),"%Y-%m-%dT%H:%M:%SZ")
            data=stripHtml(review.find('div',id=re.compile('^post_message_.*')).renderContents())
            next_page_link = 'http://forum.xda-developers.com/' + soup.find('a',text='&gt;').parent['href']
        """
        try:
            reviews =self.soup.findAll('div',id=re.compile('^edit.*?'))               
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
            unique_key = review['id']
            if checkSessionInfo(self.genre, self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]):
                    continue
            page = self.__getData(review ,post_type)
            try:
                review_hash = get_hash( page )                
                result=updateSessionInfo(self.genre, self.session_info_out, unique_key, \
                            review_hash,page['entity'], self.task.instance_data.get('update'),\
                                                        parent_list=[self.parent_uri])
                if not result['updated']:
                    continue
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
                page['category'] = self.task.instance_data.get('category','')
                page['task_log_id']=self.task.id
                #page['uri'] = self.currenturi
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append( page )
                #log.info(page)
                log.info(self.log_msg('Review Added'))
            except:
                log.exception(self.log_msg('Error while adding session info'))

    @logit(log , '__getThreads')
    def __getThreads(self):
            """This will fetch the thread links and create Tasks
            """
            try:
                threads=[each.findParent('tr') for each in self.soup.find('table',id='threadslist').findAll('td',id=re.compile('^td_threadtitle_.*$'))]
                if not threads:
                    log.info(self.log_msg('No threads are found for this page, stop picking therads'))
                    return False
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False
            for thread in threads:
                if  self.max_posts_count <= self.total_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    last_post_and_author = [each.strip() for each in stripHtml(thread.find('td',title=re.compile('^Replies.*')).find('div').renderContents()).split('\n') if not each =='']
                    post_date = re.sub("(\d+)(st|nd|rd|th)",r"\1",last_post_and_author[0]).strip()
                    thread_time = datetime.strptime(post_date,'%d %B %Y  %H:%M')
                    if checkSessionInfo('Search',self.session_info_out, thread_time,self.task.instance_data.get('update')):
                        log.info(self.log_msg('Session info return True or Reaches max count'))
                        continue
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] = normalize( 'http://www.petri.co.il/forums/' + re.sub('\?s=.*?&','?',thread.find('a',id=re.compile('^thread_title_.*$'))['href']) )
                    temp_task.pagedata['et_author_name'] = stripHtml(thread.find('a',id=re.compile('^thread_title_.*$')).findNext('div','smallfont').renderContents())
                    temp_task.pagedata['title']= stripHtml(thread.find('a',id=re.compile('^thread_title_.*$')).renderContents())
                    thread_reply_and_views = thread.find('td',title=re.compile('^Replies.*'))['title'].split('Views:')
                    try:
                        temp_task.pagedata['ei_thread_replies_count'] = int(re.sub('[^\d]','', thread_reply_and_views[0]).strip())  
                        temp_task.pagedata['ei_thread_views_count'] = int(re.sub('[^\d]','', thread_reply_and_views[1]).strip())
                        temp_task.pagedata['et_last_post_author_name'] = re.sub('by','',last_post_and_author[1]).strip()  
                        temp_task.pagedata['edate_last_post_date']=  datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    except:
                        log.info(self.log_msg('Post details may not be found'))
                    try:
                        rating_and_votes = thread.find('img',alt=re.compile('^Thread Rating:.*'))['alt']
                        rating_match = re.search('(\d+)\s*votes,\s*(.*)\s*average',rating_and_votes)
                        temp_task.pagedata['ei_thread_votes_count'] = int(rating_match.group(1).strip())
                        temp_task.pagedata['ef_thread_rating'] = float(rating_match.group(2).strip())
                    except:
                        log.info(self.log_msg('thread votes and ratings may not be found'))
                    self.linksOut.append( temp_task )
                    log.info(self.log_msg('Task Added'))
                except:
                    log.exception( self.log_msg('Posted date not found') )
                    continue
            return True
    @logit(log, '__getData')
    def __getData(self, review, post_type ):
        """ This will return the page dictionry
        for a post
            author_name = stripHtml(review.find('a','bigusername').renderContents())
            author_type = stripHtml(review.find('a','bigusername').findNext('div','smallfont').renderContents())
            date_str  = stripHtml(review.find('td','thead').renderContents()).split('\n')[-1].strip()
            posted_date = datetime.strftime(datetime.strptime(date_str,'%d-%m-%Y,  %I:%M %p'),"%Y-%m-%dT%H:%M:%SZ")            
            date_str = '01 '+ review.find('td','thead').findParent('table').find('div',text=re.compile('Join Date:.*')).split('Join Date:')[-1].strip()
            join_date = datetime.strftime(datetime.strptime(date_str,'%d %b %Y'),"%Y-%m-%dT%H:%M:%SZ")
            post_count = int(review.find('td','thead').findParent('table').find('div',text=re.compile('Posts:.*')).split('Posts:')[-1].strip())
            last_edited = review.find('div',id=re.compile('post_message_.*')).findParent('td').find('em')
            date_str= stripHtml(last_edited.renderContents()).split(';')[-1].strip()
            last_edited_date= datetime.strftime(datetime.strptime(date_str,'%d-%m-%Y at  %I:%M %p ..'),"%Y-%m-%dT%H:%M:%SZ")
            data=stripHtml(review.find('div',id=re.compile('^post_message_.*')).renderContents())
        """ 
        page = {'title':''}
        try:
            if post_type=='Question':
                page['entity']='question'
            else:
                page['entity']='answer'
        except:
            log.info(self.log_msg('Cannot find the entity'))
            page['entity']='suggestion'
        try:
            page['et_author_name'] = stripHtml(review.find('a','bigusername').renderContents())
        except:
            log.info(self.log_msg('author name not found'))
        try:
            date_str = '01 '+ review.find('td','thead').findParent('table').find('div',text=re.compile('Join Date:.*')).split('Join Date:')[-1].strip()
            page['edate_author_member_since'] = datetime.strftime(datetime.strptime(date_str,'%d %b %Y'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Author member Since not found'))
        try:
            page['et_author_membership'] = stripHtml(review.find('a','bigusername').findNext('div','smallfont').renderContents())
        except:
            log.info(self.log_msg('Author membership not found'))
        try:
            page['ei_author_posts_count'] = int(re.sub('[^\d]','',review.find('td','thead').findParent('table').find('div',text=re.compile('Posts:.*')).split('Posts:')[-1].strip()))
        except:
            log.info(self.log_msg('Author Post count not found'))
        try:
            page['uri'] = 'http://www.petri.co.il/forums/' + re.sub('\?s=.*?&','?',review.find('a',id=re.compile('postcount'))['href'] )
        except:
            log.info(self.log_msg('Uri not found'))
            page['uri'] = self.currenturi
        try:
             page['et_author_location'] = review.find('td','thead').findParent\
                    ('table').find('div',text=re.compile('Location:.*')).split\
                                                        ('Location:')[-1].strip()
        except:
            log.info(self.log_msg('Author Location not found'))    
        try:
            page['et_data_reply_to'] = self.parent_uri.split('&')[-1].split('=')[-1]
        except:
            log.info(self.log_msg('data reply to is not found'))
        try:
            page['posted_date'] = datetime.strftime(datetime.strptime(re.sub\
                ("(\d+)(st|nd|rd|th)",r"\1",stripHtml(review.find('td','thead')\
                .renderContents()).split('\n')[-1].strip()).strip(),'%d %B %Y,  %H:%M')\
                                                            ,"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Posted date not found for this post'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['data']  =  stripHtml(review.find('div',id=re.compile('^post_message_.*')).renderContents())
        except:
            page['data'] = ''
            log.info(self.log_msg('Data not found for this post'))
        try:
            page['title'] = stripHtml(review.find('td',id=re.compile('td_post')).find('div','smallfont').renderContents())
        except:
            log.info(self.log_msg('Titlenot found for this post'))
            
        try:
            if page['title']=='':
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
        except:
            log.exception(self.log_msg('title not found'))
        try:
            page['et_data_post_type'] = post_type
        except:
            log.info(self.log_msg('Page info is missing'))
        try:
            hierarchy = [each.replace('>','').strip() for each in stripHtml(self.soup.find('span','navbar').findParent('table').renderContents()).split('\n') if not each.strip()=='']
            page['et_data_forum'] = hierarchy[1]
            page['et_data_subforum'] = hierarchy[2]
            page['et_data_topic'] = hierarchy[3]
        except:
            log.info(self.log_msg('data forum not found'))
        return page

    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will get the parent info
        thread url.split('&')[-1].split('=')[-1]

        """
        if checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                                         self.task.instance_data.get('update')):
            log.info(self.log_msg('Session info return True, Already exists'))
            return False
        page = {}
        try:
            page['et_thread_hierarchy'] = [each.replace('>','').strip() for each in stripHtml(self.soup.find('span','navbar').findParent('table').renderContents()).split('\n') if not each.strip()=='']
            page['title']= page['et_thread_hierarchy'][-1]            
        except:
            log.info(self.log_msg('Thread hierarchy is not found'))
            page['title']=''
        for each in ['title','et_last_post_author_name','ei_thread_replies_count','ei_thread_views_count','edate_last_post_date','ei_thread_votes_count','ef_thread_rating']:
            try:
                page[each] = self.task.pagedata[each]
            except:
                log.info(self.log_msg('page data cannot be extracted'))
        try:
            page['et_thread_id'] = self.currenturi.split('&')[-1].split('=')[-1]
        except:
            log.info(self.log_msg('Thread id not found'))
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