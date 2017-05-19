'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Skumar

import re
import copy
from datetime import datetime
import logging
from urllib2 import urlparse
from cgi import parse_qsl

from tgimport import tg
from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('NaverConnector')
class NaverConnector(BaseConnector):
    '''
    This will fetch the info for msexchange forums
    Sample uris is
    http://kin.naver.com/list/list_noanswer.php?d1id=4&dir_id=403
    '''
    @logit(log , 'fetch')
    def fetch(self):
        """
        Fetch of forum page
        """
        self.genre="Review"
        try:
            #self.currenturi = 'http://kin.naver.com/detail/detail.php?d1id=4&dir_id=403&eid=pEHRtB/4yt6aGN+fMEExvQNPZfhMCfHl&l_url=L2xpc3QvbGlzdF9pbmcucGhwP2QxaWQ9NCZkaXJfaWQ9NDAz'
            self.parent_uri = self.currenturi
            self.base_url = 'http://kin.naver.com'
            self.total_posts_count = 0
            self.last_timestamp = datetime( 1980,1,1 )
            self.max_posts_count = int(tg.config.get(path='Connector',key='naver_forum_numresults'))
            news = self.currenturi.startswith('http://news.search.naver.com')
            blog = self.currenturi.startswith('http://cafeblog.search.naver.com/search.naver?where=post')
            cafe =  self.currenturi.startswith('http://cafeblog.search.naver.com/search.naver?where=article')
            forum = self.currenturi.startswith('http://kin.search.naver.com')
            if blog or forum or news or cafe:
                if forum:
                    self.currenturi = self.currenturi + '&kin_sort=1&df=2008-10-01'
                if blog or cafe:
                    self.currenturi = self.currenturi + '&st=date'
                if news:
                    self.currenturi = self.currenturi + '&sort=0'
                if not self.__setSoup():
                    return False
                while True:
                    try:
                        if not self.__getSearchForumResults():
                            break
                        next_uri =  self.soup.find('a','next')['href']
                        if not forum:
                            self.currenturi = 'http://' + urlparse.urlparse(self.parent_uri)[1] + '/' + next_uri
                        else:
                            self.currenturi = 'http://kin.search.naver.com/search.naver' + next_uri
                        if not self.__setSoup():
                            break
                    except:
                        log.info(self.log_msg('Next page not found'))
                        break
                if self.linksOut:
                    log.info(self.log_msg(len(self.linksOut)))
                    #self.linksOut=[]# remove
                    
                    updateSessionInfo('search', self.session_info_out,self.last_timestamp , None,'ForumThreadsPage', self.task.instance_data.get('update'))
                    log.info(self.log_msg('session info updated'))
                return True
            if self.currenturi.startswith('http://news.naver.com'):
                try:
                    if not self.__setSoup():
                        return False
                    self.__addNews()
                    return True
                except:
                    log.info(self.log_msg('news cannot be added '))
                    return False
                return True
            
            if self.currenturi.startswith('http://kin.naver.com/list'):
                self.currenturi = self.currenturi + '&sort=write_time'
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                next_page_no = 2
                while True:
                    if not self.__getThreads():
                        break
                    try:
                        self.currenturi = self.currenturi + '&page=%s'%str(next_page_no)
                        if not self.__setSoup():
                            break
                        next_page_no = next_page_no + 1
                    except:
                        log.info(self.log_msg('Next Page link not found'))
                        break
                if self.linksOut:
                    updateSessionInfo('search', self.session_info_out,self.last_timestamp , None,'ForumThreadsPage', self.task.instance_data.get('update'))
                return True
            elif self.currenturi.startswith('http://kin.naver.com/detail'):
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                self.post_type=True
                self.__getParentPage()
                
                next_page_no = 2
                while True:
                    self.__addPosts()
                    try:
                        self.currenturi = 'http://kin.naver.com' + self.soup.find('a',id=re.compile('pagearea_\d+'),text=str(next_page_no)).parent['href']
                        if not self.__setSoup():
                            break
                        next_page_no = next_page_no + 1
                    except:
                        log.info(self.log_msg('Next page not found'))
                        break
                return True
            elif self.currenturi.startswith('http://cafe.naver.com'):
                try:
                    if self.__addCafe():
                        return True
                    else:
                        return False
                except:
                    log.info(self.log_msg('Cafe cannot be added'))
                    return False
            elif self.currenturi.startswith('http://blog.naver.com'):
                try:
                    self.__addBlog()
                    return False
                except:
                    log.info(self.log_msg('Blog not found'))
                    return False
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False

    @logit(log , '__addPosts')
    def __addPosts(self):
        """ It will add Post for a particular thread
        """
        try:
            self.review_url = self.currenturi
            reviews = self.soup.findAll('div',id='question')
            reviews.extend(self.soup.findAll('div','answer'))
        except:
            log.exception(self.log_msg('Reviews are not found'))
            return False
        post_type = ''
        for i,review in enumerate(reviews):
            
            if self.post_type and i==0:
                post_type = 'Question'
                self.post_type =False
            else:
                post_type = 'Suggestion'
            page = self.__getData( review,post_type )
            try:
                review_hash = get_hash( page )
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                #unique_key = eval(re.sub('^showCommentList','',review.find('h4').find('a')['onclick'])[:-1])[2]
                if not checkSessionInfo('review', self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            =[self.parent_uri]) or self.task.instance_data.get('pick_comments') :
                    
                    result=updateSessionInfo('review', self.session_info_out, unique_key, \
                                review_hash,'review', self.task.instance_data.get('update'),\
                                                            parent_list=[self.parent_uri])
                    if result['updated']:
                        
                        #page['id'] = result['id']
                        #page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                        #page['first_version_id']=result['first_version_id']
                        parent_list =[self.parent_uri]
                        page['parent_path'] = copy.copy(parent_list)
                        parent_list.append(unique_key)
                        page['path'] = parent_list
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
                        page['entity'] = 'thread'
                        page['category'] = self.task.instance_data.get('category','')
                        
                        page['task_log_id']=self.task.id
                        page['uri'] = self.review_url
                        page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                        self.pages.append( page )
                        log.info(page)
                        log.info(self.log_msg('Review Added'))
                    else:
                        log.info(self.log_msg('result not updated'))
            except:
                log.exception(self.log_msg('Error while adding session info'))
            try:
                comments = review.find('h4').find('a')
                comments_str = stripHtml(comments.renderContents())
                if re.search('.*\d+$',comments_str):
                    self.__addComments(comments['onclick'],[self.parent_uri, unique_key])
                else:
                    log.info(self.log_msg('Comments not found'))
            except:
                log.info(self.log_msg('Comments cannot be added'))
    @logit(log,'__getSearchForumResults')
    def __getSearchForumResults(self):
        '''It will fetch the search results and and add the tasks
        '''
        try:
            results = self.soup.find('ul','type01').findAll('li')
            for result in results:
                try:
                    if self.total_posts_count >= self.max_posts_count:
                        log.info(self.log_msg('Reaching maximum post,Return false'))
                        return False
                    self.total_posts_count = self.total_posts_count + 1
                    date_str = stripHtml(result.find('dd','txt_inline').renderContents())
                    try:
                        thread_time = datetime.strptime(date_str, '%Y.%m.%d')
                    except:
                        try:
                            date_str = re.sub('\(.*?\)','',date_str.split('|')[-1].strip())
                            date_str = date_str.replace(u'\uc624\ud6c4','am')
                            date_str = date_str.replace(u'\uc624\uc804','pm')
                            log.info(self.log_msg(date_str))
                            thread_time= datetime.strptime(date_str,'%Y.%m.%d %p %I:%M')
                        except:
                            log.info(self.log_msg('Cannot find the thread time, task not added '))
                            continue
                    if checkSessionInfo('search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        log.info(self.log_msg('Session info return True or Reaches max count'))
                        return False
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] = result.find('dt').find('a')['href']
                    log.info('taskAdded')
                    self.linksOut.append( temp_task )
                except:
                    log.exception(self.log_msg('task not added'))
                    continue
            return True
        except:
            log.exception(self.log_msg('cannot get the search results'))

                

    @logit(log , '__getThreads')
    def __getThreads( self ):
            """ It will fetch the thread info and create tasks
            """
            try:
                threads = [ x.findParent('tr') for x in self.soup.findAll('div','title_area')]
            except:
                log.exception(self.log_msg('No thread found, cannot proceed'))
                return False
            for thread in threads[:]:
                if  self.total_posts_count > self.max_posts_count:
                    log.info(self.log_msg('Reaching maximum post,Return false'))
                    return False
                self.total_posts_count = self.total_posts_count + 1
                try:
                    date_reply = thread.findAll('td','date')
                    if not len(date_reply)==2:
                        log.info(self.log_msg('not enough info from thread'))
                        continue
                    date_str = stripHtml(date_reply[-1].renderContents())
                    try:
                        thread_time = datetime.strptime ( date_str,'%y.%m.%d')
                    except:
                        try:
                            match_object = re.match('^(\d{2}):(\d{2})$',date_str)
                            today_date =datetime.utcnow()
                            thread_time = datetime(year=today_date.year,month=today_date.month,day=today_date.day,hour=int(match_object.group(1)),minute=int(match_object.group(2)))
                        except:
                            log.info(self.log_msg('Cannot add the post continue'))
                            continue
                    if checkSessionInfo('search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                    #if checkSessionInfo('search',self.session_info_out, thread_time,self.task.instance_data.get('update')) and self.max_posts_count >= self.total_posts_count:
                        log.info(self.log_msg('Session info return True or Reaches max count'))
                        continue
                    self.last_timestamp = max(thread_time , self.last_timestamp )
                    temp_task=self.task.clone()
                    div_content = thread.find('div','title_area')
                    temp_task.instance_data[ 'title' ] = stripHtml(div_content.renderContents())
                    temp_task.instance_data[ 'uri' ] = 'http://kin.naver.com' + div_content.find('a')['href']
                    try:
                        temp_task.pagedata['et_author_name'] =  stripHtml(thread.find('td','write').renderContents())
                    except:
                        log.info(self.log_msg('author name not found'))
                    temp_task.pagedata['ei_thread_replies_count'] = int(stripHtml(date_reply[0].renderContents()))
                    temp_task.pagedata['edate_last_post_date']=  datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                    temp_task.pagedata['et_data_category']=  stripHtml(thread.find('td','name').renderContents())
                    log.info(temp_task.pagedata)
                    log.info('taskAdded')
                    self.linksOut.append( temp_task )
                except:
                    log.exception( self.log_msg('Task Cannot be added') )
            return True
    @logit(log, '__getData')
    def __getData(self, review,post_type ):
        """ This will return the page dictionry
        """
        log.info(post_type)
        page={}
        page = {'title':'','data':''}
        page['et_data_post_type'] = post_type
        try:
            title_div =  review.find('div','title')
            page['title']= stripHtml(title_div.find('h3').renderContents())
            page['et_author_name'] = stripHtml(title_div.find('span','member_id').renderContents())
            date_str = stripHtml(title_div.find('em','date').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%Y.%m.%d %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('Thread info is not found'))
        previous_soup = copy.copy(self.soup)
        previous_uri = self.currenturi
        try:
            page = self.__getAuthorInfo(page)
        except:
            log.info(self.log_msg('Author full info not found'))
        self.soup = copy.copy(previous_soup)
        self.currenturi = previous_uri
        try:
            page['ef_author_rating'] = float(stripHtml(review.find('p','percent').find('em').renderContents())[:-1])
        except:
            log.info(self.log_msg('Author rating not found'))
        try:
            log.info(post_type)
            if not post_type=='Question':
                
                page['data'] = stripHtml(review.find('div','answer_contents').renderContents())
            else:
                page['data'] = stripHtml(review.find('div','question_contents').renderContents())
        except:
            log.info(self.log_msg('data not found'))
            page['data'] = ''
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
            if post_type=='Question':
                 pop_str = review.find('div','title').find('div','popularity').find('script').renderContents().strip()
                 pop_str = re.sub('^showTotalCount','',pop_str)[:-1]
            else:
                pop_str = review.find('div','title').find('div','btn_recommend').find('script').renderContents().strip()[:-1]
                pop_str = re.sub('^showFlash','',pop_str)
            pop_str = re.sub('true','True',pop_str)
            pop_str = re.sub('false','False',pop_str)
            page['ei_data_popularity'] = eval(pop_str)[4]
        except:
            log.info(self.log_msg('popularity cannot be find'))
        return page

    @logit(log, '__getParentPage')
    def __getParentPage(self):
        """
        This will get the parent info
        thread url.split('&')[-1].split('=')[-1]
        """
        page = {}
        try:
            page['et_data_hierachy'] = [ x.strip() for x in stripHtml(self.soup.find('div','location').renderContents()).split('>')]
        except:
            log.info(self.log_msg('hierachy not found'))
        try:  
            page['title']= stripHtml(self.soup.find('div',id='question').find('div','title').find('h3').renderContents())
        except:
            log.info(self.log_msg('title not found'))
            page['title'] = ''
        if not checkSessionInfo('Review', self.session_info_out, self.parent_uri,\
                    self.task.instance_data.get('update')) or self.task.instance_data.get('pick_comments'):
            
                
            for each in ['et_author_name','ei_thread_replies_count','edate_last_post_date' ,'et_data_category']:
                try:
                    page[each] = self.task.pagedata[each]
                except:
                    log.info(self.log_msg('page data cannot be extracted for %s'%each))
            try:
                page['ei_thread_views_count'] = int(stripHtml(self.soup.find('div','popularity').find('span','bar').findNext('em').renderContents()))
            except:
                log.info(self.log_msg('Author thread views count not found'))
            try:
                post_hash = get_hash( page )
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result= updateSessionInfo( 'review', self.session_info_out, self.\
                       parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    #page['first_version_id']=result['first_version_id']
                    #page['id'] = result['id']
                    page['parent_path'] = []
                    page['path'] = [self.parent_uri]
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
                    page['data'] = ''
                    page['task_log_id']=self.task.id
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')
                    self.pages.append(page)
                    log.info(page)
                    log.info(self.log_msg('Parent Page added'))
            except:
                log.exception(self.log_msg("parent post couldn't be parsed"))
        
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
    @logit(log, "__getAuthorInfo")
    def __getAuthorInfo(self,page):
        '''It will fetch the author info
        '''
        try:
            self.currenturi = 'http://kin.naver.com/userinfo/index.php?member_id=%s'%page['et_author_name']
            log.info(self.currenturi)
            if not self.__setSoup():
                return page
        except:
            log.info(self.log_msg('author url not found'))
            return page
        try:
            aut_info = [int(re.sub('[^\d]','',stripHtml(x.findNext('dd').renderContents()))) for x in  self.soup.find('dl','info_count').findAll('dt')]
            page['ei_author_questions_count'] =aut_info[0]
            page['ei_author_answers_count'] =aut_info[1]
            page['ei_author_referals_count'] =aut_info[2]
        except:
            log.info(self.log_msg('author info count not found'))
        try:
            aut_info = [float(stripHtml(x.findNext('dd').renderContents()[:-1])) for x in  self.soup.find('dl','info_graph').findAll('dd','graph')]
            page['ef_author_questioning_percentage'] = aut_info[0]
            page['ef_author_answering_percentage'] = aut_info[1]
            page['ef_author_writing_percentage'] = aut_info[2]
        except:
            log.info(self.log_msg('Author info not found , float '))
        try:
            aut_info = [stripHtml(x.renderContents()) for x in self.soup.find('dl','info_rank').findAll('dd')]
            page['ei_author_energy'] =int( re.sub('[^\d]','',aut_info[0]))
            page['ei_author_rank'] =int(re.sub('[^\d]','',aut_info[1]))
        except:
            log.info(self.log_msg('rank not found'))
        return page
    
    @logit(log, "__addComments")
    def __addComments(self,comment_url,comment_parent_list):
        '''It will fetch the comment
        '''
        #review.find('h4').find('a')['onclick']
        #comment_url = 'showCommentList("KIN", 40301, 2803653, 0, 5, 0, "true");' # to do Remove
        comments_params = eval(re.sub('^showCommentList','',comment_url)[:-1])
        post_query = 'svc=%s&dir_id=%s&docid=%s&answer_no=%s&c_cnt=%s&page=%s'%(comments_params[0],str(comments_params[1]),str(comments_params[2]),str(comments_params[3]),str(comments_params[4]),str(comments_params[5]))
        data = dict([(y[0],y[1]) for y in [x.split('=') for x in post_query.split('&')]])
        headers = {'Referer':self.currenturi}
        if not self.__setSoup(url = 'http://kin.naver.com/qna/comment_action_ajax.php',data=data, headers = headers):
            return False
        comments = self.soup.find('ul').findAll('li')
        for comment in comments:
            page = {}
            try:
                page['et_author_name'] = stripHtml(comment.find('span','id').renderContents())
            except:
                log.info(self.log_msg('author name not found'))
            try:
                subject = comment.find('div','subject')
                date = subject.find('span','date').extract()
                page['et_author_name'] = stripHtml(comment.find('span','id').renderContents())
                date_str = stripHtml(date.renderContents())
                try:
                    thread_time = datetime.strptime ( date_str,'%y.%m.%d %H:%M')
                except:
                    try:
                        match_object = re.match('^(\d{2}):(\d{2})$',date_str)
                        today_date =datetime.utcnow()
                        thread_time = datetime(year=today_date.year,month=today_date.month,day=today_date.day,hour=int(match_object.group(1)),minute=int(match_object.group(2)))
                    except:
                        log.info(self.log_msg('Cannot add the post continue'))
                        thread_time = datetime.utcnow()
                        #datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['posted_date'] = datetime.strftime(thread_time,"%Y-%m-%dT%H:%M:%SZ")
                page['data'] = stripHtml(subject.renderContents())
                if len(page['data']) > 50:
                    page['title'] = page['data'][:50] + '...'
                else:
                    page['title'] = page['data']
            except:
                log.info(self.log_msg('author name not found'))
                page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
            try:
                review_hash = get_hash( page )
                unique_key = get_hash( {'data':page['data'],'title':page['title']})
                if checkSessionInfo('Review', self.session_info_out, unique_key,\
                             self.task.instance_data.get('update'),parent_list\
                                                            = comment_parent_list):
                    continue
                result=updateSessionInfo('Review', self.session_info_out, unique_key, \
                            review_hash,'Review', self.task.instance_data.get('update'),\
                                                        parent_list= comment_parent_list)
                if not result['updated']:
                    continue
                #page['id'] = result['id']
                #page['first_version_id']=result['first_version_id']
                #page['parent_id']= '-'.join(result['id'].split('-')[:-1])
                parent_list = copy.copy(comment_parent_list)
                page['parent_path'] = copy.copy(parent_list)
                page['path'] = parent_list.append(unique_key)
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
                page['entity'] = 'Comment'
                page['category'] = self.task.instance_data.get('category','')
                page['task_log_id']=self.task.id
                page['uri'] = self.review_url
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append( page )
                log.info(page)
                log.info(self.log_msg('Comment Added'))
            except:
                log.exception(self.log_msg('Error while adding comment session info'))

    @logit(log, "__addBlog")
    def __addBlog(self):
        '''It will fetch the comment
        '''
        try:
            '''my_params  = self.currenturi.split('?')[0]
            my_data = dict(parse_qsl(self.currenturi.split('?')[-1]))
            blog_id = my_params[0].split('/')[-1]
            log_no = dict(parse_qsl(my_params[-1]))['logNo']
            my_data = {'blogId':blog_id,'logNo':log_no}

            headers = {'Host':'blog.naver.com'}
            headers['Referer']= 'http://blog.naver.com/NBlogMain.nhn?blogId=nagom22&Redirect=Log&logNo=20068098038&'
            headers['Accept-Language']= 'en-us,en;q=0.5'
            headers['Accept-Charset']= 'ISO-8859-1,utf-8;q=0.7,*;q=0.7'
            headers['Accept-Language'] =  'en-us,en;q=0.5'
            headers['Accept'] = 'application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5'
            headers['Accept-Encoding'] =  'gzip,deflate,bzip2,sdch'
            self.currenturi = 'http://blog.naver.com/BlogTagListInfo.nhn?blogId=%s&logNo=%s'%(blog_id,log_no)
            stripHtml(soup.find('div','htitle').find('span').renderContents())

            #http://blog.naver.com/PostView.nhn?blogId=agapeuni&logNo=60068483650&beginTime=0&jumpingVid=&from=search&widgetTypeCall=true
            #http://blog.naver.com/NBlogMain.nhn?blogId=agapeuni&Redirect=Log&logNo=60068483650&'''
            if not self.__setSoup():
                return False
            source_url = self.soup.find('frame')['src']
            my_data = dict(parse_qsl(source_url.split('?')[-1]))
            uri_template = 'http://blog.naver.com/PostView.nhn?blogId=%s&logNo=%s&beginTime=0&jumpingVid=&from=search&widgetTypeCall=true'
            self.currenturi = uri_template%(my_data['blogId'],my_data['logNo'])
            log.info(self.currenturi)
            if not self.__setSoup():
                return False
        except:
            log.info(self.log_msg('Soup cannto be set '))
            return False
        page={}
        try:
            page['title'] = stripHtml(self.soup.find('div','htitle').find('span').renderContents())
        except:
            log.info(self.log_msg('title page not found'))
        try:
            page['data'] = stripHtml(self.soup.find('div',id='post-view').renderContents())
        except:
            page['data'] =''
            log.info(self.log_msg('data not found'))
        try:
            date_str = stripHtml(self.soup.find('p',attrs={'class':re.compile('^date.*')}).renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%Y/%m/%d %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['et_author_name'] = stripHtml(self.soup.find('div','htitle').findAll('span')[1].renderContents())
        except:
            log.info(self.log_msg('author name not found'))
        if not checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                    self.task.instance_data.get('update')):
            try:
                post_hash = get_hash( page )
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo( self.genre, self.session_info_out, self.\
                       parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    #page['first_version_id']=result['first_version_id']
                    #page['id'] = result['id']
                    page['parent_path'] = []
                    page['path'] = [self.parent_uri]
                    page['uri'] = normalize( self.currenturi )
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    #page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    #page['data'] = ''
                    page['task_log_id']=self.task.id
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')
                    self.pages.append(page)
                    log.info(self.log_msg('Blog Page is added'))
            except:
                log.exception(self.log_msg("parent post couldn't be parsed"))
            




            
        
            
    @logit(log,'__addNews')
    def __addNews(self):
        ''' It will add the news
        '''
        page={}
        try:
            page['title'] = stripHtml(self.soup.find('h4','tit_article').renderContents())
        except:
            log.info(self.log_msg('title not found'))
            page['title'] = ''
        try:
            date_str = stripHtml(self.soup.find('div','info_article').find('span','time').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%Y-%m-%d %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('posted date not found'))
        try:
            page['data'] = stripHtml(self.soup.find('div','article').renderContents())
        except:
            log.info(self.log_msg('dat not found'))
            page['data'] = ''
        if not checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                    self.task.instance_data.get('update')):
            try:
                post_hash = get_hash( page )
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo( self.genre, self.session_info_out, self.\
                       parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    #page['first_version_id']=result['first_version_id']
                    #page['id'] = result['id']
                    page['parent_path'] = []
                    page['path'] = [self.parent_uri]
                    page['uri'] = normalize( self.currenturi )
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    #page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    #page['data'] = ''
                    page['task_log_id']=self.task.id
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')
                    self.pages.append(page)
                    log.info(self.log_msg('News Page is added'))
            except:
                log.exception(self.log_msg("parent post couldn't be parsed"))

    @logit(log,'__addCafe')
    def __addCafe(self):
        ''' It will add the news
        '''
        try: 
            self.currenturi = 'http://cafe.naver.com/studyforever.cafe?iframe_url=/ArticleRead.nhn%3Farticleid=' + self.currenturi.split('/')[-1]
            
            if not self.__setSoup():
                return False 
            #parent_soup = copy.copy(self.soup)
            headers = {'Referer':self.currenturi}
            headers['Host'] = 'cafe.naver.com'
            self.currenturi = 'http://cafe.naver.com'+ self.soup.find('iframe',id='cafe_main')['src']
            my_data = dict(parse_qsl(self.currenturi.split('?')[-1]))
            if not self.__setSoup(data= my_data, headers = headers):
                return False
        except:
            log.info(self.log_msg('cafe cannot be added'))
            return False
        page= {}
        try:
            page['title'] = stripHtml(self.soup.find('span','b m-tcol-c').renderContents())
        except:
            log.info(self.log_msg('title not found'))
            page['title'] = ''
        try:
            page['et_author_name'] = stripHtml(self.soup.find('td','p-nick').renderContents())
        except:
            log.info(self.log_msg('Author name not found'))
        try:
            data_contents =  self.soup.find('div','tbody m-tcol-c')
            data_str = ''
            for x in data_contents:
                try:
                    if x.name=='table' and x['class']=='tag_n_id':
                        break
                except:
                    log.info(self.log_msg())
                data_str= data_str + x.__str__()
            page['data'] = stripHtml(data_str)
        except:
            log.info(self.log_msg('data not found'))
            page['data']= ''
        try:
            date_str= stripHtml(self.soup.find('td','m-tcol-c date').renderContents())
            page['posted_date'] = datetime.strftime(datetime.strptime(date_str,'%Y.%m.%d %H:%M'),"%Y-%m-%dT%H:%M:%SZ")
        except:
            log.info(self.log_msg('posted date not found'))
            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        try:
            page['et_author_title'] =  stripHtml(self.soup.find('td','m-tcol-c step').renderContents())
        except:
            log.info(self.log_msg('author title not found'))
        if not checkSessionInfo(self.genre, self.session_info_out, self.parent_uri,\
                    self.task.instance_data.get('update')):
            try:
                post_hash = get_hash( page )
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo( self.genre, self.session_info_out, self.\
                       parent_uri, post_hash,'Post',self.task.instance_data.get('update'), Id=id)
                if result['updated']:
                    #page['first_version_id']=result['first_version_id']
                    #page['id'] = result['id']
                    page['parent_path'] = []
                    page['path'] = [self.parent_uri]
                    page['uri'] = normalize( self.currenturi )
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    #page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    #page['data'] = ''
                    page['task_log_id']=self.task.id
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')
                    self.pages.append(page)
                    log.info(self.log_msg('CafePage is added'))
            except:
                log.exception(self.log_msg("parent post couldn't be parsed"))
            
            