#Prerna
import logging
import gdata.youtube
import feedparser
import gdata.youtube.service
from urllib2 import urlparse
from datetime import datetime


from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash,cleanUnicode
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
log = logging. getLogger("YoutubeConnector")

class YoutubeConnector(BaseConnector):
    @logit(log,'fetch')
    def fetch(self):
        """
        Sample Url:
        http://gdata.youtube.com/feeds/api/videos?vq=ipad+2&racy=include&orderby=relevance
        """
        try:
            self.yt_service = gdata.youtube.service.YouTubeService()
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
            video_range = range(1)
            video_id_list = []
            self.parenturi = self.currenturi
            for start_index in video_range:
                max_count = 2
                self.currenturi = self.parenturi+'&start-index=%d&max-results=%d'%((start_index*max_count)+1,max_count)
                log.info(self.currenturi)
                parser = feedparser.parse(self.currenturi)
                if parser is not None:
                    log.info('number of entries %s'%(len(parser.entries)))
                    if len(parser.entries) == 0:
                        log.info('no entries found for uri %s'%self.currenturi)
                        break
                    #video_id_list = []
                    for entry in parser.entries:
                        try:
                            video_id = entry.link.split('?')[-1].split('&')[0].split('=')[-1]
                            log.info(video_id)
                            video_id_list.append(video_id)
                        except:
                            log.exception('no link found for this entry')
                            continue    
            log.info('total video found %s'%(len(set(video_id_list))))                
            for video_id in set(video_id_list):            
                try:
                    video_entry = self.yt_service.GetYouTubeVideoEntry(video_id=video_id)
                    self.__total_num_of_comments = int(video_entry.comments.feed_link[0].count_hint)
                    #log.info(self.__total_num_of_comments)
                    if not self.__total_num_of_comments :
                        log.info('no comments found for this video %s'%video_entry)
                        continue
                    if not self.__addComments(video_id):
                        log.debug(self.log_msg('comment not added to self.pages for url\
                                                            %s'%self.currenturi))
                except:
                    log.exception(self.log_msg('no comments found for this video %s'%video_entry))
            return True           
            #self.__setParentPage(video_id)
        except:
            log.exception(self.log_msg("Exception in fetch of the url %s"%self.currenturi))
        return True
    
    @logit(log, '__addComments')
    def __addComments(self,video_id):
        '''This will add the comments for given video id
        '''
        MAX_COUNT_PER_PAGE = 5
        try:
            for start_index in range( (self.__total_num_of_comments / MAX_COUNT_PER_PAGE) + 1):
                if start_index >1:
                    break
                else:
                    comments_feed_uri = 'http://gdata.youtube.com/feeds/api/videos/%s/comments?start-index=%d&max-results=%d&orderby=published'%(video_id,(start_index*MAX_COUNT_PER_PAGE)+1,MAX_COUNT_PER_PAGE)
                    #log.info(comments_feed_uri)
                    log.info(self.log_msg('Fetching comments in %s'%comments_feed_uri))
                    if not self.__iterateComments(comments_feed_uri,video_id):
                        log.info(self.log_msg('Reached last crawled Comment, Return \
                                        False in url %s'%self.currenturi))
                        continue
            return True    
        except:
            log.exception(self.log_msg('exception in adding comments %s'%self.currenturi))

    @logit(log, '__iterateComments')
    def __iterateComments(self,comments_feed_uri,video_id):
        '''This will add the comments on the given feed
        '''
        try:
            comments_feed = self.yt_service.GetYouTubeVideoCommentFeed(comments_feed_uri)
            for entry in comments_feed.entry:
                try:
                    page = {}
                    unique_key = entry.id.text
                    log.info(self.log_msg('unique_key %s'%unique_key))
                    page['uri'] = 'http://www.youtube.com/watch?v=' + video_id
                    #page['et_data_comment_uri'] = entry.id.text
                    page['et_author_name'] = entry.author[0].name.text
                    page['et_author_profile'] = 'http://www.youtube.com/user/' + page['et_author_name']
                    #entry.author[0].uri.text
                    page['data'] = stripHtml(entry.content.text)
                    page['title'] = stripHtml(entry.title.text)
                    page['posted_date'] =  datetime.strftime(datetime.strptime(entry.published.text,'%Y-%m-%dT%H:%M:%S.%fZ'),"%Y-%m-%dT%H:%M:%SZ")
                    page['edate_data_updated'] = datetime.strftime(datetime.strptime(entry.updated.text,'%Y-%m-%dT%H:%M:%S.%fZ'),"%Y-%m-%dT%H:%M:%SZ")
                    #log.info(page)
                    #page = self.__getUserInfo(page)
                    user_entry = self.yt_service.GetYouTubeUserEntry(username=page['et_author_name'])
                    if user_entry:
                        try:
                            if user_entry.age:
                                page['et_author_age'] = user_entry.age.text
                            if user_entry.gender:
                                page['et_author_gender'] = user_entry.gender.text    
                            if user_entry.location:
                                page['et_author_location'] = user_entry.location.text 
                            if user_entry.first_name:
                                page['et_author_first_name'] = user_entry.first_name.text 
                            if user_entry.last_name:
                                page['et_author_last_name'] = user_entry.last_name.text 
                            if user_entry.company:
                                page['et_author_company'] = user_entry.company.text 
                            if user_entry.relationship:
                                page['et_author_relationship'] = user_entry.relationship.text 
                            if user_entry.description:
                                page['et_author_description'] = user_entry.description.text  
                                ###3
                            if user_entry.occupation:
                                page['et_author_occupation'] = user_entry.occupation.text
                            if user_entry.school:
                                page['et_author_school'] = user_entry.school.text    
                            if user_entry.hobbies:
                                page['et_author_hobbies'] = user_entry.hobbies.text 
                            if user_entry.movies:
                                page['et_author_movies'] = user_entry.movies.text 
                            if user_entry.music:
                                page['et_author_music'] = user_entry.music.text 
                            if user_entry.books:
                                page['et_author_books'] = user_entry.books.text 
                            if user_entry.hometown:
                                page['et_author_hometown'] = user_entry.hometown.text 
                            for link in user_entry.link:
                                if link.rel == 'related':
                                    page['et_author_website'] = link.href
                        except:
                            log.exception(self.log_msg('exception while fetching author_info %s'%page['et_author_profile']))            
                    #else:
                     #   log.info(self.log_msg('user entry not found for %s'%page['et_author_name']))
                      #  continue         
                    if not checkSessionInfo('review', self.session_info_out, unique_key,\
                                self.task.instance_data.get('update'),parent_list=[ video_id]):
                        log.info(unique_key)
                        try:        
                            for key in page:
                                if key.startswith('et_'):
                                    page[key] = stripHtml(page[key])
                            result = updateSessionInfo('review', self.session_info_out, unique_key,\
                                get_hash(page),'video', self.task.instance_data.get('update')\
                                                                        ,parent_list=[video_id])
                        except:
                            log.exception(self.log_msg('data not in correct format'))
                            continue                                    
                        if result['updated']:         
                            page['path'] = [video_id, unique_key] 
                            page['parent_path'] = [video_id]
                            page['uri_domain'] = unicode(urlparse.urlparse(self.task.instance_data['uri'])[1])
                            page['entity'] = 'video'
                            page.update(self.__task_elements_dict)
                            self.pages.append(page)
                            log.info(self.log_msg('Page added'))
                        else:
                            log.info(self.log_msg('Result[updated] returns False in url %s'%self.currenturi))
                    else:
                        log.info(self.log_msg('Session info return True for uri %s'%self.currenturi))
                        return False
                except:
                    log.exception(self.log_msg('exception while adding posts in url %s'%self.currenturi))
            return True        
        except:
            log.exception(self.log_msg('exception in iteratePosts %s'%self.currenturi))            
                
        #return True

      