'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#modified by harsh, prerna for posted_date
import tweepy
from baseconnector import BaseConnector
from urllib import quote_plus, urlencode, quote
from urllib2 import HTTPError
from datetime import datetime, timedelta
    import urlparse
from tgimport import config
from utils.task import Task
from utils.httpconnection import HTTPConnection
from utils.urlnorm import normalize
from utils.utils import stripHtml, get_hash
from utils.sessioninfomanager import *
from utils.decorators import logit
import oauth2 as oauth
import logging
import json
from urllib2 import urlopen
from math import ceil
from ast import literal_eval
from cgi import parse_qsl
from dateutil.parser import parse as dparser
import traceback

from utils.authlib import twitteroauth, klout
from utils.authlib.apilib import NoActiveHandlersAvailable

log = logging.getLogger('TwitterConnector')

class TwitterConnector(BaseConnector):

    @logit(log, '__prepareUrl')
    def __prepareUrl(self, uri, since_id=None, next_page=None):
        '''
        Parses necessary params (queryterm, rpp etc)
        And returned url to be used
        '''
        try:
            print "hello"
            #search_Host = "http://search.twitter.com/search.json"
            search_Host = "https://api.twitter.com/1.1/search/tweets.json"
            if next_page: #if next page provided,  just join and return it as it is
                return search_Host + next_page
            
            #parse Query params as dict
            parsed_url = urlparse.urlparse(uri)
            url_params = dict(urlparse.parse_qsl(parsed_url.query))
            # query_term = url_params.pop('q')
            url_params['show_user'] = True 
            url_params['result_type']='recent' #Setting results to be timewise descending order explicitely

            #Parse number of results from crawler configuration
            num_results = config.get(section='Connector', option='twitter_search_numresults')
            if num_results > 100:  #we need to paginate in case number of results reqd. are more than 100,  limit/paginate
                url_params['count']=100
            else:
                url_params['count']=num_results
                do_paginate=False
        
            if self.task.instance_data.get('queryterm'):
                url_params['q'] = self.task.instance_data.get('queryterm') #get query term from instance_data

            # if not url_params.has_key('q'):
            #     log.info(self.log_msg("No Search keyword provided,  quitting"))
            #     return False

            if since_id: #If since_id is present,  include it
                url_params['since_id'] = since_id
                log.debug(self.log_msg('continuing from since_id : %s'%since_id))

            #parsed_url = parsed_url._replace(path='/1.1/search/tweets.json',query=urlencode(url_params))
            if parsed_url.netloc == 'search.twitter.com':
                query_term = url_params.pop('q')
                #parsed_url = parsed_url._replace(netloc='api.twitter.com',path='/1.1/search/tweets.json',query=urlencode(url_params))
                parsed_url =  parsed_url._replace(netloc='api.twitter.com',path='/1.1/search/tweets.json',query=urlencode(url_params)+'&q='+quote(query_term))
                log.info(self.log_msg("Url_Modified"))
            else:
                query_term = url_params.pop('q')
                # parsed_url = parsed_url._replace(netloc='api.twitter.com',path='/1.1/search/tweets.json',query=urlencode(url_params))
                parsed_url =  parsed_url._replace(netloc='api.twitter.com',path='/1.1/search/tweets.json',query=urlencode(url_params)+'&q='+quote(query_term))
                log.info(self.log_msg("Url_Not_Modified"))
            
            return urlparse.urlunparse(parsed_url)
        except:
            import traceback
            print traceback.format_exc()
            log.exception("Some problem while parsing the url")
            return False


    @logit(log, 'fetch')
    def fetch(self):
        """
        Fetched all the tweets for a given self.currenturi and returns Fetched 
        staus depending on the success and faliure of the task
        """
        try:
            print "in fetch"
            # self.objectpool.addObjects(obj_class=twitteroauth.TwitterOauth, pool_key='twitter_oauth',args_section='twitter_oauth')
            # self.objectpool.addObjects(obj_class=klout.Klout, pool_key='klout',args_section='klout') #prerna
            self.task.instance_data['source']='twitter.com'
            consumer_key = config.get(section="twitter_oauth", option="1.consumer_key")
            consumer_secret = config.get(section="twitter_oauth", option="1.consumer_secret")
            access_token_key = config.get(section="twitter_oauth", option="1.access_token_key")
            access_token_secret = config.get(section="twitter_oauth", option="1.access_token_secret")
            auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token_key, access_token_secret)

            api = tweepy.API(auth)
            print "got api"
        except:
            print traceback.format_exc()
        try:
            print "in try"
            next_page = True #Initialized to True
            page_num = 0
            TWITTER_MAX_ALLOWED_PAGINATION=15
            #A function to get value given a path in session_info
            since_id = getSessionInfoHashSearch('search', self.session_info_out)
            print "got since_id", since_id
            max_id = None
            max_post_limit_reached = False
            self.currenturi = self.__prepareUrl(self.currenturi, next_page=None, since_id=since_id)
            log.info('###current uri %s'%self.currenturi)
            print "current uri %s" %(self.currenturi)
            while page_num < TWITTER_MAX_ALLOWED_PAGINATION and next_page and not max_post_limit_reached:
                print "in while"
                page_num+=1
                print "before getjson"
                apiOutput = self._getJson()
                print "after getjson"
                if apiOutput and apiOutput.get('statuses'):
                    print "in while if"
                    next_page = apiOutput['search_metadata'].get('next_results')
                    log.info(self.log_msg("Next_Page: %s"%next_page))
            
                    #This check is needed as we are changing genre from "review" to "search",  
                    #since_id will be present once we have migrated to "search",  till then we have to check using "genre review"
                    #for one last time before we switch to search genre.
                    if apiOutput.get('search_metadata'):
                        max_id = apiOutput['search_metadata']['max_id']
                        log.info(self.log_msg("Max_ID: %s"%max_id))

                    if since_id == None: #the genre is review here
                        max_post_limit_reached = self.__parseSearchresultsReview(apiOutput['statuses'])
                    else: #genre s Search 
                        max_post_limit_reached = self.__parseSearchresultsSearch(apiOutput['statuses'])

                    self.currenturi = self.__prepareUrl(self.currenturi, since_id=since_id, next_page=next_page)
                else:
                    print "in while else"
                    log.info('Some problem while fetching data from twitter,  or No more posts left')
                    break
                            
            if max_id: #Update Session info with max_id from this search
                updateSessionInfo('search',  self.session_info_out,  max_id,  None,   
                                  'Tweet',  self.task.instance_data.get('update'))
            
            #self.updateKloutKol() #prerna
            
            return True
        except Exception, e:
            print traceback.format_exc()
            log.exception(self.log_msg("Exception occured in fetch()")) 
            return False

    #here I have two functions with name __parseSearchresultsSearch,  __parseSearchresultsReview for different genre's
    # TO keep code readability.

    def __parseSearchresultsSearch(self, results):
        '''
        Parses search result (per page) from twitter API
        So far We have been using  "review" genre,  we should switch to "search" genre and use twitter's since_id param.
        '''
        log.info(self.log_msg('Using genre - Search for session info'))
        num_results = int(config.get(section='Connector', option='twitter_search_numresults'))
        log.info(self.log_msg("Number of results Found :: %d"%len(results)))
        screen_names = list(set([each['user']['screen_name'].strip() for each in results]))
        log.info(self.log_msg("Screen_Names: %s")%screen_names)
        bulkUserInfo = self.__getBulkUserList(screen_names)

        for result in results:
            if len(self.pages) >= num_results:
                log.info(self.log_msg("number of posts to be crawled limit reached, returning"))
                return True
            link = 'http://twitter.com/%s/status/%d'%(result['user']['screen_name'], result['id'])
            log.info(self.log_msg('Link: %s'%link))
            if not checkSessionInfo('search',  self.session_info_out,  
                                    result['id'],  self.task.instance_data.get('update') ):
                page={}
                try:
                    page['title']=page['data']=result['text']
                except:
                    print traceback.format_exc()
                    log.info(self.log_msg("Error occured while fetching the data/title from twitter"))
                    continue                        

                try:
                    page['ei_data_tweet_id'] = result['id']
                except:
                    print traceback.format_exc()
                    log.exception("Couldn't get ID of the tweet, skipping the post")
                    continue 
                try:
                    if 'place' in result.keys():
                        try:
                            if type(result['place'] ) is dict:
                                place_key = result['place'].keys()    
                                log.info(place_key)
                                for key in place_key:
                                    value = result['place'][key]
                                    if not value :
                                        continue
                                    elif isinstance(value, unicode):
                                        page['et_tweet_place_' + key.lower()] = value.encode('utf')
                                    elif key=='type':
                                        page['et_tweet_place_type'] = str(value)    
                                    else:
                                        page['et_tweet_place_' + key.lower()] = str(value)
                            else:
                                page['et_data_tweet_place'] = str(result['place'])
                                log.info(page['et_data_tweet_place'])
                        except:
                            print traceback.format_exc()
                            log.exception(self.log_msg('cannot find place info'))            
                    else:
                        log.info('no place info found') 
                except:
                    print traceback.format_exc()
                    log.exception(self.log_msg('cannot find place info'))
                try:
                    if 'in_reply_to_screen_name' in result.keys():
                        page['et_data_tweet_reply'] = 'yes'
                        page['et_data_reply_to screen_name']= str(result['in_reply_to_screen_name'])
                    else:
                        page['et_data_tweet_reply'] = 'no'
                except:
                    print traceback.format_exc()
                    log.exception(self.log_msg('cannot find in_reply_to_screen_name'))
                try:
                    if 'retweet' in result.keys() or 'retweeted' in result.keys():
                        page['et_data_retweeted'] = 'yes'
                    else:
                        page['et_data_retweeted'] ='no'
                except:
                    print traceback.format_exc()
                    log.exception(self.log_msg('cannot find data retweeted or not'))
                try:
                    tweet_keys = result.keys()
                    for key in tweet_keys:
                        value = result[key]
                        if not value :
                            continue
                        if key=='text':
                            log.info('already fetched')
                        elif key=='user':
                            log.info('already fetched')
                        elif key=='id':
                            log.info('already fetched')
                        # elif key == 'geo' and result['geo'] is dict:

                           # page['ef_data_tweet_geo_latitude'] = result['geo']['latitude']
                           #  page['ef_data_tweet_geo_longitude'] = result['geo']['longitude']
                        elif key =='favorite_count':
                            page['ei_data_tweet_favorite_count'] = result['favorite_count']
                        elif key =='favorited':
                            page['et_data_tweet_favorited'] = str(result['favorited'])
                        # elif key =='entities':
                        #     log.inf("not found")
                        elif isinstance(result[key], unicode):
                                page['et_data_tweet_' + key.lower()] = result[key].encode('utf')
                        elif key=='place':
                            log.info("alreday fetched")
                        elif key=='created_at':
                            log.info('alreday created')
                        elif key=='in_reply_to_screen_name':
                            log.info('already created')
                        elif key=='retweet' or key=='retweeted' or key=='retweeted_status':
                            log.info('already created')
                        else:
                            page['et_data_tweet_'+ key.lower()] = str(result[key])
                        log.info(self.log_msg("page :"%page))
                except:
                    print traceback.format_exc()
                    log.exception(self.log_msg("Error occured while fetching all keys"))

                try:
                    screen_name = result['user']['screen_name'].lower()
                    author_info = bulkUserInfo.get(screen_name)
                    if author_info:
                        author_info = self.__parseAuthorInfo(author_info)
                        page.update(author_info)
                    elif bulkUserInfo: #If some information is returned from this api,  but not for this user
                        author_info = {'et_author_missing_information':'true'} #mark this tweet from a possibly tweet account
                        page.update(author_info)
                except:
                    print traceback.format_exc()
                    log.exception(self.log_msg("Error occured while fetching author profile link"))

                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = page['parent_path'][:]
                page['path'].append(link)
                page['uri'] = normalize(link)
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
                try:
                    #posted_date = datetime.strptime(result['created_at'].split('+')[0].strip(),
                           #                         '%a,  %d %b %Y %H:%M:%S')
                    #posted_date = datetime.strptime(result['created_at'].split('+')[0].strip(),
                    #                                '%a,  %d %b %Y %H:%M:%S')
                    #page['posted_date'] = posted_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                    page['posted_date'] = datetime.strftime(dparser(result['created_at']), "%Y-%m-%dT%H:%M:%SZ")
                    log.info(self.log_msg("search_date_SK_NEW:%s"%page['posted_date']))
                except:
                    print traceback.format_exc()
                    page['posted_date'] = page['pickup_date']
                    log.info(self.log_msg("exp_search_date_SK_NEW:%s"%page['posted_date']))
                try:
                    data_tweeted_from = str(result['source'].split('">')[-1].split('</a>')[0])
                    page['et_data_tweeted_from'] = data_tweeted_from
                    log.info(self.log_msg("data_tweeted_from:%s"%page['et_data_tweeted_from']))
                except:
                    print traceback.format_exc()
                    log.info(self.log_msg("data tweeted from not found"))
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id  # TODO: Get the client from the project
                page['client_name'] = self.task.client_name
                page['last_updated_time'] = page['pickup_date']
                page['versioned'] = False
                page['task_log_id']=self.task.id
                page['entity'] = 'post'
                page['category']=self.task.instance_data.get('category', '')
                self.pages.append(page)
                log.info(self.log_msg("page_data***" %page))
                log.debug(self.log_msg("tweet %s info added to self.pages" %(result['id'])))
            else:
                log.info(self.log_msg("Already picked the link,  quitting"))
                return True
        return False



    def __parseSearchresultsReview(self, results):
        '''
        Parses search result (per page) from twitter API
        So far We have been using  "review" genre,  we should switch to "search" genre and use twitter's since_id param.
        This will only be used for one time call genre transition (review -> search)  
        '''
        log.info(self.log_msg('Using genre - Review for session info'))
        num_results = int(config.get(section='Connector', option='twitter_search_numresults'))
        log.info(self.log_msg("Number of results Found :: %d"%len(results)))
        screen_names = list(set([each['user']['screen_name'].strip() for each in results]))
        #log.info(self.log_msg("Screen_Names: %s")%screen_names)   
        bulkUserInfo = self.__getBulkUserList(screen_names)

        for result in results:
            if len(self.pages) >= num_results:
                log.info(self.log_msg("number of posts to be crawled limit reached, returning"))
                return True

            #link = 'http://twitter.com/statuses/%s/%d'%(result['user']['screen_name'], result['user']['id'])
            link = 'http://twitter.com/%s/status/%d'%(result['user']['screen_name'], result['id'])
            log.info(self.log_msg('Link: %s'%link))
            if not checkSessionInfo('review',  self.session_info_out,  
                                link,  self.task.instance_data.get('update'),  
                                parent_list=[self.task.instance_data['uri']] ):
                page={}
                try:
                    page['title']=page['data']=result['text']
                except:
                    print traceback.format_exc()
                    log.info(self.log_msg("Error occured while fetching the data/title from twitter"))
                    continue                        

                try:
                    page['ei_data_tweet_id'] = result['id']
                except:
                    print traceback.format_exc()
                    log.exception("Couldn't get ID of the tweet, skipping the post")
                    continue 
                try:
                    if 'place' in result.keys():
                        try:
                            if type(result['place']) is dict:
                                place_key = result['place'].keys()
                                log.info(place_key)
                                for key in place_key:
                                    value = result['place'][key]
                                    if not value :
                                        continue
                                    elif isinstance(value, unicode):
                                        page['et_tweet_place_' + key.lower()] = value.encode('utf')
                                    elif key=='type':
                                        page['et_tweet_place_type'] = str(value)
                                    else:
                                        page['et_tweet_place_' + key.lower()] = str(value)
                            else:
                                page['et_data_tweet_place'] = str(result['place'])
                        except:
                            print traceback.format_exc()
                            log.exception(self.log_msg('cannot find place info'))
                    else:
                        log.info('no place info found')
                except:
                    print traceback.format_exc()
                    log.exception(self.log_msg('cannot find place info'))
                try:
                    if 'in_reply_to_screen_name' in result.keys():   
                        page['et_data_tweet_reply'] = 'yes'
                        page['et_data_reply_to screen_name']= str(result['in_reply_to_screen_name'])
                    else:
                        page['et_data_tweet_reply'] = 'no'       
                except:
                    print traceback.format_exc()
                    log.exception(self.log_msg('cannot find in_reply_to_screen_name'))
                try:
                    if 'retweet' in result.keys() or 'retweeted' in result.keys():
                        page['et_data_retweeted'] = 'yes'
                    else:
                        page['et_data_retweeted'] ='no'
                except:
                    print traceback.format_exc()
                    log.exception(self.log_msg('cannot find data retweeted or not'))
                try:
                    tweet_keys = result.keys()
                    for key in tweet_keys:
                        value = result[key]
                        if not value :
                            continue
                        if key=='text':
                            log.info('already fetched')
                        elif key=='user':
                            log.info('already fetched')
                        elif key=='id':
                            log.info('already fetched')
                        elif key == 'geo':
                            page['et_dat_tweet_geo']  = str(result['geo'])
                            # page['ef_data_tweet_geo_latitude'] = result['geo']['latitude']
                    
                            # page['ef_data_tweet_geo_longitude'] = result['geo']['longitude']
                        elif key =='favorite_count' is not None:
                            page['ei_data_tweet_favorite_count'] = result['favorite_count']
                        elif key =='favorited':
                            page['et_data_tweet_favorited'] = str(result['favorited'])
                        # elif key =='entities':
                        #     log.inf("not found")
                        elif isinstance(result[key], unicode):
                                page['et_data_tweet_' + key.lower()] = result[key].encode('utf')
                        elif key=='place':
                            log.info("alreday fetched")
                        elif key=='created_at':
                            log.info('alreday created')
                        elif key=='in_reply_to_screen_name': 
                            log.info('already fetched')
                        elif key=='retweet' or key=='retweeted' or key=='retweeted_status':
                            log.info('already fetched')
                        else:
                            page['et_data_tweet_'+ key.lower()] = str(result[key])
                        log.info(self.log_msg("Page :"%page))
                except:
                    print traceback.format_exc()
                    log.exception(self.log_msg("Error occured while fetching all keys"))
                


                try:
                    screen_name = result['user']['screen_name'].lower()
                    author_info = bulkUserInfo.get(screen_name)
                    if author_info:
                        author_info = self.__parseAuthorInfo(author_info)
                        page.update(author_info)                                
                    elif bulkUserInfo: #If some information is returned from this api,  but not for this user
                        author_info = {'et_author_missing_information':'true'} #mark this tweet from a possibly tweet account
                        page.update(author_info)                                
                except:
                    print traceback.format_exc()
                    log.exception(self.log_msg("Error occured while fetching author profile link"))

                try:
                    tweet_hash = get_hash(page)
                except:
                    print traceback.format_exc()
                    log.info(page)
                    log.exception(self.log_msg("Error occured while creating hash for %s"%link))
                    break

                result_session=updateSessionInfo('review',  self.session_info_out,  'link',  tweet_hash,  
                                         'Tweet',  self.task.instance_data.get('update'),  
                                         parent_list=[self.task.instance_data['uri']])
                if result_session['updated']:
                    page['parent_path'] = [self.task.instance_data['uri']]
                    page['path'] = page['parent_path'][:]
                    page['path'].append(link)
                    page['uri'] = normalize(link)
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
                    try:
                    #    posted_date = datetime.strptime(result['created_at'].split('+')[0].strip(), 
                     #                               '%a,  %d %b %Y %H:%M:%S')
                      #  page['posted_date'] = posted_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                        page['posted_date'] = datetime.strftime(dparser(result['created_at']), "%Y-%m-%dT%H:%M:%SZ")
                        log.info(self.log_msg("review_date:%s"%page['posted_date']))
                    except:
                        print traceback.format_exc()
                        page['posted_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
                        log.info(self.log_msg("exp_review_date:%s"%page['posted_date']))
                    try:
                        data_tweeted_from = str(result['source'].split('">')[-1].split('</a>')[0])
                        page['et_data_tweeted_from'] = data_tweeted_from
                        log.info(self.log_msg("data_tweeted_from:%s"%page['et_data_tweeted_from']))
                    except:
                        print traceback.format_exc()
                        log.info(self.log_msg("data tweeted from not found"))
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project       
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['task_log_id']=self.task.id
                    page['entity'] = 'post'
                    page['category']=self.task.instance_data.get('category', '')
                    self.pages.append(page)
                    log.info(self.log_msg("page_data in review***" %page))
                    log.debug(self.log_msg("tweet %s info added to self.pages" %(link)))
            else:
                log.info(self.log_msg("Already picked the link,  quitting"))
                # As far as my observation,  twitter results are newest first
                break
        return False

    @logit(log, '__getBulkUserList')
    def __getBulkUserList(self, screen_names):
        try:
            if config.get(section='Connector', option='twitter_crawl_author_info'): #Gathering Authormetadata is optional, default True
                num_times = 0
                while True:
                    try:
                        log.info("Calling Bulk user info for - %d users"%len(screen_names))
                        #log.debug(self.objectpool)
                        num_times+=1
                        api_output = self.objectpool.get('twitter_oauth').lookup_users(screen_names=screen_names)
                        #log.info("api_output: %s"%api_output)
                        break
                    except tweepy.TweepError, e:
                        print traceback.format_exc()
                        try:
                            reason = literal_eval(e.reason)[0] #For some reason tweepy keeps e.reason attr json encoded
                        except:
                            print traceback.format_exc()
                            reason = e.reason

                        self.log_msg(log.info(e.__dict__))
                        log.exception("bulk user lookup Error %d"%num_times)
                        if num_times >=3 : #try for a maximum of 3 times
                            break
                        if hasattr(e,'response') and e.response.status == 400:
                            self.objectpool.recheckRateLimit('twitter_oauth') #recheck handlers for rate-limit
                        elif isinstance(reason, dict) and reason.get('code') == 18 and len(screen_names) > 98:
                            self.log_msg(log.info("Breaking existing screen_names into "\
                                                      "50,50 users instead of sending all 100 at once"))
                            api_output = []
                            api_output.extend(self.objectpool.get('twitter_oauth').\
                                                  lookup_users(screen_names=screen_names[:50]))
                            api_output.extend(self.objectpool.get('twitter_oauth')\
                                                  .lookup_users(screen_names=screen_names[50:]))

                user_info = [each.__dict__ for each in api_output]
                for user in user_info:
                    if user.has_key('_api'): #tweepy object attribute
                        del user['_api']
                    if user.has_key('status'): #tweepy object attribute
                        del user['status']

                #normalizing to lowercase name for lookup,  as twitter returns screen_name in correct case if sent wrong in request.
                ret = dict( [ [each['screen_name'].lower(), each] for each in user_info]) 
                log.info(self.log_msg('Returned bulk user_information for %d users'%len(ret.keys())))
                return ret
            else:
                return {}
        except Exception, e:
            print traceback.format_exc()
            log.exception(self.log_msg('Problem occured while calling bulk_user_lookup twitter API'))
            return {}
                          
    @logit(log, 'getAuthorInfo')
    def __parseAuthorInfo(self,  author_profile_data):
        """
        it gets the author profile and sends user info
        It uses Twitter API to lookup user profiles in bulk
        """
        author_info = {}
        try:
            author_keys = ['description', 'time_zone', 'followers_count', 'statuses_count', 'friends_count', 
                           'screen_name', 'favourites_count', 'location', 'name', 'created_at', 'url', 
                           'verified', 'profile_image_url']

            for key in author_keys:
                value = author_profile_data[key]
                if value == None:
                    continue
                if key.endswith('count') and isinstance(value, int):
                    author_info['ei_author_' + key ] = value
                elif key=='created_at':
                    # sample date ,  Thu Jun 04 05:10:08 +0000 2009
                    author_info['edate_author_member_since'] = value.strftime("%Y-%m-%dT%H:%M:%SZ")
                elif key=='url':
                    author_info['et_author_web_url'] = value
                elif key=='description':
                    author_info['et_author_bio'] = value
                elif key == 'verified':
                    author_info['et_author_verified'] = str(value) #Either True or False
                elif key =='geo_enabled':
                    author_info['et_author_geo_enabled'] = str(value)
                elif isinstance(value, unicode):                               
                    author_info['et_author_' + key.lower()] = value.encode('utf')                                            
                else:
                    author_info['et_author_' + key ] = str(value)
                log.info(self.log_msg("author_info***" %author_info))
        except:
            print traceback.format_exc()
            log.exception(self.log_msg('cannot find the author info'))

        return author_info

    def _getJson(self, uri=None):
        '''
        Implemented in twitterconnector for now, 
        TODO: move it to baseconnector
        '''
        print "in _getJson"
        if not uri:
            print self.currenturi
            uri = self.currenturi 

        log.info(self.log_msg("Fetching Uri : %s"%uri))
        try:
            conn = HTTPConnection()
        except:
            print traceback.format_exc()
        print "got httpconn"
        while True:
            print "in getjson while"
            try:
                """#conn.createrequest(uri)
                #output = json.loads(conn.fetch(timeout=60).read())
                consumer_key=tg.config.get(path='keys', key='consumer_key')
                consumer_secret=tg.config.get(path='keys', key='consumer_secret')
                consumer=oauth.Consumer(consumer_key,consumer_secret)
                access_token_key=tg.config.get(path='keys', key='access_token_key')
                log.info(self.log_msg("Access_token_key: %s"%access_token_key))
                access_token_secret=tg.config.get(path='keys', key='access_token_secret')
                log.info(self.log_msg("Access_token_secret: %s"%access_token_secret))
                access_token=oauth.Token(key=access_token_key, secret=access_token_secret)
                client=oauth.Client(consumer,access_token)
                res=client.request(uri)x
                output=json.loads(res[1])
                #log.info(self.log_msg('Output: %s'%output)) """
                print "before api_output"
                print uri
                api_output =self.objectpool.get('twitter_oauth').fetch(uri)
                print "after api_output"
                #log.info("Apioutput: %s"%api_output)
                break 
            except HTTPError, e:
                print traceback.format_exc()
                log.exception(self.log_msg('Error with Twitter'))
                if e.code == 403:
                    error_message = json.loads(e.read())
                    log.info(self.log_msg('The Error message from Twitter is %s'%error_message))
                    if error_message.get('error') and error_message['error'] == 'since date or since_id is too old':
                        url_parts = uri.split('?') 
                        uri_params = dict(parse_qsl(uri.split('?')[1]))
                        if 'since_id' in uri_params.keys():
                            uri_params.pop('since_id')
                        if 'since' in uri_params.keys():
                            uri_params.pop('since')
                        uri = url_parts[0] + '?' + urlencode(uri_params)
                        log.info(self.log_msg('Since ID/Since is too old, reset it to today and being crawled'))
                        log.info(self.log_msg(uri))
                        continue
                    else:
                        log.info(self.log_msg('No message found here'))                     
                #That's the twitter HTTP response to rate limit excedded.                                                                
                #Refer to http://dev.twitter.com/pages/rate-limiting#search                                                              
                elif e.code == 420 and e.headers.get('Retry-After'):
                    
                    waitTill = e.headers.get('Retry-After')
                    log.info(self.log_msg('Rate limit excedded for Search,  Will reset after %s Seconds,  skipping now, '\
                                         'needs better way of handing this'%waitTill))
                log.exception(self.log_msg('Twitter error :%s:%s for  %s'%(e.msg, e.code, uri)))
                return None
        return api_output

    # def
    # (self):
    #     api_renew_time = config.get(section='Connector', option='klout_api_renew_time')
    #     kol_tmpl = {'api_source' : 'klout.com',
    #                 'last_updated' : datetime.utcnow(),
    #                 'renew_date' : datetime.utcnow() + timedelta(minutes = api_renew_time),
    #                 'workspace_id' :self.task.workspace_id,
    #                 'source': 'twitter.com'
    #                 }
    #
    #     author_info = {}
    #
    #     for post in self.pages:
    #         if post.get('et_author_screen_name') and post['et_author_name'].strip() \
    #                 and post.has_key('edate_author_member_since'):
    #             author_info[post['et_author_screen_name']] = ( {'author_name':post['et_author_screen_name'],
    #                                                      'author_profile': 'http://twitter.com/%s'%post['et_author_screen_name'],
    #                                                      'author_real_name':post.get('et_author_name')
    #                                                      })
    #             author_info[post['et_author_screen_name']].update(kol_tmpl)
    #
    #     API_USERNAME_LIMIT = 25
    #     for split in self.sequence_split(author_info.keys(), API_USERNAME_LIMIT):
    #         params = {}
    #         try:
    #             params['users'] = ','.join([name.encode('utf-8','ignore') for name in split])
    #             klout_url = 'http://api.klout.com/1/klout.json?%s'%urlencode(params)
    #             self.log_msg(log.info("Fetching %s"%klout_url))
    #             ret = json.loads(self.objectpool.get('klout').fetch(klout_url))
    #             self.log_msg(log.info("Got results, %s"%str(ret)))
    #             if ret['status'] == 200:
    #                 score_dict = dict([ [ o['twitter_screen_name'].lower(),
    #                                       o['kscore']] for o in ret['users']])
    #                 for name in split:
    #                     if name.lower() in score_dict.keys():
    #                         author_info[name]['score'] = score_dict[name.lower()] #score exists
    #                     else:
    #                         del author_info[name] #This means KOL doesn't exist OR is a spam user acct.
    #             else: #Some error happened at klout end, rerequest API for these usernames later.
    #                 for name in split:
    #                     author_info[name]['api_error'] = True
    #                     author_info[name]['score'] = 0
    #         except HTTPError,e :
    #             print traceback.format_exc()
    #             #Some HTTPError occured at KLOUT END
    #             self.log_msg(log.exception("Error occured while accessing Klout API"))
    #             if e.code != 404:
    #                 for name in split:
    #                     author_info[name]['api_error'] = True
    #                     author_info[name]['score'] = 0
    #             else:
    #                 for name in split: #404 error, raises, delete kol info for these users
    #                     del author_info[name]
    #         except:
    #             print traceback.format_exc()
    #             #Some unknown error occured
    #             self.log_msg(log.exception("Error occured while accessing Klout API"))
    #             for name in split:
    #                 author_info[name]['api_error'] = True
    #                 author_info[name]['score'] = 0
    #
    #     self.kol.extend(author_info.values())

    def sequence_split(self, input_list, subset_length):
        for start in xrange(0,len(input_list),subset_length):
            yield input_list[start:start+subset_length]
