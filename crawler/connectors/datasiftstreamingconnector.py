'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#prerna
#modified by prerna
import logging
import re
try:
    import json
except ImportError:
    import simplejson as json

from urllib2 import urlopen
from urllib import quote_plus
from datetime import datetime
import urlparse
import traceback
import os
import redis
from dateutil.parser import parse
from tgimport import *
from turbogears.database import session
from knowledgemate import model

from baseconnector import BaseConnector
from utils.task import Task
from utils.urlnorm import normalize
from utils.utils import stripHtml,get_hash
from utils.sessioninfomanager import *
from utils.decorators import logit

log = logging.getLogger('DataSiftStreamingConnector')

class DataSiftStreamingConnector(BaseConnector):

    @logit(log,'_init')
    def _init(self):
        """Initialise the default settings for the connector
        """
        self.to_requeue = True
        
        self.num_results = tg.config.get(path='Connector',key='datasift_numresults')
        log.info(self.log_msg('Number of results is %d'%self.num_results))
        self.redis_host = tg.config.get(path='Connector', key='redis_host')
        self.redis_port = tg.config.get(path='Connector', key='redis_port')
        self.timeout = tg.config.get(path='Connector', key='twitterstream_timeout')
        self.rds = redis.Redis(host=self.redis_host, port=self.redis_port) 
        self.queue_name = 'dsqueue_' + self.task.workspace_id
        self.task.instance_data['source']='datasift.com'

    @logit(log, '__updateConnInst')
    def __updateConnInst(self):
        """Update the current connector instance with current date/time to avoid duplicate task while requeueing
        """
        try:
            session.begin()
            connector_instance = model.ConnectorInstance.query().get(self.task.connector_instance_id)
            instance_data = json.loads(connector_instance.instance_data)
            instance_data['uri'] = 'http://trigger.datasift.com/?' + datetime.now().strftime('%Y%m%d%H%M%S%f')           
            connector_instance.instance_data = json.dumps(instance_data)
            session.save_or_update(connector_instance)
            session.commit()
            session.close()
        except:
            log.exception(traceback.format_exc())
            log.exception(self.log_msg('exception while updating twitter streaming random URL'))
            session.rollback()
            session.close()


    @logit(log, '_getKeywords')
    def _getKeywords(self):
        """Get the twitter keywords from list of connector instances and its product_id
        """
        # select * from connector_instances where instance_data like '%search.twitter.com%'
        log.info("Getting keywords")
        self.kwd_conn_inst_id = {} # Has double quotes stripped from keywords
        self.kwd_product_name = {} # Has double quotes stripped from keywords
        self.keywords = [] # Has keywords as it was entered by user with double quotes
        query = "select id, instance_data from common.connector_instances where instance_data like '%%%%//datasift.com%%%%' and delete_status='f' and active_status='t' and workspace_id='%s'" % self.task.workspace_id
        instances = list(model.metadata.bind.execute(query))
        #f = open('test.out','w')
        for conn_inst_id, instance in instances:
            instance = json.loads(instance)
            prod_name = instance['product_name']
            #log.info(instance)
            #parse Query params as dict
            parsed_url = urlparse.urlparse(instance['uri'])
            url_params = urlparse.parse_qs(parsed_url.query)
            #keyword= re.escape(url_params['q'][0].lower().strip('"'))
            keyword= [x.strip() for x in url_params['q'][0].lower().strip('"').split(',')]
            log.info('keyword %s'%keyword)
            for each in keyword:
                self.keywords.append(each)
            #self.keywords = ['Iphone 4ss']
                self.kwd_conn_inst_id[each] = conn_inst_id
                self.kwd_product_name[each] = prod_name
        # Construct a regex using the keywords for matching the tweets
        sorted_kwds = sorted(self.kwd_conn_inst_id.keys(), reverse=True, key=lambda i: len(i))
        regex_str = r"(%s)" %('|'.join(sorted_kwds))
        self.keywords_re = re.compile(regex_str, re.I+re.U)
        log.info(self.log_msg('keywords_re %s'%self.keywords_re))
        #self.keywords = re.escape(','.join(self.keywords)).split(',')
        log.info(self.log_msg('keywords %s'%self.keywords))
    
    @logit(log, '_updateKeywords')
    def _updateKeywords(self):
        """Check if the keywords currently being fetched are same as currently loaded keywords from DB

        Check the set 'kwds_WORKSPACE_ID' in redis and compare with the set from previously loaded keywords.
        If anything has changed, update the set in redis and issue a reboot of the daemon.
        Note: The tweets previously picked up, will be processed using the new keywords since, 
        anyway the user wanted a change in the keywords and product_ids, And using the stale keywords list
        will lead to inconsistency.
        """
        log.info("Updating keywords")
        keywords_key = 'kwds_' + self.task.workspace_id
        old_list = self.rds.smembers(keywords_key)
        new_list = set(self.keywords) #Use the double quoted keywords as we use that in redis
        if (old_list - new_list) or (new_list - old_list): # Something has been added/removed
            self.rds.delete(keywords_key)
            self.rds.sadd(keywords_key, *self.keywords) #Multiple add to set requires redis 2.4.*
            # Issue command to reload the daemon with the new keywords
            self.rds.publish('crawlnode_cmd_' + self.task.workspace_id, 'RELOAD')


    @logit(log, '__fixMetadata')
    def __fixMetadata(self, tweet):
        """Given a tweet, fill in metadata that are not present in it

        Metadata includes:
        * URL of the tweet
        * URL of the user profile
        """
        #tweet['link'] = 'http://twitter.com/%s/status/%s' %(tweet['user']['screen_name'], tweet['id_str'])
        #tweet['user']['author_profile'] = 'http://twitter.com/%s' %tweet['user']['screen_name']
        #tweet['interaction']['link'] = 


    @logit(log,'fetch')
    def fetch(self):
        """Fetch the feeds from a redis queue and parse it and save it
        """
        self._init()
        self._getKeywords()
        self._updateKeywords()
        path = globals()['__file__'].rsplit(os.sep,4)[0] + os.sep +'KnowledgeMate'  + os.sep + 'TaskMaster.shard' + os.sep + 'daemons' + os.sep + 'msmathers-datasift-python-545ead4' + os.sep + 'credentialinfo.cfg'
        credinfo = open(path)
        api_credentials = [x.strip() for x in credinfo.readlines() if x.strip()]
        for each in api_credentials:
            if 'user_name' in each:
                self.username= each.split('user_name=')[-1]
            elif 'api_key' in each:    
                self.apikey = each.split('api_key=')[-1]
            else:
                print 'user_name  and api_key not available'
        credinfo.close()
        
        try:
            for i in range(self.num_results):
                page ={}
                is_retweet = False
                log.info("Fetching tweet")
                log.info(self.queue_name)
                temp_result = self.rds.blpop(self.queue_name, self.timeout)
                try:
                    if not temp_result:
                        log.info(self.log_msg("No Result fetched from the queue"))
                        break
                    result = json.loads(temp_result[1])
                except:
                    log.exception(self.log_msg('Error while loading the results'))
                   # log.info(temp_result)
                    break
                log.info(self.log_msg('tweet:%s'%result))
                if 'message' in result.keys():
                    log.info('waiting  for tweet')
                    continue
##                #if not 'message' in tweet.keys():
##                else:
                if not result.get('twitter'):
                    log.info('No tweet came')
                    log.info(result)
                    continue
                if not result.get('twitter',{}).get('text'):
                    log.info('No tweet came')
                    log.info(result)
                    continue 
                if 'twitter' in result.keys():
                    tweet = result['twitter']    
                    if 'retweet' in tweet.keys() or 'retweeted' in tweet.keys():
                        tweet = result['twitter']['retweet']
            #print ("Its a retweet")
                        page['et_data_retweeted'] = 'yes'
                        is_retweet = True
                    else:
                        page['et_data_retweeted'] ='no'   
                               
                        if not result.get('twitter',{}).get('text'):
                            log.info('No tweet came')
                            log.info(tweet)
                            continue
                else:
                    print 'No tweet found'
                    return         
                s = tweet['text']
                log.info(self.keywords_re.findall(s))
                log.info(type(self.keywords_re.findall(s)))
                matched_keywords = set([i.lower() for i in self.keywords_re.findall(s)])                
                log.info(matched_keywords)
                if not matched_keywords:
                    log.info(self.log_msg('No matched Keywords'))
                    break
                match = matched_keywords.pop() 
                if match:
                    #page = {}
                    if is_retweet:
                        try:
                            page['edate_posted_date_original'] = datetime.strftime(parse(result['twitter']['retweeted']['created_at']), "%Y-%m-%dT%H:%M:%SZ")
                            page['et_author_original_profile'] = 'http://twitter.com/%s'  %(result['twitter']['retweeted']['user']['screen_name'])
                            page['ei_data_original_tweet_id'] = result['twitter']['retweeted']['id']    
                            original_author_keys = result['twitter']['retweeted']['user'].keys()
                            for key in original_author_keys:
                                value = result['twitter']['retweeted']['user'].get(key,'')
                                if value == None:
                                    continue
                                if key.endswith('count') and isinstance(value, int):
                                    page['ei_author_original_' + key.lower() ] = value
                                elif key=='created_at':
                                    page['edate_author_original_member_since'] = datetime.strftime(parse(value), "%Y-%m-%dT%H:%M:%SZ")
                                elif key=='url':
                                    page['et_author_original_web_url'] = value
                                elif key=='description':
                                    page['et_author_original_bio'] = value
                                elif key == 'verified':
                                    page['et_author_original_verified'] = str(value) #Either True or False
                                elif key =='geo_enabled':
                                    page['et_author_original_geo_enabled'] = str(value)
                                elif isinstance(value, unicode):
                                    page['et_author_original' + key.lower()] = value.encode('utf-8')                                            
                                else:
                                    page['et_author_original' + key.lower()] = str(value)
                        except:
                            import traceback
                            print traceback.format_exc()
                            print ('cannot find the original author info')
            
                    try:
                        tweeted_from = result.get('interaction', {}).get('source')
                        if tweeted_from:
                            page['et_data_tweeted_from'] = tweeted_from
                        
                        author_keys = tweet['user'].keys()
                        for key in author_keys:
                            value = tweet['user'][key]
                            if not value:
                                continue
                            if key.endswith('count') and isinstance(value, int):
                                page['ei_author_' + key.lower() ] = value
                            elif key=='created_at':
                    # sample date ,  Thu Jun 04 05:10:08 +0000 2009
                                date_str = value.split('+')[0].strip()
                                page['edate_author_member_since'] = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S').strftime("%Y-%m-%dT%H:%M:%SZ")
                            elif key=='url':
                                page['et_author_web_url'] = value
                            elif key=='description':
                                page['et_author_bio'] = value
                            elif key == 'verified':
                                page['et_author_verified'] = str(value) #Either True or False
                            elif key =='geo_enabled':
                                page['et_author_geo_enabled'] = str(value)
                            elif isinstance(value, unicode):
                                page['et_author_' + key.lower()] = value.encode('utf')                                            
                            else:
                                page['et_author_' + key.lower()] = str(value)
                    except:
                        log.exception(self.log_msg('cannot find the author info'))
                    if 'place' in tweet.keys():
                        try:
                            place_key = tweet['place'].keys()    
                            log.info(place_key)
                            for key in place_key:
                                value = tweet['place'][key]
                                if not value :
                                    continue
                                elif isinstance(value, unicode):
                                    page['et_tweet_place_' + key.lower()] = value.encode('utf')
                                elif key=='type':
                                    page['et_tweet_place_type'] = str(value)    
                                else:
                                    page['et_tweet_place_' + key.lower()] = str(value)
                        except:
                            log.exception(self.log_msg('cannot find place info'))            
                    else:
                        log.info('no place info found')        
                    #page['et_author_gender'] = tweet.get('demographic',{}).get('gender','NA')
                    gender = result.get('demographic',{}).get('gender')
                    if gender:
                        page['et_author_gender'] = gender
                    if 'geo' in tweet.keys():
                        page['ef_data_tweet_geo_latitude'] = tweet['geo']['latitude']
                        page['ef_data_tweet_geo_longitude'] = tweet['geo']['longitude']  
                    else:
                        log.info('geo info not found')    
                    #elif 'source' in tweet['twitter'].keys():
                    #    page['et_data_tweet_source'] =  tweet['twitter']['source']
                    if 'mentions' in tweet.keys():
                        page['et_data_tweet_mentions'] = tweet['mentions']     
                    else:
                        log.info('mentions not found')    
                    if 'in_reply_to_screen_name' in tweet.keys():   
                        page['et_data_tweet_reply'] = 'yes'
                        page['et_data_reply_to screen_name']= tweet['in_reply_to_screen_name']    
                    else:
                        page['et_data_tweet_reply'] = 'no'       
                    if 'in_reply_to_status_id' in tweet.keys():
                        page['et_data_reply_to_status_id'] = tweet['in_reply_to_status_id']
                    else:
                        log.info('reply to status_id not found')       
                    if 'in_reply_to_user_id' in tweet.keys():   
                        page['et_data_reply_to_user_id'] = tweet['in_reply_to_user_id']
                    else:
                        log.info('reply to user_id  not found')
                    page['title'] =  page['data'] = tweet['text']
                    page['et_data_user_name'] = self.username
                    page['et_data_api_key'] = self.apikey
                    page['et_data_category'] = self.kwd_product_name[match]
                    if matched_keywords:
                        page['et_data_matched_other_keywords'] = ','.join([each for each in matched_keywords])
                    page['et_data_matched_keyword'] = match
                    page['connector_instance_id'] = self.kwd_conn_inst_id[match]
                    #page['et_data_retweeted'] ='NO'
                    if 'klout' in tweet.keys():
                        page['et_author_klout_score'] = str(result['klout']['score'])
                    page['ei_data_tweet_id'] = tweet['id']
                    if 'links' in tweet:
                        mentioned_links = tweet['links']
                        page['et_data_mentioned_links'] = ','.join(mentioned_links)
                    else:
                        log.info('mentioned links not found')    
                    if 'link' in result['interaction']['author'].keys():  
                        page['et_author_profile'] = result['interaction']['author']['link']
                    else:    
                        log.info('author profile not found') 
                    if 'avatar' in result['interaction']['author'].keys():
                        page['et_author_avatar'] = result['interaction']['author']['avatar']
                    else:
                        log.info('author profile and image not found')    
                    if 'type' in result['interaction'].keys():
                        page['et_data_tweet_type'] = result['interaction']['type']
                    else:
                        log.info('type not found')    
                    if 'domains' in tweet.keys():
                        page['et_data_tweet_domains'] = tweet['domains']
                    else:
                        log.info('tweet domains not found')    
##                    page['et_data_reply_to_status_id'] = tweet['twitter']['in_reply_to_status_id']
##                    page['et_data_reply_to_user_id'] = tweet['twitter']['in_reply_to_user_id']
                    if 'salience' in result.keys():
                        page['ei_data_sentiment'] = result['salience']['content']['sentiment']
                    else:
                        log.info('data sentiment not found in key')    
                    page['parent_path'] = [self.task.instance_data['uri']]
                    page['path'] = []
                    page['path'].append(result['interaction']['link'])
                    page['uri'] = normalize(result['interaction']['link'])
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
                    page['posted_date'] = datetime.strftime(parse(tweet['created_at']), "%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id 
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id 
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['task_log_id']=self.task.id
                    page['entity'] = 'microblog'
                    page['category']=self.task.instance_data.get('category','')
                    self.pages.append(page)
                else:
                    log.info(self.log_msg('No matched keywords found'))
            log.info("Appended %s posts" %(len(self.pages)))
            self.__updateConnInst()
            return True
        except:
            log.exception(self.log_msg("Exception occured in fetch()"))
            #log.exception(traceback.format_exc())
            return False
        
                
##    @logit(log,'__retweetData')            
##    def __retweetData(self, tweet):
##        """
##        {u'twitter': {u'retweeted': {u'source': u'<a href="http://www.visibli.com" rel="nofollow">Visibli</a>', u'created_at': u'Tue, 17 Jan 2012 09:56:28 +0000', u'user': {u'lang': u'en', u'statuses_count': 252, u'name': u'storedium', u'friends_count': 2265, u'url': u'http://storedium.com', u'time_zone': u'Pacific Time (US & Canada)', u'followers_count': 2289, u'id_str': u'432421031', u'listed_count': 1, u'id': 432421031, u'screen_name': u'storedium'}, u'id': u'159212484375478272'}, u'retweet': {u'count': 0, u'links': [u'http://goo.gl/Nr5Bb'], u'text': u'Luxury Unique Best Leopard Leather Czech Rhinestone Case Cover For Apple iPhone 4 4G 4S\u2026 http://t.co/n8kjX7GI', u'created_at': u'Tue, 17 Jan 2012 09:57:21 +0000', u'source': u'web', u'user': {u'lang': u'en', u'statuses_count': 26, u'name': u'prerana dubey', u'friends_count': 1, u'followers_count': 14, u'id_str': u'138325826', u'id': 138325826, u'screen_name': u'prernasona'}, u'domains': [u'goo.gl'], u'id': u'159212702781292544'}, u'id': u'159212702781292544'}, u'interaction': {u'author': {u'username': u'prernasona', u'link': u'http://twitter.com/prernasona', u'name': u'prerana dubey', u'avatar': u'http://a1.twimg.com/sticky/default_profile_images/default_profile_2_normal.png', u'id': 138325826}, u'created_at': u'Tue, 17 Jan 2012 09:57:21 +0000', u'content': u'RT @storedium: Luxury Unique Best Leopard Leather Czech Rhinestone Case Cover For Apple iPhone 4 4G 4S\u2026 http://t.co/n8kjX7GI', u'source': u'web', u'link': u'http://twitter.com/prernasona/statuses/159212702781292544', u'type': u'twitter', u'id': u'1e140f1a5c0da680e0746a29d63ef41c'}, u'salience': {u'content': {u'sentiment': 5}}, u'language': {u'tag': u'en'}, u'links': {u'url': [u'http://www.amazon.com/Leopard-Rhinestone-Apple-iPhone-4S/dp/B005G21PGA'], u'created_at': [u'Tue, 17 Jan 2012 02:36:59 +0000'], u'retweet_count': [5], u'title': [u'']}}"""
##        matched_keywords = set([i.lower() for i in self.keywords_re.findall(tweet['twitter']['retweet']['text'])])
##            
##        log.info(tweet['twitter']['retweet']['text'])
##        log.info(matched_keywords)
##                
##            # Take the first match of the regex search and consider it as the product
##            # As per discussion on Nov 8, 2011
##            # We are anyway picking up same tweets under different product names and deleting everything except one.
##            # Here, we find the matching products and discard everything except the first.
##        if not matched_keywords:
##            log.info(self.log_msg('No matched Keywords in retweet'))
##            return
##        match = matched_keywords.pop() 
##        if match:
##            page={}
##            try:
##                page['et_data_category'] = self.kwd_product_name[match]
##                tweeted_from = tweet.get('interaction', {}).get('source')
##                if tweeted_from:
##                    page['et_data_tweeted_from'] = tweeted_from
##                original_author_keys = tweet['twitter']['retweeted']['user'].keys()
##                for key in original_author_keys:
##                    value = tweet['twitter']['retweeted']['user'][key]
##                    if value == None:
##                        continue
##                    if key.endswith('count') and isinstance(value, int):
##                        page['ei_author_original_' + key.lower() ] = value
##                    elif key=='created_at':
##            # sample date ,  Thu Jun 04 05:10:08 +0000 2009
##                        page['edate_author_original_member_since'] = value.strftime("%Y-%m-%dT%H:%M:%SZ")
##                    elif key=='url':
##                        page['et_author_original_web_url'] = value
##                    elif key=='description':
##                        page['et_author_original_bio'] = value
##                    elif key == 'verified':
##                        page['et_author_original_verified'] = str(value) #Either True or False
##                    elif key =='geo_enabled':
##                                    page['et_author_original_geo_enabled'] = str(value)
##                    else:
##                        page['et_author_original_' + key.lower()] = value
##            except:
##                log.exception(self.log_msg('cannot find the author info'))
##            try:
##                author_keys = tweet['twitter']['retweet']['user'].keys()
##
####                        author_keys = ['description', 'time_zone', 'followers_count', 'statuses_count', 'friends_count', 
####                           'screen_name', 'favourites_count', 'location', 'name', 'created_at', 'url', 
####                           'verified', 'profile_image_url']
##
##                for key in author_keys:
##                    value = tweet['twitter']['retweet']['user'][key]
##                    if value == None:
##                        continue
##                    if key.endswith('count') and isinstance(value, int):
##                        page['ei_author_' + key ] = value
##                    elif key=='created_at':
##            # sample date ,  Thu Jun 04 05:10:08 +0000 2009
##                        date_str = value.split('+')[0].strip()
##                        page['edate_author_member_since'] = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S').strftime("%Y-%m-%dT%H:%M:%SZ")
##                        #page['edate_author_member_since'] = value.strftime("%Y-%m-%dT%H:%M:%SZ")
##                    elif key=='url':
##                        page['et_author_web_url'] = value
##                    elif key=='description':
##                        page['et_author_bio'] = value
##                    elif key == 'verified':
##                        page['et_author_verified'] = str(value) #Either True or False
##                    elif key =='geo_enabled':
##                        page['et_author_geo_enabled'] = str(value)
##                    elif isinstance(value, unicode):
##                        page['et_author_' + key.lower()] = value.encode('utf')
##                    else:
##                        page['et_author_' + key.lower()] = value
##            except:
##                log.exception(self.log_msg('cannot find the author info'))
##            gender = tweet.get('demographic',{}).get('gender')
##            if gender:
##                page['et_author_gender'] = gender
##            #page['et_author_gender'] = tweet.get('demographic',{}).get('gender','NA')
##            if 'place' in tweet['twitter']['retweet'].keys():
##                try:
##                    place_key = tweet['twitter']['retweet']['place'].keys()    
##                    for key in place_key:
##                        value = tweet['twitter']['retweet']['place'][key]
##                        if value == None:
##                            continue
##                        elif isinstance(value, unicode):
##                            page['et_tweet_place_' + key.lower()] = value.encode('utf')
##                        else:
##                            page['et_tweet_place_' + key.lower()] = str(value)
##                except:
##                    log.exception(self.log_msg('cannot find place info'))            
##            else:
##                log.info('no place info found')        
##            if 'geo' in tweet['twitter']['retweet'].keys():
##                page['et_data_tweet_geo'] = tweet['twitter']['retweet']['geo']  
##            elif 'source' in tweet['twitter']['retweet'].keys():
##                page['et_data_tweet_source'] =  tweet['twitter']['retweet']['source']        
##            elif 'mentions' in tweet['twitter']['retweet'].keys():
##                page['et_data_tweet_mentions'] = tweet['twitter']['retweet']['mentions']      
##            else:
##                log.info('geo and source not found')    
##            if 'in_reply_to_screen_name' in tweet['twitter']['retweet'].keys():   
##                page['et_data_tweet_reply'] = 'yes'
##            else:
##                page['et_data_tweet_reply'] = 'no'    
##                    
##            page['title'] = page['data'] = tweet['twitter']['retweet']['text']
##            page['et_data_mentioned_links'] = tweet['twitter']['retweet']['links']
##            page['et_data_domains'] = tweet['twitter']['retweet']['domains']
##            page['et_data_user_name'] = self.username
##            page['et_data_api_key'] = self.apikey
##            page['connector_instance_id'] = self.kwd_conn_inst_id[match]
##            page['et_data_retweeted'] ='yes'
##            if 'klout' in tweet.keys():
##                page['et_author_klout_score'] = tweet['klout']['score']
##            page['et_data_search_keywords'] = ','.join([each for each in self.keywords])
##            page['et_data_matched_keyword'] = match            
##            if matched_keywords:
##                page['et_data_matched_other_keywords'] = ','.join([each for each in matched_keywords])
##            page['ei_data_tweet_id'] = tweet['twitter']['retweet']['id']
##            page['ei_data_original_tweet_id'] = tweet['twitter']['retweeted']['id']
##            #original is not there in tweet
##            #page['et_data_original'] = tweet['twitter']['retweeted']['text']
##            page['et_author_profile'] = 'http://twitter.com/%s'  %(tweet['twitter']['retweet']['user']['screen_name'])
##            page['et_author_original_profile'] = 'http://twitter.com/%s'  %(tweet['twitter']['retweeted']['user']['screen_name'])
##            page['parent_path'] = [self.task.instance_data['uri']]
##            page['path'] = []
##            page['path'].append(tweet['interaction']['link'])
##            #page['path'].append('http://twitter.com/%s'  %(tweet['twitter']['retweet']['user']['screen_name']))
##            #page['et_original_path'].append('http://twitter.com/%s'  %(tweet['twitter']['retweeted']['user']['screen_name']))
##            #page['uri'] = normalize('http://twitter.com/%s'  %(tweet['twitter']['retweet']['user']['screen_name']))
##            page['uri'] = normalize(tweet['interaction']['link'])
##            #page['et_uri_original'] = normalize('http://twitter.com/%s'  %(tweet['twitter']['retweeted']['user']['screen_name']))
##            page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
##            page['priority']=self.task.priority
##            page['level']=self.task.level
##            page['pickup_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
##            page['posted_date'] = datetime.strftime(parse(tweet['twitter']['retweet']['created_at']), "%Y-%m-%dT%H:%M:%SZ")
##            page['edate_posted_date_original'] = datetime.strftime(parse(tweet['twitter']['retweeted']['created_at']), "%Y-%m-%dT%H:%M:%SZ")
##            page['connector_instance_log_id'] = self.task.connector_instance_log_id # Points to the twitter stream connector
##            page['workspace_id'] = self.task.workspace_id
##            page['client_id'] = self.task.client_id 
##            page['client_name'] = self.task.client_name
##            page['last_updated_time'] = page['pickup_date']
##            page['versioned'] = False
##            page['task_log_id']=self.task.id
##            page['entity'] = 'post'
##            page['category']=self.task.instance_data.get('category','')
##            self.pages.append(page)
##            log.info("Retweet Added")
##        else:
##            log.info(self.log_msg('No matched keywords found'))
##        return page            
