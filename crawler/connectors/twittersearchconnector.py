#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback
import urlparse
from datetime import datetime, timedelta
from dateutil.parser import parse as dparser
import traceback

from TwitterSearch import *

from baseconnector import BaseConnector
from tgimport import config
from utils.task import Task
from utils.urlnorm import normalize


class TwitterSearchConnector(BaseConnector):
    def fetch(self):
        """
        Fetched all the tweets for a given self.currenturi and returns Fetched
        staus depending on the success and faliure of the task
        """
        try:
            print "in fetch"
            self.task.instance_data['source'] = 'twitter.com'
            consumer_key = config.get(section="api_keys", option="consumer_key")
            consumer_secret = config.get(section="api_keys", option="consumer_secret")
            access_token_key = config.get(section="api_keys", option="access_token_key")
            access_token_secret = config.get(section="api_keys", option="access_token_secret")
            print consumer_key
            print consumer_secret
            print access_token_key
            print access_token_secret
            tso = TwitterSearchOrder()
            keyword = self.task.instance_data['keyword']
            tso.set_keywords([keyword])
            print tso
            print keyword
            print "before ts object"
            # it's about time to create a TwitterSearch object with our secret tokens
            ts = TwitterSearch(consumer_key=consumer_key, consumer_secret=consumer_secret, access_token=access_token_key, access_token_secret=access_token_secret)
            print "after ts object"
            print ts
        except:
            print traceback.format_exc()

        try:
            # search & get output
            for tweet in ts.search_tweets_iterable(tso):
                page = {}
                link = 'http://twitter.com/%s/status/%d' % (tweet['user']['screen_name'], tweet['id'])
                try:
                    page['title'] = page['data'] = tweet['text']
                except:
                    print traceback.format_exc()
                try:
                    page['ei_data_tweet_id'] = tweet['id']
                except:
                    print traceback.format_exc()
                try:
                    if 'place' in tweet.keys():
                        try:
                            if type(tweet['place']) is dict:
                                place_key = tweet['place'].keys()
                                for key in place_key:
                                    value = tweet['place'][key]
                                    if not value:
                                        continue
                                    elif isinstance(value, unicode):
                                        page['et_tweet_place_' + key.lower()] = value.encode('utf')
                                    elif key == 'type':
                                        page['et_tweet_place_type'] = str(value)
                                    else:
                                        page['et_tweet_place_' + key.lower()] = str(value)
                            else:
                                page['et_data_tweet_place'] = str(tweet['place'])
                        except:
                            print traceback.format_exc()
                    else:
                        print 'no place info found'
                except:
                    print traceback.format_exc()

                try:
                    if 'in_reply_to_screen_name' in tweet.keys():
                        page['et_data_tweet_reply'] = 'yes'
                        page['et_data_reply_to screen_name'] = str(tweet['in_reply_to_screen_name'])
                    else:
                        page['et_data_tweet_reply'] = 'no'
                except:
                    print traceback.format_exc()
                try:
                    if 'retweet' in tweet.keys() or 'retweeted' in tweet.keys():
                        page['et_data_retweeted'] = 'yes'
                    else:
                        page['et_data_retweeted'] = 'no'
                except:
                    print traceback.format_exc()
                try:
                    tweet_keys = tweet.keys()
                    for key in tweet_keys:
                        value = tweet[key]
                        if not value:
                            continue
                        if key == 'text':
                            print 'already fetched'
                        elif key == 'user':
                            print 'already fetched'
                        elif key == 'id':
                            print 'already fetched'
                            # elif key == 'geo' and tweet['geo'] is dict:

                            # page['ef_data_tweet_geo_latitude'] = tweet['geo']['latitude']
                            # page['ef_data_tweet_geo_longitude'] = tweet['geo']['longitude']
                        elif key == 'favorite_count':
                            page['ei_data_tweet_favorite_count'] = tweet['favorite_count']
                        elif key == 'favorited':
                            page['et_data_tweet_favorited'] = str(tweet['favorited'])
                        # elif key =='entities':
                        # log.inf("not found")
                        elif isinstance(tweet[key], unicode):
                            page['et_data_tweet_' + key.lower()] = tweet[key].encode('utf')
                        elif key == 'place':
                            print "alreday fetched"
                        elif key == 'created_at':
                            print 'alreday created'
                        elif key == 'in_reply_to_screen_name':
                            print 'already created'
                        elif key == 'retweet' or key == 'retweeted' or key == 'retweeted_status':
                            print 'already created'
                        else:
                            page['et_data_tweet_' + key.lower()] = str(tweet[key])
                except:
                    print traceback.format_exc()

                page['parent_path'] = [self.task.instance_data['uri']]
                page['path'] = page['parent_path'][:]
                page['path'].append(link)
                page['uri'] = normalize(link)
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                page['priority'] = self.task.priority
                page['level'] = self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")

                try:
                    # posted_date = datetime.strptime(tweet['created_at'].split('+')[0].strip(),
                    #                         '%a,  %d %b %Y %H:%M:%S')
                    #posted_date = datetime.strptime(tweet['created_at'].split('+')[0].strip(),
                    #                                '%a,  %d %b %Y %H:%M:%S')
                    #page['posted_date'] = posted_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                    page['posted_date'] = datetime.strftime(dparser(tweet['created_at']), "%Y-%m-%dT%H:%M:%SZ")
                except:
                    print traceback.format_exc()
                    page['posted_date'] = page['pickup_date']
                try:
                    data_tweeted_from = str(tweet['source'].split('">')[-1].split('</a>')[0])
                    page['et_data_tweeted_from'] = data_tweeted_from
                except:
                    print traceback.format_exc()
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id  # TODO: Get the client from the project
                page['client_name'] = self.task.client_name
                page['last_updated_time'] = page['pickup_date']
                page['versioned'] = False
                page['task_log_id'] = self.task.id
                page['entity'] = 'post'
                page['category'] = self.task.instance_data.get('category', '')
                self.pages.append(page)
            return True
        except:
            print traceback.format_exc()
            return False
