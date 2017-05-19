'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

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

log = logging.getLogger('TwitterStreamingConnector')

class TwitterStreamingConnector(BaseConnector):

    @logit(log,'_init')
    def _init(self):
        """Initialise the default settings for the connector
        """
        self.to_requeue = True
        self.num_results = tg.config.get(path='Connector',key='twitterstream_numresults')
        log.info(self.log_msg('Number of results is %d'%self.num_results))
        self.redis_host = tg.config.get(path='Connector', key='redis_host')
        self.redis_port = tg.config.get(path='Connector', key='redis_port')
        self.timeout = tg.config.get(path='Connector', key='twitterstream_timeout')
        self.rds = redis.Redis(host=self.redis_host, port=self.redis_port) 
        self.queue_name = 'tweets_' + self.task.workspace_id
        self.task.instance_data['source']='twitter.com'

    @logit(log, '__updateConnInst')
    def __updateConnInst(self):
        """Update the current connector instance with current date/time to avoid duplicate task while requeueing
        """
        try:
            session.begin()
            connector_instance = model.ConnectorInstance.query().get(self.task.connector_instance_id)
            instance_data = json.loads(connector_instance.instance_data)
            instance_data['uri'] = 'http://twitter.com/?' + datetime.now().strftime('%Y%m%d%H%M%S%f')            
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
        self.keywords = [] # Has keywords as it was entered by user with double quotes
        query = "select id, instance_data from common.connector_instances where instance_data like '%%%%search.twitter.com%%%%' and delete_status='f' and active_status='t' and workspace_id='%s'" % self.task.workspace_id
        instances = model.metadata.bind.execute(query)
        for conn_inst_id, instance in instances:
            instance = json.loads(instance)
            #parse Query params as dict
            parsed_url = urlparse.urlparse(instance['uri'])
            url_params = urlparse.parse_qs(parsed_url.query)
            keyword = url_params['q'][0].lower().strip('"')
            self.keywords.append(keyword)
            self.kwd_conn_inst_id[keyword] = conn_inst_id
        # Construct a regex using the keywords for matching the tweets
        sorted_kwds = sorted(self.kwd_conn_inst_id.keys(), reverse=True, key=lambda i: len(i))
        regex_str = r"(%s)" %('|'.join(sorted_kwds))
        self.keywords_re = re.compile(regex_str, re.I+re.U)
    
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
        tweet['link'] = 'http://twitter.com/%s/status/%s' %(tweet['user']['screen_name'], tweet['id_str'])
        tweet['user']['author_profile'] = 'http://twitter.com/%s' %tweet['user']['screen_name']


    @logit(log,'fetch')
    def fetch(self):
        """Fetch the feeds from a redis queue and parse it and save it
        """
        self._init()
        self._getKeywords()
        self._updateKeywords()
        try:
            for i in range(self.num_results):
                log.info("Fetching tweet")
                temp_result = self.rds.blpop(self.queue_name, self.timeout)
                try:
                    if not temp_result:
                        log.info(self.log_msg("No Result fetched from the queue"))
                        break
                    tweet = json.loads(temp_result[1])
                except:
                    log.exception(self.log_msg('Error while loading the results'))
                    log.info(temp_result)
                    break
                    
                self.__fixMetadata(tweet)                
                matched_keywords = set([i.lower() for i in self.keywords_re.findall(tweet['text'])])
                # Take the first match of the regex search and consider it as the product
                # As per discussion on Nov 8, 2011
                # We are anyway picking up same tweets under different product names and deleting everything except one.
                # Here, we find the matching products and discard everything except the first.
                if not matched_keywords:
                    log.info(self.log_msg('No matched Keywords'))
                    break
                match = matched_keywords.pop() 
                if match:
                    page={}
                    page['title'] = ''
                    page['connector_instance_id'] = self.kwd_conn_inst_id[match]
                    page['ei_data_tweet_id'] = tweet['id']
                    page['data'] = tweet['text']
                    page['et_author_profile'] = tweet['user']['author_profile']
                    page['parent_path'] = [self.task.instance_data['uri']]
                    page['path'] = []
                    page['path'].append(tweet['link'])
                    page['uri'] = normalize(tweet['link'])
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%SZ")
                    page['posted_date'] = datetime.strftime(parse(tweet['created_at']), "%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id # Points to the twitter stream connector
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id 
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['task_log_id']=self.task.id
                    page['entity'] = 'post'
                    page['category']=self.task.instance_data.get('category','')
                    self.pages.append(page)
                else:
                    log.info(self.log_msg('No matched keywords found'))
            log.info("Appended %s posts" %(len(self.pages)))
            self.__updateConnInst()
            return True
        except:
            log.exception(self.log_msg("Exception occured in fetch()"))
            log.exception(traceback.format_exc())
            return False
                

