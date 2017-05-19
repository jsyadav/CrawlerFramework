'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#Pooja

from baseconnector import BaseConnector
import urllib2
from tgimport import *
from utils.task import Task
from utils.urlnorm import normalize
import urlparse
from urllib2 import urlopen
import logging
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
import simplejson
import re

log = logging.getLogger('YahooSiteExplorerConnector')
class YahooSiteExplorerConnector(BaseConnector):
    def fetch(self):
        '''
            Sample url : http://search.yahooapis.com/SiteExplorerService/V1/pageData?appid=ID:%2033A2XTXV34HTqmAQ_6NgE1C2KF3z1_RjXFEwO1mHnKmq.Lkmn99r06ji9WP7G6E9dEb1qg.8Lg--&query=http://www.topix.com/business/banking/&results=100&output=json
        '''
        try:
            self.task.instance_data['source_type'] = self.task.instance_data.get('source_type','rss')
            if self.task.level > 1:
                log.debug(self.log_msg('Recursion level greater then MAX, returning '))
                return True
            uniq_urls = set()
            search_url = self.currenturi.split('&query=')[1].split('&results=')[0]
            if search_url[-1] == '/':
                search_url = ''.join(search_url[:-1])
            start = 1
            url = self.currenturi + '&start=' + str(start)
            log.debug(url)
            result_json = simplejson.load(urlopen(url))
            total_url = int(result_json['ResultSet']['totalResultsAvailable'])
            dateRe = x = re.compile(r'(20)?((08)/(8|08|9|09|10|11|12))|((09)/(1|01|2|02|3|03|4|04|5|05|6|06|7|07|8|08|9|09))')
            while True :
                for result in result_json['ResultSet']['Result']:
                    genre = 'Generic'
                    key = result['Url']
                    try:
                        if not checkSessionInfo(genre,self.session_info_out, key, self.task.instance_data.get('update')) and not result['Url'] in uniq_urls and len(result['Url']) > len(search_url) and (len(result['Url'].split('/')) - len(search_url.split('/'))) > 2 and dateRe.search(result['Url']):
                            uniq_urls.add(result['Url'])
                            log.debug('URL :: '+result['Url'])
                            temp_task = self.task.clone()
                            temp_task.instance_data['uri'] = normalize(result['Url'])
                            #temp_task.instance_data['connector_name'] = 'RSSConnector'
                            temp_task.pagedata['title'] = result['Title']
                            temp_task.pagedata['source'] = urlparse.urlparse(self.currenturi)[1]
                            updateSessionInfo(genre, self.session_info_out, key, '', 'Post', self.task.instance_data.get('update'))
                            self.linksOut.append(temp_task)
                    except:
                        log.exception(self.log_msg("Exception in adding temptask to linksout"))
                        continue
                start += 100
                total_url -= 100
                if total_url > 0 and start < 1000:
                    url = self.currenturi + '&start=' + str(start)
                    log.debug(url)
                    result_json = simplejson.load(urlopen(url))
                else:
                    break
            log.info(self.log_msg("No of new links added %d"%(len(uniq_urls))))
            return True
        except Exception, e:
            log.exception(log.info('Exception in Fetch'))
            return False
