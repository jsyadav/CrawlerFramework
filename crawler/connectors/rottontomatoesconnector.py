
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Ashish
from baseconnector import BaseConnector
import urllib2
import urlparse
import feedparser
from datetime import datetime, timedelta
import time
import copy
from tgimport import *
from utils.task import Task
from utils.urlnorm import normalize
from utils.utils import stripHtml
from utils.sessioninfomanager import *
import logging
import traceback
import cgi
import re
#from logging import config
#logging.config.fileConfig('logging.cfg')

log = logging.getLogger('RottonTomatoesConnector')
class RottonTomatoesConnector(BaseConnector):
    def fetch(self):
        """
        url type supported : http://www.rottentomatoes.com/m/matrix/
        """
        try:
            self.unq_feedurls = []
            self.genre = 'Review'
            self.entity = 'Review'
            if re.search('/articles/[0-9]+/',self.currenturi):
                res = self._getHTML()
                temp_task = self.task.clone()
                temp_task.pagedata = self.task.pagedata.copy()
                temp_task.instance_data['uri'] = self.related_uris[0]
                self.linksOut.append(temp_task)
                return True
            last_timestamp= datetime(1,1,1) # initialize it to least possible value 
            parent_uri = self.currenturi
            self.currenturi = parent_uri + '?page=1&critic=columns&sortby=date&name_order=desc&view=text'
            res =self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            try:
                data_fresh = int(self.soup.find('p',{'id':'tomatometer_data_fresh'}).renderContents().split(':')[-1].split()[0])
            except:
                data_fresh = null
                log.info('could not parse movie fresh scale')
            try:
                data_rotton = int(self.soup.find('p',{'id':'tomatometer_data_rotten'}).renderContents().split(':')[-1].split()[0])
            except:
                data_rotton = null
                log.info('could not parse movie rotton scale')

            while True:
                results = [each.parent for each in self.soup.findAll('td',{'class':'category'})]
                for each in results:
                    try:
                        url = normalize('http://www.rottentomatoes.com' + each.find('a',text=re.compile('Full Review')).parent['href'])
                        posted_date = datetime.strptime(each.find('span',{'class':'date'}).renderContents().strip(),'%b., %d %Y %I:%M %p')
                        if url not in self.unq_feedurls and not \
                                checkSessionInfo(self.genre,
                                                 self.session_info_out, posted_date,
                                                 self.task.instance_data.get('update')):
                            last_timestamp = max(posted_date,last_timestamp)
                            self.unq_feedurls.append(url)
                            temp_task=self.task.clone()
                            temp_task.instance_data['uri'] = url
                            try:
                                rating = each.find('div',{'class':'ratingText'}).a['title']
                                if rating:
                                    temp_task.pagedata['et_rating_overall'] = rating
                            except:
                                log.info('could not parse review rating')
                            temp_task.pagedata['posted_date']=datetime.strftime(posted_date , "%Y-%m-%dT%H:%M:%SZ")
                            temp_task.pagedata['et_author_name'] = stripHtml(each.find('a',{'href':re.compile('/author/')}).renderContents())
                            temp_task.pagedata['et_publisher_name'] = stripHtml(each.find('td',{'class':'category'}).a.renderContents())
                            if data_fresh:
                                temp_task.pagedata['ei_data_fresh']  = data_fresh
                            if data_rotton:
                                temp_task.pagedata['ei_data_rotton'] = data_rotton
                            self.linksOut.append(temp_task)
                    except:
                        log.exception(self.log_msg("exception in adding temptask to linksout while parsing feed"))
                        continue
                log.info(self.log_msg("no. of unique new links added %d" %(len(self.linksOut))))
                if self.soup.find('a',text=re.compile(r'&gt;&gt;')):
                    self.currenturi = normalize(parent_uri + self.soup.find('a',text=re.compile(r'&gt;&gt;')).parent['href'])
                    res =self._getHTML()
                    self.rawpage=res['result']
                    self._setCurrentPage()
                else:
                    break
            if self.linksOut:
                updateSessionInfo(self.genre, self.session_info_out,last_timestamp , None,
                                      self.entity,self.task.instance_data.get('update'))
            return True
        except Exception, e:
            log.exception(log.info('exception in fetch'))
            self.task.status['fetch_status']=False
            return False
        
    def saveToSolr(self):
        return True
