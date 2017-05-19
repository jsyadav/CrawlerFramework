
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved
This software is confidential and proprietary information of Serendio Software.
It is disclosed pursuant to a non-disclosure agreement between the recipient
and Serendio. This source code is provided for informational purposes only, and
Serendio makes no warranties, either express or implied, in this. Information 
in this program, including URL and other Internet website references, is 
subject to change without notice. The entire risk of the use or the results of 
the use of this program  with the user. Complying with all applicable 
copyright laws is the responsibility of the user. Without limiting the rights 
under copyright, no part of this program may be reproduced, stored in, or 
introduced into a retrieval system, or distributed or transmitted in any form 
or by any means (electronic, mechanical, photocopying, recording, on a website,
or otherwise) or for any purpose, without the express written permission of 
Serendio Software.
Serendio may have patents, patent applications, trademarks, copyrights, or 
other intellectual property rights covering subject matter in this program. 
Except as expressly provided in any written license agreement from Serendio, 
the furnishing of this program does not give you any license to these patents, 
trademarks, copyrights, or other intellectual property.
'''
#Saravanakumar K
#prerna

import logging, time
from datetime import datetime, timedelta
from urllib2 import urlparse

from utils.authlib import boardreader 
from utils.utils import get_hash
from utils.decorators import logit
from knowledgemate import model
from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('BoardReaderConnector')
class BoardReaderConnector(BaseConnector):
    '''This connector uses the Board Reader API to get the Data
    '''
    @logit(log , 'fetch')
    def fetch(self):
        '''
        When the Connector is called first time, It needs to pick the start time 
        and end time given in the instance data, after that It needs to pick the data 
        from the last crawled date to previous day of crawling
        '''                       
        self.objectpool.addObjects(obj_class=boardreader.BoardReader, pool_key='boardreader', args_section='boardreader')
        self.task.instance_data.update(dict([x.split('=') for x in self.task.instance_data['instance_filter_words'].split('&')]))
        
        self.task.instance_data.pop('instance_filter_words')
        self.task.instance_data.pop('source')
        search_options = 'forums'
        fetch_comments = False
        if 'blogs' in self.task.instance_data['uri']:
            search_options='blogs'
            self.task.instance_data['source_type'] = 'blog'
        if 'comments' in self.task.instance_data['uri']:
            fetch_comments = True
        if 'news' in self.task.instance_data['uri']:
            search_options='news'
            self.task.instance_data['source_type'] = 'news'    
        log.info(search_options)    
        if not self.task.instance_data['keyword']:
            log.info(self.log_msg('No Keywords are found'))
            return False 
        task_dict = {'priority':self.task.priority,
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
        keyword = "|".join(['"%s"' %k for k in self.task.instance_data['keyword'].strip().split(",")]) # Check with arun for deleimeter
        last_crawled_date = self.__isAlreadyCrawled()
        search_params = {'max_matches':10000}
        
        filter_from_date = filter_to_date = None
        if not last_crawled_date:
            log.info(self.log_msg('Connector is going to be crawled first time, so taking the param from instance_data'))
            filter_from_date = self.task.instance_data.get('filter_date_from')
            filter_to_date = self.task.instance_data.get('filter_date_to')
        else:
            log.info(self.log_msg('This Connector instance already Crawled'))
            log.info(str(last_crawled_date))
            filter_from_date = str(last_crawled_date)
            
        if not filter_to_date:            
            log.info(self.log_msg('No To date is present, so taking the yesterday as default to date'))
            filter_to_date = datetime.strftime(datetime.utcnow() - timedelta(days=1),'%Y-%m-%d')
            
        if filter_from_date:
            search_params['filter_date_from'] = time.mktime(datetime.strptime(filter_from_date, "%Y-%m-%d").timetuple())
            
        if filter_to_date:
            search_params['filter_date_to'] = time.mktime(datetime.strptime(filter_to_date, "%Y-%m-%d").timetuple())
        if self.task.instance_data.get('dn'):
            domains = [x.strip() for x in self.task.instance_data['dn'].split(',') if x.strip()]
            log.info(domains)
        else:
            log.info(self.log_msg('No Domain is specified, so searching in all domains'))
            domains = ['all']
        results_key_matching = {'uri':'Url', 'data':'Text', 'title':'Subject'}
        author_info_key_matching = {'et_author_profile':'Url', 'et_author_age':'Age', 'et_author_location':'Location', 'et_author_sex':'Sex'}
        for each_domain in domains:
            try:
                if not each_domain=='all':
                    search_params['dn'] = each_domain
                offset = 0
                total_found = None
                while True:
                    results = None
                    try:
                        if offset == search_params['max_matches']:
                            log.info(self.log_msg('Reached the max matches, so stopeed fetching the from current domain'))
                            break
                        search_params['offset'] = offset
                        #results = self.__getResults(keyword, **search_params) # Will get the Results
                        
                        results = self.objectpool.get('boardreader').fetch(keyword, search_options=search_options, fetch_comments=fetch_comments, **search_params) 
                        offset += 100
                        if not total_found:
                            total_found = int(results['response']['TotalFound'])
                        log.info(self.log_msg('# of Matches found in this request is %d'%len(results['response']['Matches']['Match'])))  
                        log.info(self.log_msg('# of Matches found is %s'%results['response']['TotalFound']))
                        if not results['response']['Matches']['Match']:
                            log.info(self.log_msg('No Resulsts found'))
                            break

                        for each_result in results['response']['Matches']['Match']:
                            try:
                                page = {}
                                for each in results_key_matching.keys():
                                    page[each] = each_result[results_key_matching[each]]
                                is_author_info_available = each_result.get('AuthorInfo')
                                if is_author_info_available:
                                    try:
                                        for each in author_info_key_matching:
                                            value = is_author_info_available.get(author_info_key_matching[each]).strip()
                                            if each=='et_author_age' and str(value)=='0':
                                                continue
                                            if value:
                                                page[each] = value
                                    except:
                                        log.exception(self.log_msg('author_info_keynot found'))            
                                author_name_value = each_result.get('Author')
                                if author_name_value:
                                    page['et_author_name'] = author_name_value
                                page['posted_date'] = datetime.strftime(datetime.strptime(each_result['Published'], '%Y-%m-%d %H:%M:%S'),"%Y-%m-%dT%H:%M:%SZ")
                                if search_options=='forums':
                                    log.info(search_options)
                                    page['entity'] = 'Question' if each_result['Grouped'] == 1 else 'Reply'
                                else:
                                    page['entity'] = 'Post'
                                unique_key = get_hash( {'data':page['data'], 'title':page['title'], 'uri':page['uri']})
                                log.info(unique_key)
                                if checkSessionInfo('review', self.session_info_out, unique_key, \
                                                        self.task.instance_data.get('update')):
                                    log.info(self.log_msg('Session info returns True, not adding'))
                                    continue
                                
                                result = updateSessionInfo('review', self.session_info_out, unique_key, \
                                            get_hash(page),'Review', self.task.instance_data.get('update'))
                                if not result['updated']:
                                    continue
                                page['parent_path'] = []
                                page['path'] = [unique_key]
                                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                                page['source'] = page['uri_domain'].replace('www.','')
                                if search_options == 'blogs':
                                    page['source_type'] = 'blog'
                                elif search_options == 'news':
                                    page['source_type'] = 'news'
                                else:      
                                    page['source_type'] = 'forum'
                                page.update(task_dict)
                                self.pages.append(page)
                            except:
                                log.exception(self.log_msg('Cannot add the result'))
                        if offset > total_found:
                            log.info(self.log_msg('Offset reached the total number of matches, so, stopped fetching data'))
                            break
                        #break# To do Remove After Testing
                    except:
                        #errors.append([keyword, row['dn'], offset, results, traceback.format_exc()])
                        log.exception(self.log_msg('Cannot add posts'))
                        break
            except:
                log.exception(self.log_msg('Couldnot add posts for the site'))
                    
        return True
                    
    @logit(log , 'fetch')
    def __isAlreadyCrawled(self):
        '''This will check wheather the connector instance is already crawled or not
        If already crawled, It will return the Last crawled date'''
        query = "select date(start_time) from logs.connector_instance_logs where connector_instance_id = '%s' order by created_date desc" % self.task.connector_instance_id
        result = list(model.metadata.bind.execute(query).fetchall())
        if len(result)>1:
            return datetime.strftime(result[1][0],'%Y-%m-%d')
        else:
            return None
        
