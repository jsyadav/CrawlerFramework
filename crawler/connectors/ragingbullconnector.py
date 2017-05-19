'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#Rakesh Soni


import re
from datetime import datetime,timedelta
import logging
from urllib2 import urlparse

from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

baseurl = 'http://ragingbull.quote.com'


log = logging.getLogger('RagingBullConnector')
initial_uri = 'http://ragingbull.quote.com/cgi-bin/static.cgi/a=index.txt&d=mainpages'

class RagingBullConnector(BaseConnector):

    '''
        This will fetch info for www.ragingbull.quote.com
        Sample uri is
        http://ragingbull.quote.com/cgi-bin/static.cgi/a=index.txt&d=mainpages
    
    '''
    @logit(log , 'fetch')
    def fetch(self):
        
        """
            Fetch of ragingbull 
            sample uri : http://ragingbull.quote.com/cgi-bin/static.cgi/a=index.txt&d=mainpages
        
        """
        
        self.genre="Review"
        
        try:
            if self.currenturi==initial_uri:
                '''
                    initial_uri is used just for if...else condition.
                    In if we are creating task instances, in else part we are executing and fetching information
                    related to News.
                '''
                
                if not self.__setSoup():
                    '''
                        setSoup() will create a new soup.
                    '''
                    
                    log.info(self.log_msg('Soup not set , Returning False from Fetch'))
                    return False
                
                urls = [ baseurl + each.find('a')['href'] for each in self.soup.find('ul',id='homepage_news').findAll('li')]
                for url in urls:
                    temp_task=self.task.clone()
                    temp_task.instance_data[ 'uri' ] = url
                    self.linksOut.append( temp_task )
            
                return True
        
            else:
                if checkSessionInfo(self.genre, self.session_info_out, self.task.instance_data['uri'],\
                             self.task.instance_data.get('update'),parent_list=[]):
                
                    log.info(self.log_msg('session info return True'))
                    return False
                
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set , Conttinue to next uri'))
                    return False
                
                thread = self.soup.find('div','top_story')
                if not thread:
                    log.info(self.log_msg('No data available, Continue to next uri'))
                    return False
                
                page = {}
                try:
                    href_tag = self.soup.find('h3').find('a')
                    page['title']  = href_tag.renderContents().strip()
                    page['uri'] = href_tag['href']
                except:
                    log.info(self.log_msg('Title not found'))
                    page['title']=''
                
                try:
                    posted_date = self.soup.find('h3').find('span').renderContents().strip().split('EDT')[0].strip()
                    page['posted_date'] = datetime.strftime(datetime.strptime(posted_date,'%A %B %d, %Y %H:%M:%S'),\
                                                                                                "%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.info(self.log_msg('posted date not found'))
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                try:
                    page['data'] = self.__getData(page['uri'])
                    '''
                        getData() will fetch article related to News.
                        
                    '''
                    
                except:
                    log.info(self.log_msg('Data not found'))
                
                review_hash = get_hash( page )
                result=updateSessionInfo(self.genre, self.session_info_out, self.task.instance_data['uri'], \
                            review_hash,'news', self.task.instance_data.get('update'),\
                                                        parent_list=[])
                if not result['updated']:
                    print 'updateSessionInfo returned False..........'
                    return False
                
                
                page['path'] = [self.task.instance_data['uri']]
                page['parent_path'] = []
                page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")

                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                page['client_name'] = self.task.client_name
                page['last_updated_time'] = page['pickup_date']
                page['versioned'] = False
                page['task_log_id']=self.task.id
                page['entity'] = 'Post'
                page['category']=self.task.instance_data.get('category','')
                self.pages.append(page)
                log.info(page)
                
                return True
        except:
            log.exception(self.log_msg('Exception in fetch'))
            return False
        
    def __getData(self, uri):
        '''
            It will fetch news article
        '''
        self.currenturi = uri
        print "Within getData uri ===",uri
        data = ''
        if not self.__setSoup():
            log.info(self.log_msg('Soup not set , Returning blank text from getData'))
            return data
    
        terminating_text = 'Continued...'
        
        article_tag = self.soup.find('div','story_body')
        if not article_tag:
            return data
        
        para_tags = article_tag.findAll('p')
        if not para_tags:
            return data
        continue_uri = para_tags[-1].find('a')
        
        if continue_uri and continue_uri.renderContents().strip()==terminating_text:
            flag = 1
            length = len(para_tags) - 1
            next_uri = 'http://www.quote.com' + continue_uri['href'].strip()
            print "next_uri========================================="
            print next_uri
            
        else:
            flag = 0
            length = len(para_tags) - 2
        
        for i in range(length):
            data += stripHtml(para_tags[i].renderContents())
            
        if flag:
            data += self.__getData(next_uri) 
            #Recursive call to getData b'coz 'continue' link is there for next part of article
        
        return data
        
        
    def __setSoup( self, url = None, data = None, headers = {} ):
        
        """
            Create a new soup
            Set uri to current page, written in seperate
            method so as to avoid code redundancy
        """

        if url:
            self.currenturi = url
        try:
            print "Within setSoup uri :::",self.currenturi
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

    
        
