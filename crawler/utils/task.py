
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

from sessioninfo import SessionInfo

class Task(object):
    def __init__(self, instance_data, connector_instance_log_id,
                 workspace_id, client_id, client_name, connector_instance_id,
                 dburi, priority = 10, keywords = None, level = 1,
                 highlight_words=None, token = (), times_reenqueued=0):

        ##minimum requirements begin##

        ##for sending a task, getting linksout, getting dbentry for a task
        self.connClass = None
        self.instance_data = instance_data
        self.token = token
        self.times_reenqueued = times_reenqueued

        #this will set the meta page level to 0,
        #thus increment/decrement is done at this one place and done with
        self.level=level
#        if self.instance_data.get('metapage'):
#            self.level= self.level - 1 

        self.session_info=SessionInfo()

        self.keywords = []
        if keywords:
            self.keywords=keywords
        self.highlight_words = []
        if highlight_words:
            self.highlight_words = highlight_words
        #maintained by linkouts, saved in Solr,
        self.priority = priority
        self.connector_instance_log_id = connector_instance_log_id

        #to be put in Solr
        self.client_id = client_id
        self.client_name=client_name
        self.workspace_id = workspace_id
        self.connector_instance_id = connector_instance_id
        self.dburi = dburi
        ##minimum requirements end##


        ##extras
        self.start_time=None
        self.end_time=None

        #the status'es of the four phases
        self.status = {}
        self.status['fetch_status'] = False
        self.status['fetch_message'] = ''
        self.status['filter_status'] = False
        self.status['extract_status'] = False

        self.pagedata = {}
        #used to set the title for a followed link
        #after following heuristics like length and same title
        self.pagedata['title'] = ''
        #used to set the created_date for 1. current article(now) and 2. A followed link(from link)
       #  self.pagedata['posted_date'] = datetime.utcnow()#.utctimetuple()
#         #utcnow always - override
#         self.pagedata['pickup_date'] = datetime.utcnow()#.utctimetuple()
#         #store current page's hash
#         self.pagedata['content_hash'] = ''
#         #needed 1. when updated content found, retrieved from solr using the url
#         #and sent back for making the parent child relation 2. None default
        self.pagedata['parent_task_id'] = None #This data is stored in saveToDB
#         # using jsontext for storing the related uris
#         self.pagedata['related_uris']= []
#         #number of posts from a review or forum
#         self.pagedata['num_posts']=None
        

    def clone(self):
        '''
        Create a new task from the current task. Increment the level and
        set appropiate values for other elements
        '''
        #level+1 not always true - metapage
        if self.instance_data.get('metapage'):
            level = self.level
        else:
            level = self.level + 1
        t = Task(instance_data=self.instance_data.copy(), #session_info=self.session_info.copy(),
                    connector_instance_log_id=self.connector_instance_log_id, workspace_id=self.workspace_id,
                    client_id=self.client_id, client_name=self.client_name, 
                    connector_instance_id=self.connector_instance_id,dburi=self.dburi,
                    priority=self.priority, keywords=self.keywords, level=level,
                    highlight_words=self.highlight_words,token=self.token,
                    times_reenqueued = self.times_reenqueued)

        # Other than original pages, others cannot be meta pages.
        t.instance_data['metapage'] = False
        t.instance_data['already_parsed'] = True # so that pages from metapage is not pickedup by googlesiteconnector
        return t

    
    def __repr__(self):
        res=str(self.instance_data)+" : "+\
            str(self.priority)+" : "+str(self.level)
        return res


def getDummyTask():
    return Task(instance_data={'uri':'http://news.google.co.in',
                               'queryterm':u'motorola and rfid',
                               'metapage':False,
                               'category':u'News',
                               'versioned':False,
                               'apply_keywords':False
                               },
                connector_instance_log_id=1,
                workspace_id=1,
                client_id=1,
                client_name='dummy',
                connector_instance_id=1,
                dburi=None
                )
