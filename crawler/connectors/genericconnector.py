
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#JV
#Ashish

import re
import time
import random
import logging
import traceback
import cgi
import copy
import md5
from BeautifulSoup import *

from baseconnector import BaseConnector
from utils.httpconnection import HTTPConnection
from rssconnector import RSSConnector
from htmlconnector import HTMLConnector
import feedparser

from urllib2 import urlparse
import traceback
#from logging import config
#logging.config.fileConfig('logging.cfg')

log = logging.getLogger('GenericConnector')
class GenericConnector(BaseConnector):


    def processTask(self):
        """
        """
        print "started process"
        try:
            # try:
            #     print self.task.instance_data.get('source_type','rss')
            #     log.info(self.task.instance_data.get('source_type','rss'))
            #     if self.task.instance_data.get('source_type','rss') == 'rss':
            #         parser = feedparser.parse(self.task.instance_data['uri'])
            #         log.info('probing for rss feed')
            #         if parser and len(parser.version) > 0:
            #             log.debug('TaskID:%s::Client:%s::feed found' % (self.task.id, self.task.client_name))
            #             cInstance=RSSConnector(self.task, parser=parser)
            #             cInstance.processTask()
            #             return
            # except:
            #     print traceback.format_exc()
            #     log.debug('TaskID:%s::Client:%s::exception in parsing feed' % (self.task.id, self.task.client_name))
            #     # self.task.instance_data['metapage']=True

            try:    
                log.debug('TaskID:%s::Client:%s::taking up htmlconnector' % (self.task.id, self.task.client_name))
                cInstance=HTMLConnector(self.task, self.rawpage)
                cInstance.processTask()
                return
            except:
                print traceback.format_exc()
                log.exception('TaskID:%s::Client:%s::no suitable connector found - returning' % (self.task.id, self.task.client_name))
                log.critical('TaskID:%s::Client:%s::no suitable connector found - returning' % (self.task.id, self.task.client_name))
                
            return

        except:
            print traceback.format_exc()
            log.exception('TaskID:%s::Client:%s::generic connector processTask failed' % (self.task.id, self.task.client_name))
            log.critical('TaskID:%s::Client:%s::generic connector processTask failed' % (self.task.id, self.task.client_name))
            return
