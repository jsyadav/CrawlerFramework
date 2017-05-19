
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''


import os
import re
import md5
import tempfile
import logging
import datetime
import urllib2

from jpype import *

from utils.urlnorm import normalize
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from utils.textimporter import TextImporter

from baseconnector import BaseConnector

log = logging.getLogger('CustomDataConnector')

class CustomDataConnector(BaseConnector):
    """CustomDataConnector - to fetch and process custom data like uploads, bookmarklets, etc
    """

    def __create_tmp_file(self, data):
        """Create a temp file and store the data, returning back the filename
        """
        # get extension
        ext = os.path.splitext(self.currenturi)[1]
        # Create a tmp file and save it for processing
        f = tempfile.NamedTemporaryFile()
        fname = f.name
        f.close()
        f = open(fname + ext,'wb')
        f.write(data)
        f.close()
        return fname + ext

    def __convert_text(self, fname):
        """Given a filename, use diskoverer's TextImport to extract the text from the file
        """
        if not isJVMStarted():
            startJVM(getDefaultJVMPath(),"-ea",'-Djava.ext.dirs=/home/cnu/project/VoomNew/New/diskoverer/jars/diskoverer.jar:/home/cnu/project/VoomNew/New/diskoverer/jars/:.') # move to config file
        ti = TextImporter()
        log.debug("File name %s " %fname)
        text = ti.processFile(fname)
        log.debug("Text length %s" %(len(text)))
        #shutdownJVM()  # There is some bug with jpype as shutting down the jvm will cause problem when restarting it
        if text:
            return text
        
    @logit(log,'fetch')    
    def fetch(self):
        if not self.rawpage:            # Have to fetch the page from scratch
            log.info('Opening URL %s' %(self.currenturi))
            r = urllib2.urlopen(self.currenturi) # self.task.instance_data['uri']
            data = r.read()
        else:
            data = self.rawpage         # Get the data from the already fetched page
        fname = self.__create_tmp_file(data)
        text = self.__convert_text(fname)
        if not text: # unknown file type or some error in converting to text
            log.error("unknown file type or some error in converting to text")
            return False
        log.info(self.task.instance_data)
        self.genre = "generic"
        page = {}
        page['data'] = text
        page['title'] = self.task.instance_data.get('title','')

        page['path'] = [self.currenturi]
        page['parent_path'] = []
        page['uri'] = normalize( self.currenturi )
        page['uri_domain'] = unicode(urllib2.urlparse.urlparse(page['uri'])[1])
        page['priority'] = self.task.priority
        page['level'] = self.task.level
        page['pickup_date'] = datetime.datetime.strftime(datetime.datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        page['posted_date'] = datetime.datetime.strftime(datetime.datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
        page['connector_instance_log_id'] = self.task.connector_instance_log_id
        page['connector_instance_id'] = self.task.connector_instance_id
        page['workspace_id'] = self.task.workspace_id
        page['client_id'] = self.task.client_id
        page['client_name'] = self.task.client_name
        page['last_updated_time'] = page['pickup_date']
        page['versioned'] = False
        page['task_log_id'] = self.task.id
        page['category'] = self.task.instance_data.get('category','')
        page['entity'] = 'post'
        page['orig_data'] = data    # the entire file in binary format
        self.pages.append(page)
        return True


