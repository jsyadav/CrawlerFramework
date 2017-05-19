
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
#prerna
import logging
import os
import csv
import md5
import copy
import urlparse
import smtplib
import traceback
from tgimport import tg

from datetime import datetime
from email import Encoders
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from baseconnector import BaseConnector
from utils.decorators import logit
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo
from utils.utils import cleanUnicode


log = logging.getLogger("FilesystemConnector")

class FilesystemConnector(BaseConnector):

    """
    Dummy connector that tries to fetch from the local filesystem.
    Current uri in this case should be a directory with the files (in csv)
    Each of the files should have headers
    """
 

    @logit(log, 'fetch')
    def fetch(self):
        self.__exceptions_list = []
        #self.error_count = 0
        self.genre = 'review'
        source_directory = urlparse.urlparse(self.currenturi).path
        log.info(source_directory)
        try:
            files = []
            if source_directory.endswith(".csv"):
                files = [source_directory]
            else:
                # convert into absolute paths
                files = [os.path.join(source_directory, _f) for _f in os.listdir(source_directory)
                         if _f.endswith(".csv")]
        
            hash = md5.md5(source_directory).hexdigest()
            result = updateSessionInfo(self.genre, self.session_info_out,
                                       self.currenturi, hash, 'Post',
                                       self.task.instance_data.get('update'), parent_list=[source_directory], Id=self.task.id)
            # Parent-child mapping
            # Python dictionary of unique_id -> post_hash
            # This is used in maintaining the parent-child relationship in the posts. For example,
            # if we have a dump of forum data in CSV, the relationship between the threads are maintained
            # using this dictionary.
            # Each post in the CSV should have a unique_id field
            # The child posts must have a parent_id field which points to this unique_id
            # For each iteration, if we have a post with a parent_id, that id is looked up in this dictionary
            # and its hash is added to the parent path, thereby uniquely identifying the parent.
            # Having it here allows to keep parent and child in different files (but within the same directory)
            parent_hashes = {}

            for f in files:
                log.info("Processing %s" % f)
                self.process(f, parent_hashes)
            self.sendEmail(source_directory)
            if self.__exceptions_list:
                log.info(self.log_msg('Since Exception occured, Reverting Session info and self.pages'))
                self.pages = []
                self.session_info_out = copy.copy(self.task.session_info)
            return True
        except:
            self.task.status['fetch_status'] = False
            log.exception(self.log_msg('Exception in fetch: ' + traceback.format_exc()))
        
        return True
    
    def sendEmail(self,source_directory):
        subject = " Report of filesystem connector"
        from_addr = 'services@serendio.com'
        to_addr = [address for address in tg.config.get(path='Connector', key=\
                                            'email_address').split(",")]#['prerna@serendio.com'] 
        smtp_host = 'smtp.gmail.com'
        smtp_port = 587
        smtp_username = 'services@serendio.com'
        smtp_pw = 'kmsystemmailer'
        server = smtplib.SMTP(smtp_host,smtp_port)
        server.ehlo()
        server.starttls()
        server.ehlo()
        msg = MIMEMultipart()
        if self.__exceptions_list:
            body = 'Report of filesystem connector:' + '\n\n' + 'File Name:' +' '+ source_directory +'\n\n'+ \
                    'No of rows processed successfully : %d' %(len(self.pages)) + '\n\n'+ \
                    'No of rows failed : %d' %(self.error_count) + '\n\n' + \
                    'Exceptions list :'+'\n\n' + '\n'.join(self.__exceptions_list) + '\n\n\n\n' + 'Regards,' + \
                    '\n' + 'Serendio Services Team'
        else:
            body = 'Report of filesystem connector:' + '\n\n' + 'File Name:' +' '+ source_directory +'\n\n'+ \
                    'No of rows Processed successfully : %d' %(len(self.pages)) + '\n\n'+ \
                    'No of rows Failed : %d' %(self.error_count) + '\n\n' + \
                    'There is no Exception' + '\n\n\n\n' + 'Regards,' + \
                    '\n' + 'Serendio Services Team' 
        msg.attach(MIMEText (body))
        msg['Subject'] = subject
        msg['From'] = from_addr
        msg['To'] = ','.join(to_addr)
        server.login(smtp_username, smtp_pw)
        print msg
        response = server.sendmail(from_addr, to_addr, msg.as_string())
        server.quit()
    
    @logit(log, 'process')
    def process(self, source, parent_hashes):
        # csv makes sense it allows to have newlines within the cell.
        # FIXME: Handle different file formats?
        reader = csv.DictReader(open(source, "r"))
        has_title = False

        normalize = lambda x: x.lower().replace(" ", "_").strip()
        headers = [normalize(x) for x in reader.fieldnames if x.strip()]
        self.error_count = 0

        log.info("Got the following headers: %s" % reader.fieldnames)
        # Do some sanity check
        if 'data' not in headers:
            self.log_msg("Required header 'data' not present. The headers are %s" % headers)
            return
        if 'title' in headers:
            has_title = True

        for row_num, row in enumerate(reader):
            try:
                page = {}
                page['source_type'] = 'review'
                page['uri'] = source
                
                row = dict([(normalize(k), v) for k,v in row.items()])
                # Headers are extracted entities keys
                unique_id = row.pop('unique_id', "").strip()
                parent_id = row.pop('parent_id', "").strip()
                log.debug("%s: Unique ID, %s Parent ID", unique_id, parent_id)
                for h in headers:
                    if h in ['unique_id', 'parent_id']:
                        continue
                    if h in ['data', 'title', 'uri', 'posted_date', 'source_type', 'source',
                             'entity']:
                        page[h] = cleanUnicode(row[h].strip())
                    else:
                        # By default assume all extracted entities are strings
                        # FIXME: Handle other types of data. For now only et_* is supported
                        #value = self.isnumber(row[i])
                        #if value:
                        #    ee = 'ef_%s' % h
                        #    row[i] = value
                        #else:
                        ee = 'et_%s' % h
                        if h == 'author': # <--- WTF?!
                            ee = 'et_author_name'
                        log.info("%s:%s" %(ee, row[h]))
                        page[ee] = cleanUnicode(row[h].strip())

                if not has_title:        
                    page['title'] = page['data'][0:20] +  "..."
                if not page['data']: # Data cannot be an empty string
                    page['data'] = page['title']
                    
                post_hash = md5.md5(str(page)).hexdigest()
                if unique_id:
                    parent_hashes[unique_id] = post_hash

                if checkSessionInfo(self.genre, self.session_info_out, post_hash, \
                                        self.task.instance_data.get('update')):
                    log.info(self.log_msg('sessioninfo return True. Skipping the post with title %s' % page['title']))
                    continue
                result = updateSessionInfo(self.genre, self.session_info_out,
                                       post_hash, hash, 'Post',
                                       self.task.instance_data.get('update'))
                if result['updated']:                    
                    log.info("Keys: %s" %(','.join(page.keys())))
                    page['path'] = [source] # Flat absolute path to the file. And unique_id identifying the post if the field exists
                    if unique_id:
                        page['path'].append(unique_id)
                    
                    # Check if there is a parent for this post and add parent_path accordingly
                    page['parent_path'] = [os.path.split(source)[0]]
                    if parent_id and parent_hashes.has_key(parent_id):
                        page['parent_path'].append(parent_id)
                        
                    page['priority'] = self.task.priority
                    page['level'] = self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                
                    page['posted_date'] = datetime.strftime(datetime.strptime(page['posted_date'],"%d-%b-%Y"),
                                                            "%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['task_log_id'] = self.task.id
                    page['category'] = self.task.instance_data.get('category','')
                    page['uri_domain'] = urlparse.urlparse(page['uri'])[1]

                    if 'source' not in page:
                        page['source'] = urlparse.urlparse(page.get('uri', self.currenturi))[1]
                    page['source_type'] = page['source_type'].lower().strip()                    
                    #self.updateParentExtractedEntities(page)
                    log.info(":".join([str(row_num), page['title'], page['uri'], page['source'], page['posted_date']]))
                    print "********"
                    if not self.validate_page(page):
                        raise Exception("Page at row_no:%d couldn't be validated!" %row_num)
                    self.pages.append(page)
            except Exception, e:
                self.error_count += 1
                log.info("Reason %s" %traceback.format_exc())
                log.exception(self.log_msg("Exception at %d" %row_num))
                self.__exceptions_list.append(traceback.format_exc())
                #self.pages = {}
                continue # onto next page
        
        # WARNING: hack.
        # Delete the following information from instance_data.
        # This forces the baseconnector to set source and source_type at the Page level.
        # Otherwise, we would have the last source, source_type in the CSV for all the rows.
        # In short, this little hack allows us to mix difference source, source_types in the
        # same CSV file without problems.
        # See #641 for more
        self.task.instance_data.pop('source', '')
        self.task.instance_data.pop('source_type', '')
        
        log.info("Processed %d posts successfully: %d posts Failed" %(len(self.pages), self.error_count))
        #return exception_list
        return True


    def validate_page(self, page):
        if not page['uri'] or not (page['data'] or page['title']):
            return False
        if (page['data'] or page['title']) and not (page['data'] and page['title']):
            #XOR
            if not page['data']:
                page['data'] = page['title']
            elif not page['title']:
                page['title'] = page['data'][:300]
        return True

    
    def isnumber(self, text):
        """Check if a given string is a float, return False on failure.
        float(text) on success
        """
        try:
            value = float(text)
        except ValueError:
            value = False
        return value
