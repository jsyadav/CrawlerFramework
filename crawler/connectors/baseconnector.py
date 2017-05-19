
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#VAIDHY
#JV
#ASHISH YADAV

## JV (2009-02-19)
####The doc strings are generations old and obsolete - needs update####

import time
import traceback
import urllib2
import urllib
from urllib2 import *
import urlparse
import re
import cgi
import pickle
from datetime import datetime
import json
import copy
from xml.sax import saxutils
import xml.etree.cElementTree as ET
import StringIO
import gzip
import md5

from extractors.extractor import Extractor
from tgimport import config
from database import session
import model

from keywordfilter import KeywordFilter
from utils.httpconnection import HTTPConnection
from utils.utils import cleanUnicode,email_exception,to_bool
from utils.urlnorm import normalize
from utils.checksandactions import Checks, Actions
from utils.authlib.apilib import NoActiveHandlersAvailable, ObjectPool
from utils.decorators import *
from utils import taskmanager

from xml.etree.ElementTree import Element, SubElement, Comment, tostring

from BeautifulSoup import BeautifulSoup

import logging
log = logging.getLogger('BaseConnector')

HDFS_THRIFT_HOST = config.get(section='HDFSThriftServer', option='hdfs_thrift_url')
HDFS_THRIFT_PORT = config.get(section='HDFSThriftServer', option='hdfs_thrift_port')


class BaseConnector():
    valid_mime_types=['text/plain','text/html','text/csv','text/xml','text/richtext', 'text/rtf',
                      'application/atom+xml', 'application/msword','application/pdf',
                      'application/rtf', 'application/xhtml+xml', 'application/xml',
                      'application/vnd.ms-excel', 'application/vnd.ms-powerpoint',
                      'application/rss+xml','application/rdf+xml']

    use_memcache = to_bool(config.get(section="CrawlNode", option='use_memcache'))
    if use_memcache:
        from ..utils.cache import MemCache
        client_uri = str(config.get(section='Cache', option='uri'))
        client_port = str(config.get(section='Cache', option='port'))
        timeout = config.get(section='Cache', option='timeout') or 21600 #6 hours
        keep_uncompressed = to_bool(config.get(section='Cache', option='keep_uncompressed'))
        cacheObj = MemCache(client_uri, client_port, timeout, keep_uncompressed)

    @logit(log, '__init__')
    def __init__(self, task, rawpage=None, parser=None):
        """
        initialize the connector instance essentials
        """
        self.task=task
        self.to_requeue = False
        self.objectpool = ObjectPool() #Initialize the pool
        log.info('TaskID:%s::Client:%s::Initializing connector::URI:%s' % (self.task.id, self.task.client_name, self.task.instance_data['uri']))
        log.debug('TaskID:%s::Client:%s::Initializing connector::URI:%s' % (self.task.id, self.task.client_name, self.task.instance_data['uri']))
        self.linksOut=[]
        self.currenturi=self.task.instance_data['uri']
        self.rawpage=rawpage #when it comes from the generic connector - else None
        self.parser=parser
        self.max_recursion_level=config.get(section='MaxRecursion', option='max_recursion')
        self.numofpages=1
        self.level = self.task.level
        self.pages=[]
        self.old_uris=set()
        self.related_uris=[]
        self.task.start_time = datetime.utcnow()
        self.session_info_in=copy.copy(self.task.session_info)
        self.session_info_out=copy.copy(self.task.session_info)
        self.misc_status_dict={}
        self.exception_context_q = []
        self.content_fetched = 0
        self.times_fetched = 0
        self.parent_extracted_entites = {}
        self.kol = []
        self.prefixes = {'et_':'text', 'es_':'text', 's_':'text', 'edate_':'date', 'ei_':'integer', 'ef_':'float'}

        #in case when the metapage is fetched - it should not report failure
        #fetch in this case returns a False and set the reportFailure to False
        #self.mimeType = mimeType
        log.debug('TaskID:%s::Client:%s::Connector Initialized::URI:%s' % (self.task.id, self.task.client_name, self.task.instance_data['uri']))


    @logit(log, 'processTask')
    def processTask(self):
        """
        """
        log.info('TaskID:%s::Client:%s::start processTask for uri: %s' % (self.task.id, self.task.client_name, self.task.instance_data['uri']))
        try:
            try:
                log.debug("TaskID:%s::Client:%s::trying to fetch" % (self.task.id, self.task.client_name))
                self.task.status['fetch_status'] = self.status = self.fetch()
                log.debug("TaskID:%s::Client:%s::Fetch Status:%s" % (self.task.id, self.task.client_name, self.status))
                if self.linksOut: # will be true only if fetch added something in it
                    log.debug('TaskID:%s::Client:%s::calling sendBackLinks linksOut present:%d' % (self.task.id, self.task.client_name, len(self.linksOut)))
                    self.sendBackLinks()

            except NoActiveHandlersAvailable:
                #This exception seems to be raised by a API connectors, 
                #which got rate-limited and hence the task is aborted, Right now I am just terminating the task
                #It needs better handling, maybe reenqueing x times before terminating.
                self.status = False
                
            if self.status:
                log.debug('TaskID:%s::Client:%s::trying to filter' % (self.task.id, self.task.client_name))
                self.task.status['filter_status'] = self.status = self.filter()
            log.debug("TaskID:%s::Client:%s::Filter Status:%s" % (self.task.id, self.task.client_name, self.status))

            if self.status:
                log.debug('TaskID:%s::Client:%s::trying to extract' % (self.task.id, self.task.client_name))
                self.task.status['extract_status'] = self.status = self.extract()
            log.debug('TaskID:%s::Client:%s::extract status: %s' % (self.task.id, self.task.client_name, self.task.status['extract_status']))

            if not self.status:
                #if any of the above 3 fails the session info should not change
                #for eg. if fetch was true and extract was false - db will get new session info and since extract failed there are no saved things
                #and if i let the session info be edited - the next time many things(posts) will not be picked up
                log.info("TaskID:%s::Client:%s::'status' is False - reverting back to old session info" % (self.task.id, self.task.client_name))
                self.session_info_out=self.session_info_in

            log.debug('TaskID:%s::Client:%s::trying to save' % (self.task.id, self.task.client_name))
            self.status = self.save()
            if not self.status:
                log.critical('TaskID:%s::Client:%s::save failed')
                raise
            #self.email_exception(email=True)
            
            if self.to_requeue:
                # Default this will be False, individual connectors can override it
                self.requeue()
                log.info('TaskID:%s::Client:%s::requeuing task for uri: %s' % (self.task.id, self.task.client_name, self.task.instance_data['uri']))

            log.info('TaskID:%s::Client:%s::finish processTask for uri: %s' % (self.task.id, self.task.client_name, self.task.instance_data['uri']))
            
        except Exception,e:
            log.exception('TaskID:%s::Client:%s::processTask failed for uri: %s' % (self.task.id, self.task.client_name, self.task.instance_data['uri']))
            #raise e

    def requeue(self):
        """Add the task back into the queue

        Some cases, the task might need to be added back into the queue. 
        Check for the attribute self.to_requeue and call this function
        By default this attribute will be False and individual connectors can choose to enable it.
        
        Note: Change the url of the connector instance before exiting out of fetch() 
        Else taskmaster will reject it because its a duplicate task.
        """
        log.info('Requeueing the task')
        taskmanager.crawlNow(connector_instance_ids =[self.task.connector_instance_id],
                             workspace_id=self.task.workspace_id)




    def run(self,task_identifier):
        log.info('calling processTask Function')
        taskmanager.bindModelToDb(dburi = self.task.dburi)
        self.processTask()
        taskmanager.removeTask(task_identifier) #We are done with the task now remove it.
        return (self.task.priority, self.task.task_identifier)

    @logit(log, 'fetch')
    def fetch(self):
        """
        has: taskId, URL, instanceData, level
        needs: httpconnection, utils (optional junk data removal, don't do if the junkFreeData=''
        provides: Article - title and body, fetchStatus, fetchMsg(code,...)

        overriden in every connector
        what does fetch do?
        fetch and parse the page, removeJunkData, read linkouts, increase level,
            return the task object with new attributes - article: title and body.

        ##dummy implementation
        ##try:
        ##    .....implementation.....
        ##    update self.task.fetch_status=True
        ##    return True
        ##except:
        ##    log.debug exception
        ##    return False

        """
        log.debug('TaskID:%s::Client:%s::baseconnector fetch' % (self.task.id, self.task.client_name))
        return True

    @logit(log, 'filter')
    def filter(self):
        """
        OPTIONAL

        has: self.task.Keyword, self.article['title'], self.article['body']
        needs: keywordFilter
        provides: set of matching keywords, filterStatus

        gets the task object
        uses attributes: keywords, title, body.
        makes an object of keywordFilter
        calls the run method on the keywordFilter object
        returns back the keyword set

        ##dummy implementation
        ##try:
        ##    ##if not self.task.apply_filters:
        ##    ##    return True
        ##    keyword_searcher=KeywordFilter()
        ##    keyword_search=keyword_searcher.checkfilter(self.data, self.task.keywords)
        ##    gets a list of keywords found or empty if no keyword match
        ##    ....implementation....
        ##    update self.task.filter_status=True
        ##    return True
        ##except:
        ##    log.debug exception
        ##    return False
        ##dummy implementation

        """
        try:
            exclusion_keywords = [kwd_obj.keyword for kwd_obj in self.task.keywords \
                                  if kwd_obj.exclude==True]
            filter_word_list = []
            spam_word_list = []
            log.info('i m in filter')
            if self.task.instance_data.get('apply_keywords'):
                filter_word_list.extend([word.keyword for word in \
                                         self.task.keywords if word.filter==True])

            if self.task.instance_data.get('instance_filter_words'):
                filter_word_list.extend([each.strip() for each in \
                                             self.task.instance_data['instance_filter_words'].split(",")])
            log.info(filter_word_list)                                
            
            if self.task.instance_data.get('spam_filter_words'):
                spam_word_list.extend([each.strip() for each in \
                                             self.task.instance_data['spam_filter_words'].split(",")])
            if not filter_word_list and not spam_word_list:
                log.debug(self.log_msg("No keywords to apply for url: %s"%(self.task.instance_data['uri'])))
                self.task.status['filter_status']=True
                return True
                                                    
            #log.info(filter_word_list)
            if filter_word_list:                                
                keyword_searcher=KeywordFilter()
                all_pages = copy.copy(self.pages)
                log.debug(len(self.pages))
                for page in all_pages:
                    
                    #if re.search('#',page):
                     #   page = re.sub('#','hash',page)
                    try:
                        data = page['data'].replace("'","").replace('"','')
                        #log.info(data)
                        # Any exclusion keyword present in the post means immediate removal of the post
                        excluded_keywords = keyword_searcher.checkFilter(cleanUnicode(data).encode('utf-8','ignore'), 
                                                                         [unicode(word.encode('utf-8','ignore'),'utf-8') for word \
                                                                          in exclusion_keywords])
                        log.info(self.log_msg("excluded keywords == %s"%(','.join(excluded_keywords))))
                        if excluded_keywords:
                            log.debug(self.log_msg("Excluded words: %s" %(','.join(excluded_keywords))))
                            self.pages.remove(page)
                            continue
                        
                        matched_keywords=keyword_searcher.checkFilter(cleanUnicode(data).encode('utf-8','ignore'), 
                                                                      [unicode(each.encode('utf-8','ignore'),'utf-8') for each \
                                                                           in filter_word_list] )
                        log.info(self.log_msg("Matched keywords == %s"%(','.join(matched_keywords))))
                        if not matched_keywords:
                            log.info(self.log_msg("Page data does not contain filter keywords, ignoring page"))
                            self.pages.remove(page)
                            log.info('page removed')
                    except:
                        print "error occured while checking for filter in one post"
                        log.exception(self.log_msg("Error occured while checking for filter in once post, iterating to the next post"))
                        log.debug(self.log_msg(traceback.format_exc()))
                        continue
                log.info(len(self.pages))    
            if spam_word_list:
                all_pages = copy.copy(self.pages)
                log.debug(len(self.pages))
                for page in all_pages:
                    try:
                        data = cleanUnicode(page['data'].replace("'","").replace('"',''))
                        #data = cleanUnicode(temp_data).encode('utf-8','ignore')
                        #data = cleanUnicode(temp_data)
##                        spam_words = [unicode(each.encode('utf-8','ignore'),'utf-8') for each \
##                                                                               in spam_word_list]
                        spam_words = [unicode(each) for each in spam_word_list]
                        log.info(spam_words)
                        for each in spam_words:
                            match_spam = re.search(r"[^A-Za-z0-9_](%s)[^A-Za-z0-9_]?" %re.escape(each), data, re.DOTALL and re.IGNORECASE)
                            #match_spam = re.search(r'\b%s\b'%re.escape(each),data,re.DOTALL and re.IGNORECASE)
                            log.info(match_spam)    
                            if match_spam:    
                                log.info(self.log_msg("Page data contain spam filter keywords, ignoring page"))
                                self.pages.remove(page)
                                log.info('page removed')
                    except:
                        print "error occurec while checking for spam filter"
                        log.exception(self.log_msg("Error occured while checking for spam filter in post, iterating to the next post"))
                        #log.debug(self.log_msg(traceback.format_exc()))
                        continue            
                            
                #log.debug(self.log_msg("No spam keywords to apply for url: %s"%(self.task.instance_data['uri'])))
                #self.task.status['filter_status']=True
                #return True    
            self.task.status['filter_status']=True
            return True
        except:
            log.exception(self.log_msg(traceback.format_exc()))
            self.task.status['filter_status']=False
            return False

    @logit(log, 'extract')
    def extract(self):
        """
        has: title, body, rawPage(to extract html fields)
        needs: -
        provides: dict of lists - {'extracted_field':[values]}, extractStatus

        ##dummy implementation
        ##try:
        ##    .....implementation.....
        ##
        ##    extraction1: pass the self.title+self.orig_content thru stanford NER and
        ##    extraction2: pass it thru specific extractors like email, phone
        ##    extraction3: use the self.rawpage to do page specific html extractions
        ##
        ##    update self.task.extract_status=True
        ##
        ##    return True
        ##except:
        ##    log.debug exception
        ##    return False

        source,source_type can be from, as per priority
        1)task.page_data['source'], task.page_data['source_type'] # from rssconnector , source shld be feed's url_domain.
        1)task.instance_data['source'],task.instance_data['source_type'] # url_segment for the connectors
        2)if any of the above keys are not present , put it as url_segment from the current uri.
        sample_metadata_structure
        <AllMetaData>
           <Author>
              <Real_name>
                  <item>Tim Bryan</item>
              </Real_name>
           </Author>
        </AllMetaData>

        """
        # TODO: refactor
        try:
            #keyword_extraction - We are not checking apply keywords here as tag-keyword association 
            #depends on it. So we always run this extractor, irrespective of whether the user has asked to apply_keywords for a 
            # connector instance or not, to be able to associate the posts properly
            try:
                keyword_words = [cleanUnicode(word.keyword) for word in self.task.keywords]
                if keyword_words:
                    for page in self.pages:
                        data = page['data'].replace("'","").replace('"','')
                        extractor=Extractor(cleanUnicode(data).encode('utf-8','ignore'),page['workspace_id'])
                        page['keywords']= extractor.extract_keyword(keyword_words)
                    self.misc_status_dict['keyword_extraction']='True'
            except:
                print "error in keyword extraction"
                log.exception(self.log_msg('Error in keyword extraction'))
                self.misc_status_dict['keyword_extraction']='False'
            try:
                for page in self.pages:
                    ##NER Extraction
                    try:
                        if self.task.instance_data.get('ner_extraction'):
                            extractor=Extractor(page['data'],page['workspace_id'])
                            page.update(extractor.extract_ner())
                            self.misc_status_dict['ner_extraction']='True'
                        else:
                            self.misc_status_dict['ner_extraction']='NA'
                            log.info('Ner Extraction not needed')
                    except:
                        print "exception in ner_extraction"
                        self.misc_status_dict['ner_extraction']='False'
                        log.exception(self.log_msg('exception in ner_extration'))
                        self.email_exception(self.log_msg(traceback.format_exc()),interval = 600)
                        return False
            except:
                print "error in ner extraction"
                log.exception(self.log_msg('Error in NER extraction'))
                self.misc_status_dict['ner_extraction']='False'
                return False

            try:
                if self.task.instance_data.get('nlp_extraction'):
                    ##NLP Extraction
                    for page_count,page in enumerate(self.pages):
                        try:
                            extractor=Extractor(page['data'],page['workspace_id'])
                            page['nlp_payload'] = extractor.extract_nlp()
                            log.debug('Nlp Extraction Done for page number %d'%page_count)
                            self.misc_status_dict['nlp_extraction']='True'
                        except:
                            print "exception in nlp extraction"
                            self.misc_status_dict['nlp_extraction']='False'
                            log.exception(self.log_msg('exception in nlp_extration'))
                            self.email_exception(self.log_msg(traceback.format_exc()),interval = 600)
                            return False
                else:
                    self.misc_status_dict['nlp_extraction']='NA'
                    log.info('Nlp Extraction not needed')
            except:
                print "error in nlp extraction"
                log.exception(self.log_msg('Error in Nlp extraction'))
                self.misc_status_dict['nlp_extraction']='False'
                return False

            # code for sentiment extraction goes here
            try:
                if self.task.instance_data.get('sentiment_extraction'):
#                    self.sentiments = []
                    host = self.task.instance_data['voom_url']
                    for page in self.pages:
                        try:
                            senti_doc={}
                            senti_doc['date']=page['posted_date']
                            senti_doc['title']=cleanUnicode(page['title']) #cleans up data from junk characters like ['\x7f-\xff']
                            if self.task.pagedata.get('source'):
                                senti_doc['source'] = self.task.pagedata['source']
                            else:
                                senti_doc['source']=self.task.instance_data.get('source',urlparse.urlparse(page['uri'])[1])
                            if self.task.pagedata.get('source_type'):
                                senti_doc['source_type']=self.task.pagedata['source_type']
                            else:
                                senti_doc['source_type']=self.task.instance_data.get('source_type','others')
#                            senti_doc['source']=self.task.instance_data.get('source',urlparse.urlparse(page['uri'])[1])
#                            senti_doc['source_type']=self.task.instance_data.get('source_type','generic')
                            senti_doc['entity'] = page['entity']
                            senti_doc['url']=cleanUnicode(page['uri'])
                            senti_doc['product_id']=self.task.instance_data['product_id']
                            senti_doc['orig_content']=cleanUnicode(page['data'])
                            senti_doc['posted_date'] = page['posted_date']
                            #senti_doc['solr_id'] = page['id']
                            senti_doc['connector_instance_id'] = self.task.connector_instance_id
                            root = ET.Element("AllMetaData")
                            for key,value in page.iteritems():
                                try:
                                    if re.search('^et_.*|^ei_.*|^edate_.*|^ef_*.',key): #say key is as et_author_real_name
                                        key = re.sub('^et_|^ei_|^edate_|^ef_','',key)
                                        entity,property = key.split('_',1)  #entity = author , property = real_name
                                        entity = saxutils.escape(entity.replace(' ','_'))
                                        entity_node = None
                                        for children in root.getchildren(): #search for author child node in allmetadata node .
                                            if children.tag == entity:
                                                entity_node = children
                                        if not entity_node:
                                            entity_node = ET.SubElement(root,entity) #create author node , if not already present
                                        property = saxutils.escape(property.replace(' ' ,'_'))
                                        property_node = ET.SubElement(entity_node,property) #create real_name node
                                        value = [value] if not isinstance(value,list) else value
                                        for each in value:
                                            try:
                                                item = ET.SubElement(property_node,'item')
                                                item.text = cleanUnicode(saxutils.escape(str(each)))#if it's a multivalued item,
                                                                                                    #put it as <item>*</item>
                                            except:
                                                print "exception while iterating over one of extracted attr"
                                                log.info(self.log_msg('exception while iterating over one of the extracted* attributes'))
                                except Exception, e:
                                    print "exception occurred while iterating over page.keys() \n" + traceback.format_exc()
                                    log.exception(self.log_msg('Exception occured while iterating over page.keys(): ' + traceback.format_exc()))
                            senti_doc['metadata'] = ET.tostring(root, encoding='UTF-8')
                            req = Request(host+'/data/extract',urllib.urlencode({'data':json.dumps(senti_doc)}))
                            sentiment = urlopen(req).read()
                            page['ignore_sentiment']=  json.loads(sentiment)
#                            self.sentiments.append(json.loads(sentiment))
                        except:
                            print "exception occurred while iterating over pages, returning false"
                            #self.email_exception(self.log_msg(traceback.format_exc()),interval = 600)
                            log.exception(self.log_msg('Exception occured while iterating over pages , returning false'))
                            self.misc_status_dict['sentiment_extraction']='False'
                            return False
                    self.misc_status_dict['sentiment_extraction'] = 'True'
                else:
                    self.misc_status_dict['sentiment_extraction'] = 'NA'
            except:
                print "error in sentiment extraction"
                log.exception(self.log_msg('Error in sentiment extraction'))
                self.misc_status_dict['sentiment_extraction']='False'
                return False
            self.task.status['extract_status'] = True
            return True
        except:
            print "error in normal extraction"
            log.exception(self.log_msg('Error in normal extraction'))
            self.task.status['extract_status']=False
            return False


    @logit(log, 'save')
    def save(self):
        """
        HAS SOLE IMPLEMENTATION IN BASE CONNECTOR
        has: title, body, url, keyword set, extracted fields dict, connectorData(category - used for categorizing articles)
        needs: pysolr
        products: saveStatus

        ##dummy implementation
        ##try:
        ##    .....implementation.....
        ##    if updateDB:
        ##        try:
        ##            ....updateSolr....implementation...
        ##        except:
        ##            ....deleteFromDB...implementation...
        ##
        ##    update self.task.save_status=True
        ##    return True
        ##except:
        ##    log.debug exception
        ##    return False

        #TAKEN FROM TASK MASTER
        #def taskComplete(self, task):

        -is called by each crawler process when its done with the task
        -a task object with data attribute containing the article/doc and
         status=True comes back -> over ride the corresponding log db entry

        __doc__
        takes in the task uri and the related uris and load them onto the old urli's set
        creates a task log entry in db and 0 or more entries in the related uris table
        send back succces=True/Flase based, task_log_id (if True)
        """
        log.debug("TaskID:%s::Client:%s::in baseconnector save" % (self.task.id, self.task.client_name))
        try:

#            if self.status:# and not self.report_failure:

#             ####SAVING TO SOLR
#             try:
#                 savetosolr_status='False'
#                 if self.task.instance_data.get('save_to_solr'):
#                     if self.status:
#                         log.debug("TaskID:%s::Client:%s::calling saveToSolr" % (self.task.id, self.task.client_name))
#                         savetosolr_status=self.saveToSolr()
#                         log.debug("TaskID:%s::Client:%s::returned saveToSolr" % (self.task.id, self.task.client_name))
#                         savetosolr_status='True'
#                 else:
#                     savetosolr_status='NA'
#                     log.debug("TaskID:%s::Client:%s::save on solr engine is NA")
#             except:
#                 savetosolr_status='False'
#                 log.exception("TaskID:%s::Client:%s::saveToSolr failed" % (self.task.id, self.task.client_name))
#             ####

            ####SAVING TO DB
            try:
                log.debug("TaskID:%s::Client:%s::calling saveToDB" % (self.task.id, self.task.client_name))
                self.saveToDB()
                log.debug("TaskID:%s::Client:%s::returned from  saveToDB" % (self.task.id, self.task.client_name))
            except:
                print "error while saving pages to db"
                log.info("Error while saving pages to DB")
                log.info(self.pages)
                log.info("instance_data = " + str(self.task.instance_data))
#                log.info(pagedataself.task.instance_data
                log.exception("TaskID:%s::Client:%s::saveToDB failed" % (self.task.id, self.task.client_name))
                log.critical("TaskID:%s::Client:%s::saveToDB failed" % (self.task.id, self.task.client_name))
                raise
            ####


            ### FOR NLP EXTRACTOR SAVE

            try:
                nlp_save_status = 'False'
                if self.task.instance_data.get('nlp_extraction'):
                    if self.status:
                        for page_count, page in enumerate(self.pages):
                            log.debug(self.log_msg('Saving the nlp extracted entities for page %d'%(page_count)))
                            try:
                                nlp_data = page['nlp_payload']
                                for key in nlp_data:
                                    ##Key is the extracted_entity_name:
                                     datatype = 'text'
                                     extracted_entity_name = session.query(model.ExtractedEntityName).filter_by(name=key, data_type = datatype).first()
                                     if not extracted_entity_name:
                                         extracted_entity_name = model.ExtractedEntityName(name=key, data_type=datatype)
                                         session.save_or_update(extracted_entity_name)
                                         session.flush()
                                     values = nlp_data[key]
                                     for value,mention in values:
                                         extracted_entity_value = model.ExtractedEntityValue()
                                         extracted_entity_value.post_id = page['id']
                                         extracted_entity_value.value = value
                                         extracted_entity_value.mentions = mention
                                         extracted_entity_value.extracted_entity_name_id= extracted_entity_name.id
                                         session.save_or_update(extracted_entity_value)
                                         session.flush()
                            except:
                                print "nlp extractor save failed for another one senti doc"
                                self.log_msg("nlp extractor save failed for another one senti doc")
                                #self.email_exception(self.log_msg(traceback.format_exc()),interval = 600)
                                log.exception(page)
                        nlp_save_status = 'True'
                    else:
                        nlp_save_status = 'NA'
            except:
                print "save on nlp extractor failed"
                log.exception("save on nlp extractor failed")
                #self.email_exception(self.log_msg(traceback.format_exc()),interval = 600)

            self.misc_status_dict['nlp_save_status']=nlp_save_status

            ####FOR SENTIMENT EXTRACTION

            try:
                sentiment_save_status='False'
                if self.task.instance_data.get('sentiment_extraction'):#key is there or not #sentiment_extraction:#is the value true/false
                    if self.status:
                        log.debug("TaskID:%s::Client:%s::calling save on sentiment engine" % (self.task.id, self.task.client_name))
                        host=self.task.instance_data['voom_url']
                        log.info(self.log_msg('sentiments saved :: %d'%(len(self.pages))))
                        for page in self.sentiment_save_pages:
                            try:
                                sentiment = page['ignore_sentiment']
                                if sentiment.get('status',1)==0:
                                    sentiment['task_log_id']=self.task.id
                                    sentiment['id'] = page['id']
                                    req=Request(host+'/data/save',urllib.urlencode({'data':json.dumps(sentiment)}))
                                    sentiment_response=urlopen(req).read()
                                    log.debug("TaskID:%s::Client:%s::saved sentiment and recieved response: %s" % \
                                                  (self.task.id, self.task.client_name,sentiment_response))
                            except:
                                print "sentiment save failed"
                                log.debug("TaskID:%s::Client:%s::sentiment save failed for another one senti doc")
                                #self.email_exception(self.log_msg(traceback.format_exc()),interval = 600)
                                log.exception(page)
                        sentiment_save_status='True'
                        log.debug("TaskID:%s::Client:%s::done with save on sentiment engine")
                else:
                    sentiment_save_status='NA'
                    log.debug("TaskID:%s::Client:%s::save on sentiment engine is NA")
            except:
                print "save on sentiment engine failed"
                log.exception("TaskID:%s::Client:%s::save on sentiment engine failed")
            ####

            #self.misc_status_dict['savetosolr_status']=savetosolr_status
            self.misc_status_dict['sentiment_save_status']=sentiment_save_status
            
            # Why are we having a big method for all the saves?!
            if self.task.instance_data.get('hdfs_save'):
                log.debug("HDFS save required")
                self.misc_status_dict['hdfs_save_status'] = self.__saveToHDFS()                          
            
            return True
        except Exception,e:
            print "save failed"
            log.exception("TaskID:%s::Client:%s::save failed" % (self.task.id, self.task.client_name))
            return False

    def __essentialFieldValidator(self, page):
        if not page['uri'] or not (page['data'] or page['title']):
            return False
        if (page['data'] or page['title']) and not (page['data'] and page['title']):
            #XOR
            if not page['data']:
                page['data'] = page['title']
            elif not page['title']:
                page['title'] = page['data'][:300]
        return True

    @logit(log, 'saveToDB')
    def saveToDB(self):
        """
        #COMMENTED OUT TO TEST SAVE
        ##i don't care about the result list from the next line beacuse the
        purpose is to update the Patricia Trie objects in old_urls
        # #which is adequately solved without getting the list

        #MOVED TO TASK MASTER AND MADE A METHOD updateOldUris,
        coz i can't access the vars of tm directly from a proxy
        ###NOT WORKING< SO COMMENTED OUT while the rest of the system works
        #[self.tm.uri_dict[self.task.workspace_id].insert(uri) for uri in self.related_uris]
        #updating the old_uris cache
        #         self.tm.uri_dict[task.workspace_id].insert(task.instance_data['uri'])
        ###

        #commented for testing html connector for mulipage article - circuitcity
        #         try:
        #             self.tm.updateOldUris(self.related_uris.append(self.task.instance_data['uri']))
        #         except Exception,e:
        #             print "updateolduris failed"
        #             print traceback.format_exc()
        #             raise e
        """
        try:
            session.begin()                

            if self.kol:
                log.debug(self.log_msg("Saving Kol entities - %d"%len(self.kol)))
                for kol_author in self.kol:
                    author = session.query(model.KOLScore).get( (kol_author['source'],
                                                      kol_author['author_name'].lower(), 
                                                      kol_author['workspace_id'] ) )
                    if not author:
                        author = model.KOLScore()

                    author.source = kol_author['source']
                    author.author_name = kol_author['author_name'].lower()
                    author.workspace_id = kol_author['workspace_id']
                    author.api_source = kol_author['api_source']
                    author.score = kol_author['score']
                    author.last_updated = kol_author['last_updated']
                    author.renew_date = kol_author['renew_date']

                    if kol_author.get('api_error'):
                        author.api_error = kol_author['api_error']
                    if kol_author.get('author_real_name'):
                        author.author_real_name = kol_author['author_real_name']
                    if kol_author.get('author_profile'):
                        author.author_profile = kol_author['author_profile'].strip()

                    session.save_or_update(author)
                session.commit()
            session.close()
        except:
            print "exception while saving KOL"
            log.exception(self.log_msg('exception while saving KOL'))
            session.rollback()
            session.close()
            
        try:
            log.debug("TaskID:%s::Client:%s::in save" % (self.task.id, self.task.client_name))
            session.begin()
            '''
            save to crawler_metrics:
            '''
            MAX_LENGTH_OF_EXTRACTED_ENTITY_VALUES = 2000 #config.get(section="connector", option='max_length_of_extracted_entity_value')
            crawler_metrics = model.CrawlerMetrics()
            crawler_metrics.connector_instance_id = self.task.connector_instance_id
            crawler_metrics.articles_crawled = len(self.pages) or 1
            crawler_metrics.articles_added = len(self.pages)
            crawler_metrics.created_date = datetime.utcnow()
            session.save(crawler_metrics)
            session.flush()
            log.debug(self.log_msg("LENGTH OF PAGES: %d. Beginning to insert pages" % len(self.pages)))
            self.sentiment_save_pages = []
            for page in self.pages:
                log.debug(self.log_msg("inserting page %d here" % self.pages.index(page)))
                if self.task.instance_data.get('update') and not self.task.instance_data.get('versioned'):
                    #case 1 : i want to update post copy in place , but do not want to keep the old versions
                    log.debug("existing copy to be updated in place")
                    post = session.query(model.Post).filter_by(\
                       connector_instance_id=page['connector_instance_id'], 
                       delete_status=False,
                       uri = page['uri'], path=page['path']
                       ).order_by(model.Post.id.desc()).first()
                    if not post: # if no previous versins found , create the first one 
                        post=model.Post()
                    version_number = 1
                elif self.task.instance_data.get('update') and self.task.instance_data.get('versioned'):
                    #case 2 : i want to keep the old versions too , and create a new post and mark that as the latest post
                    log.debug("creating a new post object and marking previous copies not latest")
                    previous_posts = session.query(model.Post).filter_by(\
                       connector_instance_id=page['connector_instance_id'],
                       delete_status=False,
                       uri = page['uri'],
                       path=page['path']
                       ).all()
                    for each in previous_posts:
                        each.is_latest = False
                        session.save_or_update(each)
                    version_number = len(previous_posts)+1
                    log.info('number of previous versions present %s'%(len(previous_posts)))
                    post=model.Post()
                elif not (self.task.instance_data.get('update') and self.task.instance_data.get('versioned')):
                    #case 3 : create a new copy , in this case no update or versioning is required
                    log.debug("creating a new post object")
                    post=model.Post()
                    version_number = 1
                    
                res=self.__essentialFieldValidator(page)
                if not res:
                    raise Exception("one of essential fields cannot be populated")
                post.workspace_id = page['workspace_id']
                post.connector_instance_id = page['connector_instance_id']
                post.level = page['level']
                post.posted_date = page['posted_date']
                post.pickup_date = page['pickup_date']
                post.last_updated_time = page['last_updated_time']
                post.version_number = version_number
                post.is_latest = True
                post.title = page['title']
                post.data = page['data']
                post.uri = page['uri']
                post.path = page['path']
                post.parent_path = page['parent_path']
                page['et_source'] = post.source = self.task.instance_data.get('source',page.get('source'))
                page['et_source_type'] = post.source_type = self.task.instance_data.get('source_type',page.get('source_type'))
                post.entity = page['entity'].lower()
                post.orig_data = page.get('orig_data')
                post.active_status = True
                post.delete_status = False
                language  = self.__getPostLanguageName()
                if language:
                    post.language_id = session.query(model.Language).filter_by(language=language).one().id

                    # if not explicitly set to false, considered as true.
                    if self.task.instance_data.get('translate') == None:
                        self.task.instance_data['translate'] = True

                    if language != 'en' and self.task.instance_data.get('translate'):
                        try:
                            post.translated_data = gtrans.translate(language, 
                                                                    'en', 
                                                                    post.data.encode('utf-8','ignore'))
                        except:
                            print "translating data failed"
                            log.exception(self.log_msg('Translating data failed.'))

                        try:
                            post.translated_title = gtrans.translate(language, 
                                                                    'en', 
                                                                    post.title.encode('utf-8','ignore'))
                        except:
                            print "translating title failed"
                            log.exception(self.log_msg('Translating title failed.'))


                ####apply rules
                checks = Checks(page)
                actions = Actions(page)
                rules = session.query(model.Rule).filter_by(workspace_id=self.task.workspace_id).all()
                for rule in rules:
                    checkname = session.query(model.Function).get(int(rule.operation_id)).name
                    checkmethod = getattr(checks, checkname)
                    checkvalue = checkmethod(rule.operation_param)
                    if checkvalue:
                        actionname = session.query(model.Function).get(int(rule.action_id)).name
                        actionmethod = getattr( actions, actionname)
                        actionmethod(rule.action_param)
                ####
                if page.get('keywords'):
                    post.keywords = [session.query(model.Keyword).filter_by(keyword=keyword,
                                                                   workspace_id=self.task.workspace_id).first()
                                     for keyword in page['keywords']]
                if page.get('tags'):
                    post.tags = [session.query(model.Tag).filter_by(workspace_id=self.task.workspace_id,
                                                          name = tag).first()
                                 for tag in page['tags']]
                post.referenced_posts = page.get('referenced_posts',[])
                
                session.save_or_update(post)
                        
                session.flush()
                log.debug("saved post : %s" % post.id)
                page['id'] = post.id
                self.sentiment_save_pages.append(page)
                #if isinstance(page['id'], (tuple,)):
                #    #self.session_info_out.transform()
                #    print "self.session_info_out, page['id']", self.session_info_out, page['id']
                #    self.session_info_out[page['id']].update({'id':post.id})
                #    self.session_info_out[page['id']+(post.id,)] = self.session_info_out[page['id']]
                #    self.session_info_out.pop(page['id'])
                ##    if self.pages.index(page) == 0:
                #        self.session_info_out[('first_version_id',)] = post.id
                f=lambda x,y: x.startswith(y)
                for k,v in self.task.instance_data.get('custom_entities',{}).iteritems():
                    if isinstance(v,int):
                        page['ei_%s'%k]=v
                    elif isinstance(v,float):
                        page['ef_%s'%k]=v
                    elif isinstance(v,str):
                        page['et_%s'%k]=v
                    elif isinstance(v,unicode):
                        page['et_%s'%k]=v

                for k,v in page.items():
                    if any([f(k, each) for each in self.prefixes.keys()]):
                        pref, name = k.split('_', 1)
                        if pref in ['et','es','s'] and (isinstance(v,str) or isinstance(v, unicode)) and len(v) > MAX_LENGTH_OF_EXTRACTED_ENTITY_VALUES:
                            log.info(self.log_msg('Extracted entity value exceeds the limit, so truncating it'))
                            v = v[:MAX_LENGTH_OF_EXTRACTED_ENTITY_VALUES]
                        datatype = self.prefixes[pref+'_']
                        extracted_entity_name = session.query(model.ExtractedEntityName).filter_by(name=name, data_type = datatype).first()
                        if not extracted_entity_name:
                            extracted_entity_name = model.ExtractedEntityName(name=name, data_type=datatype)
                            session.save_or_update(extracted_entity_name)
                            session.flush()
                        values = v
                        if isinstance(values , (str, unicode, int, float, long, datetime)):
                            values = [values]
                        for value in values:
                            extracted_entity_value = model.ExtractedEntityValue()
                            extracted_entity_value.post_id = post.id
                            extracted_entity_value.value = value
                            extracted_entity_value.extracted_entity_name_id= extracted_entity_name.id
                            session.save_or_update(extracted_entity_value)
                session.flush()
                log.debug("post flushed")
    #                    time.sleep(10)
#            except:
#                log.exception("post failed")
#                raise
#                    time.sleep(10)
            #print "here too"
#            time.sleep(10)
            session.commit()
            session.close()
            self.misc_status_dict['post_save_status']=True
        except:
            print "exception while saving posts"
            print traceback.format_exc()
            self.misc_status_dict['post_save_status']=False
            log.exception(self.log_msg('exception while saving posts'))
            session.rollback()
            session.close()

        try:
            session.begin()
            task_log=session.query(model.TaskLog).filter_by(id=self.task.id).one()
            task_log.workspace_id=self.task.workspace_id
            task_log.parent_task_log_id=self.task.pagedata['parent_task_id']#page['parent_task_id'],
            task_log.connector_instance_log_id=self.task.connector_instance_log_id
            task_log.num_of_new_posts=len(self.pages)#page['num_posts'],
            #self.session_info_out.reverseTransform()
            task_log.session_info = pickle.dumps(self.session_info_out)
            task_log.level=self.task.level
            task_log.uri=self.task.instance_data['uri']
            task_log.fetch_status=self.task.status['fetch_status']
            task_log.fetch_message=self.task.status['fetch_message']
            task_log.filter_status=self.task.status['filter_status']
            task_log.extract_status=self.task.status['extract_status']
            task_log.completion_time=datetime.utcnow()
            task_log.misc_status=unicode(json.dumps(self.misc_status_dict))
            session.save_or_update(task_log)
            log.debug("TaskID:%s::Client:%s::db entry updated with a return time." % (self.task.id, self.task.client_name))
            for related_uri in self.related_uris:
                related_uri=model.RelatedURI(task_log_id=self.task.id,
                                             related_uri=related_uri
                                             )
                session.save_or_update(related_uri)
            session.flush()

            '''
            save the product_price if it\'s updated:
            '''
            for extracted_entity_name, extracted_entity_value in self.parent_extracted_entites.iteritems():
                name,datatype = extracted_entity_name
                parent_extracted_entity_name = session.query(model.ParentExtractedEntityName).filter_by(name=name,
                                                                                               data_type = datatype).first()
                if not parent_extracted_entity_name:
                    parent_extracted_entity_name = model.ParentExtractedEntityName(name=name, data_type=datatype)
                    session.save(parent_extracted_entity_name)
                    session.flush()
                parent_extracted_entity_value = model.ParentExtractedEntityValue()
                parent_extracted_entity_value.connector_instance_id = self.task.connector_instance_id
                parent_extracted_entity_value.value = extracted_entity_value
                parent_extracted_entity_value.created_date = datetime.utcnow()
                parent_extracted_entity_value.extracted_entity_name_id = parent_extracted_entity_name.id
                session.save(parent_extracted_entity_value)

            '''
            save the connector type guessed (rss/html) back to connector instance
            '''
            connector_instance = session.query(model.ConnectorInstance).get(self.task.connector_instance_id)
            instance_data = json.loads(connector_instance.instance_data)
            if not instance_data.get('source_type') and (self.task.instance_data.get('source_type') in ['rss','others']) and \
                                                                                                         self.task.level == 1 :
                instance_data['source_type'] = self.task.instance_data.get('source_type','others')
                connector_instance.instance_data = json.dumps(instance_data)
                session.save_or_update(connector_instance)

            session.commit()
            session.close()
            log.info("TaskID:%s::Client:%s::db entry done for related uris" % (self.task.id, self.task.client_name))
            return True
        except:
            print "db update failed"
            session.rollback()
            session.close()
            log.exception(self.log_msg('DB update failed'))
            raise

    @logit(log, '__saveToHDFS')
    def __saveToHDFS(self):
        """Save posts to HDFS needs the HDFS thrift server
        to be running
        """
        fs_handle = hdfs.PyHDFS(HDFS_THRIFT_HOST, HDFS_THRIFT_PORT)
        log.info("Connecting to %s:%s", HDFS_THRIFT_HOST, HDFS_THRIFT_PORT)
        if not fs_handle.connect():
            log.info("Connecting to HDFS failed.")
            return False
        date = datetime.utcnow()
        fname = 'input-data/activity-analysis/' + str(date.year)+'/'+str(date.month).zfill(2)+'/'+str(date.day).zfill(2)+'/'+str(date.hour).zfill(2)+'/'+str(date.minute).zfill(2)+'/'+self.task.id+ ".xml"
        try:
            job_xml = self.__getJobXML()
            is_success = fs_handle.open(fname, hdfs.Mode.WRITE)
            if not is_success:
                log.warning("Couldn't open file:%s" % fname)
                return False
            fs_handle.write(job_xml)
            log.info("Wrote %s successfully", fname)
        except Exception, e:
            print "Error in __saveToHDFS:%s - Task:%s - Traceback:%s", fname, self.task.id, traceback.format_exc()
            log.error("Error in __saveToHDFS:%s - Task:%s - Traceback:%s", fname, self.task.id, traceback.format_exc())
            return False
        finally:
            fs_handle.close()
        return True

    
    @logit(log, '__getJobXML')
    def __getJobXML(self):
        generated_on = str(datetime.now())
        root = Element('itb')
        root.set('version', '1.0')
        tid = SubElement(root, 'taskID')
        tid.text = self.task.id        
        ci = SubElement(root, 'connectorInstanceID')
        ci.text = self.task.connector_instance_id
        wi = SubElement(root, 'workspaceID')
        wi.text = self.task.workspace_id
        date = SubElement(root, 'generatedOn')
        date.text = generated_on
        extractors = self.task.instance_data.get("extractors")
        if not extractors:
            extractors = {}
        ex = SubElement(root, "extractors")

        # Set all the extractors. Global params to
        # the extractors are part of the instance data
        for extractor, params in extractors.items():
            _e = SubElement(ex, "extractor")
            _e.set("name", extractor)
            for p in params:
                pr = SubElement(_e, p)
                pr.text = params[p]

        posts = SubElement(root, "posts")
        for post in self.pages:
            ps = SubElement(posts, 'post')
            post_elements = {}
            keys = ['id', 'title', 'data', 'pickup_date',
                    'posted_date', 'uri', 'entity']# 'path', 'parent_path']
            for key in keys:
                k = SubElement(ps, key)
                if isinstance(key, list): # Some metadata values are list. Enumerate them in the XML
                    items = post[key]
                    for i, item in enumerate(items):
                        _i = SubElement(k, str(i))
                        _i.text = item
                else:
                    k.text = post[key].encode('utf-8')
            # Entities from the connector
            es = SubElement(ps, 'entities')
            entities = {}
            entity_regex = re.compile('^(et_|ei_|edate_|ef_)(.*)', re.U)
            for p in post: # Get the entities that match self.prefixes
                m = entity_regex.search(p)
                if m:
                    e = SubElement(es, 'entity')
                    prefix = m.groups()[0]
                    entity_type = m.groups()[1]
                    e.set("type", self.prefixes[prefix.strip()]) # text or integer or date or.. etc
                    e.set("name", entity_type)
                    if isinstance(post[p], unicode):
                        e.text = post[p].encode('utf-8')
                    else:
                        e.text = str(post[p])
                        
        return tostring(root)

    
    @logit(log,'__getPostLanguageName')
    def __getPostLanguageName(self):
        """
        """
        return self.task.instance_data.get('language')
    
    @logit(log, 'sendBackLinks')
    def sendBackLinks(self):
        """
        HAS SOLE IMPLEMENTATION IN BASE CONNECTOR

        send the linksout back to master

        ##dummy implementation
        ##try:
        ##    .....implementation.....
        ##    update self.task.linksOut_status=True
        ##    return True
        ##except:
        ##    log.debug exception
        ##    return False

        """
        log.debug('TaskID:%s::Client:%s::trying to sendBackLinks')
        result = False
#        intentionally , not checking for self.related_uris because it will be problem
#        cases in which page acts as a proxy page and sendsback updated links, like ebayconnector
        try:
            self.linksOut = [task for task in self.linksOut if task.instance_data['uri'] != self.task.instance_data['uri']]
            result=taskmanager.readLinksOut(pickle.dumps(self.linksOut))
        except:
            print "failed to send back links"
            log.exception('TaskID:%s::Client:%s::failed to sendBackLinks')
        return result

    @logit(log, '_getHTML')
    def _getHTML(self, uri=None,data=None,headers={}):
        """
        urlopen and data acquire
        408- request timeout
        500- internal server error
        502- bad gateway
        503- service unavailable
        504- gateway timeout
        104- partial content
        """
        try:
            if not uri:
                uri=self.currenturi
            log.info('FETCHING %s'%uri)
            if uri.strip() == '':          # No url (due to #named anchors)
                #log.info(uri)
                self.task.status['fetch_message']='URL is empty'
                return ''
            if self.use_memcache:
                cache_response = self.cacheObj.get(uri)
                #log.info(cache_response)
                if cache_response:
                    log.debug(self.log_msg("GOT VALUE FROM CACHE FOR URI:%s"%(uri)))
                    headers = cache_response[0]
                    response_code = cache_response[1]
                    result = cache_response[2]
                    self.mimeType = headers['Content-Type'].split(';')[0]
                    return dict(result=result,fetch_message=response_code)
            conn = HTTPConnection()
            headers['Accept-encoding'] = headers.get('Accept-encoding','gzip,deflate,*')
            if not data:
                conn.createrequest(uri,headers=headers)
            else:
                conn.createrequest(uri,data,headers=headers)

            request_no = 0
            while request_no < 3:
                try:
                    response = conn.fetch() # Wait for 10 secs to fetch
                    self.task.status['fetch_message'] = response.code
                    if self.task.status['fetch_message'] in [408,500,502,503,504,104]:
                        log.info("requesting page again due to response code : %d"%(self.task.status['fetch_message']))
                        request_no=request_no+1
                        continue
                    else:
                        log.debug('TaskID:%s::Client:%s::httpconnection fetch: no error or unknown error, response code: %d' % (self.task.id,
                                                                                                                         self.task.client_name,
                                                                                                                         self.task.status['fetch_message']
                                                                                                                         ))
                        break
                except Exception,e:
                    print traceback.format_exc()
                    print "fetch failed nth time"
                    log.exception('TaskID:%s::Client:%s::httpconnection fetch failed %dth time' % (self.task.id, self.task.client_name, request_no))
                    if str(e) == '<urlopen error timed out>':
                        request_no=request_no+1
                        continue
                    else:
                        raise e

            if response.code == 200: # All is OK
                log.debug('TaskID:%s::Client:%s::response code is 200' % (self.task.id, self.task.client_name))
                if uri!= response.url:
                    # Handle page redirects. Page redirects can also take you to different domains
                    self.related_uris.append(response.url)
                    #only palce where it is called 'url' - coz response is a web server object with attribute url

                # Check for proper mime type in responses
                self.mimeType = response.headers['Content-Type'].split(';')[0]
                if self.mimeType in self.valid_mime_types:
                    result = response.read()
                    log.debug('TaskID:%s::Client:%s::httpconnection fetch successful' % (self.task.id, self.task.client_name))
                    try:
                        self.times_fetched +=1
                        if self.session_info_in and self.times_fetched > 1000 : # if it gets into infinite loop ,
                                                                            #return none , send out a email
                            log.exception(self.log_msg('POSSIBLE INFINITE LOOP for this connector, url %s'%uri))
                            email_exception(("INFINITE LOOP occured for this task. The URI was %s. For the task %s" %(uri, self.task.id)),
                                             interval=0)
                            return None
                        try:
                            self.content_fetched+=int(response.headers['Content-Length'])
                        except:
                            self.content_fetched+=len(result)
                    except:
                        print "could not get content fetched"
                        log.exception(self.log_msg('could not get content fetched'))
                    if response.headers.get('Content-Encoding') and 'gzip' in \
                            response.headers['Content-Encoding'].split(';'): #handle compressed data
                        log.info(self.log_msg('Decompress the gzipped content from server'))
                        compressedstream = StringIO.StringIO(result)
                        gzipper = gzip.GzipFile(fileobj=compressedstream)
                        result = gzipper.read()
                    result = re.sub('<!DOCTYPE.*?>','',result)
                    if self.use_memcache:
                        log.debug(self.log_msg("SETTING VALUE TO CACHE FOR URI:%s"%(uri)))
                        set_response = self.cacheObj.set(uri,(response.headers,response.code,result))
                        if set_response:
                            log.debug(self.log_msg("SUCCESS IN SETTING VALUE FOR URI:%s"%(uri)))
                        else:
                            log.debug(self.log_msg("VALUE ALREADY SET FOR URI:%s"%(uri)))
                else:
                    result = None
            else:
                return None
            return dict(result=result, fetch_message=response.code)
        except:
            print traceback.format_exc()
            print "httpconnection fetch failed"
            log.exception('TaskID:%s::Client:%s::httpconnection fetch failed' % (self.task.id, self.task.client_name))
            return None



    @logit(log, '_findFirstPage')
    def _findFirstPage(self):
        try:
            found_first_page = False
            alist = self.soup.findAll('a',href=True)
            for elem in alist:
                if re.sub(r'<[^>]*>','',unicode(elem)) == u'1':
                    res=self._partOfSameArticle(normalize(unicode(elem['href']), self.currenturi, self.base))
                    if res['samearticle']:
                        self.currenturi = normalize(unicode(elem['href']), self.currenturi, self.base)
                        self.related_uris.append(self.currenturi)
                        self.rawpage = res['rawpage']
                        self._setCurrentPage()

                        found_first_page = True
                    else:
                        found_first_page = False
                        break

            # Either the site does not use page numbers or we are already in the first page.
            # We need to look for link to previous
            if not found_first_page:
                prev_page_found = True
                while prev_page_found and len(alist) > 0:

                    for elem in alist:
                        if re.sub(r'</?[^<>]*/?>','',unicode(elem).lower()).find('prev') is not -1:
                            # Found previous link. Now off to check if it is part of same article
                            try:
                                res=self._partOfSameArticle(normalize(unicode(elem['href']), self.currenturi, self.base))
                                if res['samearticle']:
                                    self.related_uris.append(self.currenturi)
                                    self.currenturi = normalize(unicode(elem['href']), self.currenturi, self.base)
                                    self.rawpage = res['rawpage']
                                    self._setCurrentPage()
                                    break
                                else:
                                    prev_page_found = False
                            except:
                                continue
                        else:
                            prev_page_found = False

                    if prev_page_found:
                        alist = self.soup.findAll('a',href=True)
        except:
            print "_findfirstpage failed"
            log.exception('TaskID:%s::Client:%s::_findFirstPage failed' % (self.task.id, self.task.client_name))



    @logit(log,'_partOfSameArticle')
    def _partOfSameArticle(self, link):
        try:
            oldtitle = re.sub(r"</?[^<>]*/?>", '', unicode(self.soup.title))
            oldqueryTerms = cgi.parse_qs(urlparse.urlparse(self.currenturi).query, False, False)
            currentauthority = urlparse.urlparse(self.currenturi)[1].strip()
            newauthority = urlparse.urlparse(link)[1].strip()
            if currentauthority != newauthority and newauthority != self.baseauthority:
                return dict(samearticle=False)
            rawpage=self._getHTML(link)['result']

            #if _getHTML returns a None due to invalid MIME type or some other error - what to do?
            #currently - say that its not part of the same article and fetch it as a separate page
            if not rawpage:
                return dict(samearticle=False)

            newsoup = BeautifulSoup(rawpage)
            newtitle = re.sub(r"</?[^<>]*/?>", '', unicode(newsoup.title))
            #JV
            #             newqueryterms = cgi.parse_qs(urlparse.urlparse(link).query, False, False)

            #             for key in newqueryterms.keys():
            #                 pass

            if oldtitle == newtitle:
                return dict(samearticle=True, rawpage=rawpage)
            return dict(samearticle=False)
        except:
            print "_findFirstPage failed"
            log.exception('TaskID:%s::Client:%s::_findFirstPage failed' % (self.task.id, self.task.client_name))
            return dict(samearticle=False)


    @logit(log, '_nextPageFound')
    def _nextPageFound(self):
        try:
            alist = self.soup.findAll('a',href=True)
            for elem in alist:
                if re.sub(r'<[^>]*>','',unicode(elem)) == unicode(self.numofpages + 1):
                    self.numofpages += 1
                    try:
                        log.debug("%s-%s-%s" %(unicode(elem['href']), self.currenturi, self.base))
                        linkuri  = normalize(unicode(elem['href']), self.currenturi, self.base)
                        if linkuri == self.currenturi:
                            continue
                        self.rawpage = self._getHTML(linkuri)['result']
                        if not self.rawpage:
                            continue
                        self.currenturi = linkuri
                        self.related_uris.append(self.currenturi)#CAN BE PUT ONLY AT ONE PLACE - _setCurrentPage
                        self._setCurrentPage()
                        return True
                    except:
                        continue

            # no link to page number bigger than current page found
            # check for next or continue
            for elem in alist:
                # Is there the word continue
                if re.sub(r'</?[^<>]*/?>','',unicode(elem).lower()).find('continue') is not -1:
                    # Does it also contain from which means this link cannot be the next page
                    if re.sub(r'</?[^<>]*/?>','',unicode(elem).lower()).find('from') is not -1:
                        continue
                    # Found previous link. Now off to check if it is part of same article
                    try:
                        res=self._partOfSameArticle(normalize(unicode(elem['href']), self.currenturi, self.base))
                        if res['samearticle']:
                            self.rawpage = res['rawpage']
                            self.currenturi = normalize(unicode(elem['href']), self.currenturi, self.base)
                            self.related_uris.append(self.currenturi)
                            self._setCurrentPage()
                            return True
                    except:
                        continue

            for elem in alist:
                if re.sub(r'</?[^<>]*/?>','',unicode(elem).lower()).find('next') is not -1: # Check for only next word in link element
                    log.debug("third loop")
                    # Found previous link. Now off to check if it is part of same article
                    try:
                        res=self._partOfSameArticle(normalize(unicode(elem['href']), self.currenturi, self.base))
                        if res['samearticle']:
                            self.currenturi = normalize(unicode(elem['href']), self.currenturi, self.base)
                            self.related_uris.append(self.currenturi)
                            self.rawpage = res['rawpage']
                            self._setCurrentPage()
                            return True
                    except:
                        continue

            log.debug("TaskID:%s::Client:%s::next page not found" % (self.task.id, self.task.client_name))
            return False
        except:
            print "error while finding next page"
            log.exception('TaskID:%s::Client:%s::error while finding next page' % (self.task.id, self.task.client_name))
            return False

    @logit(log, '_setCurrentPage')
    def _setCurrentPage(self):
        """
        # Superb docstrings
        """
        try:
            self.soup = BeautifulSoup(self.rawpage)
            self.base = self.soup.find('base',href=True)
            self.baseauthority = None
            if self.base is not None:
                self.base = self.base['href']
                self.baseauthority = urlparse.urlparse(self.base)
        except Exception, e:
            log.exception('TaskID:%s::Client:%s::error in _setCurrentPage' % (self.task.id, self.task.client_name))

    @logit(log, '_pageChanged')
    def _pageChanged(self, uri, new_contents):
        """
        Given the uri of a page and new_contents, check whether that page has changed
        """
        try:
            newmd5 = md5.md5(new_contents.encode('utf-8')).hexdigest()
        except:
            print "error in _pageChanged"
            log.exception('TaskID:%s::Client:%s::error in _pageChanged' % (self.task.id, self.task.client_name))


    @logit(log,'getAssociatedFolders')
    def getAssociatedFolders(self,keywords):
        """
        A document is associated with a set of folders based on :
        1) keywords found in the document , and folders associated with all those keywords
        2)folders present for the workspace , with assign all articles to the folders option true
        """
        folders = []
        folders = session.query(model.Folder).filter_by(workspace_id = self.task.workspace_id,assign_all=True,
                                                         active_status=True,delete_status=False).all()

        log.info(self.log_msg('trying to get folders associated with keywords : %s'%','.join(self.task.keywords)))
        for keyword in keywords:
            folders.extend(session.query(model.Keyword).filter_by(keyword = keyword,active_status=True,
                                                           delete_status=False).first().folders)

        associated_folder_ids = [folder.id for folder in [each for each in folders if each.active_status==True and\
                                                              each.delete_status==False]]
        return list(set(associated_folder_ids))

    def log_msg(self,log_str):
        try:
            db_info = '@'.join(self.task.dburi.split('@')[-1].split('/')[::-1])
            return 'TaskID:%s DB:%s Workspace_id:%s Client:%s ::%s'%(self.task.id,
                                                                     db_info,self.task.workspace_id,
                                                                     self.task.client_name,
                                                                     log_str.encode('utf-8','ignore')) 
        except:
            return 'TaskID:%s DB:%s Workspace_id:%s Client:%s ::%s'%(self.task.id,
                                                                     db_info,self.task.workspace_id,
                                                                     self.task.client_name,
                                                                     '') 

    @logit(log,'updateParentExtractedEntities')
    def updateParentExtractedEntities(self,page):
        try:
            for extracted_name,extracted_value in page.iteritems():
                f=lambda x,y: x.startswith(y)
                if any([f(extracted_name, each) for each in self.prefixes.keys()]):
                    pref, name = extracted_name.split('_', 1)
                    datatype = self.prefixes[pref+'_']
                    if extracted_value and self.task.level == 1 and not self.task.instance_data.get('already_parsed') and \
                            str(extracted_value) != self.task.instance_data['parent_extracted_entities'].get((name,datatype)) \
                            and not self.task.instance_data.get('metapage'):
                        log.info(self.log_msg('for value updated extracted entity %s'%extracted_name))
                        log.info(self.log_msg('prev :: #%s# , new #%s# '%(extracted_value,self.task.instance_data['parent_extracted_entities'].get((name,datatype)))))
                        self.parent_extracted_entites[(name,datatype)] =  extracted_value
        except:
            print "couldn't parse parent information"
            log.exception(self.log_msg("couldn't parse parent information"))


#Exception helper functions
    @logit(log,"email_exception")
    def email_exception(self,msg='',interval=14400,email=False):
        try:
            if email and self.exception_context_q:
                msg_string = '''
               Task_Log_id : %s ,
               Task url : %s,
               Current url : %s ,
               Task level : %s ,
               instance_data : %s
               Individual steps status : %s
               misc_dict : %s
               ________________________________________\n\n\n

               Individual Exceptions
                             '''%(self.task.id, self.task.instance_data['uri'],self.currenturi , self.task.level ,
                                  str(self.task.instance_data),
                                  str(self.task.status) ,
                                  str(self.misc_status_dict))
                self.exception_context_q = list(set(self.exception_context_q))

                for index,msg in enumerate(self.exception_context_q):
                    msg_string += '''
                                   Exception : %s ) %s \n
                                  '''%(index , msg[0])

                email_exception(msg_string,interval = min([each[1] for each in self.exception_context_q]))
            else:
                self.exception_context_q.append((msg,interval))
        except:
            print "error"
            #email_exception(str(traceback.format_exc()),interval = 0)

class TimeoutException(Exception):
    "Indicates that the function has taken too long."
    pass
