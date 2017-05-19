
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
import sys
import os
sys.path.append(os.getcwd().rsplit(os.sep,1)[0]+'utils.py')

import datetime
import re
from stanfordner import StanfordNER
import socket

from xml.etree import cElementTree as ET

from urllib2 import *
from urllib import *
import json

from tgimport import *
from utils.decorators import logit
import logging
from keywordfilter import KeywordFilter
from utils import nlp 
from utils import chunkpreprocess
from utils.utils import getGender
#import parsedatetime.parsedatetime_consts as pdc
#from serendiocalendar import Calendar
from datetime import datetime
import nltk
log = logging.getLogger('Extractor')

class Extractor(object):
    """This module is used to extract info from the given text"""
    keyword_filter_obj = KeywordFilter()

    def __init__(self, data,workspace_id=None):
        self.data = data
        self.workspace_id = workspace_id


    @logit(log, 'extract')

    def extract_ner(self):
        try:
            socialset1 = set([u"Person", u"Organization", u"Location"])
            text = self.data.encode('ascii', 'replace')
            #text = self.data.encode('utf-8', 'ignore')
            text = re.sub(r'(\n|\r|\t)', ' ', text)

            #host = tg.config.get(path='StanfordNER', key='host')
            #port = tg.config.get(path='StanfordNER', key='port')

            url = tg.config.get(path='NERD', key='path')

            values = {}
            #values['text'] = self.data.encode('utf-8','ignore')
            values['text'] = text
            values['workspace_id']= self.workspace_id
            #log.info(values)
            data = urlencode(values)
            #log.info(data)
            req = Request(url, data)
            #log.info(req)
            opener = build_opener()
            response = opener.open(req)
            the_page = response.read()
            result_dict = {}
            result_dict = simplejson.loads(the_page)
            log.debug(result_dict)

            infocuslist =[]
            for each in result_dict.keys():
                if not each == 'tg_flash':
                  temp = result_dict[each]
                  for eachx in temp.keys():
                      temp_lis = temp[eachx]
                      for x in temp_lis:
                          infocuslist.append(x)
                       #eachx = eachx.encode('utf-8','ignore')
                       #infocuslist.append(eachx)


            #infocuslist = list(result_dict[u'PERSON'])
            #infocuslist.extend(list(result_dict[u'ORGANIZATION']))
            #infocuslist.extend(list(result_dict[u'LOCATION']))
            log.info(infocuslist)

            extractions={'et_infocus':infocuslist }


            return extractions
        except Exception,e:

            raise e


    def extract_email(self):
        emailre = emailre = re.compile('(([a-zA-Z0-9_\.\-\+])+\@(([a-zA-Z0-9\-])+\.)+([a-zA-Z0-9]{2,4})+)', re.I)
        matches = emailre.findall(self.data)
        emails = [match[0] for match in matches]
        result_dict = {'et_email':emails}
        return result_dict

    def extract_patent(self):
        patentre = re.compile(r"(?P<patent>patent\s?(\b[^[A-Z]{2}\d+]+\b)?\.?\s?(?P<patno>([A-Z]{2})?\s?((\d+[,\.\s]?)+(\d|,|\.)[A-z0-9]*)))", re.I)
        matches = patentre.findall(self.data)
        patents = [match[2] for match in matches]
        result_dict = {'et_patent':patents}
        return result_dict


    def extract_keyword(self,keywords):
        """
        """
        return self.keyword_filter_obj.checkFilter(self.data,keywords)

    def extract_gender(self, page):
        """
        """
        rapleaf_url = tg.config.get(path='api_urls',key='RAPLEAF_URL')
        return getGender(page, rapleaf_url)

    def extract_location(self, page):
        try:
            d = {}
            if page.has_key('et_author_location'):
                placefinder_url = '%s?%s'%(tg.config.get(path='api_urls',key='YAHOO_PLACEFINDER_URL'), \
                                           urlencode({'q':quote_plus(page['et_author_location'])})) 
                r = ET.fromstring(urlopen( placefinder_url ).read())
                d['latitude']=float(r.find('Result').find('latitude').text)
                d['longitude']=float(r.find('Result').find('longitude').text)
        except Exception, e:
            print "Error in location", e
            d={}
        return d

    def extract_relevancy(self, page):
        return 1.0
        
    def extract_nlp(self):
        '''extract nouns, verbs, adjective etc chunks 
        along with head of chunks. The chunker uses  Penn treebank chunk tagset. 
        The last word of the chunk is the headword of chunk.
	Input: 
	Text-Type String
	Tag Dictionary-optional
	Output-
	return_dict '''
        return_dict={}
        chunker=chunkpreprocess.ChunkPreprocess()
	tag_dict={'noun_chunk': ['NP', 'NX'], 'noun_pos': ['NN', 'NNS', 'NNP', 'NNPS'],\
                 'verb_pos': ['VBP', 'VBZ', 'VBG', 'VBN', 'VBD', 'VB'], 'adv_chunk': ['ADVP'], \
                      'verb_chunk': ['VP'], 'adj_pos': ['JJR', 'JJS', 'JJ'], \
                      'adj_chunk': ['ADJP'], 'adv_pos': ['WRB', 'RRB', 'RBR', 'RBS']}
	for each_type in tag_dict:
		word_dict = {}
		chunked_sentence = chunker.parse(self.data)
		pos_tagged_sentence = chunker.nlp.tag(self.data)
		pos_tagged_array=[each.split('/') for each in pos_tagged_sentence.split(' ')]
		if 'chunk' in each_type:
			return_dict[each_type] = self._calculate(tag_dict,chunked_sentence,each_type)
			return_dict[each_type+'_head'] = self._calculate(tag_dict,chunked_sentence,each_type,True)
					
		elif 'pos' in each_type:
			return_dict[each_type] = self._calculate(tag_dict,pos_tagged_array,each_type)
		
        return return_dict

    def extract_temporal(self,posted_date):
        c = pdc.Constants()
        parsed_time={}
        dateparser = Calendar(c)
        tokenizer = nltk.tokenize.PunktSentenceTokenizer()
        sentence_list = tokenizer.sentences_from_text(self.data)
        
        posted_date_tuple = datetime.strptime(posted_date,'%Y-%m-%dT%H:%M:%SZ').timetuple()
        for each_sentence in sentence_list:
            parsed_date_string =dateparser.parse(each_sentence,posted_date_tuple)
            
            #print dateparser.parse(each_sentence,posted_date_tuple),'Hi',type(dateparser.parse(each_sentence,posted_date_tuple))
            if isinstance(parsed_date_string[0], time.struct_time):
                
                parsed_date_string = tuple([each for each in parsed_date_string[0]]) 
            
            converted_parsed_date_str=self.__convertTime2String(parsed_date_string)
            parsed_time.setdefault(converted_parsed_date_str,0)
            parsed_time[converted_parsed_date_str] = parsed_time[converted_parsed_date_str]+1
        
        time_dict = {'edate_temporal':parsed_time}
        return time_dict

    def __convertTime2String(self,timeobj):
        #to convert time expression into string format
        if len(timeobj)==2:
            timeobj=timeobj[0]
        return_time = '%d-%d-%dT%d:%d:%dZ'%timeobj[:6]
         
        return return_time 
        
    def _calculate(self, entitydict, wordentitylist, entity_type, head = False):
	    '''The function is used to extracts or count'''
	    word_dict={}
	    for eachele in wordentitylist:
		    eachele[0]=eachele[0].lower()
		    if eachele[1] in entitydict[entity_type] and head == False:
			    word_dict[eachele[0]] = word_dict.get(eachele[0],0)+1
		    elif eachele[1] in entitydict[entity_type]:
			    word_dict[self._computehead(eachele[0])] = word_dict.get(self._computehead(eachele[0]),0)+1
		    
	    return word_dict.items()
	    
    def _computehead(self,chunk):
	    '''given the chunk compute the head 
         of the chunk. The head of chunk is the last word.'''
	    chunks=chunk.split(' ')
	    return chunks[-1]
