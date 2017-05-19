
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

import copy
import re
import cgi
from BeautifulSoup import *
import md5
#from model import *
from sqlalchemy import DateTime
from datetime import datetime
import time
#from processing.managers import BaseManager, CreatorMethod
import traceback
import thread,socket


import sys
import os
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
import smtplib
import logging
import json
from urllib import urlretrieve
#will log the exceptions to a exceptions.log
ex_log = logging.getLogger('exceptions')
fhdlr = logging.FileHandler('exceptions.log','a')
formatter = logging.Formatter('%(asctime)s:%(lineno)d: %(levelname)s %(message)s')
fhdlr.setFormatter(formatter)
ex_log.addHandler(fhdlr)
ex_log.setLevel(logging.INFO)

re_null_unicode_chr = re.compile(u'\u0000',re.UNICODE)
sys.path.append(os.getcwd().rsplit(os.sep,1)[0])

# class QueueManager(BaseManager):
#     proxyMaster = CreatorMethod(typeid='proxyMaster')

male_list = [each.strip().lower() for each in open(globals()['__file__'].rsplit(os.sep, 1)[0] + os.sep + 'male.txt').readlines() if each and each.strip()]
female_list = [each.strip().lower() for each in open(globals()['__file__'].rsplit(os.sep, 1)[0] + os.sep + 'female.txt').readlines() if each and each.strip()]

male_dict = dict(zip(male_list,["male"]*len(male_list)))
female_dict = dict(zip(female_list,["female"]*len(female_list)))

GENDER_DICT = {}
GENDER_DICT.update(male_dict)
GENDER_DICT.update(female_dict)

def getGender(page, rapleaf_url):
    """Get the value, from the page
    """
    author_gender = page.get('et_author_gender') or page.get('et_author_sex')
    if not author_gender:
        author_name = page.get('et_author_name')
        if author_name and len(author_name.split())>=1:
            first_name = author_name.split()[0].lower()
            author_gender = GENDER_DICT.get(first_name)
            if not author_gender:
                try:
                    author_gender = json.loads(open(urlretrieve("%s/%s"%(rapleaf_url, first_name))[0]).read())['answer']['gender']
                except Exception, e:
                    print e
                    author_gender = "unknown"

    if not author_gender:
        author_gender="unknown"

    return author_gender

def to_bool(s):
    truelist = [True, 'True','true','t','T','Yes','yes','Y','y']
    falselist = [False, 'False','false','f','F','No','no','N','n']
    if s in truelist:
        return True
    elif s in falselist:
        return False
    else:
        return None

def saveCrawlerLog(sess, id=None, site_id=None):
    if id is not None:                  # Update
        cl = sess.query(CrawlerLog).get(int(id))
        cl.end_date = datetime.datetime.utcnow()
    else:
        cl = CrawlerLog()
        cl.site_id = site_id
        cl.start_date = datetime.datetime.utcnow()
    sess.save_or_update(cl)
    sess.commit()
    return cl.id

def stripHtml(text):
    """Strip the given text off all HTML tags

    >>> strip_html("<p>This is a test<br/> of strip</p>")
    This is a test\n of strip
    """
    #text = re.sub(r'<br(\s)*?(\/)?>', '\n', text)
    text = re_null_unicode_chr.sub('',text)
    text = re.sub(r'</?(p|br)\s?/?>', r'\n', text)
    text = re.sub(r'<[^<>]+>', ' ', text)
    text = re.sub(r'^(\s)+', '', text)
    re1 = re.compile(r'^(\s*\n)+',re.M)
    text = re1.sub(r'\n',text)
    #re2 = re.compile(r'\n(\S)',re.M)
    #text = re2.sub(r' \1', text)
    #text = re.sub(r'\s+', ' ', text)

    #JV
    #TEMP FIX
    #added
    text=text.replace('\');" onMouseOut="setTimeout(\'hideLayer()\',500);" class=hotlink2>','')
    text=text.replace('Click for the lowest price on dmnobieblank','')
    ##
    
    text = text.replace('&nbsp;',' ')
    text = text.replace('&raquo;','')
    try:
        if not isinstance(text, unicode):
            text = text.decode('utf-8','ignore')
        else:
            text = text.encode('utf-8','ignore').decode('utf-8','ignore')
#         if not isinstance(text,unicode):
#             text = cgi.unescape(unicode(text,'utf-8'))
#         else:
        text = cgi.unescape(text)
    except:
        print traceback.format_exc()
#        print text
        
    return text.strip()


def saveUrlHistory(sess, id=None, project_id=None, crawler_log_id=None, url=None, end_date=None, response_code = None):
    if id is not None:
        uh = sess.query(UrlHistory).get(int(id))
    else:
        uh = UrlHistory()
        uh.crawler_log_id = crawler_log_id
        uh.url = url
        uh.project_id = project_id
    uh.end_date = end_date
    uh.response_code = response_code
    sess.save_or_update(uh)
    sess.commit()
    return uh.id

def saveConceptFilterLog(sess, url_history_id=None, concepts=[]):
    """Given a list of successful concepts, save them to concept_filter_logs table"""
    if len(concepts) == 0:
        cfl = ConceptFilterLog()
        cfl.url_history_id = url_history_id
    else:
        cfl = ConceptFilterLog()
        cfl.url_history_id = url_history_id
        cfl.concept = unicode(concepts)
    sess.save(cfl)
    sess.commit()

def saveExtractorLog(sess, url_history_id=None, extractions={}):
    print 'extractions' + str(extractions)
    for k,v in extractions.items():
        exl = ExtractionLog()
        exl.url_history_id = url_history_id
        exl.extraction_type = unicode(k)
        exl.extraction_data = unicode(v)
        sess.save(exl)
        print "Saving extraction log"
    sess.commit()
    updateCrawlerLog(sess, url_history_id)

def updateCrawlerLog(sess, url_history_id=None):
    print "Updating crawler log"
    url_history = sess.query(UrlHistory).get(url_history_id) # Get the URLHistory object
    print url_history
    crawler_log = sess.query(CrawlerLog).get(url_history.crawler_log_id)
    print crawler_log
    crawler_log.end_date = datetime.datetime.utcnow()
    sess.save_or_update(crawler_log)
    sess.commit()
    print "updated crawler log"

def del_from_kw(kw, *args):
    '''From a list of args remove all those items in the kw dictionary.'''
    for arg in args:
        del(kw[arg])
    return kw


class DefaultDict(dict):
    """Dictionary with a default value for unknown keys.

    http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/389639
    """
    def __init__(self, default):
        self.default = default

    def __getitem__(self, key):
        if key in self:
            return self.get(key)
        else:
            ## Need copy in case self.default is something like []
            return self.setdefault(key, copy.deepcopy(self.default))

    def __copy__(self):
        copy = DefaultDict(self.default)
        copy.update(self)
        return copy



global exception_list
exception_list = {}

def sendmail(fromaddr, toaddrs, msg,text):
    server = smtplib.SMTP('smtp.gmail.com',587)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login("services@serendio.com","kmsystemmailer")
    response = server.sendmail(fromaddr, toaddrs, msg)
    ex_log.error(text)
    server.quit()

global mail_process_running
mail_process_running = False

def email_exception(exception,interval=7200):
    "send the exception , along with interval in mins default to 2 hrs"
    print exception
    exception_list[time.time()] = [interval , exception]
    if not mail_process_running:
        thread_id = thread.start_new_thread(mail,())
        ex_log.info("started mail daemon = %s"%thread_id)

# def mail():
#     """Given a msg, email a list of addresses . sleep interval , defaults to 600 seconds
#     """
#     try:
#         global mail_process_running
#         mail_process_running = True
#         sleep_interval = int(config.get(path='global',key='exception_email_scheduler'))
#         while True:
#             now = time.time()
#             time.sleep(sleep_interval)
#             emails_to_be_sent = []
#             now = time.time()
#             for key,value in exception_list.items():
#                 if (now - key) >= float(value[0]):
#                     emails_to_be_sent.append(value)
#                     del exception_list[key]
#             to_addrs = config.get(path='global',key='exception_email_to_addrs')
#             if emails_to_be_sent and to_addrs:
#                 msg = MIMEMultipart()
#                 date = datetime.now().strftime("%d-%b-%Y %H:%M:%S")
#                 fromaddr = 'services@serendio.com'
#                 msg['To'] = str(to_addrs)
#                 msg['From'] = 'services@serendio.com'
#                 msg['Subject'] = "Error : Exception occured in crawler on %s"%date
#                 sysinfo = "Crawler running on %s"%socket.gethostname()
#                 ex_text = '\n\n\t'.join([email[1] for email in emails_to_be_sent])
#                 text = '''
#         Hi,
#         Following exception(s) occured for %s on %s ,
#          %s
#
#         Regards,
#         ''' %(sysinfo , date , ex_text)
#                 text = text[:1000000] # just putting a sanity limit to the mail
#                 msg.attach(MIMEText(text))
#                 sendmail(fromaddr, str(to_addrs).split(','),  msg.as_string(),ex_text)
#     except:
#         print "exception in email function"

def removeJunkData(rawpage, is_rawpage=True, _debug=False):
    """
      Removes unwanted data from soup object. It uses a hardcoded threshold of ratio of words, link elements and others
      Returns back the purged soup object
    """
    if is_rawpage:
        # remove xml comments
        _page = re.sub('<!--.*?-->','',rawpage)
        # remove everything before <html
        _page = _page[_page.find('<html'):]

        soup=BeautifulSoup(_page)
    else:
        #The variable named rawpage, is actually a soup variable
        soup = copy.copy(rawpage)

    try:
        title = str(soup.find('title').find(text=True))
        title = unicode(BeautifulStoneSoup(title, convertEntities=BeautifulStoneSoup.ALL_ENTITIES))
    except:
        if _debug: print "No title Found"
        pass


    nodes_to_throw_away = ['script','noscript', 'object', 'embed', 'input', 'form', 'style', 'option', 'img', 'label', 'applet']
    #nodes_to_purge = ['div','table','dl','p','h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'dl', 'li', 'dd', 'dt']
    nodes_to_purge = ['div','table','ul','ol','dl','p']
    link_nodes = ['a']

    # throw away the head
    head = soup.find('head')
    if head is not None: head.extract()

    #throw away the script tags # even the ones badly formed
    soup = BeautifulSoup(re.sub(re.compile('<script.*?</script[^>]*>',re.DOTALL),'',unicode(soup)))

    #Remove comments (only if originally it is a rawpage, not a soup)
    if is_rawpage:
        comments = soup.findAll(text=lambda text:isinstance(text, Comment))
        [elem.extract() for elem in comments]

    nodelist = soup.findAll(nodes_to_throw_away)
    [elem.extract() for elem in nodelist]

    # remove embedded htmls
    nodelist = soup.body.findAll('html')
    [elem.extract() for elem in nodelist]

    #remove all empty linefeed characters between nodes
    linefeedlist = soup.findAll(text=True)
    for elem in linefeedlist:
        if len(str(elem).strip()) == 0:
            elem.extract()

    divlist = soup.findAll(nodes_to_purge)
    for div in divlist:
        div['to_delete'] = 1

    divlist = soup.findAll(nodes_to_purge, to_delete=True)
    while len(divlist) > 0:
        # find the innermost div which has not been seen
        currentdiv = divlist[0]
        nextdiv = currentdiv
        while nextdiv is not None:
            currentdiv = nextdiv
            nextdiv = currentdiv.find(nodes_to_purge,to_delete=True)

        textlist = currentdiv.findAll(text=True)

        # check out the ratio of links to regular text in this div
        if textlist is None:
            currentdiv.extract()
        elif len(textlist) == 0:
            currentdiv.extract()
        else:
            if _debug: print '*'*30
            if _debug: print "Div = " + str(currentdiv)
            textcount = 0
            linkcount = 0
            linktextlength = 0
            textlength = 0
            linkwords = 0
            textwords = 0
            linecount = 0
            #elem = currentdiv.find()
#             if elem is None:
#                 elem = currentdiv.find(text=True)

#             while elem is not None:
#                 # This is a text node
#                 if elem.__class__.__name__ == 'NavigableString':
#                     #print "Text Elements : " + str(elem)
#                     textcount += 1
#                     textlength += len(str(elem).strip())
#                     textwords += len(str(elem).strip().split())
#                 elif not elem.__dict__.has_key('name'):
#                     # Some elements with no tag names. Typically processing instructions and such stuff
#                     elem.extract()
#                 else:
#                     if _debug : print "Element : " + str(elem)

#                     textelements = elem.findAll(text=True)
#                     if _debug : print "Text Elements : " + str(textelements)
#                     text = ' '.join(textelements)
#                     if _debug : print "All text : " + text.encode('ascii','replace')
#                     textcount += len(textelements)
#                     textlength += len(text.strip())
#                     textwords += len(text.strip().split())

#                     linkelements = elem.findAll(link_nodes)
#                     if _debug : print "Link Elements: " + str(linkelements)
#                     linkcount += len(linkelements)
#                     for linkelement in linkelements:
#                         text = ' '.join(linkelement.findAll(text=True))
#                         linktextlength += len(str(text.encode('ascii','replace')).strip())
#                         linkwords += len(str(text.encode('ascii','replace')).strip().split())

#                 elem = elem.nextSibling

            links = currentdiv.findAll('a',href=True)
            linkcount = len(links)
            linkwords = [link.renderContents() for link in links if link.renderContents()]
            linktextlength = len(''.join(linkwords))
            linkwords = len(''.join(linkwords).split())

            text =  currentdiv.findAll(text=True)
            textcount = len(text) - linkcount
            textlength = len(''.join(text)) - linktextlength
            textwords = len(''.join(text).split()) - linkwords
            
            if _debug : print "link words " + str([link.renderContents() for link in links if link.renderContents()])
            if _debug : print "text words " + str(text)
            
            if _debug: print "text count " + str(textcount)
            if _debug: print "link count " + str(linkcount)

            if _debug: print "text length " + str(textlength)
            if _debug: print "link text length " + str(linktextlength)

            if _debug: print "text word count " + str(textwords)
            if _debug: print "link word count " + str(linkwords)

            if _debug: print '\n'

            if linktextlength == 0:
                linktextlength = 1

            ## if textlength == 0 and textwords > 0:
            ##     print '*'*30
            ##     print "Error : no words.. but word count is there"
            ##     print '*'*30

            if textcount == 0:
                textcount = 1
            if textwords == 0:
                # No text data found
                if _debug: print "Deleting div - no data found"
                currentdiv.extract()
            elif ( (textlength + linktextlength)/linktextlength) < 3:
                if _debug: print "Deleting div - too many links"
                currentdiv.extract()
            elif (textwords/textcount) < 5:
                if _debug: print "Deleting div - Too short sentences"
                currentdiv.extract()
            else:
                if _debug: print "div is ok"
                del(currentdiv['to_delete'])
            if _debug: print '*'*30
        divlist = soup.findAll(nodes_to_purge, to_delete=True)

    _page = stripHtml(unicode(BeautifulStoneSoup(str(soup), convertEntities=BeautifulStoneSoup.ALL_ENTITIES)))
    if _debug: print "Final page - " + _page.encode('utf-8')
    return _page

def escapexml(xmldata):
    xmldata = xmldata.replace("&amp;", "&")
    xmldata = xmldata.replace("&lt;", "<")
    xmldata = xmldata.replace("&gt;", ">")
    xmldata = xmldata.replace("&quot;", "\"")
    xmldata = xmldata.replace("\\|", "&pipe;")
    xmldata = xmldata.replace("\\$", "&dollar;")
    return xmldata

#why get_hash??

def cleanUnicode(val):
    """converts text to utf-8 unicode before sending it for extraction
    """
    if not isinstance(val, basestring):
        val = str(val)
        
    if not isinstance(val , unicode):
        return val.decode('utf-8','ignore')
    else:
        return val.encode('utf-8','encode').decode('utf-8','ignore')


def get_hash(page):
    try:
        hash_str = ''
        for key in sorted(page.keys()):
            if isinstance(page[key],(int,float,long,bool)):
                hash_str += str(page[key])
            elif isinstance(page[key],list):
                hash_str += ''.join(page[key])
            else:
		text = page[key]
		if not isinstance(text, unicode):
       		    text = text.decode('utf-8','ignore')
	        else:
	            text = text.encode('utf-8','ignore').decode('utf-8','ignore')
                hash_str += text.encode('utf-8','ignore')
        return md5.md5(hash_str).hexdigest()
    except:
        print traceback.format_exc()
        raise 
