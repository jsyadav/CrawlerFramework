
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
import traceback

import urllib
import urllib2
import logging
import cookielib
import signal
from ConfigParser import ConfigParser

#from lib.decorators import *

log = logging.getLogger('HTTPConnection')

class HTTPConnection:
    cookie_jar = cookielib.LWPCookieJar()
    def __init__(self, useragent='Mozilla/5.0 (Windows; U; Win98; en-US; rv:1.8.1) Gecko/20061010', auth=None,
                 cookie=True, redirect=True, proxyhost=None, proxyport=None):
        try:
            print "httpconnection __init__"
            self.config = ConfigParser()
            self.config.read('/Users/cnu/Projects/Serendio/Crawler/crawler.cfg')
            self.useragent = useragent
            self.auth = auth
            self.proxyhost = proxyhost
            self.proxyport = proxyport
            handlers = []
            if self.auth is not None:
                # Auth Hanlers aren't supported yet
                pass

            # if self.proxyhost is not None and self.proxyport is not None:
            #     proxy = proxyhost + proxyport
            #     proxy_handler = urllib2.ProxyHandler({'http': proxy})
            #     handlers.append(proxy_handler)

            if cookie:
                cookie_handler = urllib2.HTTPCookieProcessor(self.cookie_jar)
                handlers.append(cookie_handler)
                print cookie_handler

            if redirect:
                redirect_handler = urllib2.HTTPRedirectHandler()
                handlers.append(redirect_handler)
                print redirect_handler
            print handlers
            print "building opener"
            import pdb
            pdb.set_trace()
            self.opener = urllib2.build_opener(cookie_handler)
            # for handler in handlers:
            #     self.opener = urllib2.build_opener(*handlers)
            print self.opener
            print "httpconnection __init__ finished"
        except:
            print traceback.format_exc()

    def createrequest(self, url, data=None, headers={}):
        if len(self.useragent) > 0:
            headers['User-Agent'] = self.useragent
        if data is not None:
            if isinstance(data,dict):
               data = urllib.urlencode(data)
        self.request = urllib2.Request(url, data, headers)

    def fetch(self,timeout=10): #default timeout 10 seconds
        #log.debug('Fetching (%s)' %(self.request.get_full_url()))
        try:
            urllib2.socket.setdefaulttimeout(timeout) 
            response = self.opener.open(self.request)
        except Exception,e:
            raise e
        finally:
            urllib2.socket.setdefaulttimeout(None)
        return response


