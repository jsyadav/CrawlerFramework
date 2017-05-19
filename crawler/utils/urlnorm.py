
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#!/usr/bin/env python

"""
urlnorm.py - URL normalisation routines

urlnorm normalises a URL by;
  * lowercasing the scheme and hostname
  * taking out default port if present (e.g., http://www.foo.com:80/)
  * collapsing the path (./, ../, etc)
  * removing the last character in the hostname if it is '.'
  * unquoting any %-escaped characters

Available functions:
  norms - given a URL (string), returns a normalised URL
  norm - given a URL tuple, returns a normalised tuple
  test - test suite
  
CHANGES:
0.92 - unknown schemes now pass the port through silently
0.91 - general cleanup
     - changed dictionaries to lists where appropriate
     - more fine-grained authority parsing and normalisation    
"""

__license__ = """
Copyright (c) 1999-2002 Mark Nottingham <mnot@pobox.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

__version__ = "0.93"

from urlparse import urlparse, urlunparse
from urllib import unquote
from string import lower
import re

_collapse = re.compile('([^/]+/\.\./?|/\./|//|/\.$|/\.\.$)')
_server_authority = re.compile('^(?:([^\@]+)\@)?([^\:]+)(?:\:(.+))?$')
_default_port = {   'http': '80',
                    'https': '443',
                    'gopher': '70',
                    'news': '119',
                    'snews': '563',
                    'nntp': '119',
                    'snntp': '563',
                    'ftp': '21',
                    'telnet': '23',
                    'prospero': '191',
                }
_relative_schemes = [   'http',
                        'https',
                        'news',
                        'snews',
                        'nntp',
                        'snntp',
                        'ftp',
                        'file',
                        ''
                    ]
_server_authority_schemes = [   'http',
                                'https',
                                'news',
                                'snews',
                                'ftp',
                            ]


def normalize(urlstring, site=None, base=None):
    """given a string URL, return its normalised form"""
    return urlunparse(_norm(urlstring, site, base))

def _norm(urlstring, site, base):
    """given a six-tuple URL, return its normalised form"""
    (scheme, authority, path, parameters, query, fragment) = urlparse(urlstring)
    scheme = lower(scheme)
    
    if scheme == 'javascript':
        return urlparse(site)
    # Form the right url
    if base is not None:
        # Check if it is a relative url
        if len(authority) == 0:
            (basescheme, baseauth, basepath, baseparam, basequery, basefrag) = urlparse(base)
            urlstring = urlunparse((basescheme, baseauth, basepath + urlstring, parameters, query, fragment))
    else:
        # No base href given
        if len(authority) == 0 and len(path) > 0: 
            if path[0] == '/':
                # we got a relative url from root
                (sitescheme, siteauth, sitepath, siteparam, sitequery, sitefrag) = urlparse(site)
                urlstring = urlunparse((sitescheme, siteauth, path, parameters, query, fragment))
            elif site is not None:
                # we have a relative url from current path
                (sitescheme, siteauth, sitepath, siteparam, sitequery, sitefrag) = urlparse(site)
                sitepath = sitepath[0:sitepath.rfind('/')+1]
                urlstring = urlunparse((sitescheme, siteauth, sitepath + path, parameters, query, fragment))

    (scheme, authority, path, parameters, query, fragment) = urlparse(urlstring)
    scheme = lower(scheme)
        
    if authority:
        userinfo, host, port = _server_authority.match(authority).groups()
        if host[-1] == '.':
            host = host[:-1]
        authority = lower(host)
        if userinfo:
            authority = "%s@%s" % (userinfo, authority)
        if port and port != _default_port.get(scheme, None):
            authority = "%s:%s" % (authority, port)
    if scheme in _relative_schemes:
        last_path = path
        while 1:
            path = _collapse.sub('/', path, 1)
            if last_path == path:
                break
            last_path = path
    path = unquote(path)
    # Removed named anchors - http://www.example.com/index.html#name
    return (scheme, authority, path, parameters, query,'') #, fragment)



def test():
    """ test suite; some taken from RFC1808. """
    tests = {    
        '/foo/bar/.':                    '/foo/bar/', 
        '/foo/bar/./':                   '/foo/bar/',
        '/foo/bar/..':                   '/foo/',
        '/foo/bar/../':                  '/foo/',
        '/foo/bar/../baz':               '/foo/baz',
        '/foo/bar/../..':                '/',
        '/foo/bar/../../':               '/',
        '/foo/bar/../../baz':            '/baz',
        '/foo/bar/../../../baz':         '/../baz',
        '/foo/bar/../../../../baz':      '/baz',
        '/./foo':                        '/foo',
        '/../foo':                       '/../foo',
        '/foo.':                         '/foo.',
        '/.foo':                         '/.foo',
        '/foo..':                        '/foo..',
        '/..foo':                        '/..foo',
        '/./../foo':                     '/../foo',
        '/./foo/.':                      '/foo/',
        '/foo/./bar':                    '/foo/bar',
        '/foo/../bar':                   '/bar',
        '/foo//':                        '/foo/',
        '/foo///bar//':                  '/foo/bar/',    
        'http://www.foo.com:80/foo':     'http://www.foo.com/foo',
        'http://www.foo.com:8000/foo':   'http://www.foo.com:8000/foo',
        'http://www.foo.com./foo/bar.html': 'http://www.foo.com/foo/bar.html',
        'http://www.foo.com.:81/foo':    'http://www.foo.com:81/foo',
        'http://www.foo.com/%7ebar':     'http://www.foo.com/~bar',
        'http://www.foo.com/%7Ebar':     'http://www.foo.com/~bar',
        'ftp://user:pass@ftp.foo.net/foo/bar': 'ftp://user:pass@ftp.foo.net/foo/bar',
        'http://USER:pass@www.Example.COM/foo/bar': 'http://USER:pass@www.example.com/foo/bar',
        'http://www.example.com./':      'http://www.example.com/',
        '-':                             '-',
        'http://www.foo.com./foo/bar.html#name': 'http://www.foo.com/foo/bar.html',
#        "javascript:__doPostBack('ctl00$MiddleColumnContent$GridControl$ProductsGrid','Page$2')": "",
    }

    n_correct, n_fail = 0, 0
    test_keys = tests.keys()
    test_keys.sort()            
    for i in test_keys:
        print 'ORIGINAL:', i
        cleaned = normalize(i)
        answer = tests[i]
        print 'CLEANED: ', cleaned
        print 'CORRECT: ', answer
        if cleaned != answer:
            print "*** TEST FAILED"
            n_fail = n_fail + 1
        else:
            n_correct = n_correct + 1
        print        
    print "TOTAL CORRECT:", n_correct
    print "TOTAL FAILURE:", n_fail


if __name__ == '__main__':
    test()
