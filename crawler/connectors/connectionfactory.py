
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#VAIDHY
#JV

import time
import urlparse

#import logging
#log = logging.getLogger('ConnectionFactory')

class ConnectionFactory(object):
    '''A singleton ConnectionFactory where all connectors must register themselves.'''

    # Singleton class : http://snippets.dzone.com/posts/show/651
    def __new__(type):
        if not '_the_instance' in type.__dict__:
            type._the_instance = object.__new__(type)
        return type._the_instance

    def getConnector(self, connectors, instance_data):
        # load the connector based on the site
        # We have to do fuzzy match

        #IF the instance_data has a category set use that as connClass else find it
        self.connectors=connectors
        url_segments = urlparse.urlparse(instance_data['uri'])
        if not url_segments.scheme:
            url_segments = urlparse.urlparse('http://' + instance_data['uri'])

        #if self.connectors.get(url_segments.scheme) is None:
        #    return None

        connclass = None
        connectors = self.connectors.get(url_segments.scheme, {})


        #Check for connector types and return one if specified by the user
        if instance_data.get('connector_name'):
            connector_name = instance_data['connector_name']
            del instance_data['connector_name'] #deleting this attribute , because otherwise it will return same connector for 
                                                #all the cases which might not be correct
            return connector_name


        # Check for known connectors
        if url_segments.scheme == 'nntp':
            return 'NNTPConnector'
        if url_segments.scheme == 'pop':
            return 'POPConnector'
        if url_segments.scheme == 'imap':
            return 'IMAPConnector'
        if url_segments.scheme == 'file':
            return 'FilesystemConnector'

#        print connectors
        url_loc = url_segments.netloc.split('.')
        url_path = url_segments.path.split('/')

        # keep removing sub-domains from the current netlocation
        for i,v in enumerate(url_loc):
            if connclass is not None:
                break
            current_loc = '.'.join(url_loc[i:])

            pathlen = len(url_path)
            # keep removing the last path segment from the path
            for j,w in enumerate(url_path):
                current_path = '/'.join(url_path[:pathlen - j])
                # check for the location and path
                current_url = current_loc + current_path
#                print "Checking for :", current_url
                if connectors.has_key(current_url):
                    connclass = connectors[current_url]
                    break

            if connectors.has_key(current_loc):
                connclass = connectors[current_loc]

        # keep removing last part from the netlocation
        # this handles sites like google which has country specific
        # extensions
        loclen = len(url_loc)
        for i,v in enumerate(url_loc):
            if connclass is not None:
                break
            current_url = '.'.join(url_loc[:loclen - i])
#            print "checking for : ", current_url
#            print connectors.get(current_url)
            if connectors.has_key(current_url):
                connclass = connectors[current_url]

        # No matches found in the connectors
        # Return google site search or generic connector
        if connclass is None:
            if ((len(url_segments.path) == 0 or
                    url_segments.path + url_segments.params + url_segments.query == '/') and
                    not instance_data.get('already_parsed')):
                connclass = 'GoogleSiteConnector' #('GoogleSiteConnector','', True)
            else:
                connclass = 'GenericConnector'#('GenericConnector','', False)
        #log.debug("Got connector %s for site %s" %(connclass, site.url))

        return connclass

    def register(self, name, class_):
        log.info('Registering %s' %(name))
        self.connectors[name] = class_

    def unregister(self, name):
        if name in self.connectors:
            del(self.connectors[name])



#         class_ = __import__(connclass[0].lower(), globals(), locals(), [connclass[0]],-1)
#         conn = class_.__dict__[connclass[0]](crawler, site)
#         conn.json_text = connclass[1]
#         conn.search_engine = connclass[2]
#         return conn
