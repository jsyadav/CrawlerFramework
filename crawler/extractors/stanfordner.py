
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

import re
import socket
import logging
import traceback

#from lib import *
#from baseextractor import BaseExtractor

#log = logging.getLogger('StanfordNER')
class StanfordNER():
    def extract(self, data):
        text = self.data.encode('ascii','replace')
        text = re.sub(r'(\n|\r|\t)', ' ', text)
        host = self.config.get('StanfordNER','host')
        port = self.config.get('StanfordNER','port')
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, int(port)))
        s.send(text)
        s.send("\n")
        rawoutput = []
        while True:
            tempdata = s.recv(8192)
            if not tempdata: break
            rawoutput.append(tempdata)
        result = ' '.join(rawoutput)
        
        #Now split by word and process each one
        words = map(lambda l: tuple(l.rsplit("/",1)), result.split())
        words = filter(lambda x: len(x) > 1, words)
        result_dict = {'PERSON':set(),
                       'ORGANIZATION':set(),
                       'LOCATION':set(),
                       }
        valid_types = ['PERSON', 'ORGANIZATION', 'LOCATION','O']
        current_name = ''
        current_type = ''
        try:
            for word,type in words:
                if type == 'O':
                    if current_type != '' and current_type in valid_types:
                        result_dict[current_type].add(current_name.strip())
                        current_name = ''
                        current_type = ''
                else:
                    if current_type == type:
                        current_name = current_name + " " + word
                    else:
                        if current_type != '' and current_type in valid_types:
                            result_dict[current_type].add(current_name.strip())
                        current_type = type
                        current_name = word
        except Exception, e:
            print "Exception " + str(e)
            #print traceback.format_exc()
            traceback.print_exc()
            #log.debug(traceback.format_exc)
            
        if current_type != '' and current_type in valid_types:
            result_dict[current_type].add(current_name.strip())
            
        return result_dict
