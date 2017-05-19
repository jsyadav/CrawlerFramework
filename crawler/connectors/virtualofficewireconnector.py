
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''


import cgi
from utils.urlnorm import normalize
from utils.task import Task
from baseconnector import BaseConnector
import logging
import urlparse
log = logging.getLogger('VirtualOfficeWireConnector')
from utils.decorators import *

class VirtualOfficeWireConnector(BaseConnector):
    @logit(log, 'fetch')
    def fetch(self):
        try:
            if urlparse.urlparse(self.currenturi)[2] in ['/','']: 
                return True #in case one of the googlesearch links point to the site as http://www.virtualofficewire.com/ , i don't want to pick up some links
            res = self._getHTML()
            self.rawpage=res['result']
            self._setCurrentPage()
            article_link = cgi.parse_qs(self.soup.find('td',{'valign':'top'}).a['href']).get('url')
            if not article_link:
                return False
            temp_task=self.task.clone()
            temp_task.instance_data['uri']=normalize(article_link[0])
            self.linksOut.append(temp_task)
            return True
        except Exception,e:
            log.exception(self.log_msg('exception in fetch'))
            return False

    @logit(log, 'saveToSolr')
    def saveToSolr(self):
        return True
