
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''


#ASHISH YADAV


import logging
#Requires utils.authlib.klout module, which has code for determining when the connector gets rate-limited
from utils.authlib import klout 
from tgimport import *
from baseconnector import BaseConnector
from utils.authlib.apilib import NoActiveHandlersAvailable

log = logging.getLogger('KloutConnector')
class KloutConnector(BaseConnector):

    def fetch(self):                                                                                         
        #Add klout objects to the pool
        self.objectpool.addObjects(obj_class=klout.Klout, pool_key='klout',args_section='klout')
        
        
        #start using the pool
        try:
            data = self.objectpool.get('klout').fetch(self.currenturi) 
            log.info(data)
        except NoActiveHandlersAvailable, e:
            raise e

        #Proceed working with data so crawled

