
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

import logging
# from logging import config

from utils import taskmanager

# logging.config.fileConfig('logging_taskfeeder.cfg')
logging.raiseExceptions = 0 #should be set to 0
log = logging.getLogger('TaskFeeder')


#FOR ACCESSING THE MODEL and devdata.sqlite FROM knowlwdgemate

#from utils.utils import QueueManager


if __name__ == '__main__':
    # taskmanager.crawlNow(connector_instance_ids =['72fae31f-09f1-4e2f-9e45-60923003afb3'],
    #                      workspace_id='1b9fcc45-f681-49db-a514-59246d08ce32')
    taskmanager.crawlNow(connector_instance_ids =['72fae31f-09f1-4e2f-9e45-60923003afb3'],
                         workspace_id='90ff4ba7-d796-4d86-80db-f3af9a3e5d4f')
#    taskmanager.crawlNow(workspace_ids =[29,1])
#    taskmanager.crawlNow(workspace_id =1 ,connector_instance_ids=[13])
#     current_time = datetime.now() #utcnow() - removed for testing purposes
#     mm = current_time.minute
#     print mm
#     if mm == 15 or mm == 45 or mm == 9:
#         freq = 96
#     elif mm == 30:
#         freq = 48
#     else:
#         hh = current_time.hour
#         print hh
#         freq=96 #dumb code for testing
#         #freq = 24/hh
#     print freq

#     m=QueueManager.from_address(address=(tg.config.get(path='taskmaster', key='host'),
#                                          tg.config.get(path='taskmaster', key='port')),
#                                 authkey='none')
#     tm = m.proxyMaster()
#     log.debug('processing for frequency: %d' % (freq))
# #    tm.readSchedule(freq)
#     tm.readSchedule(1)
