
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''
import pickle
import pprint
import datetime

d = {'key0': {'attrs': {'entity': 'p', 'hash': '34nj3h43b4n3', 'id': '4130'},
          u'key1': {'attrs': {'entity': 'r',
                              'hash': '34njasd3h43b4n3',
                              'id': '4130-1'},
                    u'key2': {'attrs': {'entity': 'c',
                                        'hash': '34njasd3h43bdsfsd4n3',
                                        'id': '4130-1-1'}}},
          u'key3': {'attrs': {'entity': 'r',
                              'hash': '34njasasasd3h43b4n3',
                              'id': '4130-2'},
                    u'key4': {'attrs': {'entity': 'c',
                                        'hash': '34njawersd3h43bdsfsd4n3',
                                        'id': '4130-2-1'}},
                    u'key5': {'attrs': {'entity': 'c',
                                        'hash': '34njawersd3h43bdsfsd4n3',
                                        'id': '4130-2-2'}}}},
     'someohterthing': 'someothervalue',
     'something': 'somevalue'}

d1 = {'key0': {'attrs': {'entity': 'p', 'hash': '34nj3h43b4n3', 'id': 4130},
          u'key1': {'attrs': {'entity': 'r',
                              'hash': '34njasd3h43b4n3',
                              'id': 4131},
                    u'key2': {'attrs': {'entity': 'c',
                                        'hash': '34njasd3h43bdsfsd4n3',
                                        'id': 4132}}},
          u'key3': {'attrs': {'entity': 'r',
                              'hash': '34njasasasd3h43b4n3',
                              'id': 4133},
                    u'key4': {'attrs': {'entity': 'c',
                                        'hash': '34njawersd3h43bdsfsd4n3',
                                        'id': 4134}},
                    u'key5': {'attrs': {'entity': 'c',
                                        'hash': '34njawersd3h43bdsfsd4n3',
                                        'id': 4135}}}},
     'someohterthing': 'someothervalue',
     'something': 'somevalue'}


class SessionInfo(dict):


    def setattr(self, k, attrDict):
        self.update({k: attrDict})
        return True


    def delete(self, k):
        """ola!!"""
        l=len(k1)
        for k2 in self.keys():
            if k2[:l] == k1:
                self[k2]['delete'] = True


    def getattr(self, k):
        result=self.get(k, None)
        return result

    def __str__(self):
        return str(dict(self.items()))

    def __repr__(self):
        return self.__str__()


def makeSessionInfoObj(session_info):
    """converts old session info format - the strutural dependent one to the new one - flat
    for one itme use (most probably)"""
    sessobj = SessionInfo()
    def walkDict( aDict, path=()):
        for k in aDict:
            if k == 'attrs':
                sessobj.setattr(path+(aDict[k]['id'],), aDict[k] )
            elif type(aDict[k]) != dict:
                sessobj.addTopLevelInfo(k, aDict[k])
            else:
                walkDict( aDict[k], path+(k,) )
    walkDict( session_info )
    return sessobj

def testSessionInfo():
    sessioninfo=SessionInfo()
    sessioninfo[('some',)] = 'someother'
    sessioninfo[('link',)]={'hash':232323}
    print sessioninfo
    for k,v in sessioninfo.items():
        if k==('link',):
            sessioninfo[k+(43,)] = v
            v['id']=43
            sessioninfo.pop(k)
    sessioninfo.transform()
    print sessioninfo
    sessioninfo.reverseTransform()
    print sessioninfo

if __name__ == '__main__':
    pass
