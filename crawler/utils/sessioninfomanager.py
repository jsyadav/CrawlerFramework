
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

import pprint
import copy
import re
import traceback
import logging
log = logging.getLogger('SessionInfoManager')


#Boolean logic
# exists | update | checksessioninfo | action by connector

# T        F          T                   dont add but Continue
# F        F          F                   add and continue

# T        T          F                   add and continue
# F        T          F                   add and continue

"""
New session info structure:

{
(a,): {'entity':'Post', ...other attributes}
(a,b,): {'entity':'Review', ...other attributes}
(a,b,c,):{'entity':'Comment', ...other attributes}
.
.
.
.
}

where (a,) , (a,b,c,) ...etc are paths and a/b/c - are unique identifiers for the entity - could be url/hash/etc...

>> using paths make it a flat structure.
"""

def getSessionInfoHashReview(genre,session_info,link,parent_list):
    '''
    Given entity,parent (path), 
    return the hash value
    '''
    path = tuple(parent_list+[link])
    if session_info.has_key(path) and \
            not session_info.get(path).get('delete_status'):
        return session_info[path]['hash']

def checkSessionInfoReview(genre, session_info, link, update, parent_list=[]):
    """
    returns True or False
    True: happens only if updates=False - chk for all - post/rev/comm
    False: could be a new or old entity, response depends on the configuration - update and versioned and delete status of an entity
    """
    path = tuple(parent_list+[link])
    if session_info.getattr(path) and \
            session_info.getattr(path).get('delete_status') == True:
        return True
    if not update:
        return (path in session_info.keys())
    return False

def updateSessionInfoReview(genre, session_info, link, 
                            hash, entity, update, 
                            parent_list=[], Id=None):
    """
    """
    path = tuple(parent_list+[link])
    updated = False
    if path not in session_info.keys():#creating a new one 
        session_info[path] =  {'entity':entity, 'hash':hash}
        session_info['genre'] = genre
        updated = True
    else:
        #updating an old one 
        if session_info[path]['hash'] != hash:
            session_info[path] = {'entity':entity, 'hash':hash}
            updated = True
    return {'updated':updated}


def doDeleteOnSessionInfoReview(session_info, solr_id):
    """
    """
    try:
        __markit(session_info, solr_id)
        #print session_info
    except:
        print traceback.format_exc()
        raise

def __markit(dictionary, idx):
    """
    Recursive method to mark the document with a given id and
    the successors (children) thereof as delete_status = true
    """
    try:
        for key, value in dictionary.items():
            if type(value)==dict:
                __markit(value, idx)
            else:
                #print idx,value, type(idx), type(value)
                if key=='id' and re.match(idx, str(value)):
                    dictionary['delete_status']=True
    except:
        print traceback.format_exc()
        raise


def checkSessionInfoSearch(genre, session_info, timestamp, update, parent_list):
    """
    {timestamp:timestamp, id:id, first_version_id:first_version_id}
    """
    if not session_info or session_info['timestamp']<timestamp or session_info.get('delete_status')==True:
        return False
    return True

def updateSessionInfoSearch(genre, session_info, timestamp, hash, entity, update, parent_list=[], Id=None):
    try:
        if Id:
            session_info['id']=Id
            session_info['first_version_id']=Id
            session_info['genre']=genre
        session_info['timestamp']=timestamp
        
        return {'id':session_info['id'],
                'updated':True,
                'first_version_id':session_info['first_version_id']
                }
    except:
        return {'updated':False}

def doDeleteOnSessionInfoSearch(session_info, solr_id):
    #a fool proof - fool's check
    if session_info['id']==solr_id:
        session_info['delete_status']=True


def getSessionInfoHashSearch(genre,session_info):
    '''
    Given entity,parent (path) for a session_info
    returns the hash value
    '''
    assert genre in genres
    if session_info and session_info.has_key('timestamp') and not session_info.get('delete_status'):
        return session_info['timestamp']

"""
JV - Apr 29th, 2009.
Seeing this code I think the polymorphism [FUNCTIONAL] that has been implemented here (by me) could be done much better.
But I do not have the time to do it now!
The genre's should have their own classes with the 3 methods each - the check, update and delete.
And the polymorphism [OO] must come from initiating the right class and calling just the chk/upd/del bound methods to objects.
The genre to class mapping still needs to be there though.
"""
genres={'review':[checkSessionInfoReview,updateSessionInfoReview,doDeleteOnSessionInfoReview,getSessionInfoHashReview],
        'generic':[checkSessionInfoReview,updateSessionInfoReview,doDeleteOnSessionInfoReview,getSessionInfoHashReview],
        'search':[checkSessionInfoSearch,updateSessionInfoSearch, doDeleteOnSessionInfoSearch,getSessionInfoHashSearch]
        }

def checkSessionInfo(genre, session_info, unique_identifier, update, parent_list=[]):
    genre=genre.lower()
    assert genre in genres
    return (lambda: genres[genre][0](genre, session_info, unique_identifier, update, parent_list))()


def updateSessionInfo(genre, session_info, unique_identifier, meta_info, entity, update, parent_list=[], Id=None):
    ##Note
    ###Id is a unsused param here - mantained for legacy purposes.
    ##
    genre=genre.lower()
    assert genre in genres
    session_info['genre']=genre#intentionally at wrong place - so that all the records get update for once
    return (lambda: genres[genre][1](genre, session_info, unique_identifier, meta_info, entity, update, parent_list, Id))()

def doDeleteOnSessionInfo(session_info, solr_id):
    """
    unique_identifier(here) - solr_id
    in 'article':  derive the task_log_id from the solr_id -> get uri -> get latest task_log_id of the uri ->get session_info
    assumption - the genre will come within session_info (or from solr).
    """
    genre=session_info.get('genre')
    genre=genre.lower()
    assert genre in genres
    try:
        (lambda: genres[genre][2](session_info, solr_id))()
        return True
    except:
        print traceback.format_exc()
        print "error in deleting the article"
        return  False

if __name__=='__main__':
    pass
