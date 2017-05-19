from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from ..tgimport import *

cp = ConfigParser()
db_mapping_file_path = os.path.normpath(common_path +'/commonmodules/config/crawler.cfg')
cp.read(db_mapping_file_path)

db_mapping = dict([ [k,eval(v)] for k,v in cp.items('db_mapping')])

assert_config_key_re = re.compile('^(db[0-9]+?_ws[-0-9a-z]{36}|default)$')
assert all([assert_config_key_re.match(x) for x in db_mapping]) #Assert that config file follows the structure                              


extract_wid_key_re = re.compile('db[0-9]+?_ws([-0-9a-z]{36})|default')
db_mapping = dict([ [extract_wid_key_re.match(k).group(1),v] for \
                      k,v in db_mapping.items() if extract_wid_key_re.match(k).group(1)] ) #Extracts all DBids
 
db_mapping['default'] = eval(dict(cp.items('db_mapping'))['default'])

default_dburi = db_mapping['default']

#sessions = {}
#engines = {}

#Get Sessions
def getSession(workspace_id):
    '''
    Get Session for a given workspace_id
    '''
    dburi =  db_mapping.get(workspace_id,default_dburi) 
    return __getSession(dburi)


def getSessions():
    '''
    Get Session for all the databases
    Possible usecases, A new connector which needs to be installed in all the databases we use
    '''
    dburis = set(db_mapping.values())
    return [__getSession(dburi) for dburi in dburis]

def __getSession(dburi):
    '''
    Get Session for a given dburi
    '''
#     if not sessions.get(dburi):
#         engine = create_engine(dburi)
#         Session = sessionmaker(bind=engine, autoflush=True, transactional=True)
# #        Session = sessionmaker(bind=engine)
#         sessions[dburi] = Session()
#     return sessions[dburi]
    engine = __getEngine(dburi)
    Session = sessionmaker(bind=engine, autoflush=True, transactional=True)
    return Session()


#get Engines
def getEngine(workspace_id):
    '''
    Get Session for a given workspace_id
    '''
    dburi =  db_mapping.get(workspace_id,default_dburi) 
    return __getEngine(dburi)


def getEngines():
    '''
    Get Session for all the databases
    Possible usecases, A new connector which needs to be installed in all the databases we use
    '''
    dburis = set(db_mapping.values())
    return [__getEngine(dburi) for dburi in dburis]



def __getEngine(dburi):
    '''
    Get Session for a given dburi
    '''
    # if not engines.get(dburi):
    #     engine = create_engine(dburi)
    #     engines[dburi] = engine
    # return engines[dburi]
    return create_engine(dburi)



#get Engines
def getDburi(workspace_id):
    '''
    Get Session for a given workspace_id
    '''
    return  db_mapping.get(workspace_id,default_dburi) 


def getDburis():
    '''
    Get Session for all the databases
    Possible usecases, A new connector which needs to be installed in all the databases we use
    '''
    return list(set(db_mapping.values()))

