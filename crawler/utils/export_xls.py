
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#! /usr/bin/python
import datetime
import getopt
import sys

import pyExcelerator as xl
import os
import simplejson
import turbogears

sys.path.append(os.getcwd().rsplit(os.sep,2)[0])
sys.path.append(os.getcwd().rsplit(os.sep,1)[0])

#Path abs path to to dev.cfg like /home/ashish/Turbogears/KnowledgeMateR/dev.cfg
turbogears.update_config(configfile='../../dev.cfg', modulename='knowledgemate.config')
from crawler.tgimport import *
from turbogears.database import session
from knowledgemate import model
from knowledgemate.pysolr import Solr
from ConfigParser import ConfigParser
import traceback
class get_xls():

    def get_solr_data(self,config_file,filename,solr_query,show_sample ):
        config_data = ConfigParser()
        config_data.read(config_file)
        mapping = dict(config_data.items('mapping'))
        try:
            keywords=map(lambda x : ' ' + x + ' ' , config_data.get('global','keywords').split(','))
        except:
            keywords=[]
        try:
            num_rows_sheet = int(config_data.get('global','num_rows_sheet'))
        except:
            num_rows_sheet=500
        columns = config_data.get('columns','columns').split(',')
        try:
            field_sep = config_data.get('multifield_columns','field.sep')
        except:
            field_sep=','

        rev_mapping = dict([[v,k] for k,v in mapping.items()])
        for col in columns:
            rev_mapping[col] = rev_mapping.get(col,col)
        try:
            multifield_columns={}
            for option in config_data.options('multifield_columns'):
                if option != field_sep:
                    multifield_columns[option] = config_data.get('multifield_columns',option).split(',')
        except:
            pass

        connector_instance_ids = map(lambda x : int(x) , config_data.get('global','connector_instance_ids').split(','))
        
        connector_instances = session.query(model.ConnectorInstance).filter(model.ConnectorInstance.id.in_(connector_instance_ids)).all()
        if not connector_instances:
            print "no connector instances found for these ids %s" ,",".join(connector_instance_ids)
            
        client = session.query(model.Workspace).filter(model.Workspace.id == connector_instances[0].workspace_id).first().client

        solr = Solr(tg.config.get(path='solr',key='url')+'/'+str(client.name)+'_solr/')
        solr.commit()
        data = []
        
        for connector_instance in connector_instances:
            hits = solr.search(q ='connector_instance_id:%s'%connector_instance.id+solr_query,qt='article_list',rows=10000)['hits']
            print simplejson.loads(connector_instance.instance_data)['uri'] , len(hits)
            data.extend(hits)

        temp_data=[]
        for d in data:
            temp_data.append(dict([ [k.lower(),v] for k,v in  d.items()]))
        data=temp_data

        if show_sample:
            junk = ['last_updated_time','category','content','priority','parent_id','pickup_date','workspace_id','versioned','task_log_id','first_version_id','connector_instance_log_id','client_id','entity','level','connector_instance_id','id','snippet','client_name']

            keys=[]
            for d in data:
                keys.extend(filter(lambda x : x not in junk , d.keys()))
            solr_fields = list(set(keys))
            print "solr_fields superset :: "
            for k in sorted(solr_fields):
                print "%s:%s" %(k,mapping.get(k,''))
            return

        for d in data:
            for key,value in d.iteritems():
                if isinstance(value, list):
                    d[key] = ' '.join(map(lambda x : str(int(x)) if isinstance(x , (int,float)) else x , value))

        for d in data:
            date = datetime.datetime.strptime(d['posted_date'] , '%Y-%m-%dT%H:%M:%SZ')
            if date.timetuple()[3:6] != (0,0,0):
                d['posted_date'] = datetime.datetime.strftime(date,"%b %d, %Y %I:%M %p")
            else:
                d['posted_date'] = datetime.datetime.strftime(date,"%b %d, %Y")

        unique_data = []
        for d in data:
            if not d in unique_data:
                unique_data.append(d)

        data = unique_data
        try:
            parent_pages = dict([ [ d['id'].split('-')[0] , d['title'] ] for d in [d for d in data if '-' not in d['id']] ])
        except Exception ,e:
            print e
            parent_pages = {}
    
        data = [d for d in data if d['data'].strip()]

        for d in data:
            if 'Product name' in columns:
                d['Product name'] = parent_pages.get(d['id'].split('-')[0],'')
            if 'Comments' in columns:
                comments = [c for c in data if c['id'].startswith(d['id']+'-')]
                d['Comments'] = [c['et_author_name']+ ' on ' + c['posted_date'] + ' commented :: '+c['data'] if c.get('et_author_name') else c['data'] for c in comments]
            if 'Keywords'in columns:
                d['Keywords'] = [','.join( map( lambda x: x.strip() , filter( lambda x : re.search( re.compile(x , re.DOTALL| re.IGNORECASE) , d['data'] ),keywords) )) or '']
                for c in d['Comments']:
                    d['Keywords'].append(','.join(map( lambda x:x.strip() , filter( lambda x : re.search( re.compile(x ,re.DOTALL| re.IGNORECASE) ,c ),keywords))) or '')
                
        data = [d for d in data if  re.search('A[0-9]+-?[^-]?$',d['id'])]
        mapped_data=[]
        for d in data:
            temp={}
            for k,v in rev_mapping.iteritems():
                if k in multifield_columns.keys():
                    temp[k] = ','.join([d.get(rev_mapping.get(value,''),'') for value in multifield_columns[k]])
                else:
                    temp[k] = d.get(v,'')

                temp['Comments'] = d.get('Comments',[])
                temp['Product name'] = d.get('Product name','')
                temp['Keywords'] =d.get('Keywords',[])
            mapped_data.append(temp)
        self.create_workbook()
        self.get_xls(mapped_data,columns,num_rows_sheet)
        self.save_doc(str(filename).replace(' ','_'))

    def create_workbook(self):
        self.mydoc = xl.Workbook()

    def save_doc(self,file):
        filepath = './'+str(file+'.xls').replace('/','_').replace(' ','_')
        print "saving to " + filepath
        self.mydoc.save(filepath)

    def get_xls(self,result,columns,num_rows_sheet,prefix='sheet'):
        pattern_to_replace = re.compile('[\x7F-\xFF]+')
        filtered_columns = filter(lambda x : x.lower() not in ['comments','keywords'] , columns)
        suffix = 0
        i = 0        
        split_data = []    
        while i < len(result):
            split_data.append(result[i:i+num_rows_sheet])   
            i+=num_rows_sheet
        i = 0
        for result in split_data:
            suffix +=1
            mysheet = self.mydoc.add_sheet(prefix+'-' + str(suffix))
            header_font=xl.Font() #make a font object                                                           
            header_font.bold=True
            header_font.underline=True
            header_style = xl.XFStyle()
            header_style.font = header_font
            row = 0
            col = -1
            for key in columns:
                col+=1
                mysheet.write(0,col,key,header_style)
            for res in result:
                col=-1
                row+=1
                for key in filtered_columns:
                    res[key] = res.get(key,'')
                    col+=1
                    length = 32000
                    try:
                        res[key] = str(int(res[key])) if isinstance(res[key],(int,float)) else res[key]
                        res[key] = re.sub(pattern_to_replace,'',res[key]).encode('utf-8','ignore')
                        mysheet.write(row,col,res[key].decode('utf-8','ignore')[:length])
                    except Exception ,e:
                        print traceback.format_exc()
                        try:
                            mysheet.write(row,col,res[key][:length])
                        except Exception ,e:
                            print traceback.format_exc()
                            

                    if res.get('Keywords'):
                        try:
                            keyword = res['Keywords'][0]
                            mysheet.write(row,columns.index('Keywords'),re.sub('^,+','',keyword))
                            keywords_l = res['Keywords'][1:]
                        except Exception,e:
                            print traceback.format_exc()
                try:
                    init_row = row
                    for index,value in enumerate(res.get('Comments',[])):
                        try:
                            row+=1
                            value = re.sub(pattern_to_replace,'',value).encode('utf-8','ignore')
                            mysheet.write(row,columns.index('Comments'),value.decode('utf-8','ignore')[:32000])
                            keyword = keywords_l[index]
                            mysheet.write(row,columns.index('Keywords'),re.sub('^,+','',keyword))
                        except Exception,e:
                            print traceback.format_exc()
                except Exception,e:
                    print e
        return True


def usage():
    print "usage: ./export_xls.py --config_file=<config file> --filename=<filename> --solr query=< optional solr query> --show_sample<optional>"
    sys.exit(1)

if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", ["config_file=","solr_query=","filename=","show_sample"])
        config_file = filename=''
        solr_query=''
        show_sample=False
        print opts,args
        for o, a in opts:
            if o == "--config_file":
                config_file = a
            elif o == '--filename':
                filename=a
            elif o == '--show_sample':
                show_sample=True
            elif o == '--solr_query':
                solr_query=a
        if not os.path.exists(config_file) or os.path.exists(os.path.join(os.getcwd(),'export_config',filename)):
            print "config file not found"
        if config_file and filename:
            get_xls_ins = get_xls()
            get_xls_ins.get_solr_data(config_file,filename,solr_query,show_sample)
        else:
            usage()
    except getopt.GetoptError,e:
        # print help information and exit: 
        print e
