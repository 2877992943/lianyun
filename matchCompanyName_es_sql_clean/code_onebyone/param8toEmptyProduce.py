#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv
import pandas as pd

from MySQLdb import cursors
import numpy as np


#from es_update_bulk import generate_actions

from elasticsearch import Elasticsearch,helpers
 
#import pylab as plt
import sys,time
reload(sys)
sys.setdefaultencoding('utf8')

from elasticsearch import Elasticsearch
es = Elasticsearch("123.57.62.29",time=500)

db = MySQLdb.connect(host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com',
                     user='yunker',
                     passwd="yunker2016EP",
                     db="xddb",
                     use_unicode=True,
                     charset='utf8',
                     cursorclass=cursors.DictCursor)




# db = MySQLdb.connect(host='rds0710650me01y6d3ogo.mysql.rds.aliyuncs.com',
#                      user='yunker',
#                      passwd="yunke2016",
#                      db="yunketest",
#                      use_unicode=True,
#                      charset='utf8',
#                      cursorclass=cursors.DictCursor)



def query_cid():
    sql = """
        SELECT CLue_Id
        FROM crm_t_clue
        """
    df=pd.read_sql(sql,db);print df.shape
    return df


def query_by_att(attList,clue_ids):
    sql = """
        SELECT CLue_Id,%s
        FROM crm_t_clue
        WHERE CLue_Id in ('%s')

    """
    cur = db.cursor()
    #print sql % (','.join(attList),"','".join(clue_ids))
    cur.execute(sql % (','.join(attList),"','".join(clue_ids)))


    ret = {}
    for r in cur.fetchall():
       ret[r['CLue_Id']] = r #{id:{record},...}
    return ret





def update_sql_byAttribute(cid,field,value):
    cur = db.cursor()
    sql="""update crm_t_clue set %s = '%s' where CLue_Id='%s'"""
    print sql%(field,value,cid)
    cur.execute( sql%(field,value,cid))

    ####
    db.commit()
    cur.close()




if __name__ == "__main__":
    df=query_cid()
    df.to_csv('../data/all_cid.csv',index=False,encoding='utf-8')

    """
    df=pd.read_pickle('../data/all_cid.csv')
    cidlist_all=df['CLue_Id'].values.tolist()
    num=len(cidlist_all)
    batch_size=100000
    for batch in range(int(num/batch_size)+1)[:1]:
        cidlist=cidlist_all[batch*batch_size:batch*batch_size+batch_size]
        ret=query_by_att(['main_produce','param8'],cidlist)
        ### main produce empty
        cid_newMainProduce={}
        for cid,dic in ret.items():
            mp=dic['main_produce']
            p8=dic['param8']
            if isinstance(p8,float) and math.isnan(p8) or p8 in ['NULL','null',None,'']:continue
            ## param8 not empty
            if isinstance(mp,float) and math.isnan(mp) or mp in ['NULL','null',None,'']:
                cid_newMainProduce[cid]=p8
                print [mp],[p8]
            if len(cid_newMainProduce)>0:break

        #########
        for cid,param8 in cid_newMainProduce.items():
            print cid
            #update_sql_byAttribute(cid,'main_produce',param8)
    """









































