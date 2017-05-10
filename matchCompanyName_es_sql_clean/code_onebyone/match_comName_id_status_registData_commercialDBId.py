#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv
import pandas as pd

from MySQLdb import cursors
import numpy as np

from menu_to_companyName_product_list import valid_date_form
#from es_update_bulk import generate_actions

from elasticsearch import Elasticsearch,helpers
 
#import pylab as plt
import sys,time
reload(sys)
sys.setdefaultencoding('utf8')

from elasticsearch import Elasticsearch
es = Elasticsearch("123.57.62.29",time=500)


# def get_cid_companyName():
#
#     sql = "SELECT clue_id,Clue_Entry_Com_Name from crm_t_clue where param2=1"
#     df=pd.read_sql(sql,db)
#     #df.to_csv("../allClueId.csv",index=False,encoding='utf-8')
#     return df
#
# def get_capital_address():
#     sql = """
# 	SELECT clue_id, registed_capital,Com_Address,Clue_Entry_Com_Name,com_type,employees_num
# 	from crm_t_clue
# 	limit 10000"""
#     """
#     cur = db.cursor()
#     #cur.execute(sql % "','".join(clue_ids))
#
#     cur.execute(sql)
#     ret = {}
#     for r in cur.fetchall():
#        ret[r['CLue_Id']] = r['registed_capital'] #{id:{record},...}
#     """
#     df=pd.read_sql(sql,db)
#
#     return df  #dict {id:{record},..}
#
#
#
# def get_new_cid():
#     sql = """
#         SELECT clue_id
#         from crm_t_clue
#         where Create_User_Account='system_20160825' or Create_User_Account='system_20160827' or Create_User_Account='card_20160901'
#         #limit 1
#         """
#     # 24 |25 27 901
#     df=pd.read_sql(sql,db)
#
#     return df
#
#
# def get_negative_idx(clueId): #too slow
#     print '1', clueId[0],len(clueId)
#     '''
#     Get negative   Clue_Id
#     '''
#     sql = """SELECT clue_id from crm_t_clue
#             where clue_id not in ('%s')
#             limit 10"""
#
#     df=pd.read_sql(sql % "','".join(clueId),db)
#     #df=pd.read_sql(sql,db)
#     df.to_csv("../negativeClueId.csv",index=False,encoding='utf-8')
#
#
#
# def get_positive_id():
#     '''
#     Get the positive_ids from phone_response_clue.txt ->phone ,id,
#     '''
#
#     positive_responses = set(['QQ', '短信', '注册', '邮箱', '预约' ,'微信'])
#     with open('../tianxiang/phone_response_clue.txt') as f:
#         lines = f.read().split('\n')[:-1]
#     oids = [l.split(',')[2] for l in lines if l.split(',')[1] in positive_responses]#id
#     teles = [l.split(',')[0] for l in lines if l.split(',')[1] in positive_responses]#tele
#
#     pd.DataFrame({'clueId':oids,'teles':teles}).to_csv("../clueId_tele.csv",index=False,encoding='utf-8')
#     return oids,teles
#
#
#
# def get_clue_idx(tels):
#     '''
#     Get Clue_Ids by Clue_Entry_Cellphone
#     '''
#
#     sql = """
#         SELECT CLue_Id
#         FROM crm_t_clue
#         WHERE Clue_Entry_Cellphone IN
#               ('%s')
#
#     """
#     df=pd.read_sql(sql % "','".join(tels),db)
#     df.to_csv("../teleQueryClueId.csv",index=False,encoding='utf-8')


def generate_que(att,val):
    que= {
            "from" : 0, "size" : 1,  #100 200 500 1000
            "query" : {
                    "match_phrase" : { att : val }
                     },
           #"fields":['CLue_Id','main_produce']
         }
    return que
 

def update_item(es_index,k,update_dict):
    try:
        es.update(index=es_index,doc_type='clue',id=k,body=update_dict)
        return 1
    except:
        #print 'not found in '+es_index
        return 0


def match_by_comname(name):
    ret={"registrationdate":'',"commercialDB_id":'',"com_status":'',"legal_person":'',"Com_Address":''}

    res=es.search(index="company_1",doc_type="company",body=generate_que("title",str(name)))
    hits=res['hits']['hits'];
    # if not matched
    if len(hits)<=0:
        return ret
    # if matched


    for hit in hits[:]:
        # mutual match
        name_c=hit['_source']['title']
        if name!=name_c:continue
        #####
        ret["commercialDB_id"]=hit['_id']
        if 'cstatus' in hit['_source'] and hit['_source']['cstatus']!=None:ret["com_status"]=hit['_source']['cstatus']
        if 'registrationdate' in hit['_source'] and hit['_source']['registrationdate']!=None:ret['registrationdate']=hit['_source']['registrationdate']
        if 'fazheng' in hit['_source'] and hit['_source']['fazheng']!=None:ret['registrationdate']=hit['_source']['fazheng']
        if 'address' in hit['_source'] and hit['_source']['address']!=None:ret['Com_Address']=hit['_source']['address']
        if 'legalperson' in hit['_source'] and hit['_source']['legalperson']!=None:ret['legal_person']=hit['_source']['legalperson']
    return ret






























