#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv
import pandas as pd

from MySQLdb import cursors
import numpy as np

from menu_to_companyName_product_list import valid_date_form,remove_punct
from match_comName_id_status_registData_commercialDBId import match_by_comname
from process_name_comname_birthday import com_name,leads_name,birthday
from process_qq_email_position_phone_produce import qq,email,telephone,position,clean_produce
from capital import process_capital

#from es_update_bulk import generate_actions

from elasticsearch import Elasticsearch,helpers
 
#import pylab as plt
import sys,time
reload(sys)
sys.setdefaultencoding('utf8')

from elasticsearch import Elasticsearch
es = Elasticsearch("123.57.62.29",time=500)

read_host='rr-2zeg40364h2thw9m6o.mysql.rds.aliyuncs.com'
write_host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com'
db = MySQLdb.connect(host=write_host,
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

def get_cid_companyName():

    sql = "SELECT clue_id,Clue_Entry_Com_Name from crm_t_clue where param2=1"
    df=pd.read_sql(sql,db)
    #df.to_csv("../allClueId.csv",index=False,encoding='utf-8')
    return df

def get_capital_address():
    sql = """
	SELECT clue_id, registed_capital,Com_Address,Clue_Entry_Com_Name,com_type,employees_num
	from crm_t_clue
	limit 10000"""
    """
    cur = db.cursor()
    #cur.execute(sql % "','".join(clue_ids))

    cur.execute(sql)
    ret = {}
    for r in cur.fetchall():
       ret[r['CLue_Id']] = r['registed_capital'] #{id:{record},...}
    """
    df=pd.read_sql(sql,db)

    return df  #dict {id:{record},..}



def get_new_cid():
    sql = """
        SELECT clue_id
        from crm_t_clue
        where Create_User_Account='system_20160825' or Create_User_Account='system_20160827' or Create_User_Account='card_20160901'
        #limit 1
        """
    # 24 |25 27 901
    df=pd.read_sql(sql,db)

    return df


def get_negative_idx(clueId): #too slow
    print '1', clueId[0],len(clueId)
    '''
    Get negative   Clue_Id
    '''
    sql = """SELECT clue_id from crm_t_clue
            where clue_id not in ('%s')
            limit 10"""

    df=pd.read_sql(sql % "','".join(clueId),db)
    #df=pd.read_sql(sql,db)
    df.to_csv("../negativeClueId.csv",index=False,encoding='utf-8')



def get_positive_id():
    '''
    Get the positive_ids from phone_response_clue.txt ->phone ,id,
    '''

    positive_responses = set(['QQ', '短信', '注册', '邮箱', '预约' ,'微信'])
    with open('../tianxiang/phone_response_clue.txt') as f:
        lines = f.read().split('\n')[:-1]
    oids = [l.split(',')[2] for l in lines if l.split(',')[1] in positive_responses]#id
    teles = [l.split(',')[0] for l in lines if l.split(',')[1] in positive_responses]#tele

    pd.DataFrame({'clueId':oids,'teles':teles}).to_csv("../clueId_tele.csv",index=False,encoding='utf-8')
    return oids,teles



def get_clue_idx(tels):
    '''
    Get Clue_Ids by Clue_Entry_Cellphone
    '''

    sql = """
        SELECT CLue_Id
        FROM crm_t_clue
        WHERE Clue_Entry_Cellphone IN
              ('%s')

    """
    df=pd.read_sql(sql % "','".join(tels),db)
    df.to_csv("../teleQueryClueId.csv",index=False,encoding='utf-8')

def query_comNameClean_null():
    sql = """
        SELECT CLue_Id
        FROM crm_t_clue
        where clue_entry_com_name_clean is null or clue_entry_com_name_clean='' """
    df=pd.read_sql(sql,db)
    return df

def query_by_time(time):
    sql="""
    select CLue_Id
    from crm_t_clue
    where date(edit_time)>'%s'
    """
    df=pd.read_sql(sql%time,db)
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


def visual_value(k_v):
    print '*'*20
    for k,v in k_v.items():
        print k, v


def update_sql_byAttribute(cid,field,value):
    cur = db.cursor()
    sql="""update crm_t_clue set %s = '%s' where CLue_Id='%s'"""
    #print sql%(field,value,cid)
    cur.execute( sql%(field,value,cid))
    ####
    db.commit()
    cur.close()

def clean_allAttribute_1by1(cid):
    ret_sql=query_by_att(attList,[cid])
    #visual_value(ret_sql[cid])

    ##############################################
    # query es companyDB to find :commercialDB_id,

    comname=ret_sql[cid]['Clue_Entry_Com_Name']
    ret_es=match_by_comname(comname.strip(' ')) #ret {"registrationdate":'',"commercialDB_id":'',"com_status":'',"legal_person":'',"Com_Address":''}
    ## legalperson and address
    legal_person_old=ret_sql[cid]['legal_person']
    address_old=ret_sql[cid]['Com_Address']
    if isinstance(legal_person_old,float) and math.isnan(legal_person_old) or legal_person_old in ['NULL','null',None,'']:legal_person_old=ret_es['legal_person']
    #if isinstance(address_old,float) and math.isnan(address_old) or address_old in ['NULL','null',None,''] or len(address_old.decode('utf-8'))<len(ret_es['Com_Address'].decode('utf-8')):address_old=ret_es['Com_Address']
    clean_sql_dict['legal_person_clean']=legal_person_old
    clean_sql_dict['Com_Address_clean']=remove_punct(address_old)
    clean_sql_dict["registrationdate"]=ret_es["registrationdate"]
    clean_sql_dict["commercialDB_id"]=ret_es["commercialDB_id"]
    clean_sql_dict["com_status"]=ret_es["com_status"]
    ##########################################
    # capital
    cap=process_capital(ret_sql[cid]['registed_capital'])
    clean_sql_dict['registed_capital_num']=cap['digit']
    clean_sql_dict['registed_capital_currency']=cap['char']
    ############################
    #  other attribute
    fieldList=['Clue_Entry_Name','Clue_Entry_Com_Name','Clue_Entry_Cellphone','Clue_Entry_Qq','Clue_Entry_Email','Clue_Entry_Major',\
             'Clue_Entry_Birthday','main_produce']
    methodList=[leads_name,com_name,telephone,qq,email,position,birthday,clean_produce]
    fieldDict=dict(zip(fieldList,methodList))
    for field,method in fieldDict.items():
        value=ret_sql[cid][field]

        if isinstance(value,float) and math.isnan(value) or value in ['',None,'null']:
            rst=''
        else:
            rst=method(value);#print 'new',rst
        clean_sql_dict[field+'_clean']=rst
    #visual_value(clean_sql_dict)



    ###### update sql
    for k,v in clean_sql_dict.items():

        if k=="commercialDB_id" and v!='':v=int(v)
        if k in ["commercialDB_id",'Clue_Entry_Birthday_clean',"registrationdate",'registed_capital_num'] and v=='':continue

        if k=='registed_capital_num' and v!='':v=int(v)


        update_sql_byAttribute(cid,k,v)


if __name__ == "__main__":


    ### query sql comname_clean isnull
    # df=query_comNameClean_null();print df.shape
    # df=query_by_time('2017-02-01')
    # df.to_csv('../data/comname_null.csv',index=False,encoding='utf-8')


    df=pd.read_csv('../data/comname_null.csv')
    cidlist=df['CLue_Id'].values.tolist();print len(cidlist)




    attList=['Clue_Entry_Name','Clue_Entry_Com_Name','Clue_Entry_Cellphone','Clue_Entry_Qq','Clue_Entry_Email','Com_Address','Clue_Entry_Major',\
             'Clue_Entry_Birthday','legal_person','registed_capital','main_produce'] # employees  turnover  some range some value
    clean_sql_list=['Clue_Entry_Name_clean','Clue_Entry_Com_Name_clean','Clue_Entry_Cellphone_clean','Clue_Entry_Qq_clean','Clue_Entry_Email_clean','Com_Address_clean','Clue_Entry_Major_clean',\
             'Clue_Entry_Birthday_clean','legal_person_clean','registed_capital_num','registed_capital_currency','main_produce_clean',\
                    'clean_version',"registrationdate","commercialDB_id","com_status"]#'201610'
    clean_sql_dict=dict(zip(clean_sql_list,['']*len(clean_sql_list)))
    clean_sql_dict['clean_version']='201610'



    id=0
    for cid in cidlist[:]:
        print cid
        if id%1000==0:print id,cid
        clean_allAttribute_1by1(cid)
        id+=1







    # ### debug
    # attList=['Clue_Entry_Name','Clue_Entry_Com_Name','Clue_Entry_Cellphone','Clue_Entry_Qq','Clue_Entry_Email','Com_Address','Clue_Entry_Major',\
    #          'Clue_Entry_Birthday','legal_person','registed_capital','main_produce'] # employees  turnover  some range some value
    # clean_sql_list=['Clue_Entry_Name_clean','Clue_Entry_Com_Name_clean','Clue_Entry_Cellphone_clean','Clue_Entry_Qq_clean','Clue_Entry_Email_clean','Com_Address_clean','Clue_Entry_Major_clean',\
    #          'Clue_Entry_Birthday_clean','legal_person_clean','registed_capital_num','registed_capital_currency','main_produce_clean',\
    #                 'clean_version',"registrationdate","commercialDB_id","com_status"]#'201610'
    # clean_sql_dict=dict(zip(clean_sql_list,['']*len(clean_sql_list)))
    # clean_sql_dict['clean_version']='201610'
    #
    #
    # cidlist=['000003a46c0e11e696a500163e006499','0000059e6a2811e696fb00163e006499']
    # for cid in cidlist[:1]:
    #     clean_allAttribute_1by1(cid)








































