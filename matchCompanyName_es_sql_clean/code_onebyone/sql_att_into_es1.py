#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv,os
import pandas as pd

from MySQLdb import cursors
import numpy as np
#from clue import *

#from base import CategoricalTextMixIn, ClueFeature, FieldExistsMixIn, ParseNumberMixIn,get_clues
#import pylab as plt
import sys,time
#from menu_to_companyName_product_list import calculate_text,wordParsing_all
#import es_filter
#import es_filter_companyDB
from process_qq_email_position_phone_produce import clean_produce
#import jieba
reload(sys)
sys.setdefaultencoding('utf8')
from sklearn.neighbors import NearestNeighbors
from elasticsearch import Elasticsearch,helpers
es = Elasticsearch("123.57.62.29",timeout=200)

read_host='rr-2zeg40364h2thw9m6o.mysql.rds.aliyuncs.com'
write_host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com'
db = MySQLdb.connect(host=write_host,
                     user='yunker',
                     passwd="yunker2016EP",
                     db="xddb",
                     use_unicode=True,
                     charset='utf8',
                     cursorclass=cursors.DictCursor)



def get_clueId(flag): #list

    sql ="""
    SELECT CLue_Id
    from crm_t_clue
    where param2=1

    """ if flag=='clean' else """
    SELECT CLue_Id
    from crm_t_clue
    where param2!=1

    """

    # cur = db.cursor()
    # #print sql%(start,size)
    # #cur.execute(sql % "','".join(clueIds))
    # cur.execute(sql)
    # ret = []
    # for r in cur.fetchall():
    #     ret.append(r['CLue_Id']) #
    #df=pd.read_sql(sql % "','".join(clueIds),db);#print sql % "','".join(clueIds)
    df= pd.read_sql(sql,db)
    return df


def get_clue_byatt(attList,clueIds):
    sql ="""
    SELECT %s
    from crm_t_clue
    where CLue_Id in ('%s')
    """
    cur = db.cursor()
    #print sql%(start,size)
    cur.execute(sql % (','.join(attList),"','".join(clueIds)))
    #cur.execute(sql%(','.join(attList),str(start),str(batch_size)))
    #cur.execute(sql)
    ret = {}
    for r in cur.fetchall():
        ret[r['CLue_Id']]=r
    #df=pd.read_sql(sql % "','".join(clueIds),db);#print sql % "','".join(clueIds)
    return ret



def get_cid_cominfo(clueIds):
    '''
    Get all the Clue_Id
    '''
    sql = """
        SELECT clue_id,com_info from company_parse
        where clue_id in ('%s')


        """

    cur = db.cursor()
    cur.execute(sql % "','".join(clueIds))
    ret = {}
    for r in cur.fetchall():
        ret[r['clue_id']] = r['com_info'] #{id:{record},...}
    return ret

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



def get_clue_idx():


    sql = """
        SELECT CLue_Id
        FROM crm_t_clue
        # WHERE Clue_Entry_Cellphone IN
        #       ('%s')
        where param2!=1
        #and registed_capital is not null
    """
    #df=pd.read_sql(sql % "','".join(tels),db)
    df=pd.read_sql(sql,db)
    return df

def get_clue_idx1():


    sql = """
        SELECT CLue_Id
        FROM crm_t_clue
        # WHERE Clue_Entry_Cellphone IN
        #       ('%s')
        where date(edit_time)="2016-12-28"
        #and registed_capital is not null
    """
    #df=pd.read_sql(sql % "','".join(tels),db)
    df=pd.read_sql(sql,db)
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



class myAttribute(object):
    def __init__(self,fpath):
        self.fpath=fpath

    def __iter__(self):
        for fname in os.listdir(self.fpath)[:]:# for each batch   {cid:cominfo_stri_parse
            ret=pd.read_pickle(self.fpath+fname)
            for cid,r in ret.items()[:]:#for each sentence
                if r['main_produce'] in ['null','NULL',None,'']:continue
                registNum=r['main_produce']
                yield [cid,registNum]



class mySentence(object):
    def __init__(self,fpath):
        self.fpath=fpath

    def __iter__(self):
        for fname in os.listdir(self.fpath)[:]:# for each batch   {cid:cominfo_stri_parse
            cid_stri=pd.read_pickle(self.fpath+fname)
            for cid,stri in cid_stri.items()[:]:#for each sentence
                if stri in ['null','NULL',None,'']:continue
                striList=stri.split(' ')
                arr_sentence=np.zeros((100,))
                for word in striList:
                    word=word.strip(' ')
                    if word in word_ind_dict and word in word_idf_dict:
                        #print word
                        wid=word_ind_dict[word]
                        idf=word_idf_dict[word]
                        #print wid,df
                        arr_w=word2vec_model[str(wid)]
                        arr_sentence=arr_sentence+arr_w*float(idf)
                yield [cid,arr_sentence]

def calc_distance(query_arr,arr):
    dis=np.sum((query_arr-arr)*(query_arr-arr))
    dis=np.sqrt(dis)
    return dis


def update_sql_byAttribute(cid,field,value):
    cur = db.cursor()
    sql="""update crm_t_clue set %s = '%s' where CLue_Id='%s'"""
    #print sql%(field,value,cid)
    cur.execute( sql%(field,value,cid))
    ####
    db.commit()
    cur.close()



def update_sql_byAttribute_cominfo(cid,field,value):
    cur = db.cursor()
    sql="""update company_parse set %s = '%s' where clue_id='%s'"""
    #print sql%(field,value,cid)
    cur.execute( sql%(field,value,cid))
    ####
    db.commit()
    cur.close()


def update_es_byID(cid,field,value,es_index):
    body={'doc':{field:value } }
    es.update(index=es_index,doc_type='clue',id=cid,body=body)


if __name__ == "__main__":


    ### batch query sql  clean & unclean
    sql_att_list=['CLue_Id','main_produce']


    """
    ### get clueid -> attribute
    df=get_clueId('clean')
    df.to_csv('../data/cid.csv',index=False,encoding='utf-8')
    df=pd.read_csv('../data/cid.csv',encoding='utf-8');print df.shape


    cidlist=df['CLue_Id'].values.tolist()

    num=len(cidlist)
    batch_size=100000
    for i in range(int(num/batch_size)+1)[:]:
        cidlist_batch=cidlist[i*batch_size:i*batch_size+batch_size]
        ret=get_clue_byatt(sql_att_list,cidlist_batch)#{cid:r
        print ret.__len__()
        pd.to_pickle(ret,'../businessScope/%s'%cidlist_batch[0])







    #####
    df=get_clueId('unclean')
    df.to_csv('../data/cid.csv',index=False,encoding='utf-8')
    df=pd.read_csv('../data/cid.csv',encoding='utf-8');print df.shape

    cidlist=df['CLue_Id'].values.tolist()

    num=len(cidlist)
    batch_size=100000
    for i in range(int(num/batch_size)+1):
        cidlist_batch=cidlist[i*batch_size:i*batch_size+batch_size]
        ret=get_clue_byatt(sql_att_list,cidlist_batch)#{cid:r
        print ret.__len__()
        pd.to_pickle(ret,'../businessScope/%s'%cidlist_batch[0])

    """






    ### registedNum -> es
    path='../businessScope/'
    gene=myAttribute(path)
    time_begin=time.time()
    normal,normal0,fail=0,0,0
    situation_list=[]
    for cid_arr in gene:

        cid,registedNum=cid_arr;#print cid,registedNum
        registedNum=clean_produce(registedNum)
        for pct in [',',' ']:
            registedNum=registedNum.strip(pct)
        if registedNum in ['']:continue
        #print registedNum
        #if float(registedNum)==0:continue

        try:
            update_es_byID(cid,'main_produce',registedNum,'clue_not_filtered')
            update_sql_byAttribute(cid,'main_produce_clean',registedNum)

            normal+=1

        except Exception,e:
            try:
                #print Exception,e
                update_es_byID(cid,'main_produce',registedNum,'clue_filtered')
                update_sql_byAttribute(cid,'main_produce_clean',registedNum)

                normal+=1

            except Exception,e:
                #print Exception,e
                if [Exception,e] not in situation_list:
                    situation_list.append([Exception,e])
                fail+=1

                continue
        else:normal0+=1  #else means normal try1


    ########

    print 'normal %d normal0 %d fail %d'%(normal,normal0,fail)#500w 200w 1w #8w+147w
    print 'done  ', time.time()-time_begin,'seconds'
    pd.to_pickle(situation_list,'../data/situation_list')



    
































































