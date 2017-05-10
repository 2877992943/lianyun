#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv,re,os,json
import pandas as pd

from MySQLdb import cursors
import numpy as np
from clue import *

from base import *

from elasticsearch import Elasticsearch, helpers
from generate_clue_str import gen_svmlight

es=Elasticsearch(hosts='123.57.62.29',timeout=1000)
es_index='clue_not_filtered'
#import pylab as plt
import sys,time
reload(sys)
sys.setdefaultencoding('utf8')

read_host='rr-2zeg40364h2thw9m6o.mysql.rds.aliyuncs.com'
write_host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com'
db = MySQLdb.connect(host=read_host,
                     user='yunker',
                     passwd="yunker2016EP",
                     db="xddb",
                     use_unicode=True,
                     charset='utf8',
                     cursorclass=cursors.DictCursor)

def parseDictToJson(clue_cache,i):

    actions = []
    #key_set=fid_dict.keys()

    for k in clue_cache.keys()[i*10000:(i+1)*10000]:
        #print k
        #print 'v',clue_cache[k]

        try:
            #del clue_cache[k]['Create_Time']
            #del clue_cache[k]['Edit_Time']
            del clue_cache[k]['weixin_CreateTime']
            del clue_cache[k]['vip_end_date']

        except KeyError:
            pass
        #clue_cache[k]['feature_value']=str(fid_dict[k]) if k in key_set else "{}"
        jsonClue=json.dumps(clue_cache[k])
        action = {
        "_index": "clue",
        "_type": "clue1",
        "_id": k,
        "_source": jsonClue
        }

        actions.append(action)
    return actions



def get_all_idx():
    '''
    Get all the Clue_Id
    '''
    sql = "SELECT clue_id from crm_t_clue where param2!=1"
    df=pd.read_sql(sql,db)
    #df.to_csv("../allClueId.csv",index=False,encoding='utf-8')
    return df

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




def save2pickle(c,name):
    write_file=open(str(name),'wb')
    cPickle.dump(c,write_file,-1)#[ (timestamp,[motion,x,y,z]),...]
    write_file.close()

def load_pickle(path_i):
    f=open(path_i,'rb')
    data=cPickle.load(f)#[ [time,[xyz],y] ,[],[]...]
    f.close()
    #print data.__len__(),data[0]
    return data


def combine_raw_fid(cid_fid,raw):#dict dict
    for cid,rowdict in raw.items():
        if cid in cid_fid:
            raw[cid]['feature_value']=str(cid_fid[cid])
    return raw

def transform_lowDim_ind(rowDict_str,lowDimWordDic): #{duty_yuangong:v...} -> { 1:v.... {word:fid
    rowDic={}

    for f_str,v in rowDict_str.items(): # one clue {strfea:v..}  duty_yuangong
        if v!=0:
            #f_str_=f_str.lstrip(cate_p);print f_str,f_str_ # not work
            #f_str_=strip_name(f_str);#print '?',f_str,f_str_
            if f_str in lowDimWordDic:
                fid=lowDimWordDic[f_str]
                rowDic[fid]=v
    return rowDic


def generate_cid_fid(allclue_feadict,word_lowDim_dict):
    filtered_clueid_fea={} #{cid:rowdict
    for cid,feadic in allclue_feadict.items()[:]:

        if len(feadic)!=0:
            rowDict=feadic;#print 'fdic str', feadic
            rowDict=transform_lowDim_ind(rowDict,word_lowDim_dict) #{str:v..}->{fid:v...} duty_yuangong not duty_1
            #print 'rowdict fid',rowDict
            if len(rowDict)>0:
                filtered_clueid_fea[cid]=rowDict

            else:filtered_clueid_fea[cid]={}
            #print filtered_clueid_fea[cid]
        else:filtered_clueid_fea[cid]={}
     #print filtered_clueid_fea
    print len(filtered_clueid_fea)
    return filtered_clueid_fea

def process_data(raw_fid_dict): #{cid:{rawdict},,,,}
    raw_fid_dict_ret={}
    actions=[]
    for cid,rawdict in raw_fid_dict.items()[:]:
        # for each obs


        if rawdict['Clue_Entry_Cellphone']=='':continue


        ### location

        latitude = rawdict['Clue_Latitude']
        longitude = rawdict['Clue_Longitude']
        if latitude == "" or float(latitude) > 90 or float(latitude) < 0:
            latitude = 0.0
        if longitude == "" or float(longitude) > 180 or float(longitude) < 0:
            longitude = 0.0
 
        raw_fid_dict[cid]['location'] = str(latitude) + "," + str(longitude)
        ###
        #raw_fid_dict[cid]['businessScope']=raw_fid_dict[cid]['param8']
        ### time
        for it in ['Create_Time','Edit_Time','weixin_CreateTime','vip_end_date']:
            raw_fid_dict[cid][it] = time.time()
        ### str->int
        raw_fid_dict[cid]['param2'] = -1000 if raw_fid_dict[cid]['param2'] == '' else int(raw_fid_dict[cid]['param2'])
        raw_fid_dict[cid]['param3'] = -1000 if raw_fid_dict[cid]['param3'] == '' else int(raw_fid_dict[cid]['param3'])
        ### select field which exist in es
        raw_fid_dict_ret[cid]={}
        
        for att in sql_att_list:
            att_es=re.sub(r'(_clean)','',att)
            raw_fid_dict_ret[cid][att_es]=raw_fid_dict[cid][att]
            #print att_es,raw_fid_dict[cid][att]
        for att in ['location','feature_value']:
            raw_fid_dict_ret[cid][att]=raw_fid_dict[cid][att]
        #del raw_fid_dict_ret[cid]['param8']
        del raw_fid_dict_ret[cid]['sour_one']
        del raw_fid_dict_ret[cid]['sour_two']
        del raw_fid_dict_ret[cid]['clean_version']
            #print att,raw_fid_dict[cid][att]
        # for k,v in raw_fid_dict_ret[cid].items():
        #     print k,v

        ####

        #jsonClue=json.dumps(raw_fid_dict_ret[cid])
        action = {
            "_index": es_index,
            "_type": "clue",
            "_id": cid,
            "_source": raw_fid_dict_ret[cid]
            }

        actions.append(action)
    return actions


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

        
def clear():

    for key, value in globals().items():

        if callable(value) or value.__class__.__name__ == "module" or key in ['all_clueId','batch_sz','features','word_lowDim_dict','sql_att_list','es_index','es']:

            continue

        del globals()[key]



if __name__ == "__main__":
    time_start=time.time()

    ### get att sql
    sql_att_list=pd.read_csv('../data/t.txt')
    sql_att_list=sql_att_list['sql'].values.tolist()
    sql_att_list=[i.strip(' ') for i in sql_att_list if i not in ['',' ',np.nan]];#print sql_att_list

    """
    ### get clueid -> attribute
    #df=get_clueId('clean')
    #df.to_csv('../data/cid.csv',index=False,encoding='utf-8')

    """






    # #####
    # df=get_clueId('unclean')
    # df.to_csv('../data/cid.csv',index=False,encoding='utf-8')









    # generate low dim fid
    feaNameList=pd.read_pickle('../backup/feaNameList_nonzero_lr_816')
    word_lowDim_dict=dict(zip(feaNameList,range(len(feaNameList) ) ) )

    # all cid

    df=pd.read_csv('../data/cid.csv',encoding='utf-8');print df.shape
    all_clueId=df['CLue_Id'].values.tolist()



    ########## query calc fea  ->{clueid:{feaStr:feavalue...}   feaStr=duty_yuangong ,not duty_1
    from base import ClueFeature
    from clue import *
    features = ClueFeature.__subclasses__()
    ## batches
    numall=len(all_clueId)
    batch_sz=10000
    num_batches=numall/batch_sz+1
    ###
     
    # df=pd.read_csv('/root/yangrui/es_tableClue_dataPrepare/data/t.txt',encoding='utf-8')
    # sql_att=df['sql'].values.tolist()
    # sql_att_list=[s.strip(' ') for s in sql_att]




    for i in range(num_batches)[:]: # 0 1 2 3
        print 'batch',i
        model_clueid=all_clueId[i*batch_sz:(i+1)*batch_sz]
        #model_clueid=all_clueId[:2]

        ## raw data
        print 'get raw data and generate strfea...' #clue_profession_yuangong
        ret=get_clues(model_clueid[:]) # into cache  raw data
        # pd.to_pickle(ret,'../combined/'+str(i)) # still bulking memory
        clueIdDict=gen_svmlight('strDict_clue_'+str(i), features, model_clueid[:],'clue')
        ## low dim
        print 'generate low dim fid and combine '
        cid_fid_dict=generate_cid_fid(clueIdDict,word_lowDim_dict)
        # combine fid & raw
        raw_fid_dict=combine_raw_fid(cid_fid_dict,ret)# {cid:{rawdict...}...}
        #pd.to_pickle(raw_fid_dict,'../combined/'+str(i))



        # select field |time to timestamp | lagitude longitude ->location |businessScope
        actions=process_data(raw_fid_dict);print len(actions)
         
         
        #print len(raw_fid_dict),raw_fid_dict.values()[0]
        pd.to_pickle(actions,'../combined/'+str(i))
        #helpers.bulk(es, actions, chunk_size=1000, request_timeout=1000)
        ##
        CLUE_CACHE={}
        FEATURE_VALUE_CACHE = {}
        ret={}
        #clear()
        






    time_end=time.time()
    print 'time... %f sec'%(time_end-time_start)



    """
    ##############
    # insert into es
    fail=0
    path='../combined/'
    for name in os.listdir(path)[:]:
        fname=path+name
        raw_dict=pd.read_pickle(fname)
        for cid,raw in raw_dict.items()[:]:
            #print cid
            try:
                es.index(index="clue_filtered", doc_type="clue", id=cid, body=raw)
            except:fail+=1
    print 'fail',fail
    """
     
     
    
    




    




































