#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv,os,re
import pandas as pd

from MySQLdb import cursors
import numpy as np

import mainProduce_knn
import sys,time
from menu_to_companyName_product_list import calculate_text,wordParsing_all
import es_util_generatePart

import read_csv

reload(sys)
sys.setdefaultencoding('utf8')
from sklearn.neighbors import NearestNeighbors

# db = MySQLdb.connect(host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com',
#                      user='yunker',
#                      passwd="yunker2016EP",
#                      db="xddb",
#                      use_unicode=True,
#                      charset='utf8',
#                      cursorclass=cursors.DictCursor)


db = MySQLdb.connect(host='rds0710650me01y6d3ogo.mysql.rds.aliyuncs.com',
                      user='yunker',
                      passwd="yunke2016",
                      db="yunketest",
                      use_unicode=True,
                      charset='utf8',
                      cursorclass=cursors.DictCursor)


def get_cid_cominfo(clueIds):
    '''
    Get all the Clue_Id
    '''
    sql = """
        SELECT clue_id,com_info,vector from company_parse
        where clue_id in ('%s')


        """

    cur = db.cursor()
    cur.execute(sql % "','".join(clueIds))
    ret = {}
    for r in cur.fetchall():
        ret[r['clue_id']] = r #{id:{record},...}
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

def wordParsing(string):
    seg_list = jieba.cut_for_search(string)
    return list(seg_list)

def extract_tag(stri):

    from jieba import analyse
    #print stri
    ll=[]
    for x, w in jieba.analyse.textrank(stri, withWeight=True):
        if w>=0.0:
            ll.append(x);#print x
    return ll



def yield_row(allclue_feaDict):
    feaName_to_fid={} #{hospital:0,...}

    for clueid in sorted(allclue_feaDict.keys())[:]: # cid sorted in sequence
        obs={}
        for feaname,v in allclue_feaDict[clueid].items():

            if feaname not in feaName_to_fid:#new word
                feaName_to_fid[feaname]=len(feaName_to_fid)
                obs[ feaName_to_fid[feaname] ]=v
            else:
                if feaname in feaName_to_fid:
                    obs[ feaName_to_fid[feaname] ]=v
        ############3
        yield obs
    pd.to_pickle(feaName_to_fid,feaNamePath)#'../backup/feaName_to_fid')
def generate_svm(allclue_feaDict):
    item=yield_row(allclue_feaDict)#{cid:[words]
    filename=svmfilename#'../backup/filteredClue_produce.svm'
    with open(filename, 'w') as f:
        for row_dict in item: # each rowdict  {fid:fvalue
            y=0
            ##
            f.write(str(y)+' ')#f.write('0 ')
            for fid in sorted(row_dict.keys()):
                fval = row_dict[fid]
                if fval != 0.0:
                    f.write('%d:%f ' % (fid, fval))## write into  "modeling.svm"
            f.write('\n')

def remove_cominfo_comName(rowdict):
    rowdict1={}
    for feaname,v in rowdict.items():
        if 'com_info_' not in feaname and 'com_name_' not in feaname:
            rowdict1[feaname]=v
    return rowdict1



def prepare_wholeDB_svm_clue():

    #######
    cid_rawdict={}
    cid_produce=pd.read_pickle(cid_produce_path);print 'cid produce',len(cid_produce)

    for cid,raw_str in cid_produce.items()[:]:
        words=calculate_text(raw_str);#print raw_str,words
        words = filter(lambda w: w and len(w) > 0, words) #list
        words=[word.decode('utf-8') for word in words];#print words
        for w in words:
            w=w.replace('的'.decode('utf-8'),' ')
            w=w.replace('和'.decode('utf-8'),' ')
            w=w.replace('以及'.decode('utf-8'),' ')
            w=w.replace('及'.decode('utf-8'),' ')
            ll_i=w.split(' ')
            if len(ll_i)>1:words+=ll_i
        #parse and no_parse
        # for w in words:
        #     if len(w.decode('utf-8'))>4:
        #         ll_i=wordParsing_all(w)
        #         words+=ll_i
        words=[w for w in words if len(w.decode('utf-8'))>1]


        cid_rawdict[cid]=dict(zip(words,[1]*len(words))) #{cid:{str_word:1,,,,}
    generate_svm(cid_rawdict)

def normalize(vec):
    mode=np.sqrt(np.dot(vec,vec))
    return vec/(mode+0.000001)


def calc_distance(query_arr,arr):
    query_arr=normalize(query_arr)
    arr=normalize(arr)
    dis=np.sum((query_arr-arr)*(query_arr-arr))
    dis=np.sqrt(dis)
    return dis

def main_tmp(num):
    cellList_called=[]

    ###############
    # cominfo
    ## candidate cid -> cid cominfo vector
    all_cid_source=pd.read_pickle('../allcid_filtered')
    cid_vec_dict={}
    for cid,source in all_cid_source.items():
        if "cominfo_vector" in source and source["cominfo_vector"] not in ["",None]:
            cid_vec_dict[cid]=source["cominfo_vector"]


    pd.to_pickle(cid_vec_dict,'../cid_cominfo_vector')








    #####
    ret=pd.read_pickle('../cid_cominfo_vector')

    cid_cand_list=[];arr_cand_list=[]


    #### filter
    for cid,r in ret.items()[:]:
        try:
            vec=eval(r) #str->list
            vec=normalize(np.array(vec))######
            cid_cand_list.append(cid)
            arr_cand_list.append(vec)
        except:print r

    print 'cid,vector',len(arr_cand_list),len(cid_cand_list)
    pd.to_pickle([cid_cand_list,arr_cand_list],'../data/cand_cid_arrlist')




    ##### calc similar
    cid_cand_list,arr_cand_list=pd.read_pickle('../data/cand_cid_arrlist')
    arr_all=np.array(arr_cand_list);print 'x',arr_all.shape
    candidate_arr=arr_all[:,:];print 'candiate',candidate_arr.shape

    arr_query_list=pd.read_pickle('../data/targetClient_arr_list')
    #all_cid_source=pd.read_pickle('../allcid_filtered')


    X=arr_all

    ## adjust num_required by candidate num
    num_tt_candidate=X.shape[0]
    num=es_util_generatePart.adjust_num_required_by_candiate(num,num_tt_candidate)
    sample_number=num


    ## train
    print 'train knn...'
    nbrs=NearestNeighbors(sample_number,algorithm='ball_tree').fit(X)
    ###### predict
    unique_comname=[]
    dfList=[]

    for test_ind in range(len(arr_query_list))[:]:

        query_arr=arr_query_list[test_ind];print 'query',query_arr.shape
        query_arr=normalize(query_arr)####
        #print query_arr
        if np.sum(query_arr)==0:continue


        test=0
        print 'knn query...'
        # distances,indices=nbrs.kneighbors(x_query[test,:])
        distances,indices=nbrs.kneighbors(query_arr)  #sorted by distance
        print indices.shape,distances.shape,distances[0,:10]  #[1,10] [1,10]
        Idx=np.squeeze(indices);print 'idx',Idx.shape #in sequence


        ### get cid ->csv
        cidList_selected=[]
        for ind in Idx:
            cid=cid_cand_list[ind]
            cidList_selected.append(cid)
        ##### cidlist ->csv

        attList_clueDB=read_csv.attList_clueDB

        df1,_,unique_comname=mainProduce_knn.output_csv(cidList_selected,0,cellList_called,all_cid_source,attList_clueDB,unique_comname);
        ##
        dfList.append(pd.DataFrame(df1))
    ##### combine csv

    df1=pd.concat(dfList);num_tt=df1.shape[0]

    df1_clean=df1[df1['备用2']==1];num_clean=df1_clean.shape[0]#df1_clean=df[df['备用2'.decode('utf-8')]==1]
    df1_unclean=df1[df1['备用2']!=1]

    pd.DataFrame(df1_clean).to_csv('../data/result_attribute_yk_clean.csv',index=False,encoding='utf-8')
    pd.DataFrame(df1_unclean).to_csv('../data/result_attribute_yk_unclean.csv',index=False,encoding='utf-8')
    pd.DataFrame(df1_unclean).to_csv('../data/result_attribute_yk_unclean.xlsx',index=False,encoding='utf-8')

    clean_rate=num_clean/float(num_tt)
    return clean_rate

if __name__ == "__main__":





    cellList_called=[]
    ###############
    # cominfo

    ## candidate cid -> cid cominfo vector
    all_cid_source=pd.read_pickle('../allcid_filtered')
    cid_vec_dict={}
    for cid,source in all_cid_source.items():
        if "cominfo_vector" in source and source["cominfo_vector"] not in ["",None]:
            cid_vec_dict[cid]=source["cominfo_vector"]


    # cid_produce_path='../data/cid_produce'
    # cid=pd.read_pickle(cid_produce_path);print len(cid)
    # cidList=cid.keys()
    # ret=get_cid_cominfo(cidList);print len(ret)#from test_db sql
    pd.to_pickle(cid_vec_dict,'../cid_cominfo_vector')





    #####
    ret=pd.read_pickle('../cid_cominfo_vector')

    cid_cand_list=[];arr_cand_list=[]


    #### filter
    for cid,r in ret.items()[:]:
        # if r['com_info'] in ['null','NULL',None,'']:continue
        # if r['vector'] in ['None',None]:continue

        try:
            vec=eval(r) #str->list
            vec=normalize(np.array(vec))######
            cid_cand_list.append(cid)
            arr_cand_list.append(vec)
        except:print r

    print 'cid,vector',len(arr_cand_list),len(cid_cand_list)
    pd.to_pickle([cid_cand_list,arr_cand_list],'../data/cand_cid_arrlist')




    ##### calc similar
    cid_cand_list,arr_cand_list=pd.read_pickle('../data/cand_cid_arrlist')
    arr_all=np.array(arr_cand_list);print 'x',arr_all.shape
    candidate_arr=arr_all[:,:];print 'candiate',candidate_arr.shape

    arr_query_list=pd.read_pickle('../data/targetClient_arr_list')
    all_cid_source=pd.read_pickle('../allcid_filtered')

    sample_number=7000
    X=arr_all
    ## train
    print 'train knn...'
    nbrs=NearestNeighbors(sample_number,algorithm='ball_tree').fit(X)
    ###### predict
    unique_comname=[]
    dfList=[]

    for test_ind in range(len(arr_query_list))[:]:

        query_arr=arr_query_list[test_ind];print 'query',query_arr.shape
        query_arr=normalize(query_arr)####
        #print query_arr
        if np.sum(query_arr)==0:continue


        test=0
        print 'knn query...'
        # distances,indices=nbrs.kneighbors(x_query[test,:])
        distances,indices=nbrs.kneighbors(query_arr)  #sorted by distance
        print indices.shape,distances.shape,distances[0,:10]  #[1,10] [1,10]
        Idx=np.squeeze(indices);print 'idx',Idx.shape #in sequence

        #print 'similar stri:'
        ### get cid ->csv
        cidList_selected=[]
        for ind in Idx:
            cid=cid_cand_list[ind]
            cidList_selected.append(cid)
            #print ret[cid]['com_info']



        ##### cidlist ->csv

        attList_clueDB=['Clue_Entry_Name','Clue_Entry_Com_Name','Clue_Entry_Cellphone','Clue_Entry_Qq','Clue_Entry_Wechat','Clue_Entry_Email',\
           'Com_Address','Clue_Entry_Major','Clue_Entry_Birthday','Clue_Entry_Telephone','qiantai','chuanzhen','main_produce','param8',\
           'com_type','main_industry','registrationdate','com_info','param2','employees_num','registed_capital']








        df1,_,unique_comname=mainProduce_knn.output_csv(cidList_selected,0,cellList_called,all_cid_source,attList_clueDB,unique_comname);
        pd.DataFrame(df1).to_csv('../data/csv/result_attribute_ping_clueDB_%d.csv'%test_ind,index=False,encoding='utf-8')

        ##
        dfList.append(pd.DataFrame(df1))





    ##### combine csv

    df1=pd.concat(dfList);num_tt=df1.shape[0]

    df1_clean=df1[df1['备用2']==1];num_clean=df1_clean.shape[0]#df1_clean=df[df['备用2'.decode('utf-8')]==1]
    df1_unclean=df1[df1['备用2']!=1]

    pd.DataFrame(df1_clean).to_csv('../data/result_attribute_yk_clean.csv',index=False,encoding='utf-8')
    pd.DataFrame(df1_unclean).to_csv('../data/result_attribute_yk_unclean.csv',index=False,encoding='utf-8')
    pd.DataFrame(df1_unclean).to_csv('../data/result_attribute_yk_unclean.xlsx',index=False,encoding='utf-8')

    clean_rate=num_clean/float(num_tt)
    print clean_rate



























































