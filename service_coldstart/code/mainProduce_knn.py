#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv,random,re
import pandas as pd

from MySQLdb import cursors


import numpy as np
from sklearn.datasets import load_svmlight_file
import es_util_generatePart

import read_csv

import sys,time
reload(sys)
sys.setdefaultencoding('utf8')


def get_all_idx():
    '''
    Get all the Clue_Id
    '''
    sql = "SELECT clue_id from crm_t_clue limit 10000"
    df=pd.read_sql(sql,db)
    df.to_csv("../allClueId.csv",index=False,encoding='utf-8')

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


def extract_tag(stri):
    import jieba
    from jieba import analyse
    #print stri
    ll=[]
    for x, w in jieba.analyse.textrank(stri, withWeight=True):
        if w>=0.0:
            ll.append(x);#print x
    return ll



def yield_row(feastr_list):
    feaName_to_fid={} #{hospital:0,...}
    #feaInd_to_name={}#{0:hospital,...}
    #allclue_fidDict={} #{clueid:{feaIdx:v}
    for rowdict in feastr_list: # cid sorted in sequence
        obs={}
        for feaname,v in rowdict.items():
            if feaname not in feaName_to_fid:#new word
                feaName_to_fid[feaname]=len(feaName_to_fid)+1
                obs[ feaName_to_fid[feaname] ]=v
            else:
                obs[ feaName_to_fid[feaname] ]=v
        ############3
        yield obs
    pd.to_pickle(feaName_to_fid,'../backup/feaName_to_fid_qib_allfiltered')
def generate_svm(qib_allfiltered):
    item=yield_row(qib_allfiltered)
    filename='../backup/qib_allfiltered'
    with open(filename + '.svm', 'w') as f:
        for row_dict in item: # each rowdict  {fid:fvalue
            y=0
            ##
            f.write(str(y)+' ')#f.write('0 ')
            for fid in sorted(row_dict.keys()):
                fval = row_dict[fid]
                if fval != 0.0:
                    f.write('%d:%f ' % (fid, fval))## write into  "modeling.svm"
            f.write('\n')

def remove_cominfo(rowdict):
    rowdict1={}
    for feaname,v in rowdict.items():
        if 'com_info_' not in feaname:
            rowdict1[feaname]=v
    return rowdict1



def get_attribute(clueIds): #
    sql ="""
    SELECT *
    from crm_t_clue
    where CLue_Id  IN ('%s')
    #and main_produce is not null
    """
    # cur = db.cursor()
    # cur.execute(sql % "','".join(clueIds))
    # ret = {}
    # for r in cur.fetchall():
    #     ret[r['CLue_Id']] = r #{id:{record},...}
    df=pd.read_sql(sql % "','".join(clueIds),db);#print sql % "','".join(clueIds)
    return df


def prepare_result(svm_filename,feaName_filename,keywords_filename,cid_produce_dict_filename,sample_number,batch_size):

    ## load train array
    X, _ = load_svmlight_file(svm_filename, zero_based=True);print 'x',X.shape,type(X)
    feaName_to_fid=pd.read_pickle(feaName_filename);print 'feaname to fid',feaName_to_fid.keys()[0]

    ## adjust num_required by candidate num
    num_tt_candidate=X.shape[0]
    sample_number=es_util_generatePart.adjust_num_required_by_candiate(sample_number,num_tt_candidate)
    batch_size=sample_number

    # load query arr
    df=pd.read_csv(keywords_filename);print df.shape
    query_list=df['key'].values.tolist()[:]
    query_list=[p.split('_')[0] for p in query_list]

    query_ind=[]#[feaName_to_fid[w] for w in query_list if w in feaName_to_fid];print 'queryind',query_ind
    query_ind_far=[]
    for feaname,fid in feaName_to_fid.items():
        for word in query_list:
            if word !=feaname and word in feaname:
                query_ind_far.append(fid)

            if word ==feaname:query_ind.append(fid)
    query_arr=np.zeros((X.shape[1]))
    query_ind=list(set(query_ind));print 'query ind',len(query_ind)
    query_ind_far=list(set(query_ind_far));print 'query ind far',len(query_ind_far)
    query_arr[query_ind]=2
    query_arr[query_ind_far]=1

    #### choose x has common fid with query_arr
    nonzero_clue_ind=np.arange(X.shape[0])
    print 'choose x ind has common feature with query_arr'
    query_arr_t=query_arr.transpose()
    n_arr=X*query_arr_t;print query_arr_t.shape,n_arr.shape# [n,dim]*[dim,]=[n,]   |   dim,n
    nonzero_clue_ind=np.nonzero(n_arr)[0];print nonzero_clue_ind.shape,nonzero_clue_ind[0]




    from scipy.sparse import csr_matrix
    query_arr=csr_matrix(query_arr);print type(query_arr)

    # cidlist match svm
    cid_produce=pd.read_pickle(cid_produce_dict_filename)
    cidlist_match_svm=sorted(cid_produce.keys())     # when generate svm, cid is in this sequence

    #############3
    # knn  when fit knn,only use cluetable,not querylist
    from sklearn.neighbors import NearestNeighbors

    ## train
    print 'train...'
    nbrs=NearestNeighbors(sample_number,algorithm='ball_tree').fit(X)

    test=0
    print 'test query...'
    # distances,indices=nbrs.kneighbors(x_query[test,:])
    distances,indices=nbrs.kneighbors(query_arr)  #sorted by distance
    print indices.shape,distances.shape,distances.shape #[1,10] [1,10]
    clueTableIdx=np.squeeze(indices);print clueTableIdx.shape #in sequence

    #candidate_clueid_list=[cidlist_match_svm[i] for i in clueTableIdx if i in nonzero_clue_ind]  # in sequence
    candidate_clueid_list=[cidlist_match_svm[i] for i in clueTableIdx]



    ## see the last one
    # for cid in candidate_clueid_list[-20:]:
    #     print cid_produce[cid]

    batch_num=int(sample_number/batch_size)
    if batch_num==0:return [candidate_clueid_list]
    if batch_num>0:
        candidate_clueid_list_allBatch=[]
        for b in range(batch_num):
            thisBatch_cid=candidate_clueid_list[b*batch_size:b*batch_size+batch_size]
            candidate_clueid_list_allBatch.append(thisBatch_cid)
        return candidate_clueid_list_allBatch




def output_csv(cidlist,fnumber,cellList_called,all_cid_source,attList_clueDB,unique_comname):
    print 'unique comname,cid',len(unique_comname),len(cidlist)
    repeated=0

    ##
    dataDict={}
    for att in attList_clueDB:
        dataDict[att]=[]
    #for cid in all_cid_source:

    for cid in cidlist:
        ##
        source=all_cid_source[cid]



        comName=source['Clue_Entry_Com_Name']

        # avoid repeat comname
        if comName in unique_comname:continue
        unique_comname.append(comName)

        if 'Clue_Entry_Cellphone' not in source or len(str(source['Clue_Entry_Cellphone']))!=11:continue
        #if source['Clue_Entry_Name'] in ['--','']:continue

        # if "常州".decode('utf-8') in source['Com_Address'].decode('utf-8') or "常州".decode('utf-8') in source['Clue_Entry_Com_Name'].decode('utf-8'):continue
        # if "南通".decode('utf-8') in source['Com_Address'].decode('utf-8') or "南通".decode('utf-8') in source['Clue_Entry_Com_Name'].decode('utf-8'):continue
        # if "钱清".decode('utf-8') in source['Com_Address'].decode('utf-8') or "钱清".decode('utf-8') in source['Clue_Entry_Com_Name'].decode('utf-8'):continue
        if str(source['Clue_Entry_Cellphone']) in cellList_called:
            repeated+=1
            continue
        for att in attList_clueDB:
            if att in source:
                ### replace , in cominfo with .
                if att in ['com_info','main_produce','param8','main_industry','Clue_Entry_Major','Com_Address','param1']:
                    if source[att]==np.nan or source[att] in ['',None]:
                        dataDict[att].append('')
                        continue

                    source[att]=re.sub('[/r/n/t\r\n\t]+','',source[att])
                    source[att]=re.sub('[,]+','，',source[att])
                dataDict[att].append(source[att])
            else:dataDict[att].append('')
    print 'repeated',repeated

    klist=read_csv.klist

    vlist=read_csv.vlist


    vlist_model=read_csv.vlist_model

    df=pd.DataFrame(dataDict)
    #choose columns first
    df=pd.DataFrame(df,columns=klist);print df.columns
    namedict=dict(zip(klist,vlist))
    df1=df.rename(columns=namedict)
    # adjust attribute in sequence
    df1=pd.DataFrame(df1,columns=vlist_model);print df1.shape

    return df1,0,unique_comname





def main_tmp(num):

    ## train predict ->cidlist
    allClue_svm_filename='../backup/filteredClue_produce.svm'
    feaName_filename='../backup/feaName_to_fid'
    keywords_filename='./files/key_count_business.csv'
    cid_produce_dict_filename='../data/cid_produce'
    sample_number=num
    batch_size=num


    ## remove repeat
    #cellList_path='../cellList_yk'
    cellList_called=[]
    # cellList_called=pd.read_pickle(cellList_path)
    # cellList_called=[str(c) for c in cellList_called]
    ##########
    cidlist_ll=prepare_result(allClue_svm_filename,feaName_filename,keywords_filename,cid_produce_dict_filename,sample_number,batch_size)






    #### cidlist->format the csv


    #attList_companyDB='business product legalperson phone title address content id registrationdate'.split(' ')
    attList_clueDB=read_csv.attList_clueDB

    all_cid_source=pd.read_pickle('../allcid_filtered')
    unique_comname=[]


    clean_rate=0
    for i in range(len(cidlist_ll))[:1]:
        cidlist=cidlist_ll[i]
        df1,_,unique_comname=output_csv(cidlist,i,cellList_called,all_cid_source,attList_clueDB,unique_comname);num_tt=df1.shape[0]
        ##
        df1_clean=df1[df1['备用2']==1];num_clean=df1_clean.shape[0]#df1_clean=df[df['备用2'.decode('utf-8')]==1]
        df1_unclean=df1[df1['备用2']!=1]

        pd.DataFrame(df1_clean).to_csv('../data/result_attribute_yk_clean.csv',index=False,encoding='utf-8')
        pd.DataFrame(df1_unclean).to_csv('../data/result_attribute_yk_unclean.csv',index=False,encoding='utf-8')
        pd.DataFrame(df1_unclean).to_csv('../data/result_attribute_yk_unclean.xls',index=False,encoding='utf-8')

        clean_rate=num_clean/float(num_tt)
    return clean_rate









if __name__ == "__main__":



    ## train predict ->cidlist
    allClue_svm_filename='../backup/filteredClue_produce.svm'
    feaName_filename='../backup/feaName_to_fid'
    keywords_filename='../data/key_count_business.csv'
    cid_produce_dict_filename='../data/cid_produce'
    sample_number=10000
    batch_size=10000


    ## remove repeat
    cellList_path='../cellList_yk'
    cellList_called=[]
    cellList_called=pd.read_pickle(cellList_path)
    cellList_called=[str(c) for c in cellList_called]
    ##########
    cidlist_ll=prepare_result(allClue_svm_filename,feaName_filename,keywords_filename,cid_produce_dict_filename,sample_number)






    #### cidlist->format the csv


    #attList_companyDB='business product legalperson phone title address content id registrationdate'.split(' ')
    attList_clueDB=['Clue_Entry_Name','Clue_Entry_Com_Name','Clue_Entry_Cellphone','Clue_Entry_Qq','Clue_Entry_Wechat','Clue_Entry_Email',\
           'Com_Address','Clue_Entry_Major','Clue_Entry_Birthday','Clue_Entry_Telephone','qiantai','chuanzhen','main_produce','param8',\
           'com_type','main_industry','registrationdate','com_info','param2']

    all_cid_source=pd.read_pickle('../allcid_filtered')

    cidlist=cidlist_ll[0]
    unique_comname=[]
    for i in range(len(cidlist_ll)):
        cidlist=cidlist_ll[i]
        df1=output_csv(cidlist,i,cellList_called)
        ##
        df1_clean=df1[df1['备用2']==1]#df1_clean=df[df['备用2'.decode('utf-8')]==1]
        df1_unclean=df1[df1['备用2']!=1]

        pd.DataFrame(df1_clean).to_csv('../data/result_attribute_yk_companyDB_%d_clean.csv'%i,index=False,encoding='utf-8')
        pd.DataFrame(df1_unclean).to_csv('../data/result_attribute_yk_companyDB_%d_unclean.csv'%i,index=False,encoding='utf-8')






    # df_list=[]
    # for i in range(5):
    #     df=pd.read_csv('../data/result_attribute_window_companyDB_%d.csv'%i,encoding='utf-8')
    #     df_list.append(df)
    #
    # df=pd.concat(df_list)
    # pd.DataFrame(df).to_csv('../data/combined.csv')






















































































