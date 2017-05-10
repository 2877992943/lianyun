#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv
import pandas as pd

from MySQLdb import cursors
import numpy as np
#from clue import *

#from base import CategoricalTextMixIn, ClueFeature, FieldExistsMixIn, ParseNumberMixIn,get_clues
#import pylab as plt
import sys,time
from menu_to_companyName_product_list import calculate_text,wordParsing_all,remove_alegedNotParticipate,remove_brace_content
from get_client_feaStr import analyze_business



reload(sys)
sys.setdefaultencoding('utf8')

# db = MySQLdb.connect(host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com',
#                      user='yunker',
#                      passwd="yunker2016EP",
#                      db="xddb",
#                      use_unicode=True,
#                      charset='utf8',
#                      cursorclass=cursors.DictCursor)

def get_all_idx():
    '''
    Get all the Clue_Id
    '''
    sql = """
    SELECT clue_id
    from crm_t_clue
    where param2=1

    """
    df=pd.read_sql(sql,db)
    #df.to_csv("../allClueId.csv",index=False,encoding='utf-8')
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

        words_dict=analyze_business([raw_str],10)
        words=words_dict.keys();

        num_words=float(len(words))+0.00001
        cid_rawdict[cid]=dict(zip(words,[1]*len(words))) #{cid:{str_word:1,,,,}
    generate_svm(cid_rawdict)


def main_tmp():
    global svmfilename,feaNamePath,cid_produce_path
    svmfilename='../backup/filteredClue_produce.svm'
    feaNamePath='../backup/feaName_to_fid'
    #in
    cid_produce_path='../data/cid_produce'

    prepare_wholeDB_svm_clue()



if __name__ == "__main__":
    #out
    svmfilename='../backup/filteredClue_produce.svm'
    feaNamePath='../backup/feaName_to_fid'
    #in
    cid_produce_path='../data/cid_produce'

    time_start=time.time()
    '''筛选条件
    param2=1
    param1 市场推广销售采购 的不要
    param4 北京 上海 广州 成都
    '''


    prepare_wholeDB_svm_clue()



















































