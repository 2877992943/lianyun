#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv,os
import pandas as pd

from MySQLdb import cursors
import numpy as np
#from clue import *

#from base import CategoricalTextMixIn, ClueFeature, FieldExistsMixIn, ParseNumberMixIn,get_clues
from base import get_clues
import pylab as plt
import sys
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




if __name__ == "__main__":

    negative_valid_uidcid=pd.read_pickle('../data/negative_valid_uidcid_pair')    #[(uid,cid), ..]
    positive_valid_uidCid=pd.read_pickle('../data/positive_valid_uidcid_pair')
    num_neg=len(negative_valid_uidcid)
    num_pos=len(positive_valid_uidCid)
    cid_list=[positive_valid_uidCid[i][1] for i in range(num_pos)]+[negative_valid_uidcid[i][1] for i in range(num_neg)]
    ylist=[1]*num_pos+[0]*num_neg
    pd.to_pickle(ylist,'../data/ylist')


    ########## 134272 pos+neg
    # generate feature part1 clueTable


    model_clueid=cid_list

    from base import ClueFeature
    from clue import *
    features = ClueFeature.__subclasses__();print features
    from generate_clueUser_clue import gen_svmlight



    ret=get_clues(cid_list)

    gen_svmlight('../modeling_16w_clueTable', features, cid_list,'clue')#generate {cid:strFea...}


































