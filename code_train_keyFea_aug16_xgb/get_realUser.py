#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv
import pandas as pd

from MySQLdb import cursors
import numpy as np
from elasticsearch import Elasticsearch


#from base import CategoricalTextMixIn, ClueFeature, FieldExistsMixIn, ParseNumberMixIn,get_users

import sys,time
reload(sys)
sys.setdefaultencoding('utf8')

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


def query_all_uid( ):
    sql = """
        SELECT User_Id,feature_value
        FROM crm_t_portaluser
        #where feature_value is null

    """
    cur = db.cursor()
    cur.execute(sql)
    ret = {}
    for r in cur.fetchall():
        #print r['feature_value'],type(r['feature_value'])
        ret[r['User_Id']] = r #{id:{record},,,}
    #pd.to_pickle(ret,'../cache_batches/rst__%s'% str(ret.keys()[0]))

    return ret.keys() #{uid:{field:v...} }

def query_all_cid( ):
    sql = """
        SELECT CLue_Id
        FROM crm_t_clue
        #where feature_value is null

    """
    cur = db.cursor()
    cur.execute(sql)
    ret = {}
    for r in cur.fetchall():
        #print r['feature_value'],type(r['feature_value'])
        ret[r['CLue_Id']] = r #{id:{record},,,}
    #pd.to_pickle(ret,'../cache_batches/rst__%s'% str(ret.keys()[0]))

    return ret.keys() #{uid:{field:v...} }

def query_fakeUser():
    sql = """
        SELECT User_Id,feature_value
        FROM crm_t_portaluser
        where User_Company_Id='yunke'
        #limit 1
    """
    cur = db.cursor()
    cur.execute(sql)
    ret = {}
    for r in cur.fetchall():
        #print r,r.keys()
        ret[r['User_Id']] = r #{id:{record},,,}
    #pd.to_pickle(ret,'../cache_batches/rst__%s'% str(ret.keys()[0]))

    return ret.keys() #{uid:{field:v...} }

def query_userClue():
    sql = """
        SELECT Rele_Id,User_Account,Clue_Id
        FROM crm_t_user_clue
        #where User_Company_Id='yunke'
        #limit 1
    """
    cur = db.cursor()
    cur.execute(sql)
    ret = {}
    for r in cur.fetchall():
        #print r,r.keys()
        ret[r['Rele_Id']] = r #{id:{record},,,}
    #pd.to_pickle(ret,'../cache_batches/rst__%s'% str(ret.keys()[0]))

    return ret #{id:{field:v...} }

def query_exist_in_userportal(uid):
    sql="""
        SELECT count(*)
        from crm_t_portaluser
        WHERE User_Id='%s'
        #limit 1
        """
    cur = db.cursor()
    #print sql%uid
    cur.execute(sql%uid);
    for r in cur.fetchall():
        #print r,r['count(*)']==1#{'count(*)': 1L} True
        rst=r['count(*)']

    return rst

def query_exist_in_clue(cid):
    sql="""
        SELECT count(*)
        from crm_t_clue
        WHERE CLue_Id='%s'
        #limit 1
        """
    cur = db.cursor()
    #print sql%uid
    cur.execute(sql%cid);
    for r in cur.fetchall():
        #print r,r['count(*)']==1#{'count(*)': 1L} True
        rst=r['count(*)']

    return rst





if __name__=='__main__':
    """
    ### get genuin use from portalUser

    fake_uid_list=query_fakeUser()
    all_uid_list=query_all_uid()
    all_uid_list=[uid for uid in all_uid_list if len(uid)>10]
    genuine_uid_list=list(set(all_uid_list)-set(fake_uid_list));print 'all-fake',len(genuine_uid_list)
    pd.to_pickle(genuine_uid_list,'../data/portalUser_genuine_uid')

    #### get <uid,cid> from user_clue

    ret=query_userClue()
    pd.to_pickle(ret,'../data/uid_cid')#{id:{field:v..}

    ### get cid from clueTable

    cid_list=query_all_cid()
    pd.to_pickle(cid_list,'../data/clueTable_cid')


    #### positive sample | get valid <uid,cid>  [uid in portalUser] & [cid in clue]
    uid_cid=pd.read_pickle('../data/uid_cid') #{id:{uid:v,cid:v
    genuineUser_userportal=pd.read_pickle('../data/portalUser_genuine_uid')
    all_cid=pd.read_pickle('../data/clueTable_cid')
    valid_uidCid_list=[]
    uc_uid=[]
    uc_cid=[]
    for uid_cid in uid_cid.values()[:]:

        uid=uid_cid['User_Account']
        if uid not in genuineUser_userportal:continue
        cid=uid_cid['Clue_Id']
        num_uid_in_userPortal=query_exist_in_userportal(uid)
        num_cid_in_clue=query_exist_in_clue(cid)
        if num_uid_in_userPortal*num_cid_in_clue!=0:
            valid_uidCid_list.append((uid,cid))


    print 'valid genuine',len(valid_uidCid_list)
    pd.to_pickle(valid_uidCid_list,'../data/positive_valid_uidcid_pair')
    """

    ##### negative sample valid means in portalUser, clueTable
    positive_valid_uc=pd.read_pickle('../data/positive_valid_uidcid_pair')

    num_obs=len(positive_valid_uc)
    #pd.read_pickle(valid_uidCid_list,'../data/postive_valid_uidcid_pair')
    uid_cid=pd.read_pickle('../data/uid_cid') #{id:{uid:v,cid:v
    genuineUser_userportal=pd.read_pickle('../data/portalUser_genuine_uid')
    all_cid=pd.read_pickle('../data/clueTable_cid')
    downloaded_cid=[uc['Clue_Id'] for uc in uid_cid.values()]
    undownloaded_cid_valid=list(set(all_cid)-set(downloaded_cid))
    undownloaded_cid_obs=random.sample(undownloaded_cid_valid,num_obs)
    negative_valid_uidcid=[(positive_valid_uc[i][0],undownloaded_cid_obs[i]) for i in range(num_obs)]
    pd.to_pickle(negative_valid_uidcid,'../data/negative_valid_uidcid_pair')
















