#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle,re
import csv
import pandas as pd

from MySQLdb import cursors
import numpy as np
#from sklearn.datasets import load_svmlight_file



import sys,time,os

from elasticsearch import Elasticsearch

"""
first match label
second calc word2vec:labelVec,extentionVec
"""
es = Elasticsearch("123.57.62.29",timeout=500)
import elasticsearch
# from gensim import corpora
# import gensim

reload(sys)
sys.setdefaultencoding('utf8')

# product sql for parse
read_host='rr-2zeg40364h2thw9m6o.mysql.rds.aliyuncs.com'
write_host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com'

# db_product = MySQLdb.connect(host=write_host,
#                      user='yunker',
#                      passwd="yunker2016EP",
#                      db="xddb",
#                      use_unicode=True,
#                      charset='utf8',
#                      cursorclass=cursors.DictCursor)


# Test MySQLdb for insert parse
db = MySQLdb.connect(host='rds0710650me01y6d3ogo.mysql.rds.aliyuncs.com',
                     user='yunker',
                     passwd="yunke2016",
                     db="yunketest",
                     use_unicode=True,
                     charset='utf8',
                     cursorclass=cursors.DictCursor)






def query_synonym(word,exact):
    sql="select * from synonym_cominfo where word like '%s' limit 10"
    cur = db.cursor()
    if exact==False:
        cur.execute(sql % ('%'+word+'%'))
    else:
        cur.execute(sql % word)

    ret = {}
    for r in cur.fetchall():
        ret[r['word']]=r
    #df=pd.read_sql(sql % "','".join(clueIds),db);#print sql % "','".join(clueIds)
    return ret





def get_keyword_fromCSV(path):
    df=pd.read_csv(path);print df.shape
    keyword=df['keyword'].values.tolist()[:]
    return keyword



def normalize(vec):
    mode=np.sqrt(np.dot(vec,vec))
    return vec/(mode+0.000001)


def calc_distance(query_arr,arr):
    query_arr=normalize(query_arr)
    arr=normalize(arr)
    dis=np.sum((query_arr-arr)*(query_arr-arr))
    dis=np.sqrt(dis)
    return dis


if __name__=='__main__':
    ######
    # word_ind_dict=pd.read_pickle('/home/yr/yunke/lda_clue/backup/word_ind_dict')
    # word_idf_dict=pd.read_pickle('/home/yr/yunke/lda_clue/backup/word_idf_dict')
    # word2vec_model=pd.read_pickle('/home/yr/yunke/lda_clue/backup/word2vec_model_wordInd')


    #####
    keyword=get_keyword_fromCSV('../keyword_product.csv')#list
    extDict={}
    for word in keyword[:]:
        ### search original word e.g.健身
        originalVec=''

        print 'original',word

        ret=query_synonym(word,True)
        if len(ret)==0:
            print 'not searched exact'
            extDict[word]=''
            continue
        # originalVec=np.array(eval(ret.values()[0]['vector']))
        # if originalVec=='':
        #     print 'no query vector'
        #     continue
        # ### search e.g.健身房
        # ret=query_synonym(word)
        # #### searched word,vec
        # searchedWord_dis={}
        # for wordi,r in ret.items():
        #
        #     synonym=r['synonym']
        #     vec=np.array(eval(r['vector']))
        #
        #     print 'ext',wordi,synonym
        #
        #     searchedWord_dis[wordi]=calc_distance(originalVec,vec)
        # ### sort
        # ss=sorted(searchedWord_dis.iteritems(),key=lambda x:x[1],reverse=False)
        # #topn= int(len(ss)/float(2))
        # topn=1 if len(ss)>=1 else len(ss)
        # for s in ss[:topn]:
        #     print s[0],s[1]
        #     extList.append(s[0])
        for searched_word,r in ret.items():
            parse=r['synonym'].split(' ')
            extDict[searched_word]=r['synonym']









    pd.DataFrame({'keyword':extDict.keys(),'synonym':extDict.values()}).to_csv('../ext_synonym.csv',index=False,encoding='utf-8')












































































































































