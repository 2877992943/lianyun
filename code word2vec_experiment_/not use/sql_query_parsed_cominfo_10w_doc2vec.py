#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv,random
import pandas as pd

from MySQLdb import cursors
from gensim import corpora
import gensim
from random import shuffle

#from clue import *
import numpy as np
#from sklearn.datasets import load_svmlight_file
#from base import CategoricalTextMixIn, ClueFeature, FieldExistsMixIn, ParseNumberMixIn,get_clues
#import pylab as plt
import sys,time
from sklearn.neighbors import NearestNeighbors
reload(sys)
sys.setdefaultencoding('utf8')
NUM_DOC=100000
topic_z=100

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

def get_all_idx():
    '''
    Get all the Clue_Id
    '''
    sql = """
        SELECT clue_id from company_parse
        #where clue_id!=null
        limit 100000


        """

    df=pd.read_sql(sql,db)
    return df
    #df.to_csv("../allClueId.csv",index=False,encoding='utf-8')










def extract_tag(stri):
    import jieba
    from jieba import analyse
    #print stri
    ll=[]
    for x, w in jieba.analyse.textrank(stri, withWeight=True):
        if w>=0.0:
            ll.append(x);#print x
    return ll





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


def get_cominfo(start,size): #list
    sql ="""
    SELECT CLue_Id,com_info
    from crm_t_clue
    limit %s,%s
    """
    #where CLue_Id  IN ('%s')
    #and main_produce is not null
    start=str(start)
    size=str(size)
    cur = db.cursor()
    #print sql%(start,size)
    #cur.execute(sql % "','".join(clueIds))
    cur.execute(sql%(start,size))
    ret = {}
    for r in cur.fetchall():
        ret[r['CLue_Id']] = r['com_info'] #{id:{record},...}
    #df=pd.read_sql(sql % "','".join(clueIds),db);#print sql % "','".join(clueIds)
    return ret

def get_cominfo_parse(cidlist):
    sql ="""
    SELECT clue_id,com_info
    from company_parse
    where CLue_Id  IN ('%s')

    """
    cur = db.cursor()
    cur.execute(sql % "','".join(cidlist))
    ret = {}
    for r in cur.fetchall():
        ret[r['clue_id']] = r['com_info'] #{id:{record},...}
    return ret

def write2lightSVM(filename,docList):
    i=0
    with open(filename, 'w') as f:
        for row_dict in docList: # each rowdict ->1 obs  {fidx:fv...}
            ##

            f.write(str(0)+' ') # f.write('0 ')
            i+=1
            for fid in sorted(row_dict.keys()):
                fval = row_dict[fid]
                if fval != 0.0:
                    f.write('%d:%f ' % (fid, fval))## write into  "modeling.svm"
            f.write('\n')


def describe_each_topic(ind_word_dict,lda):
    for topic in range(topic_z):
        print 'topic',topic,'*'*20
        wid_prob_list=lda.get_topic_terms(topic,10)
        widList=[w[0] for w in wid_prob_list]
        for wid in widList:
            print ind_word_dict[wid+1]


def test_bow():
    bow={1:1.3,4:5.6}
    bow=sorted(bow.iteritems(),key=lambda k: k[0],reverse=False)
    return bow

def prepare_pzd_all(lda,bow_all):
    pzd= lda.get_document_topics(bow_all);print pzd
    # num= pzd.__len__()
    # pzd_ll=[]
    # for i in range(num):
    #     pzd_i=next(iter(pzd))
    #     #print pzd_i
    #     pzd_ll.append(pzd_i)
    #pzd_ll=list(pzd);print pzd_ll[0] #[(14, 0.014053555845178206), (15, 0.01136567258215288),
    return pzd

class LabeledLineSentence(object):
    def __init__(self,lineList):

        self.lineList=lineList
    def __iter__(self):
        #self.lineList=shuffle(lineList)
        for uid,line in enumerate(self.lineList):
            yield gensim.models.doc2vec.TaggedDocument(words=line.split(' '),tags=['SENT_%s'%uid])



if __name__ == "__main__":
    ##  get {cid:cominfo str


    """
    df=get_all_idx();print df.shape
    cidlist_tt=df['clue_id'].values.tolist()
    num=len(cidlist_tt)
    batch_size=100000
    for batch in range(num/batch_size+1)[:1]:
        cidlist=cidlist_tt[batch*batch_size:batch*batch_size+batch_size]
        ret=get_cominfo_parse(cidlist)#{cid:stri...}
        print len(ret)
        pd.to_pickle(ret,'../cominfo_file/cominfo_'+str(batch))#[stri,,]
    """



    """
    ## go through all doc-> {word:doc freq}  {word:ind}
    cid_stri=pd.read_pickle('../cominfo_file/cominfo_0')
    word_df={}
    word_ind={}
    lineList=[]
    for cid,stri in cid_stri.items()[:]:
        if stri in ['null','NULL',None,'']:continue
        #print stri
        # wordList=stri.split(' ')
        # wordSet=set(wordList)
        lineList.append(stri)
    #     for word in wordSet:
    #         if word not in word_df:
    #             word_df[word]=1
    #             word_ind[word]=len(word_ind)
    #         else:word_df[word]+=1
    # pd.DataFrame({'word':word_df.keys(),'df':word_df.values()}).to_csv('../word_df.csv',index=False,encoding='utf-8')
    # print len(word_df),len(word_ind)
    # pd.to_pickle(word_ind,'../data/word_ind')
    # pd.to_pickle(word_df,'../data/word_df')
    pd.to_pickle(lineList,'../data/lineList')
    """


    """
    #### word2vec
    lineList=pd.read_pickle('../data/lineList')
    print len(lineList)
    stri=lineList[0];print stri

    word=stri.split(' ')[4];
    #wordlist='电子科技 有限公司 一家 专业 信息化 软硬件 产品 技术 服务提供商'.decode('utf-8').split(' ')
    #print ' '.join(wordlist)

    sentences=LabeledLineSentence(lineList)
    model=gensim.models.Doc2Vec(alpha=0.025,min_alpha=0.025)
    model.build_vocab(sentences)

    for epoch in range(10):
        model.train(sentences)
        model.alpha-=0.002
        model.min_alpha=model.alpha
    model.save('../data/doc2vec_model')
    """




    ### test
    lineList=pd.read_pickle('../data/lineList')
    print len(lineList)
    stri=lineList[0];print stri

    model=gensim.models.Doc2Vec.load('../data/doc2vec_model')
    #print model['SENT_1']
    w_dist= model.most_similar(positive=["电子科技".decode('utf-8')])
    print 'query:'+"电子科技"
    print 'similar word as below'
    for elem in w_dist[:10]:
        print elem[0],elem[1]






























































































































