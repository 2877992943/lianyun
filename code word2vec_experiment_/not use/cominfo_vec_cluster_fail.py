#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv,random,os
import pandas as pd

from MySQLdb import cursors
from gensim import corpora
import gensim

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
from elasticsearch import Elasticsearch,helpers
es = Elasticsearch("123.57.62.29")

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
        #limit 100000


        """

    df=pd.read_sql(sql,db)
    return df
    #df.to_csv("../allClueId.csv",index=False,encoding='utf-8')




def insert_into_synonym(word,synonym):
    sql="insert into synonym_cominfo values(%s,%s)" # id word synonym
    cur=db.cursor()

    cur.execute(sql,(word,synonym))
    cur.close()
    db.commit()
    # db.close()





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

class mySentence(object):
    def __init__(self,fpath):
        self.fpath=fpath

    def __iter__(self):
        for fname in os.listdir(self.fpath):
            docList=pd.read_pickle(self.fpath+fname)
            for line in docList:
                yield line.split(' ')

class mySentence1(object):
    def __init__(self,fpath):
        self.fpath=fpath

    def __iter__(self):
        for fname in os.listdir(self.fpath)[:]:
            cid_parse=pd.read_pickle(self.fpath+fname)
            for cid,parse in cid_parse.items()[:]:
                wordList=parse.split(' ')
                wordList=[w.strip(' ') for w in wordList]
                indList=[]
                for word in wordList:
                    if word in word_ind_dict:
                        indList.append(str(word_ind_dict[word]))
                if len(indList)==0:continue
                yield indList

class generateParseCominfo(object):
    def __init__(self,fpath):
        self.fpath=fpath

    def __iter__(self):
        for fname in os.listdir(self.fpath)[:1]:
            cid_parse=pd.read_pickle(self.fpath+fname)
            for cid,parse in cid_parse.items()[:10]:
                yield [cid,parse]

def get_docfreq(ind):
    word=ind_word_dict[int(ind)]
    if word not in word_df_dict:return 0
    else:
        return word_df_dict[word]

def docInd2arrSentence(doc):
    arr_s=np.zeros((100,))
    indList=doc.split(' ')
    for ind in indList:
        df=get_docfreq(ind)
        #print ind,ind_word_dict[int(ind)]
        try:
            arr= model[ind]
            #print arr.shape
            arr_s=arr_s+arr*float(df)
        except:
            #print 'fail'
            continue
    return arr_s

def update_es_byID(cid,value,es_index):
    body={'doc':{'tfidf_keyword':value } }
    es.update(index=es_index,doc_type='clue',id=cid,body=body)

def update_sql_otherAttribute(word,field,value):
    cur = db.cursor()

    cur.execute( """update synonym_cominfo set %s = '%s' where word='%s'"""%(field,value,word))

    ####
    db.commit()
    cur.close()


def filter_idf(word_idf_dict,ll):
    wordi_idf={}
    for word in ll:
        if word in word_idf_dict:
            idf=word_idf_dict[word]
            wordi_idf[word]=idf
    ###
    ss=sorted(wordi_idf.iteritems(),key=lambda x:x[1],reverse=True)
    n=100 if len(ss)>=100 else len(ss)
    ss=[w[0] for w in ss][:n]
    return ss


if __name__ == "__main__":

    model=pd.read_pickle('../data/word2vec_model_wordInd')
    word_idf_dict=pd.read_pickle('../data/word_idf_dict')
    word_ind_dict=pd.read_pickle('../data/word_ind_dict');print len(word_ind_dict)
    ind_word_dict=dict(zip(word_ind_dict.values(),word_ind_dict.keys()))



    path='../parseFile_businessScope/'


    gene=generateParseCominfo(path)
    notFoundAtAll=0
    for it in gene:
        print 'new cominfo....'
        cid,parse=it

        ## for each doc
        wid_idfVec_dict={}

        striList=parse.split(' ')
        striList=[s.strip(' ') for s in striList]
        striList_unique=list(set(striList))
        striList_unique=filter_idf(word_idf_dict,striList);print 'idf filter',len(striList_unique)

        for word in striList_unique:
            if word in word_ind_dict and word in word_idf_dict:
                ind=word_ind_dict[word]
                idf=word_idf_dict[word]
                try:
                    vec=model[str(ind)]
                    wid_idfVec_dict[str(ind)]=[idf,vec]
                except Exception,e:
                    continue
        print len(wid_idfVec_dict)
        pd.to_pickle(wid_idfVec_dict,'../data/wid_idfVec')


        ### cluster
        wid_idfVec_dict=pd.read_pickle('../data/wid_idfVec')
        wids=[p for p in wid_idfVec_dict.keys()]#[1237,9780,1110,5,19...]
        X=np.array([p[1] for p in wid_idfVec_dict.values()])
        idfs=[p[0] for p in wid_idfVec_dict.values()]

        from sklearn.cluster import KMeans
        n_topic=5
        kmeans = KMeans(n_clusters=n_topic, random_state=0).fit(X)
        lb=kmeans.labels_#[0,0,0,1,1,0..]
        for topic in range(n_topic)[:]:
            lb0=np.where(lb==topic)[0];print 'lb0 size',len(lb0)
            idf_clusterI=[]
            word_list=[]
            ####
            for i in lb0:
                wid=int(wids[i])
                idf=idfs[i]

                if wid in ind_word_dict:
                    word=ind_word_dict[wid]
                    word_list.append(word)
                    #print topic,word,idf
                    idf_clusterI.append(idf)
            ####
            print ' '.join(word_list)
            #print 'this cluster idf',np.mean(idf_clusterI),np.std(idf_clusterI),np.min(idf_clusterI),np.max(idf_clusterI)


















































































































































