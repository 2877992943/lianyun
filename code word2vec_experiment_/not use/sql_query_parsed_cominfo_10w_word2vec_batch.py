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




def insert_into_synonym(id,word,synonym):
    sql="insert into synonym_cominfo values(%s,%s,%s)" # id word synonym
    cur=db.cursor()

    cur.execute(sql,(id,word,synonym))
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




if __name__ == "__main__":
    ##  get {cid:cominfo str



    # df=get_all_idx();print df.shape
    # df.to_csv('../data/all_cid.csv',index=False,encoding='utf-8')

    """
    df=pd.read_csv('../data/all_cid.csv')
    cidlist_tt=df['clue_id'].values.tolist()
    num=len(cidlist_tt)
    batch_size=100000
    for batch in range(num/batch_size+1)[:]:
        cidlist=cidlist_tt[batch*batch_size:batch*batch_size+batch_size]
        ret=get_cominfo_parse(cidlist)#{cid:stri...}
        print len(ret)
        pd.to_pickle(ret,'../cominfo_file/cominfo_'+str(batch))#[stri,,]
    """




    """
    ## go through all doc-> {word:doc freq}  {word:ind}
    path='../cominfo_file/'
    word_ind_dict={}
    batch=0
    for fname in os.listdir(path)[:]:
        cid_stri=pd.read_pickle(path+fname)
        print 'batch',batch,len(cid_stri)
        batch+=1
        docList=[]
        for cid,stri in cid_stri.items()[:]:#for each doc
            if stri in ['null','NULL',None,'']:continue
            striList=stri.split(' ')
            doc=[]
            for word in striList:
                word=word.strip(' ')
                if word not in word_ind_dict:
                    word_ind_dict[word]=len(word_ind_dict)
                    doc.append(word_ind_dict[word])
                else:
                    doc.append(word_ind_dict[word])
            ####
            doc=[str(s)for s in doc]
            docList.append(' '.join(doc))
        print len(docList),len(word_ind_dict)#,docList[0]


        pd.to_pickle(docList,'../cominfo_file_clean/%s'%fname)
    ######
    pd.to_pickle(word_ind_dict,'../data/word_ind_dict_1000w')
    print 'done',len(word_ind_dict)
    """


    """
    ## go through all doc-> {word:doc freq}
    path='../cominfo_file/'
    word_df_dict={}
    batch=0
    for fname in os.listdir(path)[:]:
        cid_stri=pd.read_pickle(path+fname)
        print 'batch',batch,len(cid_stri)
        batch+=1

        for cid,stri in cid_stri.items()[:]:#for each doc
            if stri in ['null','NULL',None,'']:continue
            striList=stri.split(' ')
            striList=list(set(striList))

            for word in striList:
                word=word.strip(' ')
                if word not in word_df_dict:
                    word_df_dict[word]=1.

                else:
                    word_df_dict[word]+=1.
            ####
    word_df_dict_={}
    min_freq=10
    for word,df in word_df_dict.items():
        if df<min_freq:continue
        word_df_dict_[word]=np.log(10000000./float(df))
    ######
    pd.to_pickle(word_df_dict_,'../data/word_df_dict')
    print 'done',len(word_df_dict_)#282514
    """





    #### word2vec
    """
    # train
    sentences=mySentence('../cominfo_file_clean/')
    model=gensim.models.Word2Vec(sentences)
    pd.to_pickle(model,'../data/word2vec_model')


    ### predict top10 synonym
    model=pd.read_pickle('../data/word2vec_model')

    word_ind_dict=pd.read_pickle('../data/word_ind_dict_1000w');print len(word_ind_dict)
    ind_word_dict=dict(zip(word_ind_dict.values(),word_ind_dict.keys()))
    synonym_dict={}
    for word,ind in word_ind_dict.items()[:]:
        #print word
        wordlist=str(ind).split(' ')
        try:
            w_dist= model.most_similar(positive=wordlist)
            ##
            synonymList=[]
            #print 'similar word as below'
            for elem in w_dist[:20]:
                #print elem[0],elem[1]
                #print ind_word_dict[int(elem[0])]
                synonymList.append(ind_word_dict[int(elem[0])])
            ###
            synonym_dict[word]=' '.join(synonymList);#print synonym_dict[word]
        except:
            continue

    ###
    print 'synonym',len(synonym_dict)
    pd.to_pickle(synonym_dict,'../data/synonym_dict')
    """

    """
    ### save to sql
    synonym_dict=pd.read_pickle('../data/synonym_dict');print 'dict',len(synonym_dict)

    wid=0
    for word,synonym in synonym_dict.items()[:]:
        #print wid,word.decode('utf-8'),synonym.decode('utf-8'),len(synonym.decode('utf-8').split(' '))
        insert_into_synonym(wid,word.decode('utf-8'),synonym.decode('utf-8'))
        wid+=1
    """



    #######
    # cominfo words->sentence ->vector
    model=pd.read_pickle('../data/word2vec_model')
    #
    word_ind_dict=pd.read_pickle('../data/word_ind_dict_1000w');print len(word_ind_dict)
    ind_word_dict=dict(zip(word_ind_dict.values(),word_ind_dict.keys()))
    word_df_dict=pd.read_pickle('../data/word_df_dict')

    for filename in os.listdir('../cominfo_file_clean/')[:10]:
        fpath='../cominfo_file_clean/'+filename
        docList=pd.read_pickle(fpath)
        print len(docList)
        batch_arrSentence=[]
        strSentenceList=[]
        for doc in docList[:]: #doc #2_45_90_109
            ### docind -> stri sentence
            indList=doc.split(' ')
            strList=[ind_word_dict[int(ind)] for ind in indList]
            stri=' '.join(strList)
            strSentenceList.append(stri)
            ######docind -> arr sentence
            arr_s=docInd2arrSentence(doc)
            batch_arrSentence.append(arr_s)
        ######
        print 'arr',len(batch_arrSentence)
        pd.to_pickle(batch_arrSentence,'../data/batch_arrSentence')
        pd.to_pickle(strSentenceList,'../data/strSentenceList')






    #####knn
    arrSentence=pd.read_pickle('../data/batch_arrSentence')
    strSentenceList=pd.read_pickle('../data/strSentenceList')
    arr_all=np.array(arrSentence);print arr_all.shape
    test_ind=0
    query_arr=arr_all[test_ind,:];print 'query',query_arr.shape,np.argmax(query_arr)
    candidate_arr=arr_all[:,:];print 'candiate',candidate_arr.shape


    sample_number=10
    X=arr_all
    ## train
    print 'train knn...'
    nbrs=NearestNeighbors(sample_number,algorithm='ball_tree').fit(X)

    test=0
    print 'knn query...'
    # distances,indices=nbrs.kneighbors(x_query[test,:])
    distances,indices=nbrs.kneighbors(query_arr)  #sorted by distance
    print indices.shape,distances.shape,distances[0,:10]  #[1,10] [1,10]
    Idx=np.squeeze(indices);print Idx.shape #in sequence
    print 'query stri:'+strSentenceList[test_ind]
    print 'similar stri:'
    for ind in Idx:
        print strSentenceList[ind]













































































































































