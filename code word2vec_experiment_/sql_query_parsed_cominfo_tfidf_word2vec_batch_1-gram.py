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

# db_product = MySQLdb.connect(host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com',
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




def insert_into_synonym(word,synonym,vec):
    sql="insert into synonym_cominfo values(%s,%s,%s)" # word synonym vec
    cur=db.cursor()

    cur.execute(sql,(word,synonym,vec))
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
        for fname in os.listdir(self.fpath)[:]:
            cid_parse=pd.read_pickle(self.fpath+fname)
            print len(cid_parse)
            for cid,parse in cid_parse.items()[:]:
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

def update_sql_cluetable(cid,field,value):
    cur = db.cursor()

    cur.execute( """update crm_t_clue set %s = '%s' where CLue_Id='%s'"""%(field,value,cid))

    ####
    db.commit()
    cur.close()


if __name__ == "__main__":




    """
    ## go through all doc-> {cid:parse}  idf
    path='../parseFile_businessScope/'
    word_ind_dict={}
    word_df_dict={}
    cid_tf_dict={}

    gene=generateParseCominfo(path)
    for it in gene:
        cid,parse=it
        striList=parse.split(' ')
        striList=[s.strip(' ') for s in striList]
        striList_unique=list(set(striList))

        for word in striList_unique:

            if word not in word_ind_dict:#new word
                word_ind_dict[word]=len(word_ind_dict)
                word_df_dict[word]=1.

            else:
                word_df_dict[word]+=1.
            ####
    word_idf_dict_filtered={}
    min_freq=10
    for word,df in word_df_dict.items():
        if df<min_freq:continue
        word_idf_dict_filtered[word]=np.log(10000000./float(df))
    ######
    pd.to_pickle(word_idf_dict_filtered,'../data/word_idf_dict_2gram')
    pd.to_pickle(word_ind_dict,'../data/word_ind_dict_2gram')
    print 'done',len(word_idf_dict_filtered),len(word_ind_dict)#30wan  200wan
    """




    """
    ########  calc each doc termfreq-idf     -> update es
    path='../parseFile/'
    word_ind_dict=pd.read_pickle('../data/word_ind_dict')
    word_idf_dict=pd.read_pickle('../data/word_idf_dict')
    cid_tf_dict={}

    gene=generateParseCominfo(path)
    notFoundAtAll=0
    for it in gene:
        cid,parse=it
        ## for each doc
        doc_tfidf={}
        striList=parse.split(' ')
        striList=[s.strip(' ') for s in striList]
        striList_unique=list(set(striList))

        for word in striList_unique:
            if word not in word_idf_dict:continue
            tf=striList.count(word)
            doc_tfidf[word]=float(tf)*word_idf_dict[word]
        ### each doc
        if len(doc_tfidf)==0:continue
        ss=sorted(doc_tfidf.iteritems(),key=lambda s:s[1],reverse=True)
        ss=[p[0]for p in ss]
        ss=ss[:5] if len(ss)>=5 else ss;#print ' '.join(ss)
        #### update es
        keywords=' '.join(ss)

        # try:
        #     update_es_byID(cid,keywords,'clue_not_filtered')
        #
        # except:
        #     try:
        #         update_es_byID(cid,keywords,'clue_filtered')
        #
        #     except:
        #         notFoundAtAll+=1
        #
        #         continue
        try:
            update_sql_cluetable(cid,'tfidf_keyword',keywords)
        except Exception,e:
            print Exception,e
            notFoundAtAll+=1


    ########
    print 'done and not found is ',notFoundAtAll
    """







    #### word2vec

    """
    # train
    path='../parseFile_businessScope/'
    word_ind_dict=pd.read_pickle('../data/word_ind_dict_2gram')
    word_idf_dict=pd.read_pickle('../data/word_idf_dict_2gram')
    cid_tf_dict={}

    sentences=mySentence1(path)#{cid:parse ->[wordlist]

    model=gensim.models.Word2Vec(sentences)
    pd.to_pickle(model,'../data/word2vec_model_wordInd_10w')
    """






    ### predict top10 synonym
    model=pd.read_pickle('../data/word2vec_model_wordInd');
    word_idf_dict=pd.read_pickle('../data/word_idf_dict');print len(word_idf_dict)
    word_ind_dict=pd.read_pickle('../data/word_ind_dict');print len(word_ind_dict)
    ind_word_dict=dict(zip(word_ind_dict.values(),word_ind_dict.keys()))
    synonym_num=0
    for word,ind in word_ind_dict.items()[:100]:
        #print word,ind
        #if word not in word_idf_dict or word.strip(' ').__len__()<2:continue
        word=word.strip(' ')
        if len(word)<2:continue
        ####
        #print 'word',word
        wordlist=str(ind).split(' ')
        try:
            w_dist= model.most_similar(positive=wordlist)
            ##
            synonymList=[]
            #print 'similar word as below'
            for elem in w_dist[:10]:
                #print elem[0],elem[1]
                #print ind_word_dict[int(elem[0])]
                synonymList.append(ind_word_dict[int(elem[0])])
            ###
            synonymStri=' '.join(synonymList);
            print word,synonymStri
            #### word2vec
            arr_w=model[str(ind)];#print 'vec',arr_w
            arr_str=str([round(n,8) for n in list(arr_w)]);#print len(arr_str)
            #### insert sql test
            #insert_into_synonym(word,synonymStri,arr_str)
            synonym_num+=1

        except Exception,e:
            print Exception,':',e
            continue

    print 'synonym generate save done',synonym_num

























    """
    #######
    # try example: cominfo words->sentence ->vector
    #model=pd.read_pickle('../data/word2vec_model')
    #
    model=pd.read_pickle('../data/word2vec_model_wordInd')
    word_idf_dict=pd.read_pickle('../data/word_idf_dict')
    word_ind_dict=pd.read_pickle('../data/word_ind_dict')
    ind_word_dict=dict(zip(word_ind_dict.values(),word_ind_dict.keys()))

    for filename in os.listdir('../parseFile_businessScope/')[:1]:
        fpath='../parseFile_businessScope/'+filename
        cid_parse=pd.read_pickle(fpath)
        print len(docList)
        batch_arrSentence=[]
        strSentenceList=[]
        for cid,parse in cid_parse.items()[:10]: #doc #2_45_90_109
            ### docind -> stri sentence
            indList=parse.split(' ')
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
    """














































































































































