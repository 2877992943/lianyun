#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv,re
import pandas as pd

from MySQLdb import cursors
import numpy as np
#from clue import *

#from base import CategoricalTextMixIn, ClueFeature, FieldExistsMixIn, ParseNumberMixIn,get_clues
#import pylab as plt
import sys,time,os
from menu_to_companyName_product_list import strQ2B,remove_brace_content,wordParsing,calculate_text,remove_alegedNotParticipate,strip_word,wordParsing_all,remove_digit_english_punct,combine_parsed
import read_csv
import jieba
reload(sys)
sys.setdefaultencoding('utf8')


dictPathList=['../backup/word_ind_dict','../backup/word_idf_dict','../backup/word2vec_model_wordInd',
              '../backup/noise_extend']
word_ind_dict_path,word_idf_dict_path,word2vec_model_path,noisePath=dictPathList

# db = MySQLdb.connect(host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com',
#                      user='yunker',
#                      passwd="yunker2016EP",
#                      db="xddb",
#                      use_unicode=True,
#                      charset='utf8',
#                      cursorclass=cursors.DictCursor)



def get_cid():
    '''
    Get all the Clue_Id
    '''
    sql = "SELECT clue_id from crm_t_clue where param2=1 and param4='北京'"
    df=pd.read_sql(sql,db)
    #df.to_csv("../allClueId.csv",index=False,encoding='utf-8')
    return df


def get_clue(clue_ids):
    sql = """
        SELECT *
        FROM crm_t_clue
        WHERE CLue_Id IN
              ('%s')
    """
    cur = db.cursor()
    cur.execute(sql % "','".join(clue_ids))
    ret = {}
    for r in cur.fetchall():
        ret[r['CLue_Id']] = r #{id:{record},...}

    end_time=time.time()

    print 'len cache',len(CLUE_CACHE)
    #print end_time - start_time

    return ret  #dict {id:{raw record},..}


def match_voc(product_i):#list
    rowdict={}
    for w in product_i:
        if w in word_weight_produce:
            rowdict[w]=word_weight_produce[w]
        else:rowdict[w]=1
    return rowdict





def comNameFeature():
    str="深圳 文化发展 实业发展"
    ll=str.split(' ')
    return ['com_name_'+n for n in ll]

def analyze_comname(comnameList,noisePath):

    noiseList=pd.read_pickle(noisePath)
    comname_key_dict={}
    for comname in comnameList:
        if isinstance(comname,str) or isinstance(comname,unicode):
            stri=remove_digit_english_punct(comname)
            ll=calculate_text(stri)
            ll_parse=[]
            for phrase in ll:
                ll_1gram=wordParsing_all(phrase,False)#for search|False cut  |True cut all
                ll_parse+=ll_1gram
            ll=[w.strip(' ') for w in ll_parse if w.strip(' ') not in noiseList and len(w.strip(' ').decode('utf-8'))>1]
            ### 1-gram
            for word in ll:
                if word not in comname_key_dict:comname_key_dict[word]=1
                else:comname_key_dict[word]+=1
            #### 2-gram  (comname has sequence feature,

            if len(ll)>=2:
                ll_2gram=combine_parsed(ll)
                for word in ll_2gram:
                    if word not in comname_key_dict:comname_key_dict[word]=1
                    else:comname_key_dict[word]+=1

    ####

    return comname_key_dict

def analyze_business(productList,prior_n):
    all_words_dict={}
    for item in productList[:]:
        #print 'old',item

        if isinstance(item,str) or isinstance(item,unicode):
            if item in ['',None]:continue

            item=remove_brace_content(item)#str -> str
            item=remove_digit_english_punct(item)
            ll=calculate_text(item);#str->list
            ll=remove_alegedNotParticipate(ll);#print ' '.join(ll)
            if len(ll)==0:continue

            ll=[w.strip(' ') for w in ll if len(w.strip(' ').decode('utf-8'))>1]
            ### choose first 3
            ll=ll[:prior_n] if len(ll)>=prior_n else ll

            #print 'first %d word'%prior_n,' '.join(ll)
            ###
            for word in ll:
                word=strip_word(word)
                word=word.split(' ')##stri->list
                for w in word:
                    #print [w]
                    if len(w.strip(' '))==0:continue
                    if w not in all_words_dict:all_words_dict[w]=1
                    else:all_words_dict[w]+=1
    return all_words_dict



def analyze_cominfo(cominfo,word_ind_dict_path,word_idf_dict_path,word2vec_model_path):
    cominfo_key_list=[]
    cominfoList=[]
    for item in cominfo[:]:#product str
        if isinstance(item,str) or isinstance(item,unicode):
            item=remove_brace_content(item)#str -> str
            ll=calculate_text(item);#str->list
            ll=remove_alegedNotParticipate(ll);

            ll=[w for w in ll if len(w.decode('utf-8'))>1]
            parseWord=[]
            for w in ll:
                wlist=wordParsing_all(w,True)
                wlist=[w for w in wlist if len(w.decode('utf-8'))>1]
                parseWord+=wlist
            #### parsewordList ->idf high word

    #         keylist=wordList2keyword.idf_keyword(' '.join(parseWord),10);print 'key',' '.join(keylist)
    #         cominfo_key_list+=keylist
    #         ####
            print ' '.join(parseWord)
            cominfoList.append(' '.join(parseWord))
    # pd.DataFrame({'keyword':cominfo_key_list}).to_csv('../cominfo_key.csv',index=False,encoding='utf-8')
    #########

    word_ind_dict=pd.read_pickle(word_ind_dict_path)
    word_idf_dict=pd.read_pickle(word_idf_dict_path)
    word2vec_model=pd.read_pickle(word2vec_model_path)
    #### string -> word_ind_list ->vector
    arr_sentence_list=[]
    for item in cominfoList[:]:
        arr_s=np.zeros((100,))
        print item
        wordList=item.split(' ')
        for i in range(len(wordList))[:]:
            word=wordList[i]

            if word in word_ind_dict and word in word_idf_dict:
                #print word
                wid=word_ind_dict[word]
                idf=word_idf_dict[word]
                if idf<5:continue
                #print word,idf
                #print wid,df
                arr_w=word2vec_model[str(wid)]
                arr_s=arr_s+arr_w*float(idf)

        arr_sentence_list.append(arr_s);
    print len(arr_sentence_list)
    pd.to_pickle(arr_sentence_list,'../data/targetClient_arr_list')



def sorted_dict(business_key_dict):
    business_pair=sorted(business_key_dict.iteritems(),key=lambda s:s[1],reverse=True)
    business_pair=[[s[0] for s in business_pair],[s[1]for s in business_pair]]
    return business_pair




def prepare_keywords(fpath,code_type,prior_n_business,sample_max,word_ind_dict_path,word_idf_dict_path,word2vec_model_path,noisePath):

    #df=pd.read_csv(fpath,encoding=code_type);print df.columns
    df=read_csv.csv2dataframe(fpath)
    ###########
    # product ->key word
    ##################
    # generate feature product,clear string
    # only 10
    n=sample_max if len(df['comname'].tolist())>sample_max else len(df['comname'].tolist())#sample max n from csv
    productList=df['businessScope'].tolist()[:n]
    comnameList=df['comname'].tolist()[:n]
    cominfoList=df['cominfo'].tolist()[:n]

    #####comname product cominfo
    comname_key_dict=analyze_comname(comnameList,noisePath);
    business_key_dict=analyze_business(productList,prior_n_business)
    analyze_cominfo(cominfoList,word_ind_dict_path,word_idf_dict_path,word2vec_model_path)

    ###
    business_key,business_weight=sorted_dict(business_key_dict)
    comname_key,comname_weight=sorted_dict(comname_key_dict)


    pd.DataFrame({'keyword':business_key,'w':business_weight}).to_csv('../data/keyword_business.csv',encoding='utf-8',index=False)
    pd.DataFrame({'keyword':comname_key,'w':comname_weight}).to_csv('../data/keyword_comname.csv',encoding='utf-8',index=False)


if __name__ == "__main__":
    time_start=time.time()
    '''筛选条件
    param2=1
    param1 no sales

     beijing shanghai  guangzhou chengdu

    '''


    fpath='../data/target_3fields_beauty0223.csv'
    code_type='utf-8'#'utf-8'#'gb18030'
    prepare_keywords(fpath,code_type,3,100,word_ind_dict_path,word_idf_dict_path,word2vec_model_path,noisePath)























    # ###########tfidf get keywords
    # productList=pd.read_pickle('../query_product')
    # corpus=[' '.join(product) for product in productList]
    #
    # from sklearn.feature_extraction.text import TfidfVectorizer
    # vectorizer = TfidfVectorizer(min_df=1)
    # rst=vectorizer.fit_transform(corpus);print rst.shape
    # obs=np.squeeze(rst[13,:].toarray());print obs.shape #[d,]
    #
    # fea=vectorizer.get_feature_names()
    # feaName_dict=dict(zip(range(len(fea)),fea)) #{0:feanamestr...}
    # obs_fid=np.nonzero(obs)[0]
    #
    # wordStr_w_dict={}
    # for fid in obs_fid:
    #     wordStr_w_dict[feaName_dict[fid]]=obs[fid]
    # ###
    # sorted_word2weight=sorted(wordStr_w_dict.iteritems(),key=lambda x:x[1],reverse=True)
    #
    # for word2weight in sorted_word2weight:
    #     print word2weight[0],word2weight[1]



    """
    ################
    # company name ->key word
    ####################
    comName=df['comName'].tolist()
    nameList=[]
    for name in comName:
        item=remove_brace_content(name)#str -> str
        wlist=wordParsing(item)
        wlist=[w.strip(' ') for w in wlist if w not in ['有限公司','公司','有限'] and len(w.strip(' '))>0]
        num=int(len(wlist)/2);print num
        print ' '.join(wlist)
        print ' '.join(wlist[num:])
        nameList.append(wlist[num:])
    pd.to_pickle(nameList,'../query_comName_ll')

    #### get weight, get special key word
    wordDict={}
    productList=pd.read_pickle('../query_comName_ll')
    for product_i in productList:
        if len(product_i)>0:

            rowdict=dict(zip(product_i,[1/float(len(product_i))]*len(product_i)))
            for word,weight in rowdict.items():
                #if word in first3Word:weight*=2
                if len(word)>=2:
                    if word not in wordDict:
                        wordDict[word]=weight
                    else:wordDict[word]+=weight
    #####visual
    wordDict_sort=sorted(wordDict.iteritems(),key=lambda x:x[1],reverse=True)
    cnt=0
    comName_keyword=[]
    for word_weight in wordDict_sort[:]:
        print word_weight[0],word_weight[1]
        cnt+=1
        if cnt>=5:print '.'*10# choose 10 key word for company name
        else:comName_keyword.append(word_weight[0])
    pd.to_pickle(comName_keyword,'../comName_keyword')
    """























































































