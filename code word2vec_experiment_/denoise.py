#! -*- coding:utf-8 -*-

import pandas as pd
import sys,os
reload(sys)
sys.setdefaultencoding('utf8')
import jieba
from jieba import analyse
import numpy as np
from menu_to_companyName_product_list import strip_word,is_chinese,remove_digit_english_punct,extract_tag,wordParsing_all,calculate_text,wordParsing,strQ2B,combine_parsed

# word_ind_dict=pd.read_pickle('/home/yr/yunke/lda_clue/backup/word_ind_dict')
# word_idf_dict=pd.read_pickle('/home/yr/yunke/lda_clue/backup/word_idf_dict')
#word2vec_model=pd.read_pickle('/home/yr/yunke/lda_clue/backup/word2vec_model_wordInd')

def idf_keyword(parse,n1):
    parseList=parse.split(' ')
    word_idf={}
    for word in parseList:
        if word in word_idf_dict:
            word_idf[word]=word_idf_dict[word]
    ###
    ll=sorted(word_idf.iteritems(),key=lambda s:s[1],reverse=True)
    n=n1 if len(ll)>=n1 else len(ll)
    return [p[0] for p in ll][:n]


def extract_tag(sentence,topn):
    ll=[]

    for x, w in jieba.analyse.textrank(sentence, topK=10,withWeight=True,allowPOS=('ns', 'n', 'vn', 'v')):
        if w>0.0:ll.append(x)
        #print('%s %s' % (x, w))
    return combine_parsed(ll)

def extract_tag_old(sentence,topn):
    ll=[]

    for x, w in jieba.analyse.textrank(sentence, topK=10,withWeight=True,allowPOS=('ns', 'n', 'vn', 'v')):
        if w>0.0:ll.append(x)
        #print('%s %s' % (x, w))
    return ll

def get_noise(path):
    return pd.read_pickle(path)


if __name__=='__main__':
    # s='简介：三唐信息技术有限有限公司成立于2005年12月，公司致力于企业一条龙服务，游戏大厂商一条龙服务。由三唐信息技术有限公司自主研发与运营的游戏平台(hp:www.gamevideo.c)，获得多家风险投资的青睐．公司与多家上市公司合作紧密，有着雄厚的资源和多名资深游戏顾问.业务范围：游戏视界，游戏厂商一条龙服务、Flash游戏开发[单机、手机、电视、连网]、Flash应用软件开发、Flash动画片制作项目质量监控体系：为了保证项目的质量，三唐全国首家自主开发出质量监控体系，让客户参与每一个项目，把关项目每个细节。上海三唐信息技术有限公司竭诚为您服务，欢迎新老客户前来洽谈。联系人：金元，联系电话：0571-*，移动电话：*。'
    # ll=extract_tag_old(s,3);print ' '.join(ll)



    #### remove district

    sql_att_list=['CLue_Id','param8','main_produce','com_info','Clue_Entry_Com_Name']
    #### clean parse
    fpath='../parseFile_businessScope/'
    unique_comname=[]

    noiseList=get_noise('../data/district_noise')

    for name in os.listdir(fpath)[:1]:
        cid_content=pd.read_pickle(fpath+name)

        comname_parse={}
        for comname,parse in cid_content.items()[:]:
            ll=parse.split(' ')
            ll=[i for i in ll if i not in noiseList]
            comname_parse[comname]=' '.join(ll)
            #print parse
            #print ' '.join(ll)



        pd.to_pickle(comname_parse,'../parseFile_businessScope/%s'%str(comname_parse.keys()[0]))




    """
    #### rank by idf  fail  to single out useful word
    word_idf_dict=pd.read_pickle('../backup/word_idf_dict')
    ss=sorted(word_idf_dict.iteritems(),key=lambda s:s[1],reverse=True)
    s1=[s[0] for s in ss]
    s2=[s[1] for s in ss]
    pd.DataFrame({'word':s1,'idf':s2}).to_csv('../show_word_idf.csv',index=False,encoding='utf-8')
    """



    """
    #####
    # global word vec cluster and idf rank to single out useful word
    word_ind_dict=pd.read_pickle('../backup/word_ind_dict')
    word_idf_dict=pd.read_pickle('../backup/word_idf_dict')
    model=pd.read_pickle('../backup/word2vec_model_wordInd')

    ind_word_dict=dict(zip(word_ind_dict.values(),word_ind_dict.keys()))

    wid_idfVec_dict={}
    striList_unique=word_ind_dict.keys()

    for word in striList_unique[:100000]:
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
    X=np.array([p[1] for p in wid_idfVec_dict.values()][:])
    idfs=[p[0] for p in wid_idfVec_dict.values()]

    from sklearn.cluster import KMeans
    n_topic=100
    kmeans = KMeans(n_clusters=n_topic, random_state=0).fit(X)




    lb=kmeans.labels_#[0,0,0,1,1,0..]
    pd.to_pickle(lb,'../kmeans_lb')
    lb=pd.read_pickle('../kmeans_lb')


    for topic in range(n_topic)[:]:
        lb0=np.where(lb==topic)[0];print 'lb0 size',len(lb0)

        word_idf={}
        ####
        for i in lb0:
            wid=int(wids[i])
            idf=idfs[i]

            if wid in ind_word_dict:
                word=ind_word_dict[wid]
                word_idf[word]=idf

        ####
        ss=sorted(word_idf.iteritems(),key=lambda s:s[1],reverse=True)
        s1=[s[0] for s in ss]
        s2=[s[1] for s in ss]
        #print ' '.join(word_list)
        #print 'this cluster idf',np.mean(idf_clusterI),np.std(idf_clusterI),np.min(idf_clusterI),np.max(idf_clusterI)
        pd.DataFrame({'word':s1,'idf':s2}).to_csv('../%d.csv'%topic,index=False,encoding='utf-8')
    """




















































































































