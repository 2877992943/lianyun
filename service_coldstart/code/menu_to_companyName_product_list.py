# coding=utf-8

import numbers
import json
import jieba

#import clue
#from base import ClueFeature
#from base import get_clues
import MySQLdb
from MySQLdb import cursors
import sys,cPickle,re
import pandas as pd
import numpy as np
reload(sys)
sys.setdefaultencoding('utf8')


def calculate_capital(raw):

    if raw is None:
        return 0.0
    numbers = re.findall('\d*\.\d+|\d+', raw)# 0.4 or 4

    ret = 0.0
    if numbers:
        ret = float(numbers[-1])
        if '万' in raw:
            ret *= 1e4
        elif '亿' in raw:
            ret *= 1e8
    if ret == 0.0:
        ret += 1.0
    return math.log10(ret)

def strQ2B(ustring):
    """全角转半角"""
    rstring = ""
    for uchar in ustring:
        inside_code=ord(uchar)
        if inside_code == 12288:                              #全角空格直接转换
            inside_code = 32
        elif (inside_code >= 65281 and inside_code <= 65374): #全角字符（除空格）根据关系转化
            inside_code -= 65248

        rstring += unichr(inside_code)
    return rstring

def remove_brace_content(item):
    #print '0',item
    item=strQ2B(item.decode('utf-8'))

    item=re.sub("(\(.*?\))",' ',item)  # ? means not greedy, not match as many as possible
    #item=re.sub("(（.*）)",' ',item)

    #print '1',item
    return item

def calculate_text(raw_str): #return wordlist
    try:
        ### quanjia banjiao
        raw_str=raw_str.decode('utf-8')

        raw_str=strQ2B(raw_str)
        #####
        raw_str = re.sub("[\s+\.\!\/_,$%^*()+\"\']+|[+——！，。;,#$%^&_？、~@#￥%……&*（）:“”【】]+".decode("utf8"), " ".decode("utf8"),raw_str)
        raw_str=re.sub("[\d]+","",raw_str)
        #raw_str=re.sub(u'和',' ',raw_str.decode('utf8'))
        #raw_str=re.sub(u'及',' ',raw_str.decode('utf8'))

        ####
        split_pattern = '[\\s;,/#|]+' #s->space
        words = re.split(split_pattern, raw_str)
        words = filter(lambda w: w and len(w) > 0, words)
        return [word.decode('utf-8') for word in words]
    except:
        print raw_str
        if isinstance(raw_str,str)==False and isinstance(raw_str,unicode)==False:return []

def extract_tag(stri_list):
    
    from jieba import analyse
    #print stri
    ll=[]

    for stri in stri_list:
        for x, w in jieba.analyse.textrank(stri, withWeight=True):
            if w>=0.0 and x not in ll:
                ll.append(x);#print x
    return ll

def wordParsing(string):
    seg_list = jieba.cut_for_search(string)
    ll=' '.join(seg_list)
    return ll.split(' ')

def wordParsing_all(string,flag):
    seg_list = jieba.cut(string,cut_all=flag)
    ll=' '.join(seg_list)
    return ll.split(' ')




def remove_alegedNotParticipate(ll):
    ll_rst=[]
    alegedWords=['除外','不含','不包括','但','不得','未经']
    for w in ll:
        if any(aWord in w for aWord in alegedWords):
            continue
        else:ll_rst.append(w)
    return ll_rst

def getStopWord():
    stopWordList=['技术推广','研究院','研究','电子','电子产品','预包装食品',\
                  '贸易','咨询服务','工程','无一般经营项目','中心','信息','咨询',\
                  '管理','科技','研发','推广','设计','制造','制作','生产','加工','销售',\
                  '代销','许可经营项目','一般经营项目','标准','规程','定额','零售','配件','服务',\
                  '发展','开发','电子科','电子科技','批发']
    return stopWordList
def remove_digit_english_punct(str):
    raw_str=strQ2B(str.decode('utf-8'))
    raw_str = re.sub("[\s+\.\!\/_,$%^*()+\"\']+|[+——！，。;,#$%^&_？、~@#￥%……&*（）:“”【】]+".decode("utf8"), " ".decode("utf8"),raw_str)
    raw_str=re.sub("[\d]+","",raw_str)
    raw_str=re.sub("[\w]+","",raw_str)
    return raw_str



def strip_word(s):
    s_origin=s
    s=s.decode('utf-8')
    wordList1=['经营','从事','提供','批发','业务','制造','销售','从事','等']
    wordList2=['的','以及','和','及']
    for w in wordList1:

        s=re.sub(r"(%s)"%w.decode('utf-8'),"".decode('utf-8'),s.decode('utf-8'));


    for w in wordList2:

        s=re.sub(r"(%s)"%w.decode('utf-8')," ".decode('utf-8'),s.decode('utf-8'))

    return s+' '+s_origin if s!=s_origin else s_origin

def calc_com_info_weight(cominfo_i): #cominfo_str -> {word:weight}
    # split
    ll=calculate_text(cominfo_i)
    wordList=[]
    # parsing + notParsing
    for w in ll:
        wordList+=wordParsing_all(w)
    wordList=[w for w in wordList if len(w.decode('utf-8'))>1]
    word_weight_dict={}
    ## tfidf
    idf=pd.read_pickle('../data/word_idf_dict')
    for word in wordList:
        if word in idf:
            if word not in word_weight_dict:
                word_weight_dict[word]=idf[word]
            else:word_weight_dict[word]+=idf[word]
    return word_weight_dict


def combine_parsed(ll):
    n=len(ll);
    ll_ret=[]
    for i in range(n)[:-1]:

        pair=ll[i]+ll[i+1];
        ll_ret.append(pair)
    return ll_ret
    

if __name__ == '__main__':
    ## xls ->string ->  [company_name,..] [ [product1,p2,p3..],...  ]
    filename='../backup1/'
    df_company_name=pd.read_csv(filename+'company_name.csv',encoding='utf-8',skip_blank_lines=False);
    df_produce=pd.read_csv(filename+'produce.csv',encoding='utf-8',skip_blank_lines=False);
    #df_capital=pd.read_csv(filename+'capital.csv',encoding='utf-8',skip_blank_lines=False);
    print df_company_name.values.shape,df_produce.values.shape

    #df=pd.concat((df_company_name,df_produce,df_capital),axis=1)
    #print df.values.shape,type(df)

    #df.to_csv('../threeColumns.csv',index=False,encoding='utf-8')

    #########
    # clueid =test_i


    ################
    # produce split
    rawstrList=np.squeeze(df_produce.values).tolist();print 'produce',rawstrList.__len__()
    productList=[]
    for i in range(len(rawstrList))[:]:
        raw_i=rawstrList[i]

        word_list=calculate_text(raw_i) #[w,w]
        #print raw_i,word_list
        productList.append(word_list)

    print 'product',len(productList)

    companyNameList=np.squeeze(df_company_name.values).tolist()
    pd.to_pickle([productList,companyNameList],'../backup1/query_companyName_product_list')
    pd.DataFrame({'companyName':companyNameList[:],'product':productList}).to_csv('../backup1/query_comName_product.csv',index=False,encoding='utf-8')


    ############
    # capital
    #raw=np.squeeze(df_capital.values)[0]
    #rst=calculate_capital(raw)








