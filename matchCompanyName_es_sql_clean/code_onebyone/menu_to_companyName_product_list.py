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

def remove_digit_english_punct(str):
    raw_str=strQ2B(str.decode('utf-8'))
    raw_str = re.sub("[\s+\.\!\/_,$%^*()+\"\']+|[+——！，。;,#$%^&_？、~@#￥%……&*（）:“”【】]+".decode("utf8"), " ".decode("utf8"),raw_str)
    raw_str=re.sub("[\d]+","",raw_str)
    raw_str=re.sub("[\w]+","",raw_str)
    return raw_str

def remove_punct(str):
    
    raw_str=strQ2B(str.decode('utf-8'))
    raw_str=raw_str.replace('\"','')
    raw_str=raw_str.replace('\\',' ')
    raw_str = re.sub("[\s+\!_$%^*()+\"\']+|[+——！#$%^&_？~@#￥%……&*（）【】]+".decode("utf8"), "".decode("utf8"),raw_str)
    raw_str = re.sub("[\s+\.\!\/_,$%^*()+\"\']+|[+——！，。;,#$%^&_？、~@#￥%……&*（）:“”【】]+".decode("utf8"), ",".decode("utf8"),raw_str)
    
    return raw_str


def is_chinese(uchar):
        #char=uchar.decode('utf-8')
    """whether unicode is chinese"""
    if uchar >= u'u4e00' and uchar<=u'u9fa5':
            return True
    else:
            return False

def remove_chinese(s):
    s=strQ2B(s.decode('utf-8'))
    s=re.sub(u"[\u4e00-\u9fa5]",'',s)
    return s


def is_chinese_1(uchar):
        #char=uchar.decode('utf-8')
    """whether unicode is chniese"""
    if uchar >= u'\u4e00' and uchar<=u'\u9fa5':
            return True
    else:
            return False



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

def extract_tag(stri,allowPOS):
    
    from jieba import analyse
    #print stri
    ll=[]
    for x, w in jieba.analyse.textrank(stri, withWeight=True,allowPOS=allowPOS):#('ns', 'n', 'vn', 'v')
        if w>=0.0 and x not in ll:
            ll.append(x);#print x
    return ll

def wordParsing(string):
    seg_list = jieba.cut_for_search(string)
    ll=' '.join(seg_list)
    return ll.split(' ')

def wordParsing_all(string):
    seg_list = jieba.cut(string,cut_all=False)
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

def strip_word(w):
    w=w.decode('utf-8')
    wordList=['经营','从事','提供','批发','业务']
    for v in wordList:
        w=w.strip(v.decode('utf-8'))
    return w

def recognize_strip_verb(stri):
    ll_v=extract_tag(stri,('vn', 'v'))#assuming parsing not good at some chemical phrase
    if len(ll_v)>0:
        for v in ll_v:
            stri=stri.replace(v,'')
    return stri

def valid_date_form(registDate):
    if registDate==None or registDate=='null':return False
    registDate=str(registDate)
    try:
        rst=re.search("[0-9]{4}-[0-9]{2}-[0-9]{2}",registDate)
        if rst==None:return False
        else:return True
    except:
        return False

def remove_single_brace(stri):
    if '('in stri and ')' not in stri:return stri.replace('(','')
    elif ')'in stri and '(' not in stri:return stri.replace(')','')
    else:return stri


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








