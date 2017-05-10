# coding=utf-8

import numbers
import json

import clue
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

def calculate_text(raw_str): #return wordlist
    try:
        ### quanjia banjiao
        raw_str=raw_str.decode('utf-8')

        raw_str=strQ2B(raw_str)
        #####
        raw_str = re.sub("[\s+\.\!\/_,$%^*()+\"\']+|[+——！，。？、~@#￥%……&*（）]+".decode("utf8"), " ".decode("utf8"),raw_str)

        ####
        split_pattern = '[\\s;,/#|]+' #s->space
        words = re.split(split_pattern, raw_str)
        words = filter(lambda w: w and len(w) > 0, words)
        return [word.decode('utf-8') for word in words]
    except:
        print raw_str
        if isinstance(raw_str,str)==False and isinstance(raw_str,unicode)==False:return []


if __name__ == '__main__':
    ## xls ->string ->  [company_name,..] [ [product1,p2,p3..],...  ]
    filename='../backup/'
    df_company_name=pd.read_csv(filename+'company_name.csv',encoding='utf-8',skip_blank_lines=False);
    df_produce=pd.read_csv(filename+'produce.csv',encoding='utf-8',skip_blank_lines=False);
    df_capital=pd.read_csv(filename+'capital.csv',encoding='utf-8',skip_blank_lines=False);
    print df_company_name.values.shape,df_capital.values.shape,df_produce.values.shape

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
    pd.to_pickle([productList,companyNameList],'../query_companyName_product_list')
    pd.DataFrame({'companyName':companyNameList[:],'product':productList}).to_csv('../query_comName_product.csv',index=False,encoding='utf-8')


    ############
    # capital
    #raw=np.squeeze(df_capital.values)[0]
    #rst=calculate_capital(raw)








