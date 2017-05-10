#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv,re
import pandas as pd

from MySQLdb import cursors
import numpy as np

import es_util_generatePart
import sys,time,os

from elasticsearch import Elasticsearch
import elasticsearch

reload(sys)
sys.setdefaultencoding('utf8')
es = Elasticsearch("123.57.62.29",timeout=200)

"""db = MySQLdb.connect(host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com',
                     user='yunker',
                     passwd="yunker2016EP",
                     db="xddb",
                     use_unicode=True,
                     charset='utf8',
                     cursorclass=cursors.DictCursor)"""




def query_clueDB(es_index,province,city,comnameMust,comnameMustNot_list,geo,position,employeeNum,registrationYear,keyword_list,keyword_field,companyCode):
    print 'param',es_index,province,city,comnameMust,comnameMustNot_list,geo,position,employeeNum,registrationYear,keyword_list,keyword_field#[u'']
    '''
    :param es_index:
    :param province:
    :param city:
    :param comnameMust:
    :param comnameMustNot_list:
    :param geo:  float list
    :param position:
    :param employeeNum: int
    :param registrationYear: int
    :param keyword_list:  cannot be empty,otherwise how to aggregate count
    :param keyword_field:
    :return:
    '''


    #### filter



    ### filtered query
    # see below


    keyword_list=denoise_keyword(keyword_list)


    #### aggs dict
    aggs_dict={}
    shd_list=[]
    for i in range(len(keyword_list)):
        keyword=keyword_list[i]

        print keyword
        aggs_dict[keyword+'_'+keyword_field]={"filter": {"match_phrase": {keyword_field: keyword}}} #keyword_field either be Clue_Entry_Com_Name or main_produce
        shd_list.append({"match_phrase": {keyword_field: keyword}})
    # #### 2-gram
    # for i in range(len(keyword_list))[:]:
    #     for j in range(len(keyword_list)):
    #         if i==j:continue
    #         pair=keyword_list[i]+' '+keyword_list[j]
    #         aggs_dict[pair+'_'+keyword_field]={"filter": {"match":{keyword_field:{"query":pair, "operator":"and"}}}} #keyword_field either be Clue_Entry_Com_Name or main_produce


    clean_filter=es_util_generatePart.generate_clean_filter()
    called_filter=es_util_generatePart.generate_called_filter(companyCode)
    comnameMustNot_list1=es_util_generatePart.generate_companyNameMustNotList(comnameMustNot_list)
    comnameMust=es_util_generatePart.generate_companyNameMust(comnameMust)
    city=es_util_generatePart.generate_city(city)
    province=es_util_generatePart.generate_province(province)
    geo=es_util_generatePart.generate_geo(geo)
    employeeNum=es_util_generatePart.generate_employeeNum(employeeNum)
    position=es_util_generatePart.generate_position(position)
    registrationYear=es_util_generatePart.generate_registrationYear(registrationYear)

    que={"query":
            {
             "filtered":
                {
                "query": {
                        "bool": {
                                "must_not": comnameMustNot_list1+clean_filter+called_filter,
                                "must": [comnameMust],
                                "filter": [province,city,geo,employeeNum,position,registrationYear]
                                }
                        },


                 "filter": {
                        "bool": {
                                "should": shd_list
                          }
                        }
                      }
                    },

        "aggs":aggs_dict
        }


    print 'query',que

    rs = es.search(index=es_index,doc_type="clue",scroll='80s',size=500,body=que)



    query_total = rs['hits']['total']
    agg=rs['aggregations']
    key_count={}
    for agg_name,v in agg.items():
        if v['doc_count']==0:continue
        key_count[agg_name]=v['doc_count']

    ####sort
    ss=sorted(key_count.iteritems(),key=lambda s:s[1])
    key_count=[[s[0] for s in ss],[s[1] for s in ss]]
    return key_count,query_total





def denoise_keyword(keyword_list):
    ll=[]
    for keyword in keyword_list:
        if isinstance(keyword,str)!=True and isinstance(keyword,unicode)!=True:continue
        if keyword in ['',None,np.nan]:continue
        keyword=keyword.decode('utf-8')
        if keyword.__len__()>8:continue
        if keyword in ll:continue
        ll.append(keyword)
    return ll













if __name__=='__main__':

    companyCode=''


    forbidden_comname=[]#'科技 器械 厂 技术 批发 进出口 商贸 宾馆 贸易 网络技术 文化传媒 进出口 酒店 设施 用品 商行 制品 仪器 设备 器材 工具 加盟'.split(' ')

    province,city,comnameMust,comnameMustNot_list,geo,position,employeeNum,registrationYear='北京','','',forbidden_comname,[],'法人','',''



    ##############

    df=pd.read_csv('../data/keyword_comname.csv');print df.shape
    keyword=df['keyword'].values.tolist()[:]


    key_count,total=query_clueDB('clue_*filtered',province,city,comnameMust,comnameMustNot_list,geo,position,employeeNum,registrationYear,keyword,'Clue_Entry_Com_Name',companyCode)#'Clue_Entry_Com_Name' main_produce
    print total
    pd.DataFrame({'key':key_count.keys(),'count':key_count.values()}).to_csv('../data/key_count_comname.csv',index=False,encoding='utf-8')



    df=pd.read_csv('../data/keyword_business.csv');print df.shape
    keyword=df['keyword'].values.tolist()[:]


    key_count,total=query_clueDB('clue_*filtered',province,city,comnameMust,comnameMustNot_list,geo,position,employeeNum,registrationYear,keyword,'main_produce',companyCode)#'Clue_Entry_Com_Name' main_produce
    print total
    pd.DataFrame({'key':key_count.keys(),'count':key_count.values()}).to_csv('../data/key_count_business.csv',index=False,encoding='utf-8')



















































































