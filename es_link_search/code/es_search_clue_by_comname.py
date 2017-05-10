#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv
import pandas as pd

from MySQLdb import cursors
import numpy as np

#from menu_to_companyName_product_list import strip_word


#import pylab as plt
import sys,time,os

from elasticsearch import Elasticsearch
import elasticsearch

reload(sys)
sys.setdefaultencoding('utf8')


"""db = MySQLdb.connect(host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com',
                     user='yunker',
                     passwd="yunker2016EP",
                     db="xddb",
                     use_unicode=True,
                     charset='utf8',
                     cursorclass=cursors.DictCursor)"""








def query_clueDB(es_index,province_list,comName_list,product_list,major_list,total_most):
    global all_cid_source
    shd_province=[];shd_comname=[];shd_product=[];shd_major=[]
    for province in province_list:
        shd_province.append({"match_phrase":{"param4":province}})
    for name in comName_list:
        shd_comname.append({"match_phrase":{"Clue_Entry_Com_Name": name }})
    for product in product_list:
        shd_product.append({"match_phrase":{"main_produce": product }})
    for major in major_list:
        shd_major.append({"match_phrase":{"Clue_Entry_Major": major}})
        shd_major.append({"match_phrase":{"param1": major}})




    que={"query":
                {
                  "filtered": {
                            "query": { "bool":{"should":shd_province}},

                            "filter": {
                                  "bool": {
                                   # "must": { "term": { "param2": 1}},
                                    "should": shd_comname+shd_product+shd_major,
                                    # "must_not": { "match_phrase": { "param1": "销售" }}


                      }
                    }
                  }
                }
                }


    print 'query',que

    rs = es.search(index=es_index,doc_type="clue",scroll='80s',search_type='scan',size=1000,body=que)





    ###### by scroll query

    print 'scroll'



    scroll_size = rs['hits']['total']
    i=0
    while (scroll_size > 0):
        #try:
        for tryi in range(1):
            scroll_id = rs['_scroll_id']
            rs = es.scroll(scroll_id=scroll_id, scroll='80s')
            #allPages += rs['hits']['hits']
            hit_list=rs['hits']['hits'];#print 'hit',len(hit_list)
            for hit in hit_list[:]:
                cid=hit['_id'];#print 'cid',cid,hit['_source']
                comname=hit['_source']['Clue_Entry_Com_Name'].decode('utf-8')
                if comname not in comName_list:
                    continue
                all_cid_source[cid]=hit['_source']#{field:v}
                # if '凡客'.decode('utf-8') in hit['_source']['Clue_Entry_Com_Name']:
                #     print hit['_source']['Clue_Entry_Com_Name']
                #     print comName_list[0]


            scroll_size = len(hit_list)

            print 'scroll',scroll_size,len(all_cid_source)

        if len(all_cid_source)>=total_most:break
        #except:
            #break





def select_attribute(cid_source):
    cid_produce={}

    for cid,source in cid_source.items():

        obs={}
        for att in attList[:]:
            if att in hit['_source'] and hit['_source'][att]!=None:
                obs[att]=hit['_source'][att]
            else:
                obs[att]=''
        cid_produce[cid]=obs

def select_produce(cid_source,attList): #{cid:sourceDict...}
    cid_produce={}

    for cid,source in cid_source.items()[:]:

        cid_produce[cid]=''
        for att in attList[:]:
            if att in source and source[att]!=None:

                cid_produce[cid]=source[att]


    return cid_produce



if __name__=='__main__':
    ####
    # df=pd.read_csv('../data/target.csv',encoding='utf-8')
    # comnameList=df['comname'].values.tolist()

    comnameList='北京牛客科技有限公司 北京博大精深信息技术有限公司 北京美丽风尚网络技术有限公司 北京创锐文化传媒有限公司 北京礼特邦商贸中心 北京紫英阁宾馆 北京金知了玩具厂 北京雅丽化妆品批发商行 \
    云之塔（北京）科技有限公司 北京京英龙腾进出口有限公司 北京东方合生缘医疗器械有限公司 北京市还了一百游乐设备有限公司 北京林衣坊制衣厂 北京长安白云酒店管理有限责任公司 北京金豪园景园林配套设施经销中心\
    北京中控科技发展有限公司门禁产品'.split(' ')





    attList_clueDB=['main_produce']

    es = Elasticsearch("123.57.62.29",timeout=100)

    # es ->query->cidlist
    province_list,comName_list,product_list,major_list=[],[],[],[]
    #province_list="北京".split(' ')
    #major_list='HR hr 人事 行政 人力'.split(' ')
    #province_list=[p.strip(' ') for p in province_list]
    comName_list=comnameList
    #product_list='外汇 杠杆 保证金 黄金 股指 外汇代理'.split(' ')
    all_cid_source={}


    for comname in comName_list[:]:
        #print 'name',comname
        if len(comname.strip(' '))<=4:continue

        query_clueDB('clue_not_filtered',province_list,[comname],product_list,major_list,10000)
        query_clueDB('clue_filtered',province_list,[comname],product_list,major_list,10000)



    pd.to_pickle(all_cid_source,'../allcid_filtered')







    ### cid ->datadict
    all_cid_source=pd.read_pickle('../allcid_filtered');print len(all_cid_source)
    cid_produce=select_produce(all_cid_source,attList_clueDB)

    pd.to_pickle(cid_produce,'../data/cid_produce')
    pd.DataFrame({'produce':cid_produce.values()}).to_csv('../produce.csv',index=False)





    ########## directly go to csv,no knn
    #### cidlist->format the csv
    #cidlist=pd.read_pickle('../result_cid')

    attList_clueDB=['Clue_Entry_Name','Clue_Entry_Com_Name','Clue_Entry_Cellphone','Clue_Entry_Qq','Clue_Entry_Wechat','Clue_Entry_Email',\
           'Com_Address','Clue_Entry_Major','Clue_Entry_Birthday','Clue_Entry_Telephone','qiantai','chuanzhen','main_produce',\
           'com_type','main_industry','registrationdate','com_info','businessScope']
    all_cid_source=pd.read_pickle('../allcid_filtered')
    dataDict={}
    unique_comname_list=[]
    for att in attList_clueDB:
        dataDict[att]=[]
    for cid in all_cid_source:
        source=all_cid_source[cid]
        if source['Clue_Entry_Com_Name'] in unique_comname_list:continue
        if source['Clue_Entry_Com_Name'] not in unique_comname_list:unique_comname_list.append(source['Clue_Entry_Com_Name'])

        if 'Clue_Entry_Cellphone' not in source or len(str(source['Clue_Entry_Cellphone']))!=11:continue
        if 'Clue_Entry_Name' not in source or str(source['Clue_Entry_Name']) in ['--','']:continue
        for att in attList_clueDB:
            if att in source:
                dataDict[att].append(source[att])
            else:dataDict[att].append('')




    klist=['Clue_Entry_Name','Clue_Entry_Com_Name','Clue_Entry_Cellphone','Clue_Entry_Qq','Clue_Entry_Wechat','Clue_Entry_Email',\
           'Com_Address','Clue_Entry_Major','Clue_Entry_Birthday','Clue_Entry_Telephone','qiantai','chuanzhen','main_produce',\
           'com_type','main_industry',\
           'shouji2','registrationdate','businessScope','com_info','beiyong4','beiyong5','beizhu','gender']

    vlist=['姓名（必填）','公司名称（必填）','电话（必填）','QQ', '微信','Email', '公司地址','职位','生日','座机','前台电话','传真','主营产品',\
           '公司类型','行业',\
           '手机2','备用1','备用2','备用3','备用4','备用5','备注','性别']


    vlist_model=['姓名（必填）','电话（必填）','公司名称（必填）','职位','行业','公司地址','公司类型','主营产品','微信','Email','QQ',\
                 '手机2','备用1','备用2','备用3','备用4','备用5','备注','性别',\
                 '生日','前台电话','座机','传真']

    df=pd.DataFrame(dataDict)
    #choose columns first
    df=pd.DataFrame(df,columns=klist);print df.columns,df.shape

    namedict=dict(zip(klist,vlist))
    df1=df.rename(columns=namedict)
    # adjust attribute in sequence
    df1=pd.DataFrame(df1,columns=vlist_model)
    pd.DataFrame(df1).to_csv('../data/target_.csv',index=False,encoding='utf-8')














































































