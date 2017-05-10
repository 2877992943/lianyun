#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv,re
import pandas as pd

from MySQLdb import cursors
import numpy as np

import es_util_generatePart
import sys,time,os,read_csv

from elasticsearch import Elasticsearch
import elasticsearch
import mainProduce_knn,read_csv

es = Elasticsearch("123.57.62.29",timeout=200)

reload(sys)
sys.setdefaultencoding('utf8')


"""db = MySQLdb.connect(host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com',
                     user='yunker',
                     passwd="yunker2016EP",
                     db="xddb",
                     use_unicode=True,
                     charset='utf8',
                     cursorclass=cursors.DictCursor)"""


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

def query_clueDB(es_index,province,city,comnameMust,comnameMustNot_list,geo,position,employeeNum,registrationYear,comName_list,product_list,most_foundby_each_company,cominfo_mode,companyCode):
    global allCompany_cid_source
    shd_comname=[];shd_product=[];
    #### filter
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

    ### should query
    comName_list=denoise_keyword(comName_list)
    product_list=denoise_keyword(product_list)

    if len(comName_list)>0:# if after select keyword,no comname keyword meet your wish,
        for name in comName_list:
            shd_comname.append({"match_phrase":{"Clue_Entry_Com_Name": name }})
    for product in product_list:
        shd_product.append({"match_phrase":{"main_produce": product }})
        shd_product.append({"match_phrase":{"param8": product }})




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
                                "should":  [{"query":  { "match": { "Clue_Entry_Major": "财务会计出纳" }}}]
                          }
                        }
                      }
                    }
            }


    print 'query',que

    rs = es.search(index=es_index,doc_type="clue",scroll='80s',search_type='scan',size=500,body=que)





    ###### by scroll query
    #whether cominfo is must included
    cominfo_flag="com_info" if cominfo_mode==True else "CLue_Id"

    print 'scroll'



    scroll_size = rs['hits']['total']
    notRepeatCid_foundBy_thisCompany=0
    while (scroll_size > 0):


        scroll_id = rs['_scroll_id']
        rs = es.scroll(scroll_id=scroll_id, scroll='80s')
        #allPages += rs['hits']['hits']
        hit_list=rs['hits']['hits'];print ' this scroll hit',len(hit_list)
        for hit in hit_list[:]:

            cid=hit['_id'];#print 'cid',cid,hit['_source']
            if cid not in allCompany_cid_source:
                if cominfo_flag in hit['_source'] and hit['_source'][cominfo_flag] not in ['',None,'null']:
                    notRepeatCid_foundBy_thisCompany+=1
                    allCompany_cid_source[cid]=hit['_source']#{field:v}

        scroll_size = len(hit_list)
        print 'scroll %d notRepeat %d and total %d'%(scroll_size,notRepeatCid_foundBy_thisCompany,len(allCompany_cid_source))

        if notRepeatCid_foundBy_thisCompany>=most_foundby_each_company:
            break












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
            if att in source and source[att] not in ['',None]:

                cid_produce[cid]=cid_produce[cid]+' '+source[att];#print att,source[att]




    return cid_produce

def select_produce_name(cid_source,attList): #{cid:sourceDict...}

    cid_produce={}


    for cid,source in cid_source.items()[:]:

        cid_produce[cid]={}
        for att in attList[:]:
            if att in source and source[att] not in ['',None]:

                cid_produce[cid][att]=source[att];#print att,source[att]
            else:cid_produce[cid][att]=''




    return cid_produce





def main_tmp(key_count_comname_path,key_count_business_path,filter,num_required,cominfo_required):

    attList_companyDB=['param8','main_produce']
    ## filter
    province,city,comnameMust,comnameMustNot_list,geo,position,employeeNum,registrationYear,companyCode=filter

    ### keyword
    #df=pd.read_csv(key_count_comname_path);print df.shape
    df=read_csv.csv2dataframe(key_count_comname_path);print df.shape
    keyword_comname=df['key'].values.tolist()[:]
    keyword_comname=[p.strip(' ').split('_')[0] for p in keyword_comname if len(p.strip(' '))>1]


    df=read_csv.csv2dataframe(key_count_business_path);print df.shape
    keyword_business=df['key'].values.tolist()[:]
    keyword_business=[p.strip(' ').split('_')[0] for p in keyword_business if len(p.strip(' '))>1]


    ### calc max_candidate_searchedby_each_word

    max_candidate_searchedby_each_word=es_util_generatePart.calc_maxnum_each_scroll(num_required,len(keyword_business)/3.0);print 'max candidate searched by each words',max_candidate_searchedby_each_word

    ##
    global allCompany_cid_source;allCompany_cid_source={}
    for i in range(len(keyword_business)/3+1)[:]:
        product_i=keyword_business[i*3:i*3+3]
        print 'company i', i,' '.join(product_i),product_i
        product_i=[p.strip(' ') for p in product_i]
        if len(product_i)<1:continue
        query_clueDB('clue_*filtered',province,city,comnameMust,comnameMustNot_list,geo,position,employeeNum,registrationYear,keyword_comname,product_i,max_candidate_searchedby_each_word,cominfo_required,companyCode)


    pd.to_pickle(allCompany_cid_source,'../allcid_filtered')

    ### cid ->datadict
    all_cid_source=pd.read_pickle('../allcid_filtered')
    cid_produce=select_produce(all_cid_source,attList_companyDB)

    pd.to_pickle(cid_produce,'../data/cid_produce')

if __name__=='__main__':

    companyCode='jjjeva'
    attList_companyDB=['param8','main_produce']
    attList_companyDB1=attList_companyDB+['Clue_Entry_Com_Name']





    # filter


    forbidden_comname=[]#'科技 器械 厂 技术 批发 进出口 商贸 宾馆 贸易 网络技术 文化传媒 进出口 酒店 设施 用品 商行 制品 仪器 设备 器材 工具 加盟'.split(' ')

    province,city,comnameMust,comnameMustNot_list,geo,position,employeeNum,registrationYear='北京','','',forbidden_comname,[],'','',''












    allCompany_cid_source={}




    keyword_comname=[]
    product_i=[]
    query_clueDB('clue_*filtered',province,city,comnameMust,comnameMustNot_list,geo,position,employeeNum,registrationYear,keyword_comname,product_i,2000,False,companyCode)


    pd.to_pickle(allCompany_cid_source,'../allcid_filtered')




    ### cid ->datadict
    all_cid_source=pd.read_pickle('../allcid_filtered')
    cid_produce=select_produce(all_cid_source,attList_companyDB)

    pd.to_pickle(cid_produce,'../data/cid_produce')


    ###
    cid_produce_name=select_produce_name(all_cid_source,attList_companyDB1)
    pd.to_pickle(cid_produce_name,'../data/cid_produce_name')
    #pd.DataFrame({'produce':cid_produce.values()}).to_csv('../produce_zhongtiekuaiyun.csv',index=False)





    #### output csv
    cell_list=[];unique_comname=[]
    df1,_,unique_comname=mainProduce_knn.output_csv(all_cid_source.keys(),0,cell_list,all_cid_source,read_csv.attList_clueDB,unique_comname)

    df1_clean=df1[df1['备用2']==1];num_clean=df1_clean.shape[0]#df1_clean=df[df['备用2'.decode('utf-8')]==1]
    df1_unclean=df1[df1['备用2']!=1]

    pd.DataFrame(df1_clean).to_csv('../data/result_attribute_yk_clean.csv',index=False,encoding='utf-8')
    pd.DataFrame(df1_unclean).to_csv('../data/result_attribute_yk_unclean.csv',index=False,encoding='utf-8')
    pd.DataFrame(df1_unclean).to_csv('../data/result_attribute_yk_unclean.xls',index=False,encoding='utf-8')


















































































