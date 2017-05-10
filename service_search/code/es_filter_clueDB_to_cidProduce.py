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
    if len(product_list)>0:
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
                                "should": shd_product+shd_comname
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





def main_tmp(filter,num_required,cominfo_required):

    attList_companyDB=['param8','main_produce']
    ## filter
    province,city,comnameMust,comnameMustNot_list,geo,position,employeeNum,registrationYear,companyCode=filter




    keyword_comname,product_i,max_candidate_searchedby_each_word=[],[],8000
    ##
    global allCompany_cid_source;allCompany_cid_source={}

    query_clueDB('clue_*filtered',province,city,comnameMust,comnameMustNot_list,geo,position,employeeNum,registrationYear,keyword_comname,product_i,max_candidate_searchedby_each_word,cominfo_required,companyCode)


    pd.to_pickle(allCompany_cid_source,'../allcid_filtered')

    ### cid ->datadict
    all_cid_source=pd.read_pickle('../allcid_filtered')
    cid_produce=select_produce(all_cid_source,attList_companyDB)

    pd.to_pickle(cid_produce,'../data/cid_produce')

if __name__=='__main__':

    companyCode='ffz3ai'
    attList_companyDB=['param8','main_produce']
    attList_companyDB1=attList_companyDB+['Clue_Entry_Com_Name']





    # filter


    forbidden_comname=[]#'科技 器械 厂 技术 批发 进出口 商贸 宾馆 贸易 网络技术 文化传媒 进出口 酒店 设施 用品 商行 制品 仪器 设备 器材 工具 加盟'.split(' ')

    province,city,comnameMust,comnameMustNot_list,geo,position,employeeNum,registrationYear='北京','','',forbidden_comname,[],'法人','',''




    df=pd.read_csv('../data/key_count_comname.csv');print df.shape
    keyword_comname=df['key'].values.tolist()[:]
    keyword_comname=[p.strip(' ').split('_')[0] for p in keyword_comname if len(p.strip(' '))>1]


    df=pd.read_csv('../data/key_count_business.csv');print df.shape
    keyword_business=df['key'].values.tolist()[:]
    keyword_business=[p.strip(' ').split('_')[0] for p in keyword_business if len(p.strip(' '))>1]







    allCompany_cid_source={}
    for i in range(len(keyword_business)/3+1)[:]:
        product_i=keyword_business[i*3:i*3+3]
        print 'company i', i,' '.join(product_i),product_i
        product_i=[p.strip(' ') for p in product_i]
        if len(product_i)<1:continue
        query_clueDB('clue_*filtered',province,city,comnameMust,comnameMustNot_list,geo,position,employeeNum,registrationYear,keyword_comname,product_i,2000,True,companyCode)


    pd.to_pickle(allCompany_cid_source,'../allcid_filtered')




    ### cid ->datadict
    all_cid_source=pd.read_pickle('../allcid_filtered')
    cid_produce=select_produce(all_cid_source,attList_companyDB)

    pd.to_pickle(cid_produce,'../data/cid_produce')


    ###
    cid_produce_name=select_produce_name(all_cid_source,attList_companyDB1)
    pd.to_pickle(cid_produce_name,'../data/cid_produce_name')
    #pd.DataFrame({'produce':cid_produce.values()}).to_csv('../produce_zhongtiekuaiyun.csv',index=False)




    """
    #### output csv
    cellCalled=[];unique_comname=[]
    attList_clueDB=['Clue_Entry_Name','Clue_Entry_Com_Name','Clue_Entry_Cellphone','Clue_Entry_Qq','Clue_Entry_Wechat','Clue_Entry_Email',\
           'Com_Address','Clue_Entry_Major','Clue_Entry_Birthday','Clue_Entry_Telephone','qiantai','chuanzhen','main_produce',\
           'com_type','main_industry','registed_capital','com_info','employees_num','param1']
    all_cid_source=pd.read_pickle('../allcid_filtered')
    dataDict={}

    for att in attList_clueDB:
        dataDict[att]=[]
    for cid in all_cid_source:
        source=all_cid_source[cid]
        if 'Clue_Entry_Cellphone' not in source or len(str(source['Clue_Entry_Cellphone']))!=11:continue
        #if 'Clue_Entry_Name' not in source or str(source['Clue_Entry_Name']) in ['--','']:continue
        if 'Clue_Entry_Cellphone' in source and str(source['Clue_Entry_Cellphone']) in cellCalled:continue
        if source['Clue_Entry_Com_Name'] in unique_comname:continue
        unique_comname.append(source['Clue_Entry_Com_Name'])
        for att in attList_clueDB:
            if att in source:
                ### replace , in cominfo with .
                if att in ['com_info','main_produce','businessScope','main_industry','Clue_Entry_Major']:
                    if source[att]==np.nan or source[att] in ['',None]:
                        dataDict[att].append('')
                        continue

                    source[att]=re.sub('[/r/n/t\r\n\t]+','',source[att])
                    source[att]=re.sub('[,]+','，',source[att])
                dataDict[att].append(source[att])
            else:dataDict[att].append('')
        #####







    klist=['Clue_Entry_Name','Clue_Entry_Com_Name','Clue_Entry_Cellphone','Clue_Entry_Qq','Clue_Entry_Wechat','Clue_Entry_Email',\
           'Com_Address','param1','Clue_Entry_Birthday','Clue_Entry_Telephone','qiantai','chuanzhen','main_produce',\
           'com_type','main_industry',\
           'shouji2','com_info','registed_capital','employees_num','beiyong4','beiyong5','beizhu','gender']

    vlist=['姓名（必填）','公司名称（必填）','电话（必填）','QQ', '微信','Email', '公司地址','职位','生日','座机','前台电话','传真','主营产品',\
           '公司类型','行业',\
           '手机2','备用1','备用2','备用3','备用4','备用5','备注','性别']


    vlist_model=['姓名（必填）','电话（必填）','公司名称（必填）','职位','行业','公司地址','公司类型','主营产品','微信','Email','QQ',\
                 '手机2','备用1','备用2','备用3','备用4','备用5','备注','性别',\
                 '生日','前台电话','座机','传真']

    df=pd.DataFrame(dataDict)[:]
    #choose columns first
    df=pd.DataFrame(df,columns=klist);print df.columns,df.shape

    namedict=dict(zip(klist,vlist))
    df1=df.rename(columns=namedict)
    # adjust attribute in sequence
    df1=pd.DataFrame(df1,columns=vlist_model)
    pd.DataFrame(df1).to_csv('../data/result_attribute_clueDB.csv',index=False,encoding='utf-8')
    """













































































