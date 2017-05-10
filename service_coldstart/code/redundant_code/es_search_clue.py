#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv,re,json
import pandas as pd

from MySQLdb import cursors
import numpy as np

#from menu_to_companyName_product_list import strip_word


#import pylab as plt
import sys,time,os

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





def diquMust_district(diqu_stri):
    r={"term": {
						            "param4":
						               diqu_stri.decode('utf-8'),
						                #"operator":"or"
						        }}
    return r
def diquMust_city(diqu_stri):
    r={"term": {
						            "param5":
						               diqu_stri.decode('utf-8'),
						                #"operator":"or"
						        }}
    return r
def comnameMust(comname_stri):
    r={"term": {
						            "Clue_Entry_Com_Name": comname_stri.decode('utf-8'),
						                #"operator":"or"
						        }}
    return r

def productMust(product_stri):

    r={"term": {
						            "main_produce":  product_stri.decode('utf-8'),
						                #"operator":"or"

						        }}
    return r

def businessScopeMust(business):
    r={"term": {
						            "businessScope":business.decode('utf-8'),
						                #"operator":"or"




						        }}
    return r
def com_infoMust(com_info):
    r={"term": {
						            "com_info": {
						                "query":  com_info.decode('utf-8'),
						                #"operator":"or"



						            }
						        }}
    return r
def industryMust(main_industry):
    r={"term": {
						            "main_industry": main_industry.decode('utf-8'),
						                #"operator":"or"




						        }}
    return r
def majorMust(major_stri):
    r={"term": {
						            "Clue_Entry_Major":major_stri.decode('utf-8'),
						                #"operator":"or"




						        }}
    return r


def registrationdateMust(registrationYear):
    r={
                                "range" : {
                                    "registrationdate" : {
                                        "gte" :str(registrationYear).strip(' ')+"-01-01",
                                       # "lt" :  "2089-01-01"
                                    }
                                }
                            }
    return r

def capitalMust(registed_capital_num):

    r={
                            "range" : {
                                "registed_capital_num" : {
                                    "gte" :float(registed_capital_num),
                                   # "lt" :  "2089-01-01"
                                }
                            }
                        }
    return r

def generate_query_body(comname_stri,diqu_stri,product_stri,major_stri,registrationYear,business,com_info,main_industry,registed_capital_num):
    fieldList=['comname','diqu_district','diqu_city','product','major','year','capital','business','cominfo','industry']
    striList=[comname_stri,diqu_stri[0],diqu_stri[1],product_stri,major_stri,registrationYear,registed_capital_num,business,com_info,main_industry]

    funcList=[comnameMust,diquMust_district,diquMust_city,productMust,majorMust,registrationdateMust,capitalMust,businessScopeMust,com_infoMust,industryMust]

    startStri="""{"from":0,"size":300,"query":{"bool":{"""
    mustSeries=''
    for i in range(striList.__len__()):
        method=funcList[i]
        value=striList[i]
        field=fieldList[i]

        if field=='year' and value.isdigit() and int(value)>1900 and int(value)<=2019:
            item="\"must\""+":"+json.dumps(method(str(value)))+","
            mustSeries+=item
        if field=='capital' and value.isdigit():
            item="\"must\""+":"+json.dumps(method(float(value)))+","
            mustSeries+=item

        if field not in ['year','capital'] and len(value.strip(' '))>=1:
            #item="\"must\""+":"+str(grammerList[i])+","   # fail to parse
            item="\"must\""+":"+json.dumps(method(str(value)))+","
            mustSeries+=item

    spellStri=startStri+mustSeries[:-1]+"}}}"
    que=re.sub('(\')','\"',spellStri)
    return que


def parse(hitsList):
    exhibit_num=10
    dictList=[]
    unique_comname=[]
    for hit in hitsList:
        hit=hit['_source']
        dic={}

        dic['diqu']=hit['Com_Address'] if 'Com_Address' in hit else ''
        dic['product']=hit['main_produce'] if 'main_produce' in hit else ''
        dic['comname']=hit['Clue_Entry_Com_Name'] if 'Clue_Entry_Com_Name' in hit else ''
        dic['position']=hit['Clue_Entry_Major'] if 'Clue_Entry_Major' in hit else ''
        dic['businessScope']=hit['businessScope'] if 'businessScope' in hit else ''
        if dic['comname'] in unique_comname or dic['comname'] in ['',None]:continue
        unique_comname.append(dic['comname'])
        cellphone=hit['Clue_Entry_Cellphone']
        if len(str(cellphone))<11:continue
        leadsName=hit['Clue_Entry_Name']
        if leadsName in ['','--']:continue
        dic['timestamp']=int(time.time())
        dictList.append(dic)
    n=exhibit_num if len(dictList)>=exhibit_num else len(dictList)
    return dictList[:n]

def write_local_csv(hitList):#100
    klist=['Clue_Entry_Name','Clue_Entry_Com_Name','Clue_Entry_Cellphone','Clue_Entry_Qq','Clue_Entry_Wechat','Clue_Entry_Email',\
           'Com_Address','Clue_Entry_Major','Clue_Entry_Birthday','Clue_Entry_Telephone','qiantai','chuanzhen','main_produce',\
           'com_type','main_industry',\
           'shouji2','registrationdate','registed_capital_num','param8','com_info','employees_num','beizhu','gender']

    vlist=['姓名（必填）','公司名称（必填）','电话（必填）','QQ', '微信','Email', '公司地址','职位','生日','座机','前台电话','传真','主营产品',\
           '公司类型','行业',\
           '手机2','备用1','备用2','备用3','备用4','备用5','备注','性别']


    vlist_model=['姓名（必填）','电话（必填）','公司名称（必填）','职位','行业','公司地址','公司类型','主营产品','微信','Email','QQ',\
                 '手机2','备用1','备用2','备用3','备用4','备用5','备注','性别',\
                 '生日','前台电话','座机','传真']
    dataDict={}
    unique_comname=[]
    for att in klist:
        dataDict[att]=[]
    #for cid in all_cid_source:
    print 'hit',len(hitList)
    for hit in hitList:

        source=hit['_source'];#print source.keys()[0];print hit['_source']['Clue_Entry_Cellphone']

        comName=source['Clue_Entry_Com_Name'] if 'Clue_Entry_Com_Name' in source else ''
        #print comName
        # avoid repeat comname
        if comName in unique_comname:continue
        unique_comname.append(comName)
        if comName in ['', None]:continue


        if 'Clue_Entry_Cellphone' not in source or len(str(source['Clue_Entry_Cellphone']))!=11:
            #print 'no cell'
            continue

        for att in klist:
            if att.decode('utf-8') in source:
                #print 'yes'
                ### replace , in cominfo with .
                if att in ['com_info','main_produce','businessScope','main_industry','Clue_Entry_Major']:
                    if source[att]==np.nan or source[att] in ['',None]:
                        dataDict[att].append('')
                        continue

                    source[att]=re.sub('[/r/n/t\r\n\t]+','',source[att])
                    source[att]=re.sub('[,]+','，',source[att])
                dataDict[att].append(source[att])
            else:
                #print att,'not in source'
                dataDict[att].append('')
            #print 'att []',len(dataDict[att])
    #print 'dd',len(dataDict)
    df=pd.DataFrame(dataDict)
    #choose columns first
    df=pd.DataFrame(df,columns=klist);print df.columns,df.shape

    namedict=dict(zip(klist,vlist))
    df1=df.rename(columns=namedict)
    # adjust attribute in sequence
    df1=pd.DataFrame(df1,columns=vlist_model)
    ### save to local
    df1=df1[:100]
    df1.to_csv('tmp.csv',index=False,encoding='utf-8')

'''
def query_clueDB(es_index,comname_stri,diqu_stri,product_stri,major_stri,registrationYear,business,com_info,main_industry,registed_capital_num,total_most):
    global all_cid_source,unique_comname

    fieldList=['comname','diqu','product','major','year','capital','business','cominfo','industry']
    striList=[comname_stri,diqu_stri,product_stri,major_stri,registrationYear,registed_capital_num,business,com_info,main_industry]
     
    funcList=[comnameMust,diquMust,productMust,majorMust,registrationdateMust,capitalMust,businessScopeMust,com_infoMust,industryMust]

    startStri="""{"from":0,"size":100,"query": {"bool":{"""
    mustSeries=''
    for i in range(striList.__len__()):
        method=funcList[i]
        value=striList[i]
        field=fieldList[i]

        if field=='year' and value.isdigit() and int(value)>1900 and int(value)<=2017:
            item="\"must\""+":"+json.dumps(method(str(value)))+","
            mustSeries+=item
        if field=='capital' and value.isdigit():
            item="\"must\""+":"+json.dumps(method(float(value)))+","
            mustSeries+=item

        if field not in ['year','capital'] and len(value.strip(' '))>=1:
            #item="\"must\""+":"+str(grammerList[i])+","   # fail to parse
            item="\"must\""+":"+json.dumps(method(str(value)))+","
            mustSeries+=item

    spellStri=startStri+mustSeries[:-1]+"}}}"
    que=re.sub('(\')','\"',spellStri)
    print que




    rs = es.search(index=es_index,doc_type="clue",scroll='80s',search_type='scan',size=500,body=que)





    ###### by scroll query

    print 'scroll'



    scroll_size = rs['hits']['total'];print 'total',scroll_size
    totalFound=scroll_size

    i=0
    while (scroll_size > 0):


        scroll_id = rs['_scroll_id']
        rs = es.scroll(scroll_id=scroll_id, scroll='80s')
        #allPages += rs['hits']['hits']
        hit_list=rs['hits']['hits'];#print 'hit',len(hit_list)
        for hit in hit_list[:]:
            cid=hit['_id'];#print 'cid',cid,hit['_source']
            comname=hit['_source']['Clue_Entry_Com_Name']
            ### remove repeat comname
            if comname in unique_comname:continue
            unique_comname.append(comname)
            cellphone=hit['_source']['Clue_Entry_Cellphone']
            if len(str(cellphone))<11:continue
            leadsName=hit['_source']['Clue_Entry_Name']
            if leadsName in ['','--']:continue
            if cid not in all_cid_source:
                all_cid_source[cid]=hit['_source']#{field:v}

        scroll_size = len(hit_list)
        print 'scroll',scroll_size,len(all_cid_source)

        if len(all_cid_source)>=total_most:break
    return totalFound

'''




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

"""

def output_csv(comname_stri,diqu_stri,product_stri,major_stri,registrationYear,business,com_info,main_industry,registed_capital_num,num_total):
    global all_cid_source,unique_comname
    all_cid_source={}
    unique_comname=[]

    #attList_clueDB=['main_produce']
    #province_list,comName_list,product_list,major_list=[],[],[],[]
    # comName_list=comname_stri.split(' ')
    # province_list=diqu_stri.split(' ')
    # product_list=product_stri.split(' ')
    # major_list=major_stri.split(' ')

     


    ttfd=query_clueDB('clue_filtered',comname_stri,diqu_stri,product_stri,major_stri,registrationYear,business,com_info,main_industry,registed_capital_num,num_total)
    ttfd1=query_clueDB('clue_not_filtered',comname_stri,diqu_stri,product_stri,major_stri,registrationYear,business,com_info,main_industry,registed_capital_num,num_total)
    ##### if found 0 pieces
    # there is difference
    # between total_found and
    # total_found_not_remove_repeat
    ttfd_remove_repeat=len(all_cid_source)
    if ttfd+ttfd1==0:
        pd.DataFrame({'empty':[]}).to_csv('tmp.csv',index=False,encoding='utf-8')
        return '',[],0,0 # [{}] length=1


    #pd.to_pickle(all_cid_source,'../allcid_filtered')




    ### cid ->datadict
    #all_cid_source=pd.read_pickle('../allcid_filtered')
    #cid_produce=select_produce(all_cid_source,attList_clueDB)

    #pd.to_pickle(cid_produce,'../data/cid_produce')
    #pd.DataFrame({'produce':cid_produce.values()}).to_csv('../produce.csv',index=False)





    ########## directly go to csv,no knn
    #### cidlist->format the csv
    #cidlist=pd.read_pickle('../result_cid')

    attList_clueDB=['Clue_Entry_Name','Clue_Entry_Com_Name','Clue_Entry_Cellphone','Clue_Entry_Qq','Clue_Entry_Wechat','Clue_Entry_Email',\
           'Com_Address','Clue_Entry_Major','Clue_Entry_Birthday','Clue_Entry_Telephone','qiantai','chuanzhen','main_produce',\
           'com_type','main_industry','registrationdate','com_info','businessScope','registed_capital_num']
    #all_cid_source=pd.read_pickle('../allcid_filtered')
    dataDict={}
    for att in attList_clueDB:
        dataDict[att]=[]
    for cid in all_cid_source:

        source=all_cid_source[cid]
        # if 'Clue_Entry_Cellphone' not in source or len(str(source['Clue_Entry_Cellphone']))!=11:continue
        # if 'Clue_Entry_Name' not in source or str(source['Clue_Entry_Name']) in ['--','']:continue
        for att in attList_clueDB:
            if att in source:
                #print source[att]   None
                add='' if source[att] in [None] else source[att]
                dataDict[att].append(add)
            else:dataDict[att].append('')
    ####



    ##### for visual

    visual_num=10 if ttfd_remove_repeat>10 else ttfd_remove_repeat

    dictList=[]
    for i in range(visual_num):
        dic={}

        dic['diqu']=dataDict['Com_Address'][i]
        dic['product']=dataDict['main_produce'][i]
        dic['comname']=dataDict['Clue_Entry_Com_Name'][i]
        dic['position']=dataDict['Clue_Entry_Major'][i]
        dic['businessScope']=dataDict['businessScope'][i]
        # for att in ['Clue_Entry_Com_Name','main_produce','businessScope','com_info','Clue_Entry_Major','Com_Address']:
        #     dic[att]=dataDict[att][i]
        dic['timestamp']=int(time.time())
        dictList.append(dic)




    klist=['Clue_Entry_Name','Clue_Entry_Com_Name','Clue_Entry_Cellphone','Clue_Entry_Qq','Clue_Entry_Wechat','Clue_Entry_Email',\
           'Com_Address','Clue_Entry_Major','Clue_Entry_Birthday','Clue_Entry_Telephone','qiantai','chuanzhen','main_produce',\
           'com_type','main_industry',\
           'shouji2','registrationdate','registed_capital_num','param8','com_info','beiyong5','beizhu','gender']

    vlist=['姓名（必填）','公司名称（必填）','电话（必填）','QQ', '微信','Email', '公司地址','职位','生日','座机','前台电话','传真','主营产品',\
           '公司类型','行业',\
           '手机2','备用1','备用2','备用3','备用4','备用5','备注','性别']


    vlist_model=['姓名（必填）','电话（必填）','公司名称（必填）','职位','行业','公司地址','公司类型','主营产品','微信','Email','QQ',\
                 '手机2','备用1','备用2','备用3','备用4','备用5','备注','性别',\
                 '生日','前台电话','座机','传真']

    df=pd.DataFrame(dataDict)
    #choose columns first
    df=pd.DataFrame(df,columns=klist);print df.columns

    namedict=dict(zip(klist,vlist))
    df1=df.rename(columns=namedict)
    # adjust attribute in sequence
    df1=pd.DataFrame(df1,columns=vlist_model)
    ### save to local
    df1=df1[:num_total]
    df1.to_csv('tmp.csv',index=False,encoding='utf-8')
    return df1,dictList,ttfd_remove_repeat,ttfd+ttfd1
"""


if __name__=='__main__':



    #
    #
    # unique_comname=[]
    # all_cid_source={}
    # #es_index,comname_stri,diqu_stri,product_stri,major_stri,registrationYear,business,com_info,main_industry,registed_capital_num,total_most
    # #tt=query_clueDB('clue_filtered','北京','','','','','','','健康服务','',100)
    # df1,dictList,ttfd_remove_repeat,tt=output_csv('北京','','','','','','','健康服务','',100)
    # print type(df1),len(dictList),ttfd_remove_repeat,tt

    generate_query_body('北京','','','','','','','健康服务','')












































































