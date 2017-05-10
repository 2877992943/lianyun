#! -*- coding:utf-8 -*-

import urllib2,time,os,urllib

import pandas as pd
import json

import MySQLdb
import random,cPickle
import csv,re


from MySQLdb import cursors
import numpy as np

import sys,time,os


read_host='rr-2zeg40364h2thw9m6o.mysql.rds.aliyuncs.com'
write_host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com'
db_product = MySQLdb.connect(host=read_host,
                     user='yunker',
                     passwd="yunker2016EP",
                     db="xddb",
                     use_unicode=True,
                     charset='utf8',
                     cursorclass=cursors.DictCursor)

## 测试库
db = MySQLdb.connect(host='rds0710650me01y6d3ogo.mysql.rds.aliyuncs.com',
                      user='yunker',
                      passwd="yunke2016",
                      db="yunketest",
                      use_unicode=True,
                      charset='utf8',
                      cursorclass=cursors.DictCursor)





def get_request_helloWorld():
    full_url='http://localhost:9300'
    data=urllib2.urlopen(full_url)
    Data=data.read()
    print Data



def get_request_testDB():
    try:
        full_url='http://localhost:9300/?userId=E8B63FF5BB1840EABB9BEB2F9DCCA731'
        data=urllib2.urlopen(full_url)
        Data=data.read()
        return Data
    except:
        return None


def send_request(cid):
    try:
        full_url='http://yunkecn.com/xdapi/esData/updateAlreadyDownload.action?userAccount=%s&clueId=%s'%(companyCode,cid)

        data=urllib2.urlopen(full_url)
        Data=data.read()
        return Data
    except:
        return None


def requst_crawler(comname):
    try:
        full_url='http://101.200.139.60:8088/crawler/QiDuoWeiSpider?companyName=%s'%(comname)
        print full_url
        data=urllib2.urlopen(full_url)
        Data=data.read()
        return Data
    except:
        return None

def requst_mp3(url,mp3Name):
    try:
        full_url=url

        data=urllib.urlretrieve(url, mp3Name+".mp3")

        return data
    except:
        return None


def query_call_by_att(att, companyCode):
    sql1 ="""SELECT %s from crm_t_call_action c left join crm_t_portaluser p
    on c.User_Id=p.User_Id
    #WHERE c.recordFile LIKE '%s' AND c.record_from = 1
    where c.record_from = 1
    limit 10000
    #where p.User_Company_Id in ('%s')
    """
    sql ="""SELECT %s from crm_t_call_action c left join crm_t_portaluser p
    on c.User_Id=p.User_Id
    WHERE c.recordFile LIKE '%s' AND c.record_from = 1
    limit 100000
    """

    cur = db_product.cursor()
    #cur.execute(sql % (att, "','".join(companyCode)))
    #print sql % (att, '%http://yunke-pcfile.oss-cn-beijing%')
    cur.execute(sql % (att, '%http://yunke-pcfile.oss-cn-beijing%'))



    ret = {}
    for r in cur.fetchall():


        ret[r['Call_Action_Id']] = r #{id:{record},...}
    return ret


def strDuration2second(duration):
    duration=re.sub('[\s+]','',duration)
    duration=re.sub('[\'\"]',' ',duration)
    ll=duration.split(' ')
    ll=[int(i) for i in ll if len(i)>=1]
    #print ll
    minute,second=ll
    second+=60*minute
    return second

if __name__=='__main__':



    #### query
    attList=['c.Call_Action_Id','c.recordFile','c.Call_Duration','c.Tip_Type','c.Tip_Name']
    companyCode=['bjnbm3','jjjeva','vnraea','ffz3ai','invjvi','mmbnn3']
    companyCode=companyCode[5:]
    ret=query_call_by_att(','.join(attList),companyCode)
    print len(ret)
    pd.to_pickle(ret,'../data/ret')





    ##### get second >60 url
    ret=pd.read_pickle('../data/ret')
    url_second_feedback={}

    for id,r in ret.items()[:]:
        recordFile=r['recordFile']
        duration=r['Call_Duration']
        tiptype=r['Tip_Type']
        tipname=r['Tip_Name']
        second=strDuration2second(duration)
        if second<=0:continue
        url_second_feedback[recordFile]=[second,str(tiptype)+' '+tipname]


    ########
    print len(url_second_feedback)
    pd.to_pickle(url_second_feedback,'../data/url_second')
    ####



"""
record_from 录音来源   1app 2pc 3yunkecc 4电话盒子
现在yunkecc渠道的录音应该是没有提示音的，电话盒子还没上线，没数据。pc的应该是null，app的录音是安卓的，带提示音
"""





















 


















































