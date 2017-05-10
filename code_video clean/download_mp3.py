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
    sql ="""SELECT %s from crm_t_call_action c left join crm_t_portaluser p
    on c.User_Id=p.User_Id
    where p.User_Company_Id in ('%s')
    #limit 10
    """

    cur = db_product.cursor()
    cur.execute(sql % (att, "','".join(companyCode)))

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


    """
    ### get video   #2 reject  1 contact

    url_sec=pd.read_pickle('../data/url_second')
    i=-1
    for url,sec_feedback in url_sec.items()[:20]:
        i+=1
        feedback=sec_feedback[1].replace(' ','_')
        d=requst_mp3(url,'../video/%d_%s'%(i,feedback));
    """


    url='http://yunke-pcfile.oss-cn-beijing.aliyuncs.com/tel-record-2016-12-15-14-46-24-7178B044C0E54426A55E8622D015D30C.wav.mp3'
    url='http://yunke-pcfile.oss-cn-beijing.aliyuncs.com/tel-record2016-11-09ad8257e9aca74015af6c64c66298a372.mp3'

    d=requst_mp3(url,'../video/2');


























 


















































