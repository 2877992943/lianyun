#! -*- coding:utf-8 -*-

import urllib2,time,os,re

import pandas as pd
import json


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

def request_crawler1(comname):
    try:
        full_url="http://101.200.139.60:8088/crawler/SpiderAPI?companyName=%s"%(comname)
        print full_url
        data=urllib2.urlopen(full_url)
        Data=data.read()
        return Data
    except:
        return None


def parse_data(data):
    data=eval(data)
    comname,business,cominfo=data['name'],data['jyfw'],data['gsjj']
    return comname,business,cominfo


def replace_dot(stri):
    stri=re.sub('[/r/n/t\r\n\t]+','',stri)
    stri=re.sub('[,]+','，',stri)
    return stri

def process_attribute(field,data):
    if field in data:
        return replace_dot(data[field])

    else:return ''


if __name__=='__main__':
    path='../data/target_dream.csv'
    df=pd.read_csv(path)
    print df.columns
    ll=df['comname'].values.tolist()
    print ' '.join(ll)


    name,business,cominfo=[],[],[]
    ####
    for comname in ll[:]:
        time.sleep(15)
        if len(comname.decode('utf-8'))<=1:continue

        data=request_crawler1(comname);
        #print data
        if data=='null':continue
        data=eval(data)
        print type(data),data.keys()
        ###
        name_i=process_attribute('name',data)
        if len(name_i)<=1:continue
        business_i=process_attribute('jyfw',data)

        cominfo_i=process_attribute('gsjj',data)
        ###
        name.append(name_i)
        business.append(business_i)
        cominfo.append(cominfo_i)

    ####
    pd.DataFrame({'comname':name,'businessScope':business,'cominfo':cominfo}).to_csv('../data/target_dreamhouse_3fields.csv',index=False,encoding='utf-8',
                                                                                     columns=['comname','businessScope','cominfo'])


















 


















































