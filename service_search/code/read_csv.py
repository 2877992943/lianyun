#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv,re
import pandas as pd

import numpy as np

import sys,time,os

from elasticsearch import Elasticsearch
import elasticsearch

reload(sys)
sys.setdefaultencoding('utf8')



def csv2dataframe(fpath):
    try:
        df=pd.read_csv(fpath,encoding='utf-8');print df.shape



    except:
        df=pd.read_csv(fpath,encoding='gb18030');print df.shape
        df.to_csv(fpath,index=False,encoding='utf-8')
        df=pd.read_csv(fpath,encoding='utf-8');print df.shape
    return df





attList_clueDB=['Clue_Entry_Name','Clue_Entry_Com_Name','Clue_Entry_Cellphone','Clue_Entry_Qq','Clue_Entry_Wechat','Clue_Entry_Email',\
           'Com_Address','Clue_Entry_Major','Clue_Entry_Birthday','Clue_Entry_Telephone','qiantai','chuanzhen','main_produce','param8',\
           'com_type','main_industry','registrationdate','com_info','param2','employees_num','registed_capital','param1','CLue_Id']




# klist=['Clue_Entry_Name','Clue_Entry_Com_Name','Clue_Entry_Cellphone','Clue_Entry_Qq','Clue_Entry_Wechat','Clue_Entry_Email',\
#            'Com_Address','param1','Clue_Entry_Birthday','Clue_Entry_Telephone','qiantai','chuanzhen','main_produce',\
#            'com_type','main_industry',\
#             'shouji2','param8','param2','employees_num','registed_capital','registrationdate','com_info','gender']

klist=['Clue_Entry_Name','Clue_Entry_Com_Name','Clue_Entry_Cellphone','Clue_Entry_Qq','Clue_Entry_Wechat','Clue_Entry_Email',\
           'Com_Address','Clue_Entry_Major','Clue_Entry_Birthday','Clue_Entry_Telephone','qiantai','chuanzhen','main_produce',\
           'com_type','main_industry',\
            'shouji2','param8','param2','employees_num','registed_capital','registrationdate','com_info','gender']



vlist=['姓名（必填）','公司名称（必填）','电话（必填）','QQ', '微信','Email', '公司地址','职位','生日','座机','前台电话','传真','主营产品',\
           '公司类型','行业',\
           '手机2','备用1','备用2','备用3','备用4','备用5','备注','性别']


vlist_model=['姓名（必填）','电话（必填）','公司名称（必填）','职位','行业','公司地址','公司类型','主营产品','微信','Email','QQ',\
                 '手机2','备用1','备用2','备用3','备用4','备用5','备注','性别',\
                 '生日','前台电话','座机','传真']




















































































