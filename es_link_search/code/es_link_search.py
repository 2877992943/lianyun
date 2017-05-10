#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv
import pandas as pd

from MySQLdb import cursors
import numpy as np



import pylab as plt
import sys,time,os

from elasticsearch import Elasticsearch

reload(sys)
sys.setdefaultencoding('utf8')

db = MySQLdb.connect(host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com',
                     user='yunker',
                     passwd="yunker2016EP",
                     db="xddb",
                     use_unicode=True,
                     charset='utf8',
                     cursorclass=cursors.DictCursor)







if __name__ == "__main__":
    print 'link'
    es = Elasticsearch("123.57.62.29")

    print 'search'




    """
    ############
    #clue table
    que= {
            "from" : 0, "size" : 10000,
            "query" : {
                    "match" : { "main_produce" : '建筑五金材料' }
                     },
           #"fields":['CLue_Id','main_produce']
         }

    res=es.search(index="yunkecn",doc_type="clue",body=que)
    """


    #############
    # user
    que= {
            "from" : 0, "size" : 10,
            "query" : {
                    "match" : { "profession" : '建筑五金材料' }
                     },
           #"fields":['CLue_Id','main_produce']
         }

    res=es.search(index="yunkecn",doc_type="user",body=que)

    #res=es.search(index="yunkecn",q=' high_status : "其他" ')
    #print res
    i=0
    hits=res['hits']['hits'];#print hits
    for hit in hits[:]:
        hit=hit['_source']
        #print hit['feature_value'],len(hit['feature_value'])
        #for k,v in hit.items():
         #   print k,v
        if len(hit['profession'])>10:
            print hit['profession']
            print hit['feature_value']

        i+=1




    ###







"""

    index - 索引名
    q - 查询指定匹配 使用Lucene查询语法
    from_ - 查询起始点  默认0
    doc_type - 文档类型
    size - 指定查询条数 默认10
    field - 指定字段 逗号分隔
    sort - 排序  字段：asc/desc
    body - 使用Query DSL
    scroll - 滚动查询


def sample_scoll():
        # Initialize the scroll
    page = es.search(
        index ='yourIndex',
        doc_type ='yourType',
        scroll ='2m',
        search_type ='scan',
        size =1000,
        body ={
        # Your query's body
    })

    sid = page['_scroll_id']
    scroll_size = page['hits']['total']

    # Start scrolling
    while(scroll_size >0):
        print "Scrolling..."
        page = es.scroll(scroll_id = sid, scroll ='2m')
        # Update the scroll ID
        sid = page['_scroll_id']
        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        print "scroll size: "+ str(scroll_size)
        # Do something with the obtained page



    #################
    search
        {
      "query": {
        "bool": {
          "must": [
            { "match": { "title":   "Search"        }},
            { "match": { "content": "Elasticsearch" }}
          ],
          "filter": [
            { "term":  { "status": "published" }},
            { "range": { "publish_date": { "gte": "2015-01-01" }}}
          ]
        }
      }
    }

    query1=  {
      "query": {
        "bool": {
         "must": [
            { "match": { "high_status":   "其他" }},
            { "match": {}}
          ],
          "filter": [
            { "term":  {}},
              { "range": { "publish_date": { "gte": "2015-01-01" }}}
          ]
        }
      }
    }

     query2=  {
            "multi-match":{
                "email":"123456@qq.com",
                "k":"v"
                    }
            }
    """















































