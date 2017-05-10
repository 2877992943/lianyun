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









if __name__ == "__main__":
    print 'link'
    es = Elasticsearch("123.57.62.29")

    print 'search'




    """
    ############
    #clue table
    que= {
            #"from" : 0, "size" : 10000,
            '_source':['CLue_Id'],
            "query" : {
                    "match_phrase" : { "Clue_Entry_Cellphone" : '' }
                     },
           #"fields":['CLue_Id','main_produce']
         }

    rs=es.search(index="clue_filtered",doc_type="clue",body=que,scroll='80s',search_type='scan',size=2000)







    cidList=[]
    scroll_size = rs['hits']['total'];print scroll_size
    print rs.keys(),rs['hits']['hits'].__len__()


    while (scroll_size > 0):


        scroll_id = rs['_scroll_id'];print scroll_id
        rs = es.scroll(scroll_id=scroll_id, scroll='200s')
        #allPages += rs['hits']['hits']
        hit_list=rs['hits']['hits'];print 'this scroll hit',len(hit_list)
        for hit in hit_list[:]:

            cid=hit['_id'];#print 'cid',cid,hit['_source']
            cidList.append(cid)
        print len(cidList)
        scroll_size = len(hit_list)
        #if len(all_cid_source)>100000:break

    pd.to_pickle(cidList,'../data/cidList')
    """








    ###




















































