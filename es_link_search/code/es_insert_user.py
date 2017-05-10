#! -*- coding:utf-8 -*-

import pandas as pd
from elasticsearch import Elasticsearch,helpers
import os,time,json

def parseDictToJson(clue_cache):

    actions = []
    #key_set=fid_dict.keys()

    for k in clue_cache.keys()[:]:
        #print k
        #print 'v',clue_cache[k]


        try:
            #del clue_cache[k]['Create_Time']
            #del clue_cache[k]['Edit_Time']
            del clue_cache[k]['weixin_CreateTime']
            del clue_cache[k]['vip_end_date']




        except KeyError:
            print clue_cache[k]
            pass
        #clue_cache[k]['feature_value']=str(fid_dict[k]) if k in key_set else "{}"
        jsonClue=json.dumps(clue_cache[k])
        action = {
        "_index": "yunkecn",
        "_type": "user",
        "_id": k,
        "_source": jsonClue
        }

        actions.append(action)
    return actions






if __name__=='__main__':




    es = Elasticsearch("123.57.62.29")

    path="../raw_batches_user/"
    filenames=os.listdir(path)
    for f in filenames[:1]:

        start_time=time.time()
        print "current filenames is %s " % f
        data=pd.read_pickle(path+f)




        actions = parseDictToJson(data)
        print "start indexing...."
        helpers.bulk(es, actions, chunk_size=1000)
        print "Batch spend time is %s s" % str(time.time() - start_time)




