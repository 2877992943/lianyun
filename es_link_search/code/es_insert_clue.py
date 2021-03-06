#! -*- coding:utf-8 -*-

import pandas as pd
from elasticsearch import Elasticsearch,helpers
import os,time,json

def parseDictToJson(clue_cache,i):

    actions = []
    #key_set=fid_dict.keys()

    for k in clue_cache.keys()[i*10000:(i+1)*10000]:
        #print k
        #print 'v',clue_cache[k]

        try:
            #del clue_cache[k]['Create_Time']
            #del clue_cache[k]['Edit_Time']
            del clue_cache[k]['weixin_CreateTime']
            del clue_cache[k]['vip_end_date']

        except KeyError:
            pass
        #clue_cache[k]['feature_value']=str(fid_dict[k]) if k in key_set else "{}"
        jsonClue=json.dumps(clue_cache[k])
        action = {
        "_index": "clue",
        "_type": "clue1",
        "_id": k,
        "_source": jsonClue
        }

        actions.append(action)
    return actions






if __name__=='__main__':


    es = Elasticsearch("123.57.62.29")

    path="../raw_batches/"
    filenames=os.listdir(path)
    fnum=0
    for f in filenames[:]:
        # each file

        print "current filenames is %s , no.%d" % (f,fnum)
        fnum+=1
        ### data 10 0000
        data=pd.read_pickle(path+f);#print data.keys()[0],data.values()[0]
        ##
        numbatch=len(data)/10000+1
        for i in range(numbatch)[:1]:#012345
            start_time=time.time()
            actions = parseDictToJson(data,i)
            print "start indexing....",i
            print len(actions)

            helpers.bulk(es, actions, chunk_size=1000,request_timeout=30)
            print "Batch spend time is %s s" % str(time.time() - start_time)



