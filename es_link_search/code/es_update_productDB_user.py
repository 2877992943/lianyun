#! -*- coding:utf-8 -*-

import pandas as pd
from elasticsearch import Elasticsearch,helpers
import os,time,json
table_name="user"#user clue

def parseDictToJson_generator(clue_cache):#{id:fiddict...}  fiddict={fid:v..}



    for k in clue_cache.keys()[:1000]:

        for it in ['Create_Time','Edit_Time','weixin_CreateTime','vip_end_date']:
            if it in clue_cache[k]:
                del clue_cache[k][it]

        #clue_cache[k]['feature_value']=str(fid_dict[k]) if k in key_set else "{}"
        jsonFeatureValue=json.dumps(clue_cache[k]);print type(jsonFeatureValue)
        jsonFeatureValue=str(jsonFeatureValue)
        print k,clue_cache[k]
        action = {
        "_op_type": 'update',
        "_index": "yunkecn",
        "_type": table_name,
        "_id": k,
        'doc': {'feature_value': jsonFeatureValue}
        }
        yield action



def query(field_query,value):

    #es.index(index="yunkecn",doc_type=es_doc_type,id=user_ids[0],body=rawdict)
    ##### query to see whether inserted into es
    que= {"from":1,"size":3,
            "query" : {
                    "match_phrase" : { field_query : value }
                     },
         }

    res=es.search(index="yunkecn",doc_type=table_name,body=que )
    print len(res['hits']['hits'])
    cache_dict={}
    for i in range(len(res['hits']['hits'])):
        print res['hits']['hits'][i]['_id']
        print res['hits']['hits'][i]['_source'][field_query]
        #print res['hits']['hits'][i]['_source'].keys()

        cache_dict[res['hits']['hits'][0]['_id']]=res['hits']['hits'][0]['_source']
    return cache_dict




if __name__=='__main__':




    es = Elasticsearch("123.57.62.29")

    path="/root/yangrui/es_tableUser_dataPrepare/data/"# {cid:feature_value_dict
    filenames=os.listdir(path)
    for f in filenames[:1]:
        f='all_cid_fid_dict'
        start_time=time.time()
        print "current filenames is %s " % f
        cached=pd.read_pickle(path+f)
        # piece by piece
        # i=0
        # for k,fid in cached.items()[:1]:
        #     if i%10000==0:print i
        #     i+=1
        #     #print k,fid
        #     #print 'update'
        #     body={'doc':{'feature_value':cached[k] } }
        #     es.update(index="yunkecn",doc_type=table_name,id=k,body=body)
        #     #es.indices.refresh(index="yunkecn")


        actions = parseDictToJson_generator(cached)
        print "start indexing...."
        helpers.bulk(es, actions, chunk_size=1000)
        print "Batch spend time is %s s" % str(time.time() - start_time)


        




