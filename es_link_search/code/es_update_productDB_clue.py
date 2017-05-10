#! -*- coding:utf-8 -*-

import pandas as pd
from elasticsearch import Elasticsearch,helpers
import os,time,json
table_name="clue"#user clue

def parseDictToJson_generator(cache_i):#{id:fiddict...}  fiddict={fid:v..}

    actionList=[]
    for k in cached_i.keys()[:]:
        #print k
        if k in ['26F85133A4B64B308CE086076F57EC67','0000245543B646D7A2A166094D31258B','5A173CBA220445F2AC9306A76B767461']:continue
       
        for it in ['Create_Time','Edit_Time','weixin_CreateTime','vip_end_date']:
            if it in cached_i[k]:
                del cached_i[k][it]

        #clue_cache[k]['feature_value']=str(fid_dict[k]) if k in key_set else "{}"
        #jsonFeatureValue=json.dumps(cached_i[k]);#print type(jsonFeatureValue)
        #jsonFeatureValue=str(jsonFeatureValue)
        jsonFeatureValue=str(cached_i[k])


        action = {
        "_op_type": 'update',
        "_index": "yunkecn",
        "_type": table_name,
        "_id": k,
        'doc': {'str_feature_value': jsonFeatureValue}
        }
        #yield action
        actionList.append(action)
    return actionList
    #yield actionList



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

    path="/root/yangrui/es_tableClue_dataPrepare/data/"# {cid:feature_value_dict

    filenames=os.listdir(path)
    #for f in filenames[:1]:
    for iii in range(1):
        f='all_cid_fid_dict'

        print "current filenames is %s " % f
        cached=pd.read_pickle(path+f)
        num=len(cached)
        num_batch=int(num/1000)+1
        for ii in range(num_batch)[2702:]:#947


            k,v=cached.keys()[ii*1000:ii*1000+1000],cached.values()[ii*1000:ii*1000+1000]
            cached_i=dict(zip(k,v))
        #cached=pd.read_pickle('../data/'+f)
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

            print "start indexing....",ii
            start_time=time.time()
            actions = parseDictToJson_generator(cached_i);print len(actions)

            helpers.bulk(es, actions, chunk_size=1000,request_timeout=80)
            print "Batch spend time is %s s" % str(time.time() - start_time)


        




