#! -*- coding:utf-8 -*-

import pandas as pd
from elasticsearch import Elasticsearch,helpers
import os,time,json
table_name="clue"#user clue

def parseDictToJson_generator(clue_cache):

    actions = []
    #key_set=fid_dict.keys()

    for k in clue_cache.keys()[:]:
        #print k
        #print 'v',clue_cache[k]

        for it in ['Create_Time','Edit_Time','weixin_CreateTime','vip_end_date']:
            if it in clue_cache[k]:
                del clue_cache[k][it]

        #clue_cache[k]['feature_value']=str(fid_dict[k]) if k in key_set else "{}"
        #jsonClue=json.dumps(clue_cache[k])
        action = {
		    '_op_type': 'delete',
		    '_index': 'index-name',
		    '_type': 'document',
		    '_id': 42,
		}
        yield action
        #actions.append(action)
    #return actions

class generate_cid(object):
    def __init__(self,fpath):
        self.fpath=fpath
    def __iter__(self):
        cidList=pd.read_pickle(self.fpath)
        for cid in cidList:
            action = {
		    '_op_type': 'delete',
		    '_index': 'clue_filtered',
		    '_type': 'clue',
		    '_id': cid,
		    }
            yield action



def query(field_query,value):

    #es.index(index="yunkecn",doc_type=es_doc_type,id=user_ids[0],body=rawdict)
    ##### query to see whether inserted into es
    que= {"from":1,"size":1,
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
    # cache_dict=query("profession","计算机".decode('utf-8'))
    # all_actions=parseDictToJson_generator(cache_dict)
    #
    #
    # ###  bulk
    # helpers.bulk(es, all_actions, chunk_size=100)
    ##

   

    #time.sleep(3)# it take time for ES to rebuild the index before query
    #cache_dict=query("profession",'计算machine计算机硬件,计算机软件'.decode('utf-8'))



    #path="../raw_batches/"
    path='../data/'
    filenames=os.listdir(path)
    for f in filenames[:]:

        start_time=time.time()
        print "current filenames is %s " % f
        gene=generate_cid(path+f)




        #actions = parseDictToJson_generator(data)
        print "start indexing...."
        helpers.bulk(es, gene, chunk_size=1000)
        print "Batch spend time is %s s" % str(time.time() - start_time)





