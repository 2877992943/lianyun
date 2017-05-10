# coding=utf-8

import time
from datetime import datetime
import os
import json
from elasticsearch import Elasticsearch, helpers
import pandas as pd


es=Elasticsearch(hosts='123.57.62.29',timeout=5000)


def parseDictToJson(clue_cache):

    actions = []
    #key_set=fid_dict.keys()

    for k in clue_cache.keys():
        #print k
        for it in ['Create_Time','Edit_Time','weixin_CreateTime','vip_end_date']:
            clue_cache[k][it] = time.time()

        clue_cache[k]['param2'] = -1000 if clue_cache[k]['param2'] == '' else int(clue_cache[k]['param2'])
        clue_cache[k]['param3'] = -1000 if clue_cache[k]['param3'] == '' else int(clue_cache[k]['param3'])
	
	latitude = clue_cache[k]['Clue_Latitude']
	longitude = clue_cache[k]['Clue_Longitude']
	if latitude == "" or float(latitude) > 90 or float(latitude) < 0:
	    clue_cache[k]['Clue_Latitude'] = 0.0
	if clue_cache[k]['Clue_Longitude'] == "" or float(longitude) > 180 or float(longitude) < 0:
            clue_cache[k]['Clue_Longitude'] = 0.0
 
	clue_cache[k]['location'] = str(clue_cache[k]['Clue_Latitude']) + "," + str(clue_cache[k]['Clue_Longitude'])

        jsonClue=json.dumps(clue_cache[k])
        action = {
        "_index": "clue_not_filtered",
        "_type": "clue",
        "_id": k,
        "_source": jsonClue
        }

        actions.append(action)
    return actions



if __name__=='__main__':
    path="../combined/"
    filenames=os.listdir(path)
    for f in filenames:
	log_file = open("./insert_log_file","a")
        start_time=time.time()
        log_file.write("current filenames is %s \n" % f)
        data=pd.read_pickle(path+f)
        actions = parseDictToJson(data)
        log_file.write("start indexing....\n")
        helpers.bulk(es, actions, chunk_size=1000, request_timeout=30)
        log_file.write("Batch spend time is %s s \n" % str(time.time() - start_time))
        log_file.close()
