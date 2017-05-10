#! -*- coding:utf-8 -*-


import random,cPickle
import csv,os,re
import numpy as np




import sys,time

reload(sys)
sys.setdefaultencoding('utf8')

from elasticsearch import Elasticsearch,helpers
es = Elasticsearch("123.57.62.29",timeout=200)




def update_es_byID(cid,k_v_dict,es_index):

    body={'doc':k_v_dict}
    es.update(index=es_index,doc_type='clue',id=cid,body=body)


def sql_atts_into_es(r):
    global attList
    cid=r['CLue_Id']
    r=r['r']
    k_v_dict={}

    for field,value in r.items():
        #print 'old',field,value,type(value)
        if field not in attList:continue

        field=re.sub('(_clean)','',field)
        ### remove null ''
        if value not in [np.nan,'',None]:
            ## value must not be none '' nan
            if field in ['param2','param3']:## these att must be int
                try:
                    value=int(value)
                except:value=-1000
            #### param8 in sql | businessScope&param8 in es
            if field=='param8':k_v_dict['businessScope']=value
            k_v_dict[field]=value
        #if value in [np.nan,'',None]:k_v_dict[field]=''
        #print field,k_v_dict[field]
    if k_v_dict.__len__()==0:return 'empty dict'
    ## insert into es
    try:
        update_es_byID(cid,k_v_dict,'clue_not_filtered')


    except Exception,e:
        try:
            #print Exception,e
            update_es_byID(cid,k_v_dict,'clue_filtered')


        except Exception,e:
            print Exception,e
    return 'done'








if __name__ == "__main__":

    global attList  ## field name in sql
    attList='Clue_Entry_Com_Name_clean,Clue_Entry_Name_clean,Clue_Entry_Cellphone_clean,' \
            'Com_Address_clean,Clue_Entry_Major_clean,legal_person_clean,main_produce_clean,' \
            'CLue_Id,com_info,com_status,com_type,employees_num,main_industry,param1,param2,param8'.split(',')
    print attList



    r={'CLue_Id':'140d901aa2d111e6a5a500163e006499','r':{"Clue_Entry_Name_clean": "黄章辉"}}
    rst=sql_atts_into_es(r);print ret

































































