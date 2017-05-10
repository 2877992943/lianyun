#! -*- coding:utf-8 -*-

import pandas as pd
import sys,os,re
reload(sys)
sys.setdefaultencoding('utf8')
import numpy as np






if __name__=='__main__':
    path='../backup/2014_district.csv'
    df=pd.read_csv(path,encoding='utf-8')
    ll=df['dis'].values.tolist()
    ll_clean=[]
    ll_clean1=[]# more than 3 words ,strip 省
    for item in ll:
        if item==np.nan:continue
        item=re.sub('[\s+]','',item)
        ll_clean.append(item)
        ###
        if item.decode('utf-8').__len__()>2:ll_clean1.append(item[:-1])
    print len(ll_clean),' '.join(ll_clean)
    print len(ll_clean1),' '.join(ll_clean1)
    ####
    pd.to_pickle(ll_clean1+ll_clean,'../data/district_noise')

















































































































