#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv
import pandas as pd

from MySQLdb import cursors
import numpy as np
#from clue import *

#from base import CategoricalTextMixIn, ClueFeature, FieldExistsMixIn, ParseNumberMixIn,get_clues
#import pylab as plt
import sys,time,os,re
reload(sys)
sys.setdefaultencoding('utf8')

db = MySQLdb.connect(host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com',
                     user='yunker',
                     passwd="yunker2016EP",
                     db="xddb",
                     use_unicode=True,
                     charset='utf8',
                     cursorclass=cursors.DictCursor)

def get_all_idx():
    '''
    Get all the Clue_Id
    '''
    sql = "SELECT clue_id from crm_t_clue limit 10000"
    df=pd.read_sql(sql,db)
    df.to_csv("../allClueId.csv",index=False,encoding='utf-8')

def get_negative_idx(clueId): #too slow
    print '1', clueId[0],len(clueId)
    '''
    Get negative   Clue_Id
    '''
    sql = """SELECT clue_id from crm_t_clue
            where clue_id not in ('%s')
            limit 10"""

    df=pd.read_sql(sql % "','".join(clueId),db)
    #df=pd.read_sql(sql,db)
    df.to_csv("../negativeClueId.csv",index=False,encoding='utf-8')



def get_positive_id():
    '''
    Get the positive_ids from phone_response_clue.txt ->phone ,id,
    '''

    positive_responses = set(['QQ', '短信', '注册', '邮箱', '预约' ,'微信'])
    with open('../tianxiang/phone_response_clue.txt') as f:
        lines = f.read().split('\n')[:-1]
    oids = [l.split(',')[2] for l in lines if l.split(',')[1] in positive_responses]#id
    teles = [l.split(',')[0] for l in lines if l.split(',')[1] in positive_responses]#tele

    pd.DataFrame({'clueId':oids,'teles':teles}).to_csv("../clueId_tele.csv",index=False,encoding='utf-8')
    return oids,teles



def get_clue_idx(tels):
    '''
    Get Clue_Ids by Clue_Entry_Cellphone
    '''

    sql = """
        SELECT CLue_Id
        FROM crm_t_clue
        WHERE Clue_Entry_Cellphone IN
              ('%s')
    """
    df=pd.read_sql(sql % "','".join(tels),db)
    df.to_csv("../teleQueryClueId.csv",index=False,encoding='utf-8')




def save2pickle(c,name):
    write_file=open(str(name),'wb')
    cPickle.dump(c,write_file,-1)#[ (timestamp,[motion,x,y,z]),...]
    write_file.close()

def load_pickle(path_i):
    f=open(path_i,'rb')
    data=cPickle.load(f)#[ [time,[xyz],y] ,[],[]...]
    f.close()
    #print data.__len__(),data[0]
    return data




def try_lr(X,y):
    import math

    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.cross_validation import cross_val_score
    from sklearn.metrics import roc_auc_score
    from sklearn.ensemble import GradientBoostingClassifier

    #######
    ss = StandardScaler(with_mean=False)
    X_norm = ss.fit_transform(X)
    lr0 = LogisticRegression(penalty='l1', class_weight='balanced', random_state=0)
    ########
    scorelist=cross_val_score(lr0, X_norm, y,lambda m, x, y: roc_auc_score(y, m.predict_proba(x)[:,1].T),cv=3)
    print scorelist
    lr0.fit(X_norm,y)

    ##########
    # select feature
    selected_fids = [];
    for i, e in enumerate(lr0.coef_[0]): #coefficient [n_class,n_feature] w parameter matrix
        if abs(e) != 0:
            selected_fids.append(i)

    print 'lr select non zero fea',len(selected_fids)
    ##############
    # select non 0 w parameter
    wMatrix=lr0.coef_ ;print 'w',wMatrix.shape
    fid,fv=remove_zeroFea_sort(np.squeeze(wMatrix))#[1,d]->[d,]
    #feaInd=np.unique(np.nonzero(wMatrix)[1]) ;print 'non 0 w lr',feaInd.shape #[2,5,9,11,..]
    #feaVal=np.squeeze(wMatrix[:,feaInd]); #[d,]
    ### sort by abs
    #feaVal_abs=np.abs(feaVal)
    #indice=np.argsort(feaVal)[::-1]
    #FeaIndSortedByVal=feaInd[indice]
    return fid,fv



    #######
    #X_norm_selected = X_norm[:, selected_fids]
    #print X_norm_selected.shape
    #save2pickle([X_norm_selected,y],'../selecteXy')
    #return selected_fids#list

def remove_zeroFea_sort(importances): #[d,]
    nonzeroFidx=np.nonzero(importances)[0];print 'non 0 fea',nonzeroFidx.shape
    #### removed zero -- nonzeroFidx:importances[nonzeroFidx]
    indices = np.argsort(importances[nonzeroFidx])[::-1]
    nonzeroFidx_sort=nonzeroFidx[indices]
    """
    for i in range(indices.shape[0])[:10]:
        print 'feature ind',nonzeroFidx[indices[i]],importances[nonzeroFidx][indices[i]],nonzeroFidx_sort[i]
    """
    return nonzeroFidx[indices],importances[nonzeroFidx][indices]
def rf_important_fea(xtrain,ytrain):
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.cross_validation import cross_val_score
    from sklearn.metrics import roc_auc_score
    ss = StandardScaler(with_mean=False)
    X_norm = xtrain#ss.fit_transform(xtrain)
    clf = RandomForestClassifier(n_estimators=10,max_depth=10)
    ########
    scorelist=cross_val_score(clf, X_norm, ytrain,lambda m, x, y: roc_auc_score(ytrain, m.predict_proba(X_norm)[:,1].T),cv=3)
    print scorelist
    clf.fit(X_norm,ytrain)
    ###########
    #rf important feature
    ###############

    from sklearn.ensemble import RandomForestClassifier

    clf = clf.fit(X_norm, ytrain)


    importances = clf.feature_importances_;#print 'important',importances.shape#[dim,] same shape as dim

    #nonzeroFidx=np.nonzero(importances)[0];print 'non 0 fea rf',nonzeroFidx.shape
    #### remove zero -- nonzeroFidx:importances[nonzeroFidx]
    #indices = np.argsort(importances[nonzeroFidx])[::-1]
    #nonzeroFidx_sort=nonzeroFidx[indices]
    #for i in range(indices.shape[0])[:10]:
    #    print 'feature ind',nonzeroFidx[indices[i]],importances[nonzeroFidx][indices[i]],nonzeroFidx_sort[i]
    fid,fv=remove_zeroFea_sort(importances)

    #############
    # filter feature
    X_norm=X_norm[:,fid];print X_norm.shape
    clf1=RandomForestClassifier(n_estimators=10,max_depth=10)
    scorelist=cross_val_score(clf1, X_norm, ytrain,lambda m, x, y: roc_auc_score(ytrain, m.predict_proba(X_norm)[:,1].T),cv=3)
    print scorelist
    return fid,fv



def draw_bar_importantF(FeaIndSortedByVal,FeaValSortedByVal,title_clf):
    plt.figure()
    plt.title("Feature importances %s"%title_clf)
    plt.bar(range(len(FeaIndSortedByVal)), FeaValSortedByVal,color="r")
    plt.xticks(range(len(FeaIndSortedByVal)),FeaIndSortedByVal,rotation='vertical',size='xx-small')#[0,1,2]->[a,c,r] 'vertical'

    #plt.xlim([-1, xtrain.shape[1]])
    #plt.xlim([-1, 15])


def yield_row(allclue_feaDict):
    feaName_to_fid={} #{hospital:0,...}
    #feaInd_to_name={}#{0:hospital,...}
    #allclue_fidDict={} #{clueid:{feaIdx:v}
    for clueid in sorted(allclue_feaDict.keys())[:]:
        obs={}
        for feaname,v in allclue_feaDict[clueid].items():
            if feaname not in feaName_to_fid:#new word
                feaName_to_fid[feaname]=len(feaName_to_fid)
                obs[ feaName_to_fid[feaname] ]=v
            else:
                obs[ feaName_to_fid[feaname] ]=v
        ############3
        yield obs
def generate_svm(allclue_feaDict):
    item=yield_row(allclue_feaDict)
    filename='../allclues'
    with open(filename + '.svm', 'w') as f:
        for row_dict in item: # each rowdict  {fid:fvalue
            y=0
            ##
            f.write(str(y)+' ')#f.write('0 ')
            for fid in sorted(row_dict.keys()):
                fval = row_dict[fid]
                if fval != 0.0:
                    f.write('%d:%f ' % (fid, fval))## write into  "modeling.svm"
            f.write('\n')


def transform_lowDim_ind(rowDict_str,lowDimWordDic): #{duty_yuangong:v...} -> { 1:v.... {word:fid
    rowDic={}

    for f_str,v in rowDict_str.items(): # one clue {strfea:v..}  duty_yuangong
        if v!=0:
            #f_str_=f_str.lstrip(cate_p);print f_str,f_str_ # not work
            #f_str_=strip_name(f_str);#print '?',f_str,f_str_
            if f_str in lowDimWordDic:
                fid=lowDimWordDic[f_str]
                rowDic[fid]=v
    return rowDic



def match_cid(cid_feaDict_str,fid_dict):
    rst=cid_feaDict_str.copy()
    for cid,fea_dict in cid_feaDict_str.items()[:1]:
        rst[cid]['fid_dict']={}
        if cid in fid_dict:
            rst[cid]['fid_dict']=fid_dict[cid]
            #print rst[cid]


    return rst



def generate_cid_fid(allclue_feadict,word_lowDim_dict):
    filtered_clueid_fea={} #{cid:rowdict
    for cid,feadic in allclue_feadict.items()[:]:

        if len(feadic)!=0:
            rowDict=feadic;#print 'fdic str', feadic
            rowDict=transform_lowDim_ind(rowDict,word_lowDim_dict) #{str:v..}->{fid:v...} duty_yuangong not duty_1
            #print 'rowdict fid',rowDict
            if len(rowDict)>0:
                filtered_clueid_fea[cid]=rowDict

            else:filtered_clueid_fea[cid]={}
            #print filtered_clueid_fea[cid]
        else:filtered_clueid_fea[cid]={}
     #print filtered_clueid_fea
    print len(filtered_clueid_fea)
    return filtered_clueid_fea

def strip_name(f_str):
    rst=f_str
    for p in categori_fnameList:
        # print p
        if f_str.find(p)!=-1:
            #pattern=re.compile(p)
            #rst=f_str.decode('utf-8').replace(p.decode('utf-8'),'')
            #rst=re.sub(pattern,'',f_str)
            rst=f_str.replace(p,'')
    return rst


if __name__ == "__main__":
    cnt=2
    ####
	#str_fea -> fid_fea


    time_start=time.time()
    # categori_fnameList=['main_industry_', 'position_', 'product_', 'company_type_', 'main_market_', 'province_', 'city_', 'high_status_']
    # cate_p=''.join(categori_fnameList)
    #######
    # read low dim feature
    # df=pd.read_csv('../backup/fscore_fname_word_nameword_notsort.csv',encoding='utf-8') # from individual file
    # fname=df.fname.values
    # fname_word=df.fname_word.values
    feaNameList=pd.read_pickle('../backup/feaNameList_nonzero_lr_816')
    #word_lowDim=df.fname_word.values.tolist() # cluetable_duty_yuangong
    word_lowDim_dict=dict(zip(feaNameList,range(len(feaNameList) ) ) ) #{fname_word:fid,,,}  duty_yuangong   fid start from 0


    fpath='../clueTable_dict/'
    #outpath='../clueTable_dict_lowDim/'
    cid_fidDict_lowDim={}

    #######
    # combine batches into dict {cid:{feastr:v}    # feastr cluetable_duty_yuangong not duty_1
    batch=0
    for filename in os.listdir(fpath)[cnt*10:(cnt+1)*10]:
        
        ############
        #  
        allclue_feadict=pd.read_pickle(fpath+filename)   #{clueid:{feastr:v}
        print 'batch clue',len(allclue_feadict),batch
        ########
        # filter empty product_industry
        filtered_clueid_fea={} #{cid:rowdict
        for cid,feadic in allclue_feadict.items()[:]:

            if len(feadic)!=0:
                rowDict=feadic;#print 'fdic str', feadic
                rowDict=transform_lowDim_ind(rowDict,word_lowDim_dict) #{str:v..}->{fid:v...} duty_yuangong not duty_1
                #print 'rowdict fid',rowDict
                if len(rowDict)>0:
                    filtered_clueid_fea[cid]=rowDict

                else:filtered_clueid_fea[cid]={}
                #print filtered_clueid_fea[cid]
            else:filtered_clueid_fea[cid]={}
        #print filtered_clueid_fea
        print len(filtered_clueid_fea)
        ##
        cid_fidDict_lowDim.update(filtered_clueid_fea)
        batch+=1

        #pd.DataFrame({'cid':filtered_clueid_fea.keys(),'fea':filtered_clueid_fea.values()}).to_csv('../cid_fea_dict_filtered.csv',index=False,encoding='utf-8')
        #pd.to_pickle(filtered_clueid_fea,outpath+'clueTable_clueid_fid'+str(batch))
    print 'total fid dict',len(cid_fidDict_lowDim)



    #
    # ######
    # # combine batches
    # outpath='../clueTable_dict_lowDim/'
    # all_dict={}
    # for filename in os.listdir(outpath)[:]:
    #     cid_feaDict=pd.read_pickle(outpath+filename) # {cid:{fid:v,,,}
    #     all_dict.update(cid_feaDict)

    pd.to_pickle(cid_fidDict_lowDim,'../data/all_cid_fid_dict_%s'%cnt)



 












 


















































