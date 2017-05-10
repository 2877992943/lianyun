#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv
import pandas as pd

from MySQLdb import cursors
import numpy as np
##from clue import *

from menu_to_companyName_product_list import add_parsing_fea
#from base import CategoricalTextMixIn, ClueFeature, FieldExistsMixIn, ParseNumberMixIn,get_clues
import pylab as plt
import sys
reload(sys)
sys.setdefaultencoding('utf8')

 

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
    import cPickle
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
    #ss = StandardScaler(with_mean=False)
    #X_norm = ss.fit_transform(X)
    X_norm=X
    lr0 = LogisticRegression(penalty='l1', class_weight='balanced', random_state=0)
    ########
    scorelist=cross_val_score(lr0, X_norm, y,lambda m, x, y: roc_auc_score(y, m.predict_proba(x)[:,1].T),cv=3)
    print scorelist
    classifier=lr0.fit(X_norm,y)

    ##########
    # select feature

    fid_w_dict={};
    for i, e in enumerate(lr0.coef_[0]): #coefficient [n_class,n_feature] w parameter matrix
        #print 'select fid',i
        if abs(e) != 0:
            fid_w_dict[i]=e

    print 'lr select non zero fea',len(fid_w_dict.keys())
    x_important=X[:,fid_w_dict.keys()]
    return fid_w_dict,classifier,x_important

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
    print 'allfea',scorelist
    clf.fit(X_norm,ytrain)
    ###########
    #rf important feature
    ###############
    print 'nonzero feature...'

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
    print type(fid),fid.shape,fid[:3] #ndarray
    from scipy.sparse import csr_matrix
    X_norm=csr_matrix(X_norm)
    X_norm=X_norm[:,fid];print X_norm.shape
    clf1=RandomForestClassifier(n_estimators=30,max_depth=15)
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
                #feaName_to_fid[feaname]=len(feaName_to_fid)+1
                feaName_to_fid[feaname]=len(feaName_to_fid)#start from 0 fid not 1
                obs[ feaName_to_fid[feaname] ]=v
            else:
                obs[ feaName_to_fid[feaname] ]=v
        ############3
        yield obs
    pd.to_pickle(feaName_to_fid,'../data/816feaName_to_fid')
def generate_svm(allclue_feaDict):
    item=yield_row(allclue_feaDict)
    filename='../data/816train_sample'
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

if __name__ == "__main__":
    """
    negative_valid_uidcid=pd.read_pickle('../data/negative_valid_uidcid_pair')    #[(uid,cid), ..]
    positive_valid_uidCid=pd.read_pickle('../data/positive_valid_uidcid_pair')
    num_neg=len(negative_valid_uidcid)
    num_pos=len(positive_valid_uidCid)
    uid_list=[positive_valid_uidCid[i][0] for i in range(num_pos)]+[negative_valid_uidcid[i][0] for i in range(num_neg)]
    cid_list=[positive_valid_uidCid[i][1] for i in range(num_pos)]+[negative_valid_uidcid[i][1] for i in range(num_neg)]
    ylist=[1]*num_pos+[0]*num_neg

    ### combine strFeaDict -> <uid,cid>  -> add parsing_word ->svm, feaStr:fid ->nonzero fid_to_feaname
    id_feaStr_dict_clue=pd.read_pickle('../data/816_id_feaStrDict_clue');print 'clue',len(id_feaStr_dict_clue)#{id:strfeadict
    id_feaStr_dict_user=pd.read_pickle('../data/816_id_feaStrDict_user');print 'user',len(id_feaStr_dict_user)

    xpair_dict={};ydict={} #{id:strFeadict,,,} {id:y...}
    for i in range(len(ylist))[:]:
        ## <uid,cid> pair
        u=uid_list[i]
        c=cid_list[i]
        y=ylist[i]
        if u in id_feaStr_dict_user and c in id_feaStr_dict_clue:
            xpair=id_feaStr_dict_user[u].copy();#print len(xpair)
            xpair.update(id_feaStr_dict_clue[c]);#print len(xpair)
            xpair=add_parsing_fea(xpair);#print len(xpair)
            xpair_dict[i]=xpair #<ufea,cfea>
            ydict[i]=y


    #############3
    pd.to_pickle([xpair_dict,ydict],'../data/xdict_ydict')



    ########3
    xpair_dict,ydict=pd.read_pickle('../data/xdict_ydict')
    generate_svm(xpair_dict)
    ylist_sort=[]
    for id,y in ydict.items():
        ylist_sort.append(y)
    pd.to_pickle(ylist_sort,'../data/ylist_sort_forsvm')
    """





    """
    ########3
    # train  lr->nonzeroFea ->xgb(low dim )-->model java
    from sklearn.datasets import load_svmlight_file
    x_clue,_= load_svmlight_file('../data/816train_sample.svm', zero_based=True);print x_clue.shape
    #y=pd.read_pickle('../data/ylist_sort_forsvm')
    y=pd.read_pickle('../data/ylist')

    fid_w_dict,_,x_important=try_lr(x_clue,y);print 'lowdim x',x_important.shape,len(y)#7332dim
    from sklearn.datasets import dump_svmlight_file
    dump_svmlight_file(x_important,np.array(y),'../data/lowDim_xy_816.svm')
    ####nonzero fea fid list ->feaStrname ,all fea
    feaname_to_fid=pd.read_pickle('../data/816feaName_to_fid') #all fea
    fid_to_feaname=dict(zip(feaname_to_fid.values(),feaname_to_fid.keys()))#start from 0
    #feaIndList=[f-1 for f in fidList]
    feaNameList=[]
    for fid in fid_w_dict.keys():#nonzero fea
        feaNameList.append(fid_to_feaname[fid])
        print fid,fid_to_feaname[fid]
    pd.to_pickle(feaNameList,'../data/feaNameList_nonzero_lr')
    """







    #######
    # low dim
    from sklearn.datasets import load_svmlight_file

    x,y= load_svmlight_file('../data/lowDim_xy_816_lr.svm', zero_based=True);print 'x y',x.shape,y.shape
    feaNameList=pd.read_pickle('../data/feaNameList_nonzero_lr_816')





    ##############
    #xgb as scikit learn
    ################

    from xgboost import XGBClassifier
    from sklearn.cross_validation import cross_val_score
    from sklearn.metrics import roc_auc_score
    xgboost_params={
  	"objective": "binary:logistic",
   	#"booster": "gbtree",
   	#"eval_metric": "auc",
  	"learning_rate": 0.1, # 0.06, #0.01,
  	#"min_child_weight": 240,
	"silent":1,
   	"subsample": 0.75,
   	"colsample_bytree": 0.75,
   	"max_depth": 5,
    "n_estimators":50,

    }

    clf=XGBClassifier(**xgboost_params)
    print 'xgb cv...'
    from scipy.sparse import csr_matrix,csc_matrix,coo_matrix
    print type(x)
    #x=coo_matrix(x);print type(x)

    scoreList=cross_val_score(clf, x, y, lambda m, x, y: roc_auc_score(y, m.predict_proba(x)[:,1].T),cv=3)
    print scoreList





    print 'xgb fit...'
    clf.fit(x,y);
    importance=clf.feature_importances_;print importance.shape#[d,]


    for fid in np.nonzero(importance)[0]:
        print feaNameList[fid]
    x_lowdim2=x[:,np.nonzero(importance)[0]]
    feaNameArr2=np.array(feaNameList)[np.nonzero(importance)[0] ]
    weight=importance[np.nonzero(importance)[0] ]
    pd.DataFrame({'name':feaNameArr2,'weight':weight}).to_csv('../data/fea_xgb.csv',encoding='utf-8',index=False)

    print 'x lowdim 2',x_lowdim2.shape
    x=x_lowdim2

    from sklearn.datasets import dump_svmlight_file
    dump_svmlight_file(x,np.array(y),'../data/lowDim2_xy_816.svm')
    #pd.to_pickle(feaNameArr2,'../data/feaName_xgb')



    # ###### cross validation
    # scoreList=cross_val_score(clf, x, y, lambda m, x, y: roc_auc_score(y, m.predict_proba(x)[:,1].T),cv=3)
    # print scoreList


    """
    feaNameList=pd.read_pickle('../data/feaNameList_nonzero_lr')
    print feaNameList[0]

    feaname=pd.read_pickle('../data/feaName_xgb')
    pd.DataFrame({'xgb':feaname}).to_csv('../data/fea_xgb.csv',encoding='utf-8',index=False)
    """


















































