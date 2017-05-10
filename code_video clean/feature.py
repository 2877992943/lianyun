#! -*- coding:utf-8 -*-



'''http://python.jobbole.com/81186/'''


import subprocess
import wave
import struct
import numpy as np
import csv
import sys,os
import scipy
import pandas as pd
import numpy

import pylab as pl

def moments(x):
    mean = x.mean()
    std = x.var()**0.5
    skewness = ((x - mean)**3).mean() / std**3
    kurtosis = ((x - mean)**4).mean() / std**4
    return [mean, std, skewness, kurtosis]
 
def fftfeatures(wavdata):
    f = numpy.fft.fft(wavdata)
    f = f[2:(f.size / 2 + 1)]
    f = abs(f)
    total_power = f.sum()
    f = numpy.array_split(f, 10)
    return [e.sum() / total_power for e in f]
 
def features(x):
    x = numpy.array(x)
    f = []
 
    xs = x
    diff = xs[1:] - xs[:-1]
    f.extend(moments(xs))
    f.extend(moments(diff))
 
    xs = x.reshape(-1, 10).mean(1)
    diff = xs[1:] - xs[:-1]
    f.extend(moments(xs))
    f.extend(moments(diff))
 
    xs = x.reshape(-1, 100).mean(1)
    diff = xs[1:] - xs[:-1]
    f.extend(moments(xs))
    f.extend(moments(diff))
 
    xs = x.reshape(-1, 1000).mean(1)
    diff = xs[1:] - xs[:-1]
    f.extend(moments(xs))
    f.extend(moments(diff))
 
    f.extend(fftfeatures(x))
    return f


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







if __name__=='__main__':
    """
    ### prepare train set
    fpath='../xfile/'
    speak_x=[]
    for f in os.listdir(fpath)[:]:
        if f.find('noise')!=-1:
            x=pd.read_pickle(fpath+f);print x.shape
            x_ll=np.array_split(x,int(x.shape[0]/10000));print 'sample num',len(x_ll)
            for x_i in x_ll:
                if x_i.shape[0]<10000:continue
                f_i=features(x_i[:10000]);
                speak_x.append(f_i)
    ####
    xset=np.array(speak_x);print xset.shape
    pd.to_pickle(xset,'../trainset_noise')
    """


    ### generate x y
    x_noise=pd.read_pickle('../trainset_noise')
    y_noise=[0]*x_noise.shape[0]
    x_speak=pd.read_pickle('../trainset_speak')
    y_speak=[1]*x_speak.shape[0]

    x=np.concatenate((x_noise,x_speak),0);print x.shape
    y=np.array(y_noise+y_speak);print y.shape

    ###
    fid_w_dict,_,_=try_lr(x,y)
    print fid_w_dict.keys()

























 


















































