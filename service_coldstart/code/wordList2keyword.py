#! -*- coding:utf-8 -*-

''' not good effect'''


import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import jieba
from jieba import analyse

word_ind_dict=pd.read_pickle('/home/yr/yunke/lda_clue/backup/word_ind_dict')
word_idf_dict=pd.read_pickle('/home/yr/yunke/lda_clue/backup/word_idf_dict')
#word2vec_model=pd.read_pickle('/home/yr/yunke/lda_clue/backup/word2vec_model_wordInd')

def idf_keyword(parse,n1):
    parseList=parse.split(' ')
    word_idf={}
    for word in parseList:
        if word in word_idf_dict:
            word_idf[word]=word_idf_dict[word]
    ###
    ll=sorted(word_idf.iteritems(),key=lambda s:s[1],reverse=True)
    n=n1 if len(ll)>=n1 else len(ll)
    return [p[0] for p in ll][:n]


def extract_tag(sentence,topn):
    ll=[]

    for x, w in jieba.analyse.textrank(sentence, topK=10,withWeight=True,allowPOS=('ns', 'n', 'vn', 'v')):
        if w>0.0:ll.append(x)
        #print('%s %s' % (x, w))
    return combine_parsed(ll)

def combine_parsed(ll):
    n=len(ll);
    ll_ret=[]
    for i in range(n)[:-1]:

        pair=ll[i]+ll[i+1];
        ll_ret.append(pair)
    return ll_ret

if __name__=='__main__':
    s='此外，公司拟对全资子公司吉林欧亚置业有限公司增资4.3亿元，增资后，吉林欧亚置业注册资本由7000万元增加到5亿元。吉林欧亚置业主要经营范围为房地产开发'
    ll=extract_tag(s,3);print ' '.join(ll)













































































































