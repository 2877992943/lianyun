#! -*- coding:utf-8 -*-

import pandas as pd
import sys,os
reload(sys)
sys.setdefaultencoding('utf8')
import jieba
from jieba import analyse
from menu_to_companyName_product_list import strip_word,is_chinese,remove_digit_english_punct,extract_tag,wordParsing_all,calculate_text,wordParsing,strQ2B

# word_ind_dict=pd.read_pickle('/home/yr/yunke/lda_clue/backup/word_ind_dict')
word_idf_dict=pd.read_pickle('/home/yr/yunke/lda_clue/backup/word_idf_dict')
#word_idf_dict=pd.read_pickle('/root/yangrui/lda_clue/data/word_idf_dict_2gram')
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


def extract_tag_combine(sentence,topn):
    ll=[]

    for x, w in jieba.analyse.textrank(sentence, topK=10,withWeight=True,allowPOS=('ns', 'n', 'vn', 'v')):
        if w>0.0:ll.append(x)
        #print('%s %s' % (x, w))
    return combine_parsed(ll)

def extract_tag_old(sentence,topn,allowPOS):
    ll=[]
    #allowPOS=('ns', 'n', 'vn', 'v')
    for x, w in jieba.analyse.textrank(sentence, topK=topn,withWeight=True,allowPOS=allowPOS):
        if w>0.0:ll.append(x)
        #print('%s %s' % (x, w))
    return ll


def combine_parsed(ll):
    n=len(ll);
    ll_ret=[]
    for i in range(n)[:-1]:

        pair=ll[i]+ll[i+1];
        ll_ret.append(pair)
    return ll_ret


class generateParseCominfo(object):
    def __init__(self,fpath):
        self.fpath=fpath

    def __iter__(self):
        for fname in os.listdir(self.fpath)[:1]:
            cid_parse=pd.read_pickle(self.fpath+fname)
            for cid,parse in cid_parse.items()[:1]:
                yield [cid,parse]



def filter_idf(word_idf_dict,ll):
    wordi_idf={}
    for word in ll:
        if word in word_idf_dict:
            idf=word_idf_dict[word]
            wordi_idf[word]=idf
    ###
    ss=sorted(wordi_idf.iteritems(),key=lambda x:x[1],reverse=True)
    n=50 if len(ss)>=50 else len(ss)
    ss=[w[0] for w in ss][:n]
    return ss

if __name__=='__main__':

    ####
    # fpath='../parseFile_businessScope/'
    # gene=generateParseCominfo(fpath)
    # for comname,parse in gene:
    #     # if parse=='': continue
    #     # print 'parse',parse,comname
    #     # ll=extract_tag_old(parse,100,('ns','n','v','vn'))# textrank split cannot do 2gram
    #     # print 'll',' '.join(ll)
    #     # ll=filter_idf(word_idf_dict,ll)
    #     # print 'filtered',' '.join(ll)
    #     # break
    #
    #     #####
    #     ll=idf_keyword(parse,10)
    #     print ' '.join(ll)


    ##### test 2-gram
    parse='可颂国际集团创建于1989年，总部在上海，是一家专门从事食品生产销售的大型企业。产品涉及冷冻面团生产、销售，面包烘焙、西点、蛋糕制作和销售，中秋月饼礼盒销售，同时进行连锁门店经营：法式面包西点精品店、法式面包咖啡精品店等。 集团旗下拥有两个知名品牌：〈可颂坊〉〈麦圃〉。 可颂坊品牌：使用于集团直营的——法式面包西点精品店——法式面包咖啡精品店。 麦圃品牌：使用于集团直营的——在大、中型卖场和超市销售面包、西点、蛋糕产品。 目前上海，杭州，深圳就已开设了100多家连锁销售网点和门店，销售产品达百余种之多。同时集团在上海和台湾拥有两座大型的冷冻面团生产工厂，为全国的餐饮企业及中小型面包房提供便捷周到的服务。我们的产品已通过了国际食品安全管理体系（HACCP）的认证，并在各经营所在地获得重点知名品牌认证。 一直以来可颂国际集团依托着自己强大的经营管理队伍和技术研发力量，坚持不懈地追求产品和服务的高品质，坚持产品的：新鲜、美味、健康；追求服务的：体贴、周到、温馨； 可颂坊“新鲜让你看见、美味让你闻见”现场烤制的经营模式一直引领着现代人健康时尚的消费潮流。可颂国际集团在上海、北京、深圳、杭州、台北等地都设有相关的企业和分支机构，集团计划近期再扩大销售网点和分支机构'
    ll=extract_tag_old(parse,100,('ns','n','v','vn'))
    print ' '.join(ll)
    ll=filter_idf(word_idf_dict,ll)
    print 'filtered',' '.join(ll)
















































































































