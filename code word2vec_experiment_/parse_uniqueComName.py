#! -*- coding:utf-8 -*-

import pandas as pd
import sys,os
reload(sys)
sys.setdefaultencoding('utf8')
import jieba
from jieba import analyse
from menu_to_companyName_product_list import strip_word,is_chinese,remove_digit_english_punct,extract_tag,wordParsing_all,calculate_text,wordParsing,strQ2B,combine_parsed

# word_ind_dict=pd.read_pickle('/home/yr/yunke/lda_clue/backup/word_ind_dict')
# word_idf_dict=pd.read_pickle('/home/yr/yunke/lda_clue/backup/word_idf_dict')
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

def extract_tag_old(sentence,topn):
    ll=[]

    for x, w in jieba.analyse.textrank(sentence, topK=10,withWeight=True,allowPOS=('ns', 'n', 'vn', 'v')):
        if w>0.0:ll.append(x)
        #print('%s %s' % (x, w))
    return ll




if __name__=='__main__':
    # s='简介：三唐信息技术有限有限公司成立于2005年12月，公司致力于企业一条龙服务，游戏大厂商一条龙服务。由三唐信息技术有限公司自主研发与运营的游戏平台(hp:www.gamevideo.c)，获得多家风险投资的青睐．公司与多家上市公司合作紧密，有着雄厚的资源和多名资深游戏顾问.业务范围：游戏视界，游戏厂商一条龙服务、Flash游戏开发[单机、手机、电视、连网]、Flash应用软件开发、Flash动画片制作项目质量监控体系：为了保证项目的质量，三唐全国首家自主开发出质量监控体系，让客户参与每一个项目，把关项目每个细节。上海三唐信息技术有限公司竭诚为您服务，欢迎新老客户前来洽谈。联系人：金元，联系电话：0571-*，移动电话：*。'
    # ll=extract_tag_old(s,3);print ' '.join(ll)



    #### parse

    sql_att_list=['CLue_Id','param8','main_produce','com_info','Clue_Entry_Com_Name']
    #### clean parse
    fpath='../businessScope/'
    unique_comname=[]

    for name in os.listdir(fpath)[:]:
        cid_content=pd.read_pickle(fpath+name)
        cid_content_parse={}
        for cid,r in cid_content.items()[:]:
            allFields_list=[]
            # comname=r['Clue_Entry_Com_Name']
            # if comname in cid_content_parse:continue

            for field in ['com_info']:

                ### too slow
                # if comname in unique_comname:continue
                # if comname not in unique_comname:unique_comname.append(comname)
                if field not in r or r[field] in ['null','NULL','',None]:continue
                stri=r[field]
                stri=remove_digit_english_punct(stri)
                ll=calculate_text(stri)
                ll_parse=[]

                for phrase in ll:
                    #print '0',phrase
                    ll_1gram=wordParsing(phrase,False)#for search|False cut  |True cut all
                    #print '1', ' '.join(ll_1gram)
                    ll_parse+=ll_1gram
                    ### 2-gram
                    # if len(ll_1gram)>=2:
                    #     ll_2gram=combine_parsed(ll_1gram)
                    #     #print '2',' '.join(ll_2gram)
                    #     ll_parse+=ll_2gram
                    ####
                ll=[w.strip(' ') for w in ll_parse if len(w.strip(' ').decode('utf-8'))>1]
                allFields_list+=ll

            #cid_content_parse[cid]=' '.join(list(set(allFields_list)))
            if len(allFields_list)==0:continue
            #cid_content_parse[comname]=' '.join((allFields_list))
            cid_content_parse[cid]=' '.join((allFields_list))
            # print ' '.join((allFields_list))
            # break

        print 'each batch',len(cid_content_parse)


        pd.to_pickle(cid_content_parse,'../parseFile_businessScope/%s'%str(cid_content_parse.keys()[0]))




    ####
    fpath='../parseFile_businessScope/'















































































































