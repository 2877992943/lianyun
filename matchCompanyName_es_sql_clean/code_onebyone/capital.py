#! -*- coding:utf-8 -*-

import MySQLdb
import random,cPickle
import csv,re
import pandas as pd

from MySQLdb import cursors
import numpy as np

from menu_to_companyName_product_list import strQ2B,is_chinese_1,is_chinese
 
#import pylab as plt
import sys,time
reload(sys)
sys.setdefaultencoding('utf8')





def get_currency(stri):
    for item in [ '万元','亿元','万','亿' ]:
        stri=stri.decode('utf-8').replace(item.decode('utf-8'),'')
    return stri

 

def process_capital(capital):

    ret={'digit':'','char':''}
    #print 'capital',capital,[capital]
    if isinstance(capital,float) and math.isnan(capital) or capital==None or capital=='':return ret

    # seperate char digit
    capital=capital.decode('utf-8')
    capital=strQ2B(capital);
    capital=re.sub('(\.[0]{0,3})','',capital)

    char_str=''
    digit_str=''

    cap=capital.decode('utf-8');
    for char in cap:

        if is_chinese_1(char.decode('utf-8')) or is_chinese(char.decode('utf-8')):char_str+=char.strip(' ')
        elif char.isdigit():digit_str+=char
    ###

    if digit_str in ['','0']:
        return ret

    digit_float=float(digit_str);
    # analyze char_str
    if '万'.decode('utf-8') in char_str:digit_float*=float(1e4)
    if '亿'.decode('utf-8') in char_str:digit_float*=float(1e8)
    if '亿'.decode('utf-8') not in char_str and '万'.decode('utf-8') not in char_str and len(digit_str) in [1,2,3,4]:digit_float*=float(1e4)
    ret['digit']=digit_float;#print cid_str_dict[cid]['digit']
    ret['char']=get_currency(char_str) if get_currency(char_str)!='' else "人民币";#print cid_str_dict[cid]['char']
    return ret



if __name__ == "__main__":
    



    #### test
    testlist=['13000.00万','0.0万','33000.00万','50.00万']
    for test in testlist:
        ret=process_capital(test);print test,'[',ret.values()[0],ret.values()[1],']'








    
































