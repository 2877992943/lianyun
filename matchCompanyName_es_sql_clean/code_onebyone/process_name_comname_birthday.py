# coding:utf-8
import re,os,time
import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from menu_to_companyName_product_list import strQ2B,remove_single_brace,remove_brace_content,remove_punct


def is_chinese(uchar):
    """判断一个unicode是否是汉字"""
    if uchar >= u'\u4e00' and uchar<=u'\u9fa5':
        return True
    else:
        return False

def is_number(uchar):
    """判断一个unicode是否是数字"""
    if uchar >= u'\u0030' and uchar<=u'\u0039':
        return True
    else:
        return False

def is_alphabet(uchar):
    """判断一个unicode是否是英文字母"""
    if (uchar >= u'\u0041' and uchar<=u'\u005a') or (uchar >= u'\u0061' and uchar<=u'\u007a'):
        return True
    else:
        return False

def is_other(uchar):
    """判断是否非汉字英文字符和（）()"""
    if not (is_chinese(uchar) or is_alphabet(uchar) or uchar == u'\uff08' or uchar == u'\uff09' or uchar == u'\u0028' or uchar == u'\u0029'):
        return True
    else:
        return False




def leads_name(name):
    '''处理姓名'''
    if name in ["--", "暂无数据","暂未提供"]:
        return ""
    name = re.sub('&amp;#8226;', '·', name)
    name = strQ2B(name.decode('utf-8'))  # 全角转半角
    name = re.sub("(\(.*?\))", ' ', name)  # 去除（）
    name = re.sub(r'\d', '', name)  # 去除数字
    name=name.replace('先生先生','先生')
    name=name.replace('女士女士','女士')
    name=remove_single_brace(name)
    clean_name = []
    for i in range(len(name)):
        char = name[i]
        if is_other(char):
            if(
                char == u'\u0020'
                and i-1>=0
                and i+1<len(name)
                and is_alphabet(name[i-1])
                and is_alphabet(name[i+1])
            ):
                clean_name.append(char)
            elif (char == u'\u002e'
                    or char == '·'.decode('utf-8')):
                clean_name.append(char)
            else:
                continue
        else:
            clean_name.append(char)
    return ''.join(clean_name)

def com_name(com_name):
    '''处理公司名称'''
    com_name = com_name.decode('utf-8')
    if len(com_name) < 2:
        return ""
    com_name = re.sub(' ', '', com_name)
    return com_name


def birthday(birthday):
    '''处理生日 日期'''
    birthday = re.sub(r'\D', '-', birthday)
    birthday = birthday.split('-')
    clean_birthday = []
    for i in birthday:
        if i.isdigit() and len(i)==1:
            clean_birthday.append('0'+str(i))
        if i.isdigit() and len(i)==2:
            clean_birthday.append(str(i))

    return '-'.join(clean_birthday)




if __name__ == '__main__':
    # print "Test telephone: \n \t %s" % telephone('1113 jkda 432dja137234s-d8si9238ad   :"dsa；放到‘；/。’"')
    # print "Test qq: \n \t %s" % qq('310.2:">"qds:516ds523')
    # print "Test email: \n \t %s" % email("hdjsadh-sja@dad.com")
    # print "Test position: \n \t %s" % position("jingli%@()Hello World$#@%4545%$^%你猜呢 (你的意思你的 )")




    fieldList={'Clue_Entry_Com_Name':com_name,'Clue_Entry_Name':leads_name,'Clue_Entry_Birthday':birthday}#birthday-name-comname
    print ' '.join(fieldList.keys())



    fpath='../backup/'
    batch=0

    for fname in os.listdir(fpath)[:3]:
        print fname
 
        start_time=time.time()
        #cid_field_dict_clean_batch={}#output
        print 'batch',batch
        batch+=1
        path=fpath+fname#input
        cid_field_dict=pd.read_pickle(path)#{cid:{field1:v,,}

        ##
        for field,method in fieldList.items()[:1]:
            batch_cid_dict={}
            for cid,dic in cid_field_dict.items()[:]:
                batch_cid_dict[cid]=''

                produce=dic[field];#print 'old',produce
                if isinstance(produce,float) and math.isnan(produce) or produce in ['',None,'null']:
                    #print produce,'->',''
                    rst=''
                else:
                    rst=method(produce);#print 'new',rst
                    if rst!=produce:print produce,'->',rst
                batch_cid_dict[cid]=rst

            #print batch_cid_dict
            print 'end this batch field',len(batch_cid_dict),'how many seconds',time.time()-start_time

            #pd.to_pickle(batch_cid_dict,'../%s/%s_%s'%(field,field,str(batch)))
            ## see result





    print 'finish clean store...'
 


