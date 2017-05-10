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


def telephone(tel):
    ''' 处理手机号码 '''

    tel = re.sub(r'\D',"", tel) # 删除非数字字符
    if(len(tel) < 11):          # 若小于11位，则返回空
        return ""
    pattern = re.compile(r'^1[34578]\d{9}$') # 匹配正确的手机号码
    if(re.match(pattern, tel[:11])):         # 前11位
        return tel[:11]
    elif(re.match(pattern, tel[-11:])):      # 后11位
        return tel[-11:]
    return ""

def email(email):
    '''处理邮件'''

    pattern = re.compile(r'^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$')
    if(re.match(pattern, email)):
        return email
    else:
        return ""

def qq(qq):
    '''处理qq'''

    return re.sub(r'\D', '', qq)

def position(position):

    '''处理职位'''
    position=position.decode('utf-8')
    position=strQ2B(position)
    position=remove_single_brace(position)

    #print len(position),[position],type(position)
    #print len(position.decode('utf-8'))# ascii codec
    #print len(position.encode('utf-8'))#18
    #if position == "--" or len(position.decode('utf-8')) < 2:
    if position == "--" or len(position) < 2:
        return ""

    position = position.replace("jingli","经理")
    position = position.replace("laoban","老板")
    position = position.replace("laopan","老板")
    position = position.replace("xiaoshou","销售")

    position = position.decode('utf-8')
    clean_position = []
    for i in range(len(position)):
        char = position[i]
        #print char
        if is_other(char):
            # 如果是英文中间的空格就保留
            if char == u'\u0020' and i-1>=0 and i+1<len(position) and is_alphabet(position[i-1]) and is_alphabet(position[i+1]):
                clean_position.append(char)
            else:
                continue
        else:clean_position.append(char)
    return ''.join(clean_position)


def clean_produce(produce):#{field:v,field:v...}
    '''处理主营产品'''
    #produce=dic['main_produce'];print 'old',produce
    #if isinstance(produce,float) and math.isnan(produce) or produce in ['',None,'null']:return ''
    produce=strQ2B(produce)
    produce=remove_brace_content(produce)
    produce=remove_punct(produce);
    return produce


if __name__ == '__main__':
    # print "Test telephone: \n \t %s" % telephone('1113 jkda 432dja137234s-d8si9238ad   :"dsa；放到‘；/。’"')
    # print "Test qq: \n \t %s" % qq('310.2:">"qds:516ds523')
    # print "Test email: \n \t %s" % email("hdjsadh-sja@dad.com")
    # print "Test position: \n \t %s" % position("jingli%@()Hello World$#@%4545%$^%你猜呢 (你的意思你的 )")






    fieldList={'Clue_Entry_Qq':qq,'Clue_Entry_Email':email,'Clue_Entry_Cellphone':telephone,'Clue_Entry_Major':position,'main_produce':clean_produce}#produce-cellphone-major-qq-email



    fpath='../backup/'
    batch=0

    for fname in os.listdir(fpath)[:]:
        start_time=time.time()
        #cid_field_dict_clean_batch={}#output
        print 'batch',batch
        batch+=1
        path=fpath+fname#input
        cid_field_dict=pd.read_pickle(path)#{cid:{field1:v,,}

        ##
        for field,method in fieldList.items()[:]:
            batch_cid_dict={}
            for cid,dic in cid_field_dict.items()[:]:
                batch_cid_dict[cid]=''

                produce=dic[field];#print 'old',cid,produce
                if isinstance(produce,float) and math.isnan(produce) or produce in ['',None,'null']:
                    rst=''
                    #print produce,'->',''
                else:
                    rst=method(produce);#print 'new',rst
                    #if produce!=rst:print produce,'->',rst
                batch_cid_dict[cid]=rst

            #print batch_cid_dict
            print 'end this batch field',len(batch_cid_dict),'how many seconds',time.time()-start_time
            #pd.to_pickle(batch_cid_dict,'../%s/%s_%s'%(field,field,str(batch)))




    print 'finish clean store...'



