# coding=utf-8

import math
import re
import MySQLdb
import time
import pandas as pd

from MySQLdb import cursors
from menu_to_companyName_product_list import calculate_text

import sys
reload(sys)
sys.setdefaultencoding('utf8')

from menu_to_companyName_product_list import calculate_text

db = MySQLdb.connect(host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com',
                     user='yunker',
                     passwd="yunker2016EP",
                     db="xddb",
                     use_unicode=True,
                     charset='utf8',
                     cursorclass=cursors.DictCursor)
 


CLUE_TABLE = "crm_t_clue"

CLUE_CACHE = {}
USER_CACHE = {}
FEATURE_VALUE_CACHE = {}

def dict_result_cache(cache_ref):#compare objectId cacheId  ->return objectIdAllContent

    def inner(object_ids, core_func):
        #print 'obj id',object_ids,'len cache',len(cache_ref),len(CLUE_CACHE)
        ret = {}
        miss = []
        for oid in object_ids:
            if oid in cache_ref:
                ret[oid] = cache_ref[oid]
            else:
                miss.append(oid)
        #print 'miss ',len(miss),len(ret)
        if miss:
            more = core_func(miss)
            cache_ref.update(more)
            ret.update(more)
        return ret

    def decorator(func):
        def decorated(object_ids):
            return inner(object_ids, func)
        return decorated

    return decorator


@dict_result_cache(CLUE_CACHE)#dict_result_cache(func,dictCache)
def get_clues(clue_ids):


    #start_time=time.time()
    sql = """
       SELECT *
       FROM crm_t_clue
       WHERE CLue_Id IN
             ('%s')
    """
    cur = db.cursor()
    cur.execute(sql % "','".join(clue_ids))
    ret = {}
    for r in cur.fetchall():
       ret[r['CLue_Id']] = r #{id:{record},...}


    print 'len cache',len(CLUE_CACHE)
    #print end_time - start_time

    return ret  #dict {id:{record},..}

@dict_result_cache(USER_CACHE)#fn(arg1,arg2)
def get_users(user_ids):

    sql = """
       SELECT *
       FROM crm_t_portaluser
       WHERE User_Id IN
             ('%s')
    """
    cur = db.cursor()
    cur.execute(sql % "','".join(user_ids))
    ret = {}
    for r in cur.fetchall():
       ret[r['User_Id']] = r #{id:{record},,,}
    return ret



class BaseFeature(object):

    name = None
    categorical = False

    @classmethod
    def get_user_dict(cls, user_id):
        return get_users([user_id])[user_id]

    @classmethod
    def get_clue_dict(cls, clue_id):
        return get_clues([clue_id])[clue_id]#->{record}

    @classmethod
    def get_data(cls, object_id):
        raise NotImplementedError()

    @classmethod
    def calculate(cls, object_id):
        raise NotImplementedError()


    @classmethod
    def cached_calculate(cls, object_id):
        if cls.name not in FEATURE_VALUE_CACHE:
            FEATURE_VALUE_CACHE[cls.name] = {}
            FEATURE_VALUE_CACHE[cls.name][object_id] = cls.calculate(object_id)#????calculate?
        elif object_id not in FEATURE_VALUE_CACHE[cls.name]:
            FEATURE_VALUE_CACHE[cls.name][object_id] = cls.calculate(object_id)
        return FEATURE_VALUE_CACHE[cls.name][object_id]


class ClueFeature(BaseFeature):

    @classmethod
    def get_data(cls, clue_id):
        return cls.get_clue_dict(clue_id)

class UserFeature(BaseFeature):

    @classmethod
    def get_data(cls, user_id):
        return cls.get_user_dict(user_id)


class UserClueFeature(BaseFeature):
    pass


class CategoricalTextMixIn(object):

    split_pattern = '[\\s;,/#|]+'

    vocabulary = None
    #vocabulary = {'a':11,'b':11,'c':11,'d':11}
    vocabulary_size = None
    #vocabulary_size = 0
    field_name = None
    categorical = True

    @classmethod
    def encode(cls, words):#sentence wordlist-> ind list
        ret = [-1] * len(words)
        for i, w in enumerate(words):
            if w in cls.vocabulary:
                ret[i] = cls.vocabulary[w] #{word:ind...}
            else:
                ret[i] = cls.vocabulary_size
                cls.vocabulary[w] = cls.vocabulary_size
                cls.vocabulary_size += 1
        return ret

    @classmethod
    def get_raw_str(cls, object_id):
        assert hasattr(cls, 'get_data')
        dic = cls.get_data(object_id)#????
        return dic[cls.field_name]

    @classmethod
    def calculate_word(cls, object_id):#filter words?
        '''
        返回此feature中指定ID的words
        '''
        raw_str = cls.get_raw_str(object_id)
        if raw_str is None:
            return []
        words = re.split(cls.split_pattern, raw_str)
        words = filter(lambda w: w and len(w) > 0, words)
        return words

    @classmethod
    def calculate(cls, object_id): #words->wordlist->widlist
        raw_str = str(cls.get_raw_str(object_id))
        if raw_str is None:
            return []
       # words = re.split(cls.split_pattern, raw_str)# only split by ,/|# ,not segmentWord
        words=calculate_text(raw_str);#print words,[words]
        words = filter(lambda w: w and len(w) > 0, words)
        #return sorted(cls.encode(words))
        return [word.decode('utf-8') for word in words]


#    @classmethod
#    def calculate(cls, object_id):
#        raw_str = cls.get_raw_str(object_id)
#        if raw_str is None:
#            return []
#        words = re.split(cls.split_pattern, raw_str)
#        words = filter(lambda w: w and len(w) > 0, words)
#        feature_index=[]
#        for word in words:
#            if word in FEATURE_SET[cls.name]:
#                feature_index.append(FEATURE_SET_INDEX[cls.name][word])
#        return sorted(feature_index)


class FieldExistsMixIn(object):

    field_name = None

    @classmethod
    def get_raw_boolean(cls, object_id):
        assert cls.field_name
        dic = cls.get_data(object_id)#??? ->{record}
        assert cls.field_name in dic
        return dic.get(cls.field_name) #{k:v}->v

    @classmethod
    def calculate(cls, object_id):
        raw = cls.get_raw_boolean(object_id)
        if raw is None:
            return 0
        return int(len(raw) > 0) #hasQQ or not ->1 or 0


class ParseNumberMixIn(object):

    field_name = None
    use_log = False
    default = 0.0

    @classmethod
    def calculate(cls, object_id):
        raw = str(cls.get_data(object_id)[cls.field_name])
        if raw is None:
            return cls.default
        numbers = re.findall('\d*\.\d+|\d+', raw)# 0.4 or 4

        ret = cls.default
        if numbers:
            ret = float(numbers[-1])
            if '万' in raw:
                ret *= 1e4
            elif '亿' in raw:
                ret *= 1e8
        if cls.use_log and ret == 0.0:
            ret += 1.0
        return math.log10(ret) if cls.use_log else ret


if __name__ == '__main__':
    print 'Test get_clues'
    ret = get_clues(['00000710C8384011A97351736D3D83BD'])
    """
    ret = get_clues(['00000710C8384011A97351736D3D83BD',
                     '0000091811A94140903DFF82DA87259A'])
    print type(ret)
    print len(ret)
    print ret.keys()

    print 'Test get_users'
    ret = get_users(['00050CAF4FB14498B7A1E2FE37BFEA8F'])
    ret = get_users(['000749EC063741D0AD033DF49540C29B',
                     '00050CAF4FB14498B7A1E2FE37BFEA8F'])
    print type(ret)
    print len(ret)
    print ret.keys()
    """
