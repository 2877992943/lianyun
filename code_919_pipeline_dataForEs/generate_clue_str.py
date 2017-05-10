import numbers
import json

import clue
from base import ClueFeature
from base import get_clues
import MySQLdb
from MySQLdb import cursors
import sys,cPickle
import pandas as pd
import numpy as np
reload(sys)
sys.setdefaultencoding('utf8')


db = MySQLdb.connect(host='rds5943721vp4so4j16ro.mysql.rds.aliyuncs.com',
                     user='yunker',
                     passwd="yunker2016EP",
                     db="xddb",
                     use_unicode=True,
                     charset='utf8',
                     cursorclass=cursors.DictCursor)

def get_related_users(pos_clue_ids,testUserList):# clueId list ->{clueid:[uid,...]
    df=pd.read_csv('../user_clue.csv',encoding='utf-8')# clueUser table
    User_Account=pd.factorize(df.User_Account.values)[0];print 'usr factorize',User_Account.shape
    Clue_Id=df.Clue_Id.values
    ##
    df=pd.DataFrame({'user':User_Account,'clueId':Clue_Id})
    grp=df.groupby('clueId')
    clueUserIdDict={}
    for name_clueId,group in grp:
        if name_clueId in pos_clue_ids:
            print name_clueId,group.user.values.shape
            clueUserIdDict[name_clueId]=list(group.user.values)
        if len(clueUserIdDict)%10==0:break
    return clueUserIdDict #{clueid:uidlist






class Generator(object):

    def __init__(self):
        self.feature_name_to_index = {}
        self.feature_index_to_name = {}
        self.next_fid = 1

    def populate_dictionary(self, categorical_features, object_ids):
        for f in categorical_features:
            if f.categorical:
                #print 'Populating categorical feature dictionary %s' % f.name
                for oid in object_ids:
                    f.cached_calculate(oid)
                #print 'Dictionary size %d' % len(f.vocabulary)



    def populate_feature_index(self, features):
        #self.next_fid=1000000
        for f in features:
            if f.categorical:
                assert sorted(f.vocabulary.values()) == range(len(f.vocabulary))
                for i in xrange(len(f.vocabulary)):
                    fname = '%s_%d' % (f.name, i)#product_10
                    self.feature_name_to_index[fname] = self.next_fid + i #1st feature not 0,is 1
                self.next_fid += len(f.vocabulary)
                #self.next_fid += 1000000
            else:
                self.feature_name_to_index[f.name] = self.next_fid
                self.next_fid += 1
        self.feature_index_to_name = dict([(fid, fn) for fn, fid in self.feature_name_to_index.items()])


    def transform(self, features, object_ids):
        """Returns a list of dictionary: feature_idx => feature_value for object_ids."""

        self.populate_dictionary(features, object_ids)
        self.populate_feature_index(features)

        X = []
        for oid in object_ids:# record by record when generate feature
            row = {} #{string:1,
            for f in features:
                x = f.cached_calculate(oid)
                #print 'f.cached_calculate',f.name,x #produce [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]->[str,...]
                if f.categorical:
                    for cat in sorted(x):#each cat is string
                        fname = '%s_%s' % (f.name, cat)#produce_apple not produce_1
                        #row[self.feature_name_to_index[fname]] = 1.0
                        #row[fname]=1
                        #cat=str(cat)
                        if fname in row:row[fname]=1  #row[word]=1  row={word:1,,,}
                        else:row[fname]=1
                elif isinstance(x, numbers.Number):
                    #row[self.feature_name_to_index[f.name]] = x
                    row[f.name]=x
                else:
                    raise Exception('Not sure how to handle %s value %s' % (f.name, str(x)))


            yield [oid,row] #each record feature ->dict {featureIndex:featureValue...}

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

def gen_svmlight(filename, features, object_ids,tableName): #filename '../f'  tablename :clue user
    g = Generator()
    it = g.transform(features, object_ids)
    clueIdDict={} ###{oid:{fid:fv,...  #product_computer
    numClue=0
    with open('no_use' + '.svm', 'w') as f:
        for oid_row_dict in it: # each rowdict ->1 obs clueid
            ## y
            #if numClue<101269/2:y=0
            #else:y=1
            y=0
            ##
            oid,row_dict=oid_row_dict
            clueIdDict[oid]=row_dict
            f.write(str(y)+' ')#f.write('0 ')
            for fid in sorted(row_dict.keys()):
                fval = row_dict[fid]
                if fval != 0.0:
                    f.write('%s:%f ' % (fid, fval))## write into  "modeling.svm"
            f.write('\n')
            numClue+=1

    # with open(filename + '.oid', 'w') as f:
    #     for oid in object_ids:
    #         f.write(oid + '\n')
    #
    # for feature in features:
    #     if feature.categorical:
    #         with open(filename + ('.%s.voc' % feature.name), 'w') as f:
    #             f.write(json.dumps(feature.vocabulary,ensure_ascii=False))
    #         save2pickle(feature.vocabulary,'../feature_vocab_%s_cpickle_%s'%(feature.name,tableName) )
    #
    # with open(filename + '.feature_index', 'w') as f:
    #     f.write(json.dumps(g.feature_index_to_name,ensure_ascii=False))

    #save2pickle(g.feature_index_to_name,'../feature_index_to_name_cpickle_'+tableName)
    #pd.to_pickle(clueIdDict,'../clueTable_dict/id_feaDict_'+filename+'_'+tableName)
    #print 'clue id feastr dict', clueIdDict  #####??? {cid:{produce_mianhua:1,,,,
    #print 'feature ind to name len cp',len(g.feature_index_to_name)
    #print_dict(clueIdDict)
    #save2pickle(clueIdDict,'../id_feaDict_'+filename)
    return clueIdDict

def print_dict(clueIdDict):
    for k,v in clueIdDict.items():
        print k
        for kk,vv in v.items():
            print '...',kk,vv


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


def write2lightSVM(filename,clueId_feaDic_dict,ylist):
    i=0
    with open(filename + '.svm', 'w') as f:
        for row_dict in clueId_feaDic_dict.values(): # each rowdict ->1 obs  {fidx:fv...}
            ##

            f.write(str(ylist[i])+' ') # f.write('0 ')
            i+=1
            for fid in sorted(row_dict.keys()):
                fval = row_dict[fid]
                if fval != 0.0:
                    f.write('%d:%f ' % (fid, fval))## write into  "modeling.svm"
            f.write('\n')




if __name__ == '__main__':

    ############
    # part 1 clue table
    clue_ids = [
        '04323D70B06544FE9187181D50B573B3',
        '014F35B3F2904A87935BD11FBCAE0F97',
    ][:1]

    g = Generator()
    features = ClueFeature.__subclasses__()
    print features

    ret = g.transform(features, clue_ids)
    print '.'*20
    for r in ret:
        print r
    print '\\'*20
    print g.feature_name_to_index
    print g.feature_index_to_name
    print '*'*20
    gen_svmlight('test', features, clue_ids,'clue')








