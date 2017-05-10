#! -*- coding:utf-8 -*-

import threading,time,random

class query_es(threading.Thread):
    def __init__(self,num,interval,att_q,val_q,returnNum):
        threading.Thread.__init__(self)
        self.thread_num=num
        self.interval=interval
        self.thread_stop=False
        self.att=att_q
        self.val=val_q
        self.returnNum=returnNum

    def run(self):
        start_time=[time.time(),time.ctime()]
        res=es.search(index="yunkecn",doc_type="clue",body=cluetable_query_param(self.att,self.val,self.returnNum))

        #while not self.thread_stop:
         #   print 'thread %d time %s'%(self.thread_num,time.ctime())
         #   time.sleep(self.interval)
        print 'len %d , thread %d'%(res['hits']['hits'].__len__(),self.thread_num)

        end_time=[time.time(),time.ctime()]
        print 'endtime - starttime',end_time[0]-start_time[0],start_time[1],end_time[1],'thread',self.thread_num

    def stop(self):
        self.thread_stop=True

def cluetable_query_param(att,val,returnNum):
    que= {
            "from" : 0, "size" : returnNum,  #100 200 500 1000
            "query" : {
                    "match" : { att : val }
                     },
           #"fields":['CLue_Id','main_produce']
         }
    return que

def test(threadN,returnNum):

    for t in range(threadN): #2 5 10   #

        word=random.choice(wordList)
        thread_i=query_es(t,1,'main_produce',word,returnNum) # each thread

        thread_i.start()

    #time.sleep(10)
    #thread1.stop()
    #thread2.stop()

if __name__=='__main__':
    from elasticsearch import Elasticsearch
    es = Elasticsearch("123.57.62.29")


    #res=es.search(index="yunkecn",doc_type="clue",body=cluetable_query_param('main_produce','建筑五金材料'))
    ###
    wordList=['建筑五金材料','五金材料建筑','五金建筑材料','建材','化妆品','家具']
    for iteri in range(1):
        test(10,100)  #2 5 10thread  100 200 500 1000
        print 'thread tt 10,return 100'