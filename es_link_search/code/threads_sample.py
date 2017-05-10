#! -*- coding:utf-8 -*-

import threading,time

class timer(threading.Thread):
    def __init__(self,num,interval):
        threading.Thread.__init__(self)
        self.thread_num=num
        self.interval=interval
        self.thread_stop=False
    def run(self):
        start_time=[time.time(),time.ctime()]

        while not self.thread_stop:
            print 'thread %d time %s'%(self.thread_num,time.ctime())
            time.sleep(self.interval)

        end_time=[time.time(),time.ctime()]
        print 'endtime - starttime',end_time[0]-start_time[0],start_time[1],end_time[1]

    def stop(self):
        self.thread_stop=True

def test():
    thread1=timer(1,1)
    thread2=timer(2,2)
    thread1.start()
    thread2.start()

    time.sleep(10)
    thread1.stop()
    thread2.stop()

if __name__=='__main__':
    test()