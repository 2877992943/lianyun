#! -*- coding:utf-8 -*-




import os.path
import os
import pandas as pd

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import es_filter_clueDB_to_cidProduce,generate_fea_mainproduce,mainProduce_knn,read_csv,cominfoVec_knn,es_util_generatePart,menu_to_companyName_product_list

import time
import numpy


import tornado.httpclient

import json
import datetime

from keyword_ext import query_synonym


from tornado.options import define, options
import es_filter_aggregation
import get_client_feaStr

global searchFilter
MAX_WORD_BUSINESS,MAX_NUM_TARGETCLIENT=3,100
dictPathList=['../backup/word_ind_dict','../backup/word_idf_dict','../backup/word2vec_model_wordInd',
              '../backup/noise_extend']
word_ind_dict_path,word_idf_dict_path,word2vec_model_path,noisePath=dictPathList

define("port", default=8701, help="run on the given port", type=int)

class FilterHandler_useTargetCompany(tornado.web.RequestHandler):

    def post(self):
        global searchFilter
        ## get argument
        province=self.get_argument('province',strip=True)
        city=self.get_argument('city',strip=True)
        comname=self.get_argument('comname',strip=True)
        comname_mustnot=self.get_argument('comname_mustnot',strip=True)
        position=self.get_argument('position',strip=True)
        employeeNum=self.get_argument('employeeNum',strip=True)
        geo=self.get_argument('geo',strip=True)
        registrationYear=self.get_argument('registrationYear',strip=True)
        comname_list=self.get_argument('comname_list',strip=True)
        businessScope_list=self.get_argument('businessScope_list',strip=True)
        companyCode=self.get_argument('companyCode',strip=True)

        ###
        comname_list=' '.join(menu_to_companyName_product_list.calculate_text(comname_list))
        businessScope_list=' '.join(menu_to_companyName_product_list.calculate_text(businessScope_list))





        ## comname
        df=pd.read_csv('../data/keyword_comname.csv');print df.shape
        keyword_comname=df['keyword'].values.tolist()[:]
        key_count,total1=es_filter_aggregation.query_clueDB('clue_*filtered',province,city,comname,comname_mustnot.split(' '),geo.split(' '),position,employeeNum,registrationYear,keyword_comname+comname_list.split(' '),'Clue_Entry_Com_Name',companyCode)#'Clue_Entry_Com_Name' main_produce
        pd.DataFrame({'key':key_count[0],'count':key_count[1]}).to_csv('../data/key_count_comname.csv',index=False,encoding='utf-8')
        ## businessScope
        df=pd.read_csv('../data/keyword_business.csv');print df.shape
        keyword_bs=df['keyword'].values.tolist()[:]
        key_count,total2=es_filter_aggregation.query_clueDB('clue_*filtered',province,city,comname,comname_mustnot.split(' '),geo.split(' '),position,employeeNum,registrationYear,keyword_bs+businessScope_list.split(' '),'main_produce',companyCode)#'Clue_Entry_Com_Name' main_produce
        pd.DataFrame({'key':key_count[0],'count':key_count[1]}).to_csv('../data/key_count_business.csv',index=False,encoding='utf-8')

        ### save filter and keylist
        searchFilter=[province,city,comname,comname_mustnot.split(' '),geo.split(' '),position,employeeNum,registrationYear,companyCode]
        pd.to_pickle(searchFilter,'../data/searchFilter')

        #### result
        self.render('link_keyword_upload.html',header_text="searched leads by comname only %d  by business only %d"%(total1,total2))


class FilterHandler_notuseTargetCompany(tornado.web.RequestHandler):

    def post(self):
        global searchFilter
        ## get argument
        province=self.get_argument('province',strip=True)
        city=self.get_argument('city',strip=True)
        comname=self.get_argument('comname',strip=True)
        comname_mustnot=self.get_argument('comname_mustnot',strip=True)
        position=self.get_argument('position',strip=True)
        employeeNum=self.get_argument('employeeNum',strip=True)
        geo=self.get_argument('geo',strip=True)
        registrationYear=self.get_argument('registrationYear',strip=True)
        comname_list=self.get_argument('comname_list',strip=True)
        businessScope_list=self.get_argument('businessScope_list',strip=True)
        companyCode=self.get_argument('companyCode',strip=True)


        ###
        comname_list=' '.join(menu_to_companyName_product_list.calculate_text(comname_list));
        businessScope_list=' '.join(menu_to_companyName_product_list.calculate_text(businessScope_list));

        ## comname

        key_count,total1=es_filter_aggregation.query_clueDB('clue_*filtered',province,city,comname,comname_mustnot.split(' '),geo.split(' '),position,employeeNum,registrationYear,comname_list.split(' '),'Clue_Entry_Com_Name',companyCode)#'Clue_Entry_Com_Name' main_produce
        pd.DataFrame({'key':key_count[0],'count':key_count[1]}).to_csv('../data/key_count_comname.csv',index=False,encoding='utf-8')
        ## businessScope

        key_count,total2=es_filter_aggregation.query_clueDB('clue_*filtered',province,city,comname,comname_mustnot.split(' '),geo.split(' '),position,employeeNum,registrationYear,businessScope_list.split(' '),'main_produce',companyCode)#'Clue_Entry_Com_Name' main_produce
        pd.DataFrame({'key':key_count[0],'count':key_count[1]}).to_csv('../data/key_count_business.csv',index=False,encoding='utf-8')

        ### save filter and keylist
        searchFilter=[province,city,comname,comname_mustnot.split(' '),geo.split(' '),position,employeeNum,registrationYear,companyCode]
        pd.to_pickle(searchFilter,'../data/searchFilter')

        #### result
        self.render('link_keyword_upload.html',header_text="searched leads by comname only %d  by business only %d"%(total1,total2))


class RecomHandler1(tornado.web.RequestHandler):
    def post(self):
        global searchFilter#filter
        searchFilter=pd.read_pickle('../data/searchFilter')

        num_required=self.get_argument('num_leads')
        num_required=es_util_generatePart.adjust_num_required(int(num_required))

        ### search candidate ->knn ->csv
        cominfo_required=False
        es_filter_clueDB_to_cidProduce.main_tmp('./files/key_count_comname.csv','./files/key_count_business.csv',searchFilter,num_required,cominfo_required)
        generate_fea_mainproduce.main_tmp()
        cleanRate=mainProduce_knn.main_tmp(num_required)


        ## to page with download button
        self.render('download_recom_csv.html',header_text="clean rate:%f"%cleanRate)

class RecomHandler2(tornado.web.RequestHandler):
    def post(self):
        global searchFilter#filter
        searchFilter=pd.read_pickle('../data/searchFilter')

        num_required=self.get_argument('num_leads')
        num_required=es_util_generatePart.adjust_num_required(int(num_required))
        ### search candidate ->knn ->csv
        cominfo_required=True
        es_filter_clueDB_to_cidProduce.main_tmp('./files/key_count_comname.csv','./files/key_count_business.csv',searchFilter,num_required,cominfo_required)

        cleanRate=cominfoVec_knn.main_tmp(int(num_required))


        ## to page with download button
        self.render('download_recom_csv.html',header_text="clean rate:%f"%cleanRate)






class DownloadHandler_business(tornado.web.RequestHandler):
    def post(self):#post(self,filename=xxx)

        filename='../data/key_count_business.csv'
        print('i download file handler ')
        #Content-Type这里我写的时候是固定的了，也可以根据实际情况传值进来
        self.set_header ('Content-Type', 'application/octet-stream')
        self.set_header ('Content-Disposition', 'attachment; filename='+'key_count_business.csv')
        #读取的模式需要根据实际情况进行修改
        with open(filename, 'rb') as f:
            while True:
                data = f.read()
                if not data:
                    break
                self.write(data)
        #记得有finish哦
        self.finish()
class DownloadHandler_comname(tornado.web.RequestHandler):
    def post(self):#post(self,filename=xxx)

        filename='../data/key_count_comname.csv'
        print('i download file handler ')
        #Content-Type这里我写的时候是固定的了，也可以根据实际情况传值进来
        self.set_header ('Content-Type', 'application/octet-stream')
        self.set_header ('Content-Disposition', 'attachment; filename='+'key_count_comname.csv')
        #读取的模式需要根据实际情况进行修改
        with open(filename, 'rb') as f:
            while True:
                data = f.read()
                if not data:
                    break
                self.write(data)
        #记得有finish哦
        self.finish()


class DownloadHandler_clean(tornado.web.RequestHandler):
    def post(self):#post(self,filename=xxx)

        filename='../data/result_attribute_yk_clean.csv'
        print('i download file handler ')
        #Content-Type这里我写的时候是固定的了，也可以根据实际情况传值进来
        self.set_header ('Content-Type', 'application/octet-stream')
        self.set_header ('Content-Disposition', 'attachment; filename='+'result_clean.csv')
        #读取的模式需要根据实际情况进行修改
        with open(filename, 'rb') as f:
            while True:
                data = f.read()
                if not data:
                    break
                self.write(data)
        #记得有finish哦
        self.finish()
class DownloadHandler_unclean(tornado.web.RequestHandler):
    def post(self):#post(self,filename=xxx)

        filename='../data/result_attribute_yk_unclean.csv'
        print('i download file handler ')
        #Content-Type这里我写的时候是固定的了，也可以根据实际情况传值进来
        self.set_header ('Content-Type', 'application/octet-stream')
        self.set_header ('Content-Disposition', 'attachment; filename='+'result_unclean.csv')
        #读取的模式需要根据实际情况进行修改
        with open(filename, 'rb') as f:
            while True:
                data = f.read()
                if not data:
                    break
                self.write(data)
        #记得有finish哦
        self.finish()

class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('index.html') # upload csv+ fill filter added_keywords


class UploadFileHandler_targetCSV(tornado.web.RequestHandler):
    def post(self):
        #print os.path.dirname(__file__)   #/home/yr/virtual_py/upload_csv_exp/code
        upload_path=os.path.join(os.path.dirname(__file__),'files')  #文件的暂存路径
        ## whether has target client csv
        try:
            file_metas=self.request.files['file_3fields']    #提取表单中‘name’为‘file’的文件元数据
            for meta in file_metas:
                filename=meta['filename']
                filename='targetClient.csv'
                filepath=os.path.join(upload_path,filename)
                with open(filepath,'wb') as up:      #有些文件需要已二进制的形式存储，实际中可以更改
                    up.write(meta['body'])
            # analysis
            upload_path=os.path.join(os.path.dirname(__file__),'files')
            fpath=os.path.join(upload_path,'targetClient.csv')
            get_client_feaStr.prepare_keywords(fpath,'utf-8',MAX_WORD_BUSINESS,MAX_NUM_TARGETCLIENT,word_ind_dict_path,word_idf_dict_path,word2vec_model_path,noisePath)
            #
            self.write('finished uploading target client csv & analysis of comname business cominfo')

        except Exception,e:
            file_metas=''
            print Exception,e
            #print 'no target client csv or fail to upload'
            self.write('fail uploading target client csv!')


class UploadFileHandler_key(tornado.web.RequestHandler):
    def post(self):
        #print os.path.dirname(__file__)   #/home/yr/virtual_py/upload_csv_exp/code
        upload_path=os.path.join(os.path.dirname(__file__),'files')  #文件的暂存路径
        ## whether has target client csv
        try:
            file_metas_name=self.request.files['file_comname']    #提取表单中‘name’为‘file’的文件元数据
            file_metas_business=self.request.files['file_businessScope']
            for meta in file_metas_name:
                filename=meta['filename']
                filename='key_count_comname.csv'
                filepath=os.path.join(upload_path,filename)
                with open(filepath,'wb') as up:      #有些文件需要已二进制的形式存储，实际中可以更改
                    up.write(meta['body'])

            for meta in file_metas_business:
                filename=meta['filename']
                filename='key_count_business.csv'
                filepath=os.path.join(upload_path,filename)
                with open(filepath,'wb') as up:      #有些文件需要已二进制的形式存储，实际中可以更改
                    up.write(meta['body'])

            self.write('finished uploading comname business')

        except Exception,e:
            file_metas=''
            print Exception,e
            #print 'no target client csv or fail to upload'
            self.write('fail uploading key csv!')





########## ajax synonym extention
class Ajax_mainHandler(tornado.web.RequestHandler):

    def get(self):
        self.render("ajax.html")
class Ajax_synonym_Handler(tornado.web.RequestHandler):

    def post(self):

        word_tobe_ext=self.get_argument("message").decode('utf-8')
        word_tobe_ext=word_tobe_ext.split(' ')
        ll=[i.strip(' ') for i in word_tobe_ext if len(i.strip(' '))>=2]
        ret_ll=[]
        try:
            for word in ll:
                ret=query_synonym(word,True)
                #print ret  #{word:{synonym:xxxxx,vector:...}
                if len(ret)==0:
                    print 'pid',os.getpid()
                    ret_ll.append(word)
                    ### restart program
                    #tornado.ioloop.IOLoop.instance().stop()

                else:
                    ret_msg=ret.keys()[0]+' '+ret.values()[0]['synonym']
                    ret_ll.append(ret_msg)
        except Exception,e:
            print 'synonym',Exception,e
            #### restart program
            tornado.ioloop.IOLoop.instance().stop()

        self.write('<br>'.join(ret_ll))
            
        

settings = {
        "template_path": os.path.join(os.path.dirname(__file__), "templates"),
        "static_path":os.path.join(os.path.dirname(__file__), 'static')

    }

if __name__ == '__main__':
    tornado.options.parse_command_line()
    app = tornado.web.Application(
        [(r'/index', IndexHandler),
         (r'/upload_targetClient_analyze', UploadFileHandler_targetCSV),

         (r'/filter_nocsv', FilterHandler_notuseTargetCompany),
         (r'/filter_csv', FilterHandler_useTargetCompany),

         (r'/download_comname', DownloadHandler_comname),
         (r'/download_business', DownloadHandler_business),

         (r'/upload_key',UploadFileHandler_key),

         (r'/recom1', RecomHandler1),
         (r'/recom2', RecomHandler2),
         (r'/download_clean', DownloadHandler_clean),
         (r'/download_unclean', DownloadHandler_unclean),

         (r"/synonym_main", Ajax_mainHandler),
         (r"/synonym", Ajax_synonym_Handler)],
        **settings
    )
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()



""" nohup python main_v3.py --port=8700 >>log.log 2>&1"""
