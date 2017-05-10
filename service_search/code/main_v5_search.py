#! -*- coding:utf-8 -*-




import os.path
import os
import pandas as pd

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import es_filter_clueDB_to_cidProduce,mainProduce_knn,read_csv,es_util_generatePart,menu_to_companyName_product_list

import time
import numpy


import tornado.httpclient

import json
import datetime

from keyword_ext import query_synonym


from tornado.options import define, options



global searchFilter



define("port", default=8700, help="run on the given port", type=int)

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


        companyCode=self.get_argument('companyCode',strip=True)
        num_required=self.get_argument('num_leads')
        num_required=es_util_generatePart.adjust_num_required(int(num_required))

        ###


        ### save filter and keylist
        searchFilter=[province,city,comname,comname_mustnot.split(' '),geo.split(' '),position,employeeNum,registrationYear,companyCode]
        pd.to_pickle(searchFilter,'../data/searchFilter')

        #### search result
        es_filter_clueDB_to_cidProduce.main_tmp(searchFilter,num_required,False)
        cleanRate=mainProduce_knn.main_tmp(num_required)

        #### result
        self.render('download_recom_csv.html',header_text="clean rate:%f"%cleanRate)
















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



        

settings = {
        "template_path": os.path.join(os.path.dirname(__file__), "templates"),
        "static_path":os.path.join(os.path.dirname(__file__), 'static')

    }

if __name__ == '__main__':
    tornado.options.parse_command_line()
    app = tornado.web.Application(
        [(r'/index', IndexHandler),


         (r'/filter_csv', FilterHandler_useTargetCompany),




         (r'/download_clean', DownloadHandler_clean),
         (r'/download_unclean', DownloadHandler_unclean)],
        **settings
    )
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()



""" nohup python main_v3.py --port=8700 >>log.log 2>&1"""
