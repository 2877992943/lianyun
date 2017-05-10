#! -*- coding:utf-8 -*-


import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.httpclient

import urllib
import json
import datetime
import time

from tornado.options import define, options
define("port", default=8000, help="run on the given port", type=int)

class searchHandler_get(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        #query = self.get_argument('q')
        client = tornado.httpclient.AsyncHTTPClient()
        url="https://www.baidu.com/"
        body=urllib.urlencode({"q": '', "result_type": "recent", "rpp": 100})
        client.fetch(url,callback=self.on_response)

    def on_response(self, response):
        print 'response',response,response.body
        #body = json.loads(response.body)
        self.finish()


class searchHandler_post(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        #query = self.get_argument('q')
        client = tornado.httpclient.AsyncHTTPClient()
        url="http://123.57.62.29:9200/clue_*filtered/clue/_search?scroll=1m"
        body={"from":0,"size":200,"query":{"match_phrase":{"Clue_Entry_Com_Name":"北京"}}}
        #body=urllib.urlencode(body)
        #body=str(body)
        body=json.dumps(body);print 'body',body
        client.fetch(url,method='POST',callback=self.on_response,body=body)

    def on_response(self, response):
        #print 'response',response
        #print response.body
        body = json.loads(response.body)
        print body['hits']['total']
        print body['hits']['hits'].__len__()
        self.finish()


class searchHandler_post_qdw(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        #query = self.get_argument('q')
        client = tornado.httpclient.AsyncHTTPClient()
        url='http://101.200.139.60:8088/crawler/QiDuoWeiSpider?companyName=%s'%(comname)
        body={"from":0,"size":200,"query":{"match_phrase":{"Clue_Entry_Com_Name":"北京"}}}
        #body=urllib.urlencode(body)
        #body=str(body)
        body=json.dumps(body);print 'body',body
        client.fetch(url,method='POST',callback=self.on_response,body=body)

    def on_response(self, response):
        #print 'response',response
        #print response.body
        body = json.loads(response.body)
        print body['hits']['total']
        print body['hits']['hits'].__len__()
        self.finish()




if __name__ == "__main__":
    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers=[(r"/", searchHandler_post)])
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()



"""
class index(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        #query = self.get_argument('q')
        body={"query":{"match_phrase":{"Clue_Entry_Com_Name":"北京东方凯和文化发展有限公司"}}}
        body=urllib.urlencode(body)
        client = tornado.httpclient.AsyncHTTPClient()

        urli="http://123.57.62.29:9200/clue_filtered/clue/_search?"
        #print urli+body
        client.fetch(urli,method='POST',callback=self.on_response,body=body)
        #client.fetch(urli+body,callback=self.on_response)

    def on_response(self,response):
        print 'response',response
        body = json.loads(response.body)
        print body
        result_count = len(body['hits']['total'])
        self.write(result_count)
        self.finish()

if __name__ == "__main__":
    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers=[(r"/", index)])
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
"""

# def on_response(response):
#     print 'response',response
#     body = json.loads(response.body)
#     print body
#     result_count = len(body['hits']['total'])
#
#
#
# body={"query":{"match_phrase":{"Clue_Entry_Com_Name":"北京东方凯和文化发展有限公司"}}}
# body=urllib.urlencode(body)
# url = "http://123.57.62.29:9200/clue_filtered/clue/_search?"
# request = tornado.httpclient.HTTPRequest(url = url, method = 'POST', body = body)
# client = tornado.httpclient.AsyncHTTPClient()
# client.fetch(request, callback = on_response)
# tornado.ioloop.IOLoop.instance().start()


# def handle_response(response):
#     print response
#     if response.error:
#         print "Error:", response.error
#     else:
#         print response.body
#
#
# url = "http://123.57.62.29:9200/clue_filtered/clue/_search?"
# body={"query":{"match_phrase":{"Clue_Entry_Com_Name":"北京东方凯和文化发展有限公司"}}}
# body=urllib.urlencode(body)
#
# http_client = tornado.httpclient.AsyncHTTPClient()
# http_client.fetch(url, handle_response,body=body,method='POST')







