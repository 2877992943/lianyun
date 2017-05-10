#-*- coding:gbk -*-

import hashlib
import time, re
import urllib2
from scrapy import Selector
from scrapy.http import HtmlResponse
from faker import Factory
from pprint import pprint
from random import choice
import json
import codecs
from splinter import Browser
f=Factory.create()
def create_authHeader():
	appkey=53000891
        secret="d6f911c7e5398d251e2a290e7890f092"
        paramMap = {"app_key": appkey,"timestamp": time.strftime("%Y-%m-%d %H:%M:%S")}
        keys = paramMap.keys()
        keys.sort()
        
        codes= "%s%s%s" % (secret,str().join('%s%s' % (key, paramMap[key]) for key in keys),secret)
        
        sign = hashlib.md5(codes).hexdigest().upper()
        paramMap["sign"] = sign
        keys = paramMap.keys()
        authHeader = "MYH-AUTH-MD5 " + str('&').join('%s=%s' % (key, paramMap[key]) for key in keys)
        print authHeader
	return authHeader

def search_comlist(hurl,b,filte_set):
	authHeader=create_authHeader()
	#b.set_proxy('http://101.200.81.102:8123')
	#b.headers.append(('Proxy-Authorization', authHeader))
	b.visit(hurl)
	result_list=[]
	total=1
	#if b.status_code.is_success():
	n=1
	total_list=[]
	while len(total_list) != 3 and n <6:
		time.sleep(2)
		string_utf8=b.html.encode("utf-8")
		renderedbody=str(string_utf8)
		scrapy_response=HtmlResponse(b.url, body=renderedbody)
		sel=Selector(scrapy_response)
		result_sel=sel.xpath('//div[@class="search_result_single ng-scope"]')
		for x in result_sel:
			xhref=x.xpath('div/div/a/@href').extract()
			#xname=x.xpath('div/div/a/span').re(ur"[\u2E80-\u9fff（）]")
			#xname="".join(xname).replace(ur"（）","")
			#xname=filte_name(xname)
			if xhref :
				#print xhref
				f=hashlib.md5(xhref[0]).hexdigest()+".txt"
				if f not in filte_set:
					print xhref
					result_list.append(xhref[0])
		total_list=sel.xpath('//div[@class="total ng-binding"]//text()').extract()
		print total_list
		total=int(total_list[1]) if len(total_list)==3 else 0
		n=n+1
	return result_list, total

def filte_name(name):
	name= name[:-1] if name and name[-1]==ur"（" else name
	if name:
		while name[-1]==ur"）":
			rlist=re.findall(ur"（[^（）]*）",name)
			name=name.replace(rlist[-1],"") if rlist else name.replace(u"）",")")
		name= name[:-1] if name and name[-1]==ur"（" else name
		return name
	else:
		return ""

def get_cominfo(com_url,b,filte_set):
	#authHeader=create_authHeader()
	print com_url
	#headers=[('Accept','text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'),
	#		('Accept-Encoding', 'gzip, deflate, sdch'),
	#		('Accept-Language', 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3'),
	#		('Connection', 'keep-alive'),
	#		('Host', 'www.tianyancha.com'),
	#		('User-Agent', f.user_agent())
	#		]   
	#for x in headers:
	#	b.headers.append(x)
	#b.set_proxy('http://101.200.81.102:8123')
	#b.headers.append(('Proxy-Authorization', authHeader))
	#b.set_proxy('http://219.148.148.180:8080')
	#user_agent=f.user_agent()
	#b.headers.append(("Referer",hurl.encode("gbk")))
	#b.create_webview()
	b.visit(com_url)
	data_dict={}
	jyfw=""
	n=1
	#if b.status_code.is_success():
	while not (jyfw) and n<7:
		time.sleep(1)
		#xx=b.webframe.toHtml().toUtf8()
		#print type(xx)
		#string_utf8=xx#b.html.encode("utf-8")
		#print string_utf8#.encode("gbk")
		#print type(string_utf8)
		string_utf8=b.html.encode("utf-8")
		renderedbody=str(string_utf8)
		scrapy_response=HtmlResponse(b.url, body=renderedbody)
		try:
		#if True:
			comsel=Selector(scrapy_response)
			################################################################################
			comhead=comsel.xpath('//div[@class="company_info_text"]')

			#name=comhead.xpath('p/text()').extract()[0].strip()         #公司名称
			name=comhead.xpath('p/text()').re(ur"[\u2E80-\u9fff（）()]")#[0].strip()         #公司名称
			name="".join(name).replace("(",u"（").replace(u")",u"）").replace(ur"（）","")
			name=filte_name(name)
			name=name.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			name=name+u"司" if name and name[-1]==u"公" else name
			data_dict["name"]=name.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			new_name=comhead.xpath('p/span[@ng-if="newNameObj"]/a/text()').extract()#[-1].strip()         #公司新名称
			new_name=new_name[-1].strip() if new_name else ""
			data_dict["new_name"]=new_name.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			old_name=comhead.xpath('p/span[@ng-if="company.baseInfo.historyNames"]/a/text()').extract()#[-1].strip()         #公司新名称
			old_name=old_name[-1].strip() if old_name else ""
			data_dict["old_name"]=old_name.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			phone=comhead.xpath('span[1]/text()').extract()#
			phone=phone[-1].strip() if phone else "" #电话
			data_dict["phone"]=phone
			email=comhead.xpath('span[2]/text()').extract()#[-1].strip()  #邮箱
			email=email[-1].strip() if email else ""
			data_dict["email"]=email
			gszy=comhead.xpath('span[3]/text()').extract()#[-1].strip()   #公司主页
			gszy=gszy[-1].strip() if gszy else ""
			data_dict["gszy"]=gszy
			gsdz=comhead.xpath('span[4]/text()').extract()#[-1].strip()   #公司地址
			gsdz=gsdz[-1].strip() if gsdz else ""
			data_dict["gsdz"]=gsdz.replace(u'\xa0',u"").replace(u"\ue6c3",u"")

			##############################################################################
			com1=comsel.xpath('//div[@class="row b-c-white company-content"]/table[1]')

			fddb=com1.xpath('tbody/tr[2]/td[1]/p/a/text()').extract()#[-1].strip()  #法定代表人
			fddb=fddb[-1].strip() if fddb else ""
			data_dict["fddb"]=fddb.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			zczb=com1.xpath('tbody/tr[2]/td[2]/p/text()').extract()#[-1].strip()    #注册资本
			zczb=zczb[-1].strip() if zczb else ""
			data_dict["zczb"]=zczb.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			jyzt=com1.xpath('tbody/tr[4]/td[1]/p/text()').extract()#[-1].strip()    #经营状态
			jyzt=jyzt[-1].strip() if jyzt else ""
			data_dict["jyzt"]=jyzt.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			clrq=com1.xpath('tbody/tr[4]/td[2]/p/text()').extract()#[-1].strip()    #注册时间
			clrq=clrq[-1].strip() if clrq else ""
			data_dict["clrq"]=clrq.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			
			###############################################################################
			com2=comsel.xpath('//div[@class="row b-c-white company-content"]/table[2]')

			sshy=com2.xpath('tbody/tr[1]/td[1]/div/span/text()').extract()[0]  #所属行业
			data_dict["sshy"]=sshy.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			zch=com2.xpath('tbody/tr[1]/td[2]/div/span/text()').extract()[0]   #注册号
			data_dict["zch"]=zch.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			gslx=com2.xpath('tbody/tr[2]/td[1]/div/span/text()').extract()[0]  #公司类型
			data_dict["gslx"]=gslx.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			zzjgdm=com2.xpath('tbody/tr[2]/td[2]/div/span/text()').extract()[0] #组织机构代码
			data_dict["zzjgdm"]=zzjgdm.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			yyqx=com2.xpath('tbody/tr[3]/td[1]/div/span/text()').extract()[0]  #营业期限
			data_dict["yyqx"]=yyqx.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			djjg=com2.xpath('tbody/tr[3]/td[2]/div/span/text()').extract()[0]  #登记机关
			data_dict["djjg"]=djjg.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			fzrq=com2.xpath('tbody/tr[4]/td[1]/div/span/text()').extract()[0]  #发照日期 或  核准日期
			data_dict["fzrq"]=fzrq.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			xydm=com2.xpath('tbody/tr[4]/td[2]/div/span/text()').extract()[0]  #信用代码
			data_dict["xydm"]=xydm.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			gsdz=com2.xpath('tbody/tr[5]/td/div/span/text()').extract()#[0]     #公司地址
			gsdz=gsdz[0] if gsdz else ""
			data_dict["gsdz"]=gsdz.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			jyfw=com2.xpath('tbody/tr[6]/td/div/span/text()').extract()#[0]     #经营范围
			jyfw=jyfw[0] if jyfw else ""
			data_dict["jyfw"]=jyfw.replace(u'\xa0',u"").replace(u"\ue6c3",u"")
			ff=hashlib.md5(com_url).hexdigest()+".txt"
			#json.dump({"companyInfo":[data_dict]},codecs.open("G:/tianyancha/"+hashlib.md5(com_url).hexdigest()+".txt","w",encoding="utf-8"))
			if name:
				json.dump({"companyInfo":[data_dict]},codecs.open("../data/"+ff,"w",encoding="utf-8"))
			filte_set.add(ff)
			for k, v in data_dict.iteritems():
				print k.encode("gb18030"), v.encode("gb18030")
			n=n+1
		except:
			n=n+1
	else:
		pass

def get_proxy_list(Purl):
	proxy_list=[]
	raw_data=urllib2.urlopen(Purl).read()
	response=HtmlResponse(Purl, body=raw_data)
	sel=Selector(response)
	sel_table=sel.xpath('//table[@class="table table-bordered table-striped"]/tbody/tr')
	for sel_tr in sel_table:
		ip=sel_tr.xpath('td[@data-title="IP"]/text()').extract()[0]
		port=sel_tr.xpath('td[@data-title="PORT"]/text()').extract()[0]
		proxy_list.append('http://%s:%s'%(ip,port))
	return proxy_list

def filte_name_set(path):
	name_set=set(os.listdir(path))
	#for x in os.listdir(path):
	#	f=os.path.join(path,x)
	#	data=json.load(codecs.open(f,"r",encoding="utf-8"))
	#	name=data["companyInfo"][0]["name"]
	#	name_set.add(name)
	#	#print name.encode("gb18030")
	return name_set
	



		

#com_url='http://www.tianyancha.com/company/10069025'
#cheng_url='http://www.tianyancha.com/company/2350588402'
#new_url='http://www.tianyancha.com/company/94815257'
#gaoni_Purl="http://www.kuaidaili.com/free/inha/"
#putong_Purl="http://www.kuaidaili.com/free/intr/"
##############################################################
from datetime import datetime
import os
file_in="../data/comname.txt"
file_out="../data/tianyancha_searched_20160919"
f_in = open(file_in,"r")
n=0
if os.path.isfile(file_out):
	os.remove(file_out)
#b=Browser("phantomjs")
filte_set=filte_name_set("G:/tianyancha")
for l_raw in f_in:
	#filte_set=set()
	print len(filte_set)
	b=Browser("phantomjs")
	f_out= open(file_out,"a")
	f_out.write(l_raw)
	f_out.close()
	com=l_raw.decode("utf-8").strip().replace(u"有限","").replace(u"公司","").replace(u"责任","")
	com=com.split("@@@")[0]
	print com.encode("gb18030")
	hurl="http://www.tianyancha.com/search?key=%s&checkFrom=searchBox" % urllib2.quote(com.encode("utf-8"))
	result_list, total=search_comlist(hurl,b,filte_set)
	if total==0:
		break
	for com_url in result_list:
		#try:
			#time.sleep(choice(range(3)))
			companyInfo=get_cominfo(com_url,b,filte_set)
		#except:
			pass
		#print com_url
	#if result_list and total >1:
	#if total >1:
	#	for i in range(2,total+1):
	#		print i,total
	#		hurl="http://www.tianyancha.com/search/p%d?key=%s" % (i,com)
	#		#try:
	#		result_list, t=search_comlist(hurl,b,filte_set)
	#		for com_url in result_list:
	#			#try:
	#			#time.sleep(choice(range(3)))
	#			companyInfo=get_cominfo(com_url,b,filte_set)
	#		#except:
	#		#	pass

	#n=n+1
	#if n<20:
	#	f_out.write(l_raw)
	#else:
	b.quit()#	break
	
#com="经纬纺机".decode("gbk")
#hurl="http://www.tianyancha.com/search?key=%s&checkFrom=searchBox" % com
#print hurl
#result_list=search_comlist(hurl,b)
#b=Browser("phantomjs")
#com_url='http://www.tianyancha.com/company/2349353247'
#get_cominfo(com_url,b)
##print com, type(com)
##for k, v in companyInfo.iteritems():
##	print k, v
#b.quit()
