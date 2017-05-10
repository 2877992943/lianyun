#!/usr/bin/env python
#coding: utf-8  
import pandas as pd

"""
http://www.cnblogs.com/leetao94/p/5460520.html
"""


import smtplib
from email.mime.text import MIMEText
from email.header import Header



import email.MIMEMultipart
import email.MIMEText
import email.MIMEBase
import os.path


def SendEmail(fromAdd, toAdd, subject, attachfile, htmlText):
    strFrom = fromAdd;
    strTo = toAdd;
    msg =MIMEText(htmlText);
    msg['Content-Type'] = 'Text/HTML';
    msg['Subject'] = Header(subject,'gb2312');
    msg['To'] = strTo;
    msg['From'] = strFrom;

    smtp = smtplib.SMTP('smtp.exmail.qq.com');
    smtp.login('yangrui@yunkecn.com','yr13371695096YR');
    try:
        smtp.sendmail(strFrom,strTo,msg.as_string());
    finally:
        smtp.close;


def send_with_attachment(From,To,filename,num_leads,csvName):
    email_csv_name='temp_'+csvName+'.csv';#print '1',From,To,filename,num_leads,csvName
    email_csv_name=email_csv_name.decode('utf-8')

    if To.find('@yunkecn.com')==-1:
        To='yangrui@yunkecn.com'


    server = smtplib.SMTP('smtp.exmail.qq.com')
    server.login('yangrui@yunkecn.com','yr13371695096YR')

    # 构造MIMEMultipart对象做为根容器
    main_msg = email.MIMEMultipart.MIMEMultipart()

    # 构造MIMEText对象做为邮件显示内容并附加到根容器
    text_msg = email.MIMEText.MIMEText("find the attachment please.")
    main_msg.attach(text_msg)

    # 构造MIMEBase对象做为文件附件内容并附加到根容器
    contype = 'application/octet-stream'
    maintype, subtype = contype.split('/', 1)

    ## 读入文件内容并格式化
    ## select num_leads
    df=pd.read_csv(filename,encoding='utf-8');#print df.shape
    if num_leads<df.shape[0]:
        df=df[:num_leads];#print df.shape[0]
        df.to_csv(filename,index=False,encoding='utf-8')
    ### if required num leads > df.shape[0],use df directly
     
        
    
    
    data = open(filename, 'rb')
    file_msg = email.MIMEBase.MIMEBase(maintype, subtype)
    file_msg.set_payload(data.read( ))
    data.close( )
    email.Encoders.encode_base64(file_msg)

    ## 设置附件头
    basename = os.path.basename(filename);print basename,filename,email_csv_name
    file_msg.add_header('Content-Disposition',
     'attachment', filename = filename)
    main_msg.attach(file_msg)

    # 设置根容器属性
    main_msg['From'] = From
    main_msg['To'] = To
    main_msg['Subject'] = "attachment :%s email to user:%s"%(csvName,To)
    main_msg['Date'] = email.Utils.formatdate( )

    # 得到格式化后的完整文本
    fullText = main_msg.as_string( )

    # 用smtp发送邮件
    try:
        server.sendmail(From, To, fullText)
    finally:
        server.quit()


if __name__ == "__main__":

    send_with_attachment('yangrui@yunkecn.com','yangrui@yunkecn.com','tmp.csv',5,'宁波_1999')# '宁波_1999' in csvName as csvname will not work



