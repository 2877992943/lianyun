#! -*- coding:utf-8 -*-

import subprocess
import wave
import struct
import numpy as np
import csv
import sys,os
import scipy
import pandas as pd

import pylab as pl

def read_wav1(wav_file):
    """Returns two chunks of sound data from wave file."""
    w = wave.open(wav_file)
    n = 60 * 10000
    if w.getnframes() < n * 2:
        raise ValueError('Wave file too short')
    frames = w.readframes(n)
    wav_data1 = struct.unpack('%dh' % n, frames);print type(wav_data1)
    frames = w.readframes(n)
    wav_data2 = struct.unpack('%dh' % n, frames)
    return wav_data1, wav_data2


def read_wave(path):


    #打开wav文件
    #open返回一个的是一个Wave_read类的实例，通过调用它的方法读取WAV文件的格式和数据
    f = wave.open(path,"rb")

    #读取格式信息
    #一次性返回所有的WAV文件的格式信息，它返回的是一个组元(tuple)：声道数, 量化位数（byte单位）, 采
    #样频率, 采样点数, 压缩类型, 压缩类型的描述。wave模块只支持非压缩的数据，因此可以忽略最后两个信息
    params = f.getparams()
    nchannels, sampwidth, framerate, nframes = params[:4]  ;print nchannels, sampwidth, framerate, nframes #1 2 10000 564748

    #读取波形数据
    #读取声音数据，传递一个参数指定需要读取的长度（以取样点为单位）
    str_data  = f.readframes(nframes)
    f.close()

    #将波形数据转换成数组
    #需要根据声道数和量化单位，将读取的二进制数据转换为一个可以计算的数组
    wave_data = np.fromstring(str_data,dtype = np.short) ;print wave_data.shape#[n,]
    #wave_data.shape = -1,2
    wave_data = wave_data.T
    time = np.arange(0,nframes)*(1.0/framerate)  #  sample rate = 10000 times per second
    len_time = len(time)/1
    time = time[0:len_time]

    ##print "time length = ",len(time)
    ##print "wave_data[0] length = ",len(wave_data[0])

    #绘制波形

    #pl.subplot(211)
    pl.plot(time,wave_data)
    # pl.subplot(212)
    # pl.plot(time, wave_data[1],c="r")
    pl.xlabel("time")
    pl.ylim(-30000,30000)
    pl.show()
    return wave_data


def compute_chunk_features(mp3_file,number):
    """Return feature vectors for two chunks of an MP3 file."""
    # Extract MP3 file to a mono, 10kHz WAV file
    mpg123_command = 'mpg123 -w "%s" -r 10000 -m "%s"'
    out_file = '../wave_data/temp_%d.wav'%number
    cmd = mpg123_command % (out_file, mp3_file);print cmd
    temp = subprocess.call(cmd,stdin=None,stdout=None,stderr=None,shell=True)



    return 0,1


def fft_show(wavedata):
    t=wavedata.shape[0]/10000
    f = np.fft.fft(wavedata)
    f = f[2:(f.size / 2 + 1)]
    f_abs = abs(f);print f_abs.shape
    total_power = f_abs.sum()
    f_ret= np.array_split(f_abs, 10)
    f_ret=[e.sum() / total_power for e in f_ret]
    ## f  f_abs  f_ret
    pl.figure()
    pl.subplot(411);pl.title(' frequency domain: half fft');
    pl.plot(f);
    pl.subplot(412);pl.title('abs(half fft)');pl.ylim(0,1e8)
    pl.plot(f_abs)
    pl.subplot(413);pl.title('abs(half fft) split 10 power')
    pl.plot(f_ret)
    pl.subplot(414)
    pl.plot(wavedata);pl.title('time domain')








if __name__=='__main__':


    """
    #### mp3 -> wave
    for path, dirs, files in os.walk('../data/'):
        i=0
        #for f in files[i:i+1]:
        for f in files:
            if not f.endswith('.mp3'):
                # Skip any non-MP3 files
                continue
            mp3_file = os.path.join(path, f);print mp3_file
            # Extract the track name (i.e. the file name) plus the names
            # of the two preceding directories. This will be useful
            # later for plotting.
            tail, track = os.path.split(mp3_file)
            tail, dir1 = os.path.split(tail)
            tail, dir2 = os.path.split(tail)
            # Compute features. feature_vec1 and feature_vec2 are lists of floating
            # point numbers representing the statistical features we have extracted
            # from the raw sound data.
            try:
                feature_vec1, feature_vec2 = compute_chunk_features(mp3_file,i)

                out_file = '../wave_data/temp_%d.wav'%i
                wave_data=read_wave(out_file)
                i+=1


            except:
                continue
    """





    ###  get x  + -

    path='../wave_data/'
    i=7
    for f in os.listdir(path)[i:i+1]:
        if not f.endswith('.wav'):continue
        ### show x, manually label noise and speak
        x=read_wave(path+f)
        pd.to_pickle(x,'../x')



        # x=pd.read_pickle('../x')
        # print x.shape
        # x1=x[70000:150000];pd.to_pickle(x1,'../xfile/noise_%d'%i)
        # x2=x[170000:];pd.to_pickle(x2,'../xfile/speak_%d'%i)
        # fft_show(x1)
        # fft_show(x2)



        pl.show()


































 


















































