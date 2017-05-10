#! -*- coding:utf-8 -*-

import pandas as pd
from wordcloud import WordCloud
import matplotlib.pyplot as plt




if __name__ == "__main__":
    df=pd.read_csv('../data/fea_xgb.csv')
    name=df.name.values
    weight=df.weight.values

    fullStr=''
    for i in range(len(name)):
        freq=int(weight[i]*10000)
        name_i=name[i]#.split('_')[-1]
        stri=[name_i]*freq
        stri=' '.join(stri)
        stri=stri+' '
        ##
        fullStr+=stri


    ### wordcloud
    #wordcloud = WordCloud().generate(fullStr)
    # plt.imshow(wordcloud)
    # plt.axis("off")


    wordcloud = WordCloud(max_font_size=40, relative_scaling=.5).generate(fullStr)
    plt.figure()
    plt.imshow(wordcloud)
    plt.axis("off")
    plt.show()






















































