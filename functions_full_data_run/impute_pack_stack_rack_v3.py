# -*- coding: latin-1 -*-

"""
Created on Thu Aug 25 13:38:30 2016

@author: smitht2

"""

import pandas as pd 
import time 
import datetime
#%%

def impute():
    items=[210111,210113,210204,210213,210214,210302,211305,211501,211709,211710,
       211807,211814,211901,212006,212011,212016,212017,212319,212360,212515,
       212519,212717,212719,212720,212722,310207,310215,310218,310401,310403,
       310405,310419,310421,310427]

    

    for i in items:
        path='/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/cleaned/missing_aug_'+str(i)+'.csv'
        print(path)
        df=pd.read_csv(path,encoding="latin_1")
        df["impmkr"]=[0
        if pd.notnull(df.loc[x,'std_price'])==True
        else 1

        for x in range(len(df))]

        b=df.fillna(method="ffill",limit=7)
        c=b[pd.notnull(b['std_price'])==True]
        path2='/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/cleaned/imputed_aug_'+str(i)+'.csv'
        print(path2)
        c.to_csv(path2,encoding="latin_1")
   
#%%


def format_month(time):
    time = str(time)
    time = datetime.datetime.strptime(time[0:10], '%Y-%m-%d').strftime('%Y%m%d')
    return time
    


def stack():

    items=[210111,210113,210204,210213,210214,210302,211305,211501,211709,211710,
       211807,211814,211901,212006,212011,212016,212017,212319,212360,212515,
       212519,212717,212719,212720,212722,310207,310215,310218,310401,310403,
       310405,310419,310421,310427]

    myBigDf = []
    for i in items:
        try:
            path='/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/cleaned/imputed_aug_'+str(i)+'.csv'
            print(path)
            df=pd.read_csv(path,encoding="latin_1")
            df = df[["Unnamed: 0","product_name","monthday","ons_item_no","store","offer","ons_item_name",
                     "std_price",	"timestamp", "month","week","unit_price","ML_score","item_price_num"]]#monthday.1	trained_cluster2	check2	QA		prodno	yy	prodname	startdate	enddate	pn2	impmkr

            myBigDf.append(df) #, ignore_index=True)
        except:
            print ("Item number %s doesn't appear to exist.....bummer" % (i))
            pass #for items no longe

    global df_stacked
    df_stacked = pd.concat([r for r in myBigDf], ignore_index=True)
    
    todays_date = str(datetime.date.today())
    today = int(format_month(todays_date))
    print (today)
    
    df_stacked = df_stacked[df_stacked["monthday"] == today]

    path2='/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/_imputed_all.csv'
    df_stacked.to_csv(path2,encoding="latin_1",index = False)

def pack_stack_rack():
    start_time = time.time()
    impute()
    stack()
    print("Time taken to impute/stack/export: --- %s seconds ---" % (time.time() - start_time))


 
pack_stack_rack()
