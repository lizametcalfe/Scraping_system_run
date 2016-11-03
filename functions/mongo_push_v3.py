# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 17:26:49 2016

@author: mint
"""

import csv
import json
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient
#CSV to JSON Conversion
def get_db():
    from pymongo import MongoClient
    try:
        conn = MongoClient('mongodb://192.168.2.20:27017')
        # Connect to DB "mydbname"
        #db = conn.wsp2        ###my database
        db = conn.prices_db
        return db    
    except pymongo.errors.ConnectionFailure, e:
        
        print "Could not connect to MongoDB: %s" % e 
        raise e
# Get the connection to our source database
#mydb = get_db()


mydb = get_db()
#df = pd.DataFrame(list(mydb.combined.find({"ons_item_no": { "$in": [210111]}})))
date1 = "2016-09-08"
{"$gt":date1}
df = pd.DataFrame(list(mydb.combined.find({"timestamp":  {"$gt":date1}})))


#%%
#mydb = get_db() not sure if this needs to be outside the function

def from_mongo(item):
   mydb = get_db()
   df = pd.DataFrame(list(mydb.combined.find({"ons_item_no": { "$in": [item]}})))
   df = pd.DataFrame(list(mydb.combined.find({"ons_item_no": { "$in": [item]}})))
   item = str(item)
   return df

def variable_trim(df):
    df = df[['timestamp',
             'ons_item_name',
             'ons_item_no',
             'product_name',
             'store',
             'offer',
             'ml_prediction',
             'month',
             'monthday',
             'std_price',
             'unit_price',
             'item_price_num'
             ]]
    return df

def export_items(item_list):
    #global df_main
    df_main = []
    global todays_date
    todays_date = str(datetime.date.today())
    for item in item_list:
        df = from_mongo(item)
        df = variable_trim(df)
        print (len(df))
        item = str(item)
        #df.to_csv('/home/mint/my-data/WebScrapingPhase2/Data/test/'+ item + '_main.csv' ,encoding = "utf-8") 
        df_main.append(df)#
    global df_stacked
    df_stacked = pd.concat([r for r in df_main], ignore_index=True)
    print("Total number of items collected from this group: %s " % (len(df_stacked)))  
    df_time = resolve_time(df_stacked) ##### <- function imported from timeformat_v2.py  ## MUTE THESE 2 LINES WHEN IMPORTING ALL DATA
    #df_stacked = df_time[df_time["monthday"] > 20160801]                                 ## ""                                       ""  

    print("Total number of items collected from this group after trim: %s " % (len(df_stacked)))    
    df_stacked.to_csv('/home/mint/my-data/WebScrapingPhase2/Data/test/'+ todays_date + '_main_'+ str(item_list[2])[2] +'.csv',encoding = "utf-8")#

def merge_export_all(): #item_list_1,item_list_2,item_list_3
    item_list_1 = [210111, 210113, 210204]#, 210213, 210214,210302, 211305, 211501, 211709, 211710, 211807, 211814] ##still something fishy going on here - these items aren't gett
    #item_list_2 = [211901, 212006, 212011, 212016, 212017, 212319, 212360, 212515, 212519, 212717, 212719, 212720]
    #item_list_3 = [212722, 310207,310215, 310218, 310401, 310403, 310405, 310419, 310421, 310427]
    
    start_time = time.time()
    export_items(item_list_1)
    #export_items(item_list_2)
    #export_items(item_list_3)
    #df1=pd.read_csv('/home/mint/my-data/WebScrapingPhase2/Data/test/'+ todays_date + '_main_'+ str(item_list_1[2])[2] +'.csv',encoding = "utf-8")#
    #df2=pd.read_csv('/home/mint/my-data/WebScrapingPhase2/Data/test/'+ todays_date + '_main_'+ str(item_list_2[2])[2] +'.csv',encoding = "utf-8")#
    #df3=pd.read_csv('/home/mint/my-data/WebScrapingPhase2/Data/test/'+ todays_date + '_main_'+ str(item_list_3[2])[2] +'.csv',encoding = "utf-8")#
    #df_all = pd.concat([df1,df2,df3],axis=0)
    #df_all.to_csv('/home/mint/my-data/WebScrapingPhase2/Data/test/'+ todays_date + '_main_.csv',encoding = "utf-8")#
    print("Time taken to pull down: --- %s seconds ---" % (time.time() - start_time))

merge_export_all()   

'''   
import schedule 
import time

schedule.every().day.at("17:09").do(merge_export_all)
while 1:
    schedule.run_pending()
    schedule.sleep(1)
'''

#%%

csvfile = pd.read_csv('/home/mint/my-data/WebScrapingPhase2/Data/august26_imputed_all.csv' ,encoding = "utf-8", index_col = False )#


#%%


df1 = pd.read_csv('/home/mint/my-data/WebScrapingPhase2/Data/august26_imputed_all.csv' ,encoding = "latin_1", index_col = False )
df2 = pd.read_csv('/home/mint/my-data/WebScrapingPhase2/Data/_imputed_all.csv' ,encoding = "latin_1", index_col = False )#
del df1['Unnamed: 0.1']
df_all = pd.concat([df1,df2],axis=0)
df_all.tail()
df_all.to_csv('/home/mint/my-data/WebScrapingPhase2/Data/complete_16092016.csv' ,encoding = "latin_1", index_col = False )#

#%%
cc = csvfile.sort(columns="monthday")

#%%

df_time.to_csv('/home/mint/my-data/WebScrapingPhase2/Data/complete_1718092016.csv' ,encoding = "utf-8", index_col = False )#





















