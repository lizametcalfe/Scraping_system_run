# -*- coding: utf-8 -*-
"""
Created on Thu Jul 21 09:58:41 2016

@author: Mint

"""

#%%
import schedule 
import csv
import json
import datetime
import time
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient



### POTATOES NOT USED  ####
#potato_list = [212361, 212399]
            

#%%


def get_db():
    from pymongo import MongoClient
    try:
        conn = MongoClient('mongodb://192.168.2.20:27017')
        # Connect to DB "mydbname"
        #db = conn.wsp2        ###my database
        db = conn.test
        return db    
    except pymongo.errors.ConnectionFailure, e:
        
        print "Could not connect to MongoDB: %s" % e 
        raise e
# Get the connection to our source database
#mydb = get_db()



#mydb = get_db() not sure if this needs to be outside the function

def from_mongo(item):
   mydb = get_db()
   df = pd.DataFrame(list(mydb.test_data3.find({"ons_item_no": { "$in": [item]}})))
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
    start_time = time.time() 
    df_main = []
    global todays_date
    todays_date = str(datetime.date.today())
    for item in item_list:
        df = from_mongo(item)
        #df = variable_trim(df)
        print (len(df))
        item = str(item)
        #df.to_csv('/home/mint/my-data/WebScrapingPhase2/Data/test/'+ item + '_main.csv' ,encoding = "utf-8") 
        df_main.append(df)#
    global df_stacked
    df_stacked = pd.concat([r for r in df_main], ignore_index=True)
    print("Total number of items collected from this group: %s " % (len(df_stacked)))  
    #df_time = resolve_time(df_stacked) ##### <- function imported from timeformat_v2.py  ## MUTE THESE 2 LINES WHEN IMPORTING ALL DATA
    #df_stacked = df_time[df_time["monthday"] > 20160801]                                 ## ""                                       ""  

    print("Total number of items collected from this group after trim: %s " % (len(df_stacked)))    
    df_stacked.to_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions_full_data_run/data/_main_'+ str(item_list[2])[2] +'.csv',encoding = "utf-8")#

def export_all(): #item_list_1,item_list_2,item_list_3
    item_list_1 = [210111, 210113, 210204, 210213, 210214,210302, 211305, 211501, 211709, 211710, 211807, 211814] ##still something fishy going on here - these items aren't gett
    item_list_2 = [211901, 212006, 212011, 212016, 212017, 212319, 212360, 212515, 212519, 212717, 212719, 212720]
    item_list_3 = [212722, 310207,310215, 310218, 310401, 310403, 310405, 310419, 310421, 310427]
    
    start_time = time.time()
    export_items(item_list_1)
    export_items(item_list_2)
    export_items(item_list_3)
    
    
def merge_all():
    item_list_1 = [210111, 210113, 210204, 210213, 210214,210302, 211305, 211501, 211709, 211710, 211807, 211814] ##still something fishy going on here - these items aren't gett
    item_list_2 = [211901, 212006, 212011, 212016, 212017, 212319, 212360, 212515, 212519, 212717, 212719, 212720]
    item_list_3 = [212722, 310207,310215, 310218, 310401, 310403, 310405, 310419, 310421, 310427]
    
    df1=pd.read_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions_full_data_run/data/_main_'+ str(item_list_1[2])[2] +'.csv',encoding = "utf-8")#
    df2=pd.read_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions_full_data_run/data/_main_'+ str(item_list_2[2])[2] +'.csv',encoding = "utf-8")#
    df3=pd.read_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions_full_data_run/data/_main_'+ str(item_list_3[2])[2] +'.csv',encoding = "utf-8")#
    df_all = pd.concat([df1,df2,df3],axis=0)
    df_all.to_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions_full_data_run/data/_main_.csv',encoding = "utf-8", index = False)#
    print("Time taken to pull down: --- %s seconds ---" % (time.time()))



def merge_export_all():
    export_all()
    merge_all()


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
#df = pd.read_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/WebScrapingSystem/_main_.csv',encoding = "utf-8")

#####

#df = pd.read_csv('/home/mint/my-data/WebScrapingPhase2/Data/test/'+ todays_date + '_main_.csv')
#df_time = resolve_time(df) ##### <- function imported from timeformat_v2.py  ## MUTE THESE 2 LINES WHEN IMPORTING ALL DATA
#df_stacked = df_time[df_time["monthday"] > 20160825]                                 ## ""                                       ""   
#####    

