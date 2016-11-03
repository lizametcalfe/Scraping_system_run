# -*- coding: utf-8 -*-
"""
Created on Wed May  4 13:07:39 2016

@author: Mint

Date:   07/04/2016
Verion: 0.2

Inputs:  None
outputs: last 7 days from the mongo dataset 
 """

import schedule 
import datetime
from datetime import timedelta
import time
import pandas as pd
import schedule 
import csv
import json
import datetime
import time
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient



#%%
#set up access to the mongo database. The database is held on a different instant on the Big Data team openstack environment. If the labs go down for some reason this will need to be restarted for access to be made
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

#####function keeps only needed variables#########
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
             'week',
             'std_price',
             'unit_price',
             'item_price_num'
             ]]
    return df
    

###########functions which create time variables##################
def format_month(time):
    time = str(time)
    time = datetime.datetime.strptime(time[0:10], '%Y-%m-%d').strftime('%Y%m%d')
    return time
    
    
def format_week(time):
    time = str(time)
    time = datetime.datetime.strptime(time[0:10], '%Y-%m-%d').strftime('%Y%U')
    return time

# change format of the timestamp variables
def its_about_time(data):
    data['monthday'] = data['timestamp'].map(lambda x: format_month(x))
    data['week'] = data['timestamp'].map(lambda x: format_week(x))
    data['month'] = data['monthday'].map(lambda x: x[0:6])
    
    data['monthday'] = data['monthday'].map(lambda x: float(x)) ##convert monthday to number for subsetting 7 imputation days
    return data 

def resolve_time(data):
    print ("Creating daily, weekly and monthly times")
    start_time = time.time()
    data = its_about_time(data) 
    print("Time taken to format: --- %s seconds ---" % (time.time() - start_time))
    return data    
    
#%%
def pull_it():
    mydb = get_db()
    #todays_date = str(datetime.date.today())
    seven_days_back = str(datetime.date.today()-timedelta(7)) ##for convenience in case continuing yesterdays work
    
    df = pd.DataFrame(list(mydb.combined.find({"timestamp":  {"$gt":seven_days_back}})))
    #df.to_csv('/home/mint/my-data/WebScrapingPhase2/Data/test/'+ todays_date + '_past7_.csv',encoding = "utf-8")#
    global df_time
    print("Total number of items collected from this group: %s " % (len(df)))  
    #from functions.timeformat_v3 import resolve_time
    df_time = resolve_time(df) ##### <- function imported from timeformat_v2.py  ## MUTE THESE 2 LINES WHEN IMPORTING ALL DATA
    df_time = variable_trim(df_time)
    
    df_time.to_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/last_7_days.csv',encoding = "utf-8")#
    #print (todays_date)


pull_it()

