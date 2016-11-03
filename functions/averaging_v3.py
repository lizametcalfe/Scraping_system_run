# -*- coding: utf-8 -*-
"""
Created on Mon Aug 15 12:42:04 2016

@author: mint
"""

import datetime
import time
from datetime import timedelta
import pandas as pd
from scipy.stats import gmean 

last_edit_date = str(datetime.date.today()-timedelta(5))
yesterdays_date = str(datetime.date.today()-timedelta(1)) ##for conveniance in case continuing yesterdays work
todays_date = str(datetime.date.today())  

#df = pd.read_csv('/Users/tomhuntersmith/Desktop/ONS/Index Numbers/WebScrapingPhase2/Data/cleaned'+ last_edit_date + '_main_.csv')
#df = pd.read_csv('/Users/tomhuntersmith/Desktop/ONS/Index Numbers/WebScrapingPhase2/Data/august24/'+ todays_date + '_main_.csv')

#%%
print ("Creating weekly and monthly average prices across each product")

def create_match(data):
    match = data.drop_duplicates(subset=['product_name'])
    match = match[["product_name","ons_item_no","store","ons_item_name","offer"]]
    return match
    
def revert_to_numeric(data):
    data['item_price_num'] = data['item_price_num'].map(lambda x: float(x))
    data['std_price'] = data['std_price'].map(lambda x: float(x))
    return data 

def average_and_send():
    
    start_time = time.time()
    data = pd.read_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions_full_data_run/data/_main_.csv',encoding = "latin_1")#
    matcher = create_match(data)
    print(matcher.head(3))
    data = revert_to_numeric(data)
    del data["month"]
    data["month"] = data["monthday"].apply(lambda x: float(str(x)[0:6]))
    
    #######DAILY###########(not averaging this one, just trimming unecessary columns)
    data_daily = data[["product_name","ons_item_no","store","ons_item_name",
                       "item_price_num","std_price","offer","monthday","offer"]].reset_index()
    data_daily.to_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/averages/_monthday_.csv',encoding = "latin_1", index = False)#+ todays_date + '_monthday_.csv',encoding = "utf-8", index = False)#  
    print("Total number of daily prices collected: %s " % (len(data_daily)))
    

    #######WEEKLY##########
    global complete_week
    data_w = data[["product_name","week","item_price_num"]]
    data_w = data_w[data_w["week"] != 201422]
    data_week = data_w.groupby(["product_name","week"])
    data_week  = data_week["item_price_num"].apply(gmean,axis=None).reset_index()

    ####match key variables back on to the averaged dataset
    complete_week = data_week.merge(matcher,how='left', left_on='product_name', right_on='product_name')
    complete_week.to_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/averages/_week_.csv',encoding = "latin_1", index = False)#  
    print("Total number of weekly prices collected: %s " % (len(data_week)))
    
    ######MONTHLY############
    global complete_month

    data_m = data[["product_name","month","item_price_num"]]
    data_month = data_m.groupby(["product_name","month"])
    data_month  = data_month["item_price_num"].apply(gmean,axis=None).reset_index()
    
    ####match key variables back on to the averaged dataset
    complete_month = data_month.merge(matcher,how='left', left_on='product_name', right_on='product_name')
    complete_month.to_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/averages/_month_.csv',encoding = "latin_1", index = False)#   
    print("Total number of monthly prices collected: %s " % (len(data_month)))


    print("Time taken to average: --- %s seconds ---" % (time.time() - start_time))

average_and_send()
#%% 
#%%
    
    
    
    
    
    
    
    
    
    
    
    