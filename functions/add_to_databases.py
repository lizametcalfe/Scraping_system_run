# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 12:22:51 2016

@author: mint

imput: the imputed data to today and the overall back dataset from 2014
output: load todays data into the mongo database and update the back data with todays data 
"""
import pandas as pd
import datetime
import time
import os
import subprocess

#import sys
#reload(sys)
#sys.setdefaultencoding('utf-8')

#add todays data to the main csv of back data
global new_data
global main_data
def combine():
    #import data
	new_data = pd.read_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/_imputed_all.csv', encoding = 'latin_1')
	main_data = pd.read_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions_full_data_run/data/_main_.csv', encoding = 'latin_1')
	new_data.offer = new_data.offer.str.encode('utf-8')
	main_data.offer = main_data.offer.str.encode('utf-8')
	new_data.product_name = new_data.product_name.str.encode('utf-8')
	main_data.product_name = main_data.product_name.str.encode('utf-8')
    #put important data into lowercase so it's matchable
	main_data["ons_item_name"] = main_data["ons_item_name"].apply(lambda x: str(x).lower())
	main_data["product_name"] = main_data["product_name"].apply(lambda x: str(x).lower())
	new_data["ons_item_name"] = new_data["ons_item_name"].apply(lambda x: str(x).lower())
	new_data["product_name"] = new_data["product_name"].apply(lambda x: str(x).lower())
    
    #take subset of the main data so it fits the format of todays data
	main_data = main_data[["product_name","monthday","ons_item_no","store","offer","ons_item_name",
                     "std_price",	"timestamp", "month","week","ML_score","item_price_num"]]

	new_data = new_data[["product_name","monthday","ons_item_no","store","offer","ons_item_name",
                     "std_price",	"timestamp", "month","week","ML_score","item_price_num"]]
    #join the to datasets together
	updated_data = pd.concat([main_data, new_data])
	print(updated_data.tail(10))

    #save over main dataset
	updated_data.to_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions_full_data_run/data/_main_.csv', encoding = 'latin_1')
    
 
combine()

#%%
#main_data.to_csv('/home/mint/my-data/WebScrapingPhase2/ScraperSystem/_main_20160918.csv', encoding = 'latin_1', index = False)




#%%

#os.system('./load_csv_single.sh') ##bash code 