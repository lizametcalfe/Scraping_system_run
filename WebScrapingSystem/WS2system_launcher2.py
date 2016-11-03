# -*- coding: utf-8 -*-
"""
Created on Sat Sep 17 18:56:01 2016

@author: mint
"""

import os
import datetime
from datetime import timedelta
import time
import pandas as pd
import schedule

import sys
sys.path.append("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system")

yesterdays_date = str(datetime.date.today()-timedelta(1)) ##for convenience in case continuing yesterdays work
todays_date = str(datetime.date.today())    

#%%

 
os.system('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions/load_csv_single.sh') ##bash code 
print ("Added days data %s from  to MongoDB") % (datetime.date.today())

print ("(6) Creates average week/month prices for each product and sends them to '/home/mint/my-data/WebScrapingPhase2/Data/test/")
print ("Stage (6) is perfomed using code from the python module averaging_v3.py")
        
from functions.averaging_v3 import average_and_send
average_and_send()
 
print ("""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""")
    
print ("(7) Creates Indices and sends them to '/home/mint/my-data/WebScrapingPhase2/Data/test/")
print ("Stage (7) is perfomed using code from the python module geks_v3.py, dailychained_v3.py, unitprice_v3.py")
    
print("Geks price index being calculated")
from functions.geks import produce_GEKS
produce_GEKS()
from functions.aggregation_v3 import aggregate_them_all
aggregate_them_all()

print("daily_chained price index being calculated")
os.system("python /media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions/dailychained.py")

print("unit price price index being calculated")
os.system("python /media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions/unitprice.py")
os.system("python /media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions/unitprice-weekly.py")
os.system("python /media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions/UNIT_double_chain_link.py")
os.system("python /media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions/UNIT_double_chain_link_itemlevel.py")
os.system("python /media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions/UNIT_double_chain_link_weekly.py")
os.system("python /media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions/UNIT_double_chain_link_itemlevel_week.py")

print("CLIP price price index being calculated")
os.system("python /media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions/CLIP.py")
os.system("python /media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions/CLIP_weekly.py")
os.system("python /media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions/CLIP_double_chain_link.py")
os.system("python /media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions/CLIP_double_chain_link_itemlevel.py")
os.system("python /media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions/CLIP_double_chain_link_week.py")
os.system("python /media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions/CLIP_double_chain_link_itemlevel_week.py")

print ("(8) Create data visualisation")
print ("Stage (8) is perfomed using code from the python module data_vis.py")
os.system("python /media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/functions/data_vis.py")
  
print ("(9) Emails indices out to onswebscraping@gmail.com")
print ("Stage (9) is perfomed using code from the python module emailer_v3.py")

from functions.emailer_v3 import emailer
emailer()  
