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


#%%

print ("Good Morning/Good Afternoon/Whatever......on my favourite day of the year %s ")  % (todays_date)
print ("Initiating WebScrapingPhase2 System using the collection of code from /home/mint/my-data/WebScrapingPhase2/ScraperSystem in Tom's Instance")
print ("HERE WE GO!!")
   
print ("(1) Pulling previous 7 days worth of data from MONGODB (192.168.2.20:27017)[prices_db][combined]")
print ("Stage (1) is perfomed using code from the python module mongo_import_v3.py")
    
from functions.first_mongo_pull_v3 import pull_it
pull_it()
print("done")
print ("""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""")
    
print ("(2) Cleaning the data using Matt's clustering algorithm and exporting 'ons_item_no' level datasets to /Clustered folder")
print ("Stage (2) is perfomed using code from the python module anomaly_detection_v3.py")
    
from functions.anomaly_detection_v3 import clean_split
clean_split()
    
print ("""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""")
   
print ("(3) Identifying the missing days in each 'ons_item_no' level datset for any product (under the 7 day imputation condition) and export them to /Missing folder")
print ("Stage (3) is performed using the R module missing_days.R")
    
##subprocess is a python module, not a user written module
import subprocess
command = 'RScript'
subprocess.call("/usr/bin/Rscript --vanilla identify_missing_v3.R",shell = True)
    
print ("""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""")
    
print ("(4) Imputing the for any missing days and then stick all of the 'ons_item_no' data sets together again, & removes all but the current days data from the dataset and s storing the data in /Imputed folder")
print ("Stage (4) is perfomed using code from the python module pack_stack_rack_v3.py")
    
from functions.impute_pack_stack_rack_v3 import pack_stack_rack   
pack_stack_rack()
    
print ("""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""")
 
print ("(5) ticks it with the other data in mongodb and also onto the main csv file _main_")
print ("Stage (5) is perfomed using code from the python module pack_stack_rack_v3.py")

from functions.add_to_databases import send_to_databases
send_to_databases()


#######Optional Stages - Pull down all cleaned/imputed data from mongoDB##########

######Generally not necessary because all of the up to date data is already readily avaialable in csv#########

###this will allow you to pull our ALL of the data from the ORIGINAL prices database......takes ages, as does the imputation etc


#from functions.second_mongo_pull_v3 
#schedule.every(1).minutes.do(job)
#schedule.every().hour.do(job)
###schedule.every().day.at("09:58").do(init)
#schedule.every().monday.do(job)
#schedule.every().wednesday.at("13:15").do(job)

#while True:
#    schedule.run_pending()
#    time.sleep(1)










