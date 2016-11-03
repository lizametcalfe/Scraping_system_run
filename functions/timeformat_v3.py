# -*- coding: utf-8 -*-
"""
Created on Mon Aug 15 12:42:04 2016

@author: tomhuntersmith
"""

import datetime
from datetime import timedelta
import time
import pandas as pd

yesterdays_date = str(datetime.date.today()-timedelta(1)) ##for convenience in case continuing yesterdays work
todays_date = str(datetime.date.today())
    

#df = pd.read_csv('/home/mint/my-data/WebScrapingPhase2/Data/test/'+ todays_date + '_main_.csv')


#%%
print ("Creating daily, weekly and monthly times")

def format_month(time):
    time = str(time)
    time = datetime.datetime.strptime(time[0:10], '%Y-%m-%d').strftime('%Y%m%d')
    return time
    
    
def format_week(time):
    time = str(time)
    time = datetime.datetime.strptime(time[0:10], '%Y-%m-%d').strftime('%Y%U')
    return time


def its_about_time(data):
    data['monthday'] = data['timestamp'].map(lambda x: format_month(x))
    data['week'] = data['timestamp'].map(lambda x: format_week(x))
    data['month'] = data['monthday'].map(lambda x: x[0:6])
    
    data['monthday'] = data['monthday'].map(lambda x: float(x)) ##convert monthday to number for subsetting 7 imputation days
    return data 
    
#%%
def resolve_time(data):
    start_time = time.time()
    data = its_about_time(data) 
    print("Time taken to format: --- %s seconds ---" % (time.time() - start_time))
    return data 

#df = pd.read_csv('/home/mint/my-data/WebScrapingPhase2/Data/test/'+ todays_date + '_ready_.csv')
#df = df[df["monthday"] > 20160825]

#%%
























    