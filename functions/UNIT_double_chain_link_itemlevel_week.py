# -*- coding: utf-8 -*-
"""
Created on Wed May  4 13:07:39 2016

@author: Mint

Date:   07/04/2016
Verion: 0.2

Inputs:  WS price data 
outputs: Unit price monthly index item level single chain linked
    
"""

import pandas as pd
import numpy as np
import time
import datetime

#create todays date
todays_date = str(datetime.date.today())

#import unit price data for each year 
unit2014= pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/unitweekly2014_raw.csv")
unit2015= pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/unitweekly2015_raw.csv")
unit2016= pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/unitweekly2016_raw.csv")

#replace na's with 100 (this generally happens when there isn't enough data available e.g. not the end of the month)
unit2014 = unit2014.fillna(100)
unit2015 = unit2015.fillna(100)
unit2016 = unit2016.fillna(100)

#function to chain link each time period to the base period (January)
def itemchain(df,baseperiod,indices):
    df=df.reset_index()
    df=df.reset_index()
    #set index number
    index=df['level_0']
    for x in index:
        #set time period
        period = df["period"]
        #if not base period then chain to base period
        if period[x] > baseperiod:
            try:
                df.loc[x,indices] = float(df.loc[x,indices])*float(df.loc[df["period"]==baseperiod,indices])/100
            except:
                df.loc[x,indices] = 100
                print("not enough data")
    return df

# use single chain link at the item level to chain together the different years
def singlechainlink(originalyear, chainedyear,chainedyear2, basedate,basedate2,freq,priceindex):
    #single chain link for new year added
    #take subset of all put the baseperiod in 2015
    unit2015 = chainedyear[chainedyear["period"]!=basedate]
    #take subset of all put the baseperiod in 2016
    unit2016 = chainedyear2[chainedyear2["period"]!=basedate2]
    #join 2015 to 2014
    aaa1415=pd.concat([originalyear,unit2015],axis=0)
    aaa15=[]
    #single chain link 2015 back to 2014
    aaa15.append(aaa1415.groupby('ons_item_number').apply(lambda L: itemchain(L,basedate,priceindex)))
    #pull together into a dataframe and add column anmes
    aaa15 = np.concatenate(aaa15, axis=0)
    aaa15 = pd.DataFrame(aaa15)
    aaa15.columns=["Unnamed: 0","Unnamed: 1", "NaN", "Unnamed: 2","ons_item_number","period",priceindex]
    aaa15 = aaa15[[priceindex, "ons_item_number","period"]]
    aaa1516=pd.concat([aaa15,unit2016],axis=0)
    aaa16=[]
    #single chain link 2015 back to 2014
    aaa16.append(aaa1516.groupby('ons_item_number').apply(lambda L: itemchain(L,basedate2,priceindex)))
    #pull together into a dataframe and add column anmes
    aaa16 = np.concatenate(aaa16, axis=0)
    aaa16 = pd.DataFrame(aaa16)
    aaa16.columns=["Unnamed: 0", "Unnamed: 1","Unnamed: 3","Unnamed:4","ons_item_number","period",priceindex]
    aaa16 = aaa16[["ons_item_number", "period", priceindex]]
    return aaa16

upto2016 = singlechainlink(unit2014, unit2015,unit2016, 201501,201601,"month","unit")

#save
upto2016.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/unitdoublechaineditemlevelweek_"+ todays_date +"_.csv")