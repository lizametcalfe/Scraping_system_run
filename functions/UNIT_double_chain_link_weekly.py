# -*- coding: utf-8 -*-
"""
Created on Wed May  4 13:07:39 2016

@author: Liz Metcalfe

Date:   07/04/2016
Verion: 0.2

Inputs:  WS price data 
outputs: Unit price monthly index double chain link weekly data and aggregate
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

#weight up item level
#import weights data as the coicop 4 and 3 level
weights=pd.read_excel('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Weights/Copy of weights-lager.xls')
weightsupper = pd.read_excel('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Weights/Weights_upper_unit.xls')
def aggregation(indices, priceindices,weights,frequency, weights14,weights15, weights16, agglevel,weightjoinvar):
    #add weights data to the price index data
    data=pd.merge(indices,weights,left_on=weightjoinvar,right_on="join",how="outer").reset_index()
    index=data['index']
    #aggregate data using weights (weights*priceindex/sum(weights))
    if frequency=="month" or frequency=="fortnight" or frequency == "weekly":
        period=np.floor(data['period'].astype(float)/100)
        for x in index.dropna():
            if period[x]==2014:
                data.loc[x,"weighted_index"]=data.loc[x,priceindices]*data.loc[x,weights14]
            elif period[x]==2015:
                data.loc[x,"weighted_index"]=data.loc[x,priceindices]*data.loc[x,weights15]
            if period[x]==2016:
                data.loc[x,"weighted_index"]=data.loc[x,priceindices]*data.loc[x,weights16]
    elif frequency=="daily":
        period=np.floor(data['period']/10000)
        for x in index:
            if period[x]==2014:
                data.loc[x,"weighted_index"]=data.loc[x,priceindices]*data.loc[x,weights14]
            elif period[x]==2015:
                data.loc[x,"weighted_index"]=data.loc[x,priceindices]*data.loc[x,weights15]
            elif period[x]==2016:
                data.loc[x,"weighted_index"]=data.loc[x,priceindices]*data.loc[x,weights16]
    b=data.groupby(by=[agglevel,"period"])["weighted_index"].sum()
    return b.reset_index()


#double chain link in two parts
#chain jan to dec at coicop4 level

#function to chain link each time period to either chain Jan to Dec or to chain each time period to the base period (January)
def itemchain(df,rebaseto,baseperiod):
    df=df.reset_index()
    df=df.reset_index()
    #set index number
    index=df['level_0']
    # chain Jan to base period if rebaseto is Dec
    if rebaseto == "Dec":
        df.loc[len(df.index)-1,"weighted_index"] = df.loc[len(df.index)-1,"weighted_index"]/df.loc[len(df.index)-2,"weighted_index"]*100
        df["year"] = str(df.loc[len(df.index)-1,"period"])[0:4]
    #otherwise chain each time period to January
    elif rebaseto == "Jan":
        for x in index:
            period = df["period"]
            if period[x]==baseperiod:
                per = x
                df.loc[x,"weighted_index"] = float(df.loc[x,"weighted_index"])*float(df.loc[x-1,"weighted_index"])/100 if df.loc[x,"period"]==baseperiod else df.loc[x,"weighted_index"]
            if period[x] > baseperiod:
                df.loc[x,"weighted_index"] = float(df.loc[x,"weighted_index"])*float(df.loc[df["period"]==baseperiod,"weighted_index"])/100

                #        df.loc[len(df.index)-1,"weighted_index"] = df.loc[len(df.index)-1,"weighted_index"]*df.loc[len(df.index)-2,"weighted_index"]/100
#        df["weighted_index"] = df.apply(lambda row: float(row["weighted_index"])*float(df.loc[df["period"]==201501,"weighted_index"])/100 if row["year"]=="2015" else row["weighted_index"], axis=1)
    else:
        df = "Wrong"
    return df

#Double chain link the whole index
#This took some doing which is why I've left the chained files separately instead of joining them into a master file.
def doublechainlink(originalyear, chainedyear,chainedyear2, basedate,basedate2,freq,priceindex):
    #aggregate to COICOP4 level using the lower weights for each year (this is the item level chaining) - step one of double chain linking
    originalyear = aggregation(originalyear, priceindex,weights,"month","weight_2_2014","weight_2_2015","weight_2_2016","level_2","ons_item_number")
    chainedyear = aggregation(chainedyear, priceindex,weights,"month","weight_2_2014","weight_2_2015","weight_2_2016","level_2","ons_item_number")
    chainedyear2 = aggregation(chainedyear2, priceindex,weights,"month","weight_2_2014","weight_2_2015","weight_2_2016","level_2","ons_item_number")
    #create empty list
    a14=[]
    #chain the COICOP4 items to the base date and add to the empty list
    a14.append(originalyear.groupby('level_2').apply(lambda L: itemchain(L,"Dec",basedate)))
    #put into a dataframe and add column names
    a14 = np.concatenate(a14, axis=0)
    a14 = pd.DataFrame(a14)
    a14.columns=["Unnamed: 0","Unnamed: 1",  "level_2", "period",priceindex,"year"]
    a15=[]
    #chain the COICOP4 items to the base date and add to the empty list
    a15.append(chainedyear.groupby('level_2').apply(lambda L: itemchain(L,"Dec",basedate2)))
    #put into a dataframe and add column names
    a15 = np.concatenate(a15, axis=0)
    a15 = pd.DataFrame(a15)
    a15.columns=["Unnamed: 0","Unnamed: 1",  "level_2", "period",priceindex,"year"]
    #Aggregate upto COICOP3 level (highest in this case) for each year
    unit2014agg2 = aggregation(a14, priceindex,weightsupper,freq,"weight_1_2014","weight_1_2015","weight_1_2016","level_3","level_2")
    unit2015agg2 = aggregation(a15, priceindex,weightsupper,freq,"weight_1_2014","weight_1_2015","weight_1_2016","level_3","level_2")
    unit2016agg2 = aggregation(chainedyear2, "weighted_index",weightsupper,freq,"weight_1_2014","weight_1_2015","weight_1_2016","level_3","level_2")
    #remove the baseperiod price index values as these will be 100 and not of use
    unit2015agg2 = unit2015agg2[unit2015agg2["period"]!=basedate]
    unit2016agg2 = unit2016agg2[unit2016agg2["period"]!=basedate2]
    #join together 2015 and 2016 data
    aaa1415=pd.concat([unit2014agg2,unit2015agg2],axis=0)
    aaa15=[]
    #chain back to base period
    aaa15.append(aaa1415.groupby('level_3').apply(lambda L: itemchain(L,"Jan",basedate)))
    #put into a dataframe and add column names
    aaa15 = np.concatenate(aaa15, axis=0)
    aaa15 = pd.DataFrame(aaa15)
    aaa15.columns=["Unnamed: 0", "Unnamed: 1", "level_3", "period","weighted_index"]
    aaa1516=pd.concat([aaa15,unit2016agg2],axis=0)
    aaa16=[]
    #chain back to base period
    aaa16.append(aaa1516.groupby('level_3').apply(lambda L: itemchain(L,"Jan",basedate2)))
    #put into a dataframe and add column names
    aaa16 = np.concatenate(aaa16, axis=0)
    aaa16 = pd.DataFrame(aaa16)
    aaa16.columns=["Unnamed: 0", "Unnamed: 1","Unnamed: 2", "Unnamed: 3", "level_3", "period","weighted_index"]
    del aaa16["Unnamed: 3"]
    del aaa16["Unnamed: 2"]
    del aaa16["Unnamed: 1"]
    del aaa16["Unnamed: 0"]
    return aaa16


upto2016 = doublechainlink(unit2014, unit2015,unit2016, 201501,201601,"month","unit")

upto2016.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/unitdoublechainedweek_"+ todays_date +"_.csv")