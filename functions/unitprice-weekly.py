# -*- coding: utf-8 -*-
"""
Created on Wed May  4 13:07:39 2016

@author: Mint

Date:   07/04/2016
Verion: 0.2

Inputs:  WS price data 
outputs: Unit price monthly index non-chained
 """

import os
import pandas as pd
import numpy as np
import math
from scipy.stats import gmean

try:
    os.remove("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/unitweekly2014_raw.csv")
    os.remove("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/unitweekly2015_raw.csv")
    os.remove("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/unitweekly2016_raw.csv")
except:
    pass
#%%

#%%
#Import Dataset
x = pd.read_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/averages/_monthday_.csv',encoding="latin_1")

#Remove rows with prices less than zero (outliers)
x=x[x['item_price_num']>0]

#create unique id variable
x["idvar"]=x["product_name"]+"_"+x["store"]

#This function creates a number of different time based variables
def weekly(df, monthday):
	##convert to string
    df["monthday"]=monthday.apply(lambda x: str(x))
    #pull out the day of the week and call it day
    df["day"] = df["monthday"].str[6:].astype(str)
    df["day"]=df["day"].apply(lambda x: str(x)[0:2])
    #pull out the month
    df["monthss"]=df["monthday"].str[4:6].astype(str)
    #pull out the year
    df["year"]=df["monthday"].str[:4].astype(str)
    #put together day, month and year into a certain format
    df["dates"]=pd.to_datetime(df.day + df.monthss + df.year, format="%d%m%Y")
    #take out the year from this date
    df['datessp'] = df['dates'].map(lambda x: x.isocalendar()[0])
    #take out the week of the year from this date
    df['datess'] = df['dates'].map(lambda x: x.isocalendar()[1])
    #if a day is say 1 (1st) change it to 01
    df["datess"]=df["datess"].apply(lambda x: "0"+str(x) if len(str(x)) == 1 else str(x))
    #add year and week of year together
    df["yearweekno"]=df['datessp'].astype(str)+df["datess"].astype(str)
    df["yearweekno"] = df["yearweekno"].apply(lambda x: int(x))
    return df

#find the week of the year date format for each row in the dataframe
x= weekly(x,x["monthday"])

#As week of the year runs across the new year (e.g. the 1st Jan can be classed as the 52 week of the previous year and the 1st year of the next year) changes had to be made to the assigned dates
x["yearweekno"]=x["yearweekno"].apply(lambda x: 201610 if x == 201609 else x)
x["yearweekno"]=x["yearweekno"].apply(lambda x: 201609 if x == 201608 else x)
x["yearweekno"]=x["yearweekno"].apply(lambda x: 201608 if x == 201607 else x)
x["yearweekno"]=x["yearweekno"].apply(lambda x: 201607 if x == 201606 else x)
x["yearweekno"]=x["yearweekno"].apply(lambda x: 201606 if x == 201605 else x)
x["yearweekno"]=x["yearweekno"].apply(lambda x: 201605 if x == 201604 else x)
x["yearweekno"]=x["yearweekno"].apply(lambda x: 201604 if x == 201603 else x)
x["yearweekno"]=x["yearweekno"].apply(lambda x: 201603 if x == 201602 else x)
x["yearweekno"]=x["yearweekno"].apply(lambda x: 201602 if x == 201601 else x)
x["yearweekno"]=x["yearweekno"].apply(lambda x: 201601 if x == 201553 else x)

#take a subset of data after June 2014
x=x[x["yearweekno"] > 201422]

#change name of the date variable
x["months"]=x["yearweekno"]
del x["yearweekno"]


#Take subsets by year (including the Jan of the next year for chaining reasons)
x14 = x[x["months"]<201502]
x15 = x[x["months"]>=201501]
x15 = x15[x15["months"]<201602]
x16 = x[x["months"]>=201601]

#function to take the geometric mean of each of the unique products within each month
def unitdate(data, date, datevar, pricevar):
    #take subset by month of interest (date)
    data_prices = data.loc[data[datevar].astype(int) == int(date)]
       #take subset by columns
    data_prices = data_prices[["product_name",pricevar]]
     #group together each unique product
    data_prices = data_prices.groupby(["product_name"])
        #take geometric mean of each set of unique products
    data_prices = data_prices[pricevar].agg({"gmean":gmean})
    data_prices.reset_index(inplace=True)
    return data_prices

#calculate the unit price for unique products
def unitprice(data,idvar,classvar,datevar,pricevar,basedate, year):
	    #find the name of the COICOP item of interest
    classvalue = pd.unique(data[classvar])[0]
        #find the year of interest
    data["years"] = data[datevar].apply(lambda x: str(x)[0:4])
        #take subset of just products in year of interest
    dataa = data[data["years"] == str(year)]
        #add the Jan of the year after the year of interest (for chaining reasons)
    datab = data[data[datevar].astype(int) == int(str(year+1)+"01")]
    #join together year of data and january data
    data = pd.concat([dataa,datab],axis=0)
        #create a list of unique dates within the newly create dataset (year+jan)
    date = pd.unique(data[datevar])
    df1 = pd.DataFrame({"i" : range(0,len(date)),"period":date,"ons_item_no":classvalue})
        #create empty varaibles (this way this doesn't work it becomes obvious)
    df1["unit"]="empty"
        #take geometric mean of the unique products in the base month (January)
    base = unitdate(data, basedate, datevar, pricevar)
        #rename (this could be done a lot better!)
    base["base_price"] = base["gmean"]
    del base["gmean"]
        #for each date in Year+Jan divide the geometric mean of the product in the month of interest with the same product in the base period.
    for i in date:
    	 #if this doesn't work put results = 100 so that the whole process doesn't break
        try:
            datamerged=pd.merge(base,unitdate(data,int(i), datevar, pricevar), how='inner', on='product_name')
            datamerged.loc[:,'price_relative'] = datamerged.loc[:,'gmean']/datamerged.loc[:,'base_price']
            datamerged['pr_log'] = datamerged['price_relative'].apply(math.log)
            datamerged["groups"] = 1
            test1 = datamerged.groupby('groups')
            lopp = test1['pr_log'].apply(np.mean).apply(np.exp)*100
            if lopp.empty:
                lopp = 100
            else:
                lopp = float(lopp)
            df1["unit"][df1["period"]==i] = lopp
        except:
            df1["unit"][df1["period"]==i] = 100
#    print(df1["ons_item_number"])
    return df1.sort_values("period")


#calculate the unit price for each of the items within ons_item_no, append the results, and put them in a dataframe
def runthrough(data,basedate,date):
    a = []
    for i in np.unique(data["ons_item_no"]):
        a.append(unitprice(data[data["ons_item_no"] == i],  'idvar', 'ons_item_no','months','item_price_num', basedate,date))
    aa = np.concatenate(a, axis=0)  # axis = 1 would append things as new columns
    aa=pd.DataFrame(aa)
    aa.columns=["i",  "ons_item_number", "period", "unit"]
 
    return aa

#run the unit price calculatino for each year and save
aa=pd.DataFrame()
a = runthrough(x14, 201423, 2014)
a.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/unitweekly2014_raw.csv")

b = runthrough(x15, 201501, 2015)
b.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/unitweekly2015_raw.csv")

c = runthrough(x16, 201601, 2016)
c.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/unitweekly2016_raw.csv")