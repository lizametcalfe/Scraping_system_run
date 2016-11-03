# -*- coding: utf-8 -*-
"""
Created on Mon Aug 22 17:05:10 2016

@author: Mint
"""
import time 
import datetime
import pandas as pd
import numpy as np
todays_date = str(datetime.date.today())  


#%% Aggregation

def weights_prep(data):
    global weights
    weights=pd.read_csv('//media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Weights/weights.csv').reset_index()
    indices=data
    df=pd.merge(indices,weights,left_on="ons_item_no",right_on="ons_item_no",how="outer")
    df = df.rename(columns={'index': 'item_name'})
    df["weighted_index"]=0
    df.reset_index(level=0, inplace=True)
    return df
#%%
#weights_attached = weights_prep(results2)

def weighting(data,frequency,species):
    index=data['index']
    if frequency=="month" or frequency=="fortnight" or frequency == "week":
        period=np.floor(data['period']/100)
        for x in index:
            if period[x]==2014:
                data.loc[x,"weighted_index"]=data.loc[x,species]*data.loc[x,'item_weight_2014']
            elif period[x]==2015:
                data.loc[x,"weighted_index"]=data.loc[x,species]*data.loc[x,'item_weight_2015']
            elif period[x]==2016:
                data.loc[x,"weighted_index"]=data.loc[x,species]*data.loc[x,'item_weight_2016']
    elif frequency=="monthday":
        period=np.floor(data['period']/10000)
        for x in index:
            if period[x]==2014:
                data.loc[x,"weighted_index"]=data.loc[x,species]*data.loc[x,'item_weight_2014']
            elif period[x]==2015:
                data.loc[x,"weighted_index"]=data.loc[x,species]*data.loc[x,'item_weight_2015']
            elif period[x]==2016:
                data.loc[x,"weighted_index"]=data.loc[x,species]*data.loc[x,'item_weight_2016']
    #WGEKSJ=np.sum(df["weighted_index"])
    #print(WGEKSJ)
    #out=pd.DataFrame({"period":pd.unique(data["period"]),"GEKSJ":WGEKSJ,"Division":pd.unique(data['Top'])})
    return(data)

#df1=weighting(weights_attached,"week")  
#%%

def aggregate_indices(frequency,species):
    print("Aggregating: %s %s" % (frequency, species))
    global data
    data = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/"+ frequency +"_"+ todays_date +"_"+ species +"_item.csv") 
    global weights_attached
    weights_attached = weights_prep(data)
    global df1
    df1=weighting(weights_attached,frequency,species)  
    global b
    b=df1.groupby(by=["Top","period"])["weighted_index"].sum()
    b=b.reset_index()
    b.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/"+ frequency +"_"+ todays_date +"_"+ species +"_agg.csv") 
    
#%%

def aggregate_them_all():
    import time 
    print ("Aggregating Indices")
    start_time = time.time()
    #aggregate_indices("week","UnitPrice") #####adjust species & frequency as and when
    #aggregate_indices("month","UnitPrice") 
    #aggregate_indices("monthday","GEKS") 
    aggregate_indices("week","GEKS") 
    aggregate_indices("month","GEKS") 
    print("Time taken to aggregate Indices: --- %s seconds ---" % (time.time() - start_time))
    
aggregate_them_all()

