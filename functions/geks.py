

###############################################

##  GEKS-Jevons Calculation for ONS WS Data  ##

###############################################
"""
Author: Mint
Date:   07/04/2016
Verion: 0.5

Inputs:  WS price data for a single item
outputs: DataFrame with index for all time periods

Changes from previous version:
    - apply function to multiple classes
    - added aggregation code


"""

import os
import pandas as pd
import numpy as np
import datetime
import time

todays_date = str(datetime.date.today())

#%%

def geksj(data,idvar,classvar,datevar,pricevar):

    #reformat
    classvalue = pd.unique(data[classvar])
    x2 = pd.DataFrame({"ID":data[idvar],"Date":data[datevar],"Price":data[pricevar]})
    #Cast
    y=x2.pivot_table(index="ID",columns="Date",values="Price")
    T=len(y.columns)
    col=y.columns.values
    #D = empty matrix	
    D=np.zeros([T,T])			
    i=0
    #loop through all periods as base    
    for base in col:
        ## create coppies of y to work on 
        B = y.copy()
        B1 = y.copy()
        #divide each column by base column
        for date in col:        
            if date < base or base == col[0]:
                B[date] = B1[base] / B1[date]   ## generate your relatives
            else:
                B[date] = np.nan
        #take logs        
        B =np.log(B)
        #calculate collumn means - This is calculation of Jevons Index i to t or 0 to i
        C = B.mean()
        #add results to result matrix
        D[:,i]=np.exp(C.values)
        i += 1 
    #Multiply J_0_i by J_i_t
    E = np.zeros([T,T])
    for date in range(1,len(col)):
        #print(date)
        E[:,date] = np.log(D[:,date]/D[:,0])
        if date > 1:
            E[0,date] = np.nan
 

    F=np.exp(pd.DataFrame(E).mean())

    Index = pd.DataFrame({"i" : range(0,len(col)),"period":col,"GEKS":F,"Item Number":classvalue[0]})
    return(Index)

#%%

def make_GEKS(freq):
    #x = pd.read_csv('/Users/tomhuntersmith/Desktop/ONS/Index Numbers/WebScrapingPhase2/Data/august24/'+ yesterdays_date + '_'+ freq + '_.csv',encoding = "utf-8")#  

    x = pd.read_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/averages/_'+ freq + '_.csv',encoding = "latin_1")  
    x=x[x['item_price_num']>0]
    x["idvar"]=x["product_name"]+"_"+["store"]


    a =  []
    start_time = time.time()

    a.append(x.groupby('ons_item_no').apply(lambda L: geksj(L, 'idvar', 'ons_item_no',freq,'item_price_num')))

    Results = np.concatenate(a, axis=0)  # axis = 1 would append things as new columns
    global results2
    results2=pd.DataFrame(Results)
    results2.columns=["GEKS","ons_item_no","i","period"]
    results2["GEKS"] = results2["GEKS"].apply(lambda x: float(x)*100)

    results2.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/"+ freq +"_"+ todays_date +"_GEKS_item.csv") 
    print("Time taken to produce GEKS: --- %s seconds ---" % (time.time() - start_time))

def produce_GEKS():
    make_GEKS("month")
    make_GEKS("week")
    #make_GEKS("monthday")

produce_GEKS()

