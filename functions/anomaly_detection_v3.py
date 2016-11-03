# -*- coding: latin-1 -*-

"""

Created on Wed Jul 20 12:33:27 2016



@author: mayhem

"""



"""

ANOMALY DECTECTION ALGORITHM V1.0

Differences from previous cleaning code
Adding a decision to do the tracking clusters across time
Initial Clusters will be calculated using both price per item and price per standard unit

"""

#%%

#first import these modules
import os
import platform
import pandas as pd
from sklearn.cluster import DBSCAN
import numpy as np
from sklearn.tree import DecisionTreeClassifier

import gc as gc
import datetime
import datetime

from datetime import timedelta
import time

todays_date = str(datetime.date.today())

#%% Attach initial clusters to most recent data



def stacker():
    global original_clusters
    global new_data
    original_clusters = pd.read_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/WebScrapingSystem/original_clusters.csv',encoding = "latin-1")
    original_clusters = original_clusters[['Unnamed: 0',
                      'item_price_num',
                      'ml_prediction',
                      'month',
                      'monthday',
                      'offer',
                      #'offercat',
                      'ons_item_name',
                      'ons_item_no',
                      'product_name',
                      'std_price',
                      'store',
                      'timestamp',
                      'unit_price',
                      'week']]
                      
    new_data = pd.read_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/last_7_days.csv',encoding = "latin-1")
    new_data = new_data[['Unnamed: 0',
                      'item_price_num',
                      'ml_prediction',
                      'month',
                      'monthday',
                      'offer',
                      #'offercat',
                      'ons_item_name',
                      'ons_item_no',
                      'product_name',
                      'std_price',
                      'store',
                      'timestamp',
                      'unit_price',
                      'week']]
    stacker = pd.concat([original_clusters,new_data]).sort(['monthday','ons_item_no'])
    return stacker

#%% 
#find the initial clusters

def clusterstrain(item_no,r,m=5):
    #df['monthday1']=df['monthday'].apply(lambda x: x - ( x % 100))
    df1=df[df['ons_item_no']==item_no]
    df2=df1[df1['month'] == 201406].sort_index(by=['std_price'], ascending=[True])
    b=DBSCAN(eps=r,min_samples=m).fit(df2[['item_price_num','std_price']])
    lab2=pd.DataFrame(b.labels_)
    df2['trained_cluster_new']=[ 
        lab2.loc[x,0]
        for x in range(len(df2))
        ]
    df2=df2
    df3=df1[df1['month'] != 201406]
    df3['trained_cluster_new']=np.nan
    df4=df2.append(df3).reset_index()
    return(df4)
#%%
#test=clusterstrain(210111,0.1)
#%% decision Tree stuff 
def decisionTree(data,basetree):
    month=pd.unique(data['month'])[0]
    if month==201406:
        data['clusters_dt']=data['trained_cluster2']
    elif month!=201406:
        c=basetree.predict(data[["item_price_num","std_price"]])
        data['clusters_dt']=c
    return(data)
    
def AllDT(data):
    data['trained_cluster2']=data['trained_cluster_new']+1
    firstmonth=data[data['month']==201406]
    dt = DecisionTreeClassifier(criterion = "entropy",max_features = "log2")
    a=dt.fit(np.array(firstmonth[["item_price_num","std_price"]]),firstmonth["trained_cluster2"])
    allmonths=[]
    allmonths.append(data.groupby('month').apply(lambda l: decisionTree(l,a)))
    Results = np.concatenate(allmonths, axis=0)  # axis = 1 would append things as new columns
    results2=pd.DataFrame(Results)
    
    results2.columns=['index',
                      'X',
                      'item_price_num',
                      'ML_score',
                      'month',
                      'monthday',
                      'offer',
                      #'offercat',
                      'ons_item_name',
                      'ons_item_no',
                      'product_name',
                      'std_price',
                      'store',
                      'timestamp',
                      'unit_price',
                      'week',
                      'trained_cluster_new',
                      'trained_cluster2', 
                      'clusters_dt']

    return(results2)
#%%

#test2=AllDT(test)

#%% comparison with Classification 

def comparison(assigned):
    assigned['clusters_dt2']=assigned['clusters_dt']-1
    assigned['check2']=[
    'test'
    for x in range(len(assigned))
    ]
    try:
        CC1=assigned[assigned['ML_score']==1]  
        CC=CC1[CC1['clusters_dt2']!=-1]
        CC['check2']=[
        'Correct Classification'
        for x in range(len(CC))
        ]
    except KeyError or ValueError:
        CC=pd.DataFrame(np.nan, index=[0], columns=['index',
                      'X',
                      'item_price_num',
                      'ML_score',
                      'month',
                      'monthday',
                      'offer',
                      #'offercat',
                      'ons_item_name',
                      'ons_item_no',
                      'product_name',
                      'std_price',
                      'store',
                      'timestamp',
                      'unit_price',
                      'week',
                      'trained_cluster_new',
                      'trained_cluster2', 
                      'clusters_dt',
                      'check2'])
    try:
        NP1=assigned[assigned['ML_score']==1]    
        NP=NP1[NP1['clusters_dt2']==-1]
        NP['check2']=[
        'New Product'
        for x in range(len(NP))
        ]
    except KeyError or ValueError:
        NP=pd.DataFrame(np.nan, index=[0],columns =  ['index',
                      'X',
                      'item_price_num',
                      'ML_score',
                      'month',
                      'monthday',
                      'offer',
                      'offercat',
                      'ons_item_name',
                      'ons_item_no',
                      'product_name',
                      'std_price',
                      'store',
                      'timestamp',
                      'unit_price',
                      'week',
                      'trained_cluster_new',
                      'trained_cluster2', 
                      'clusters_dt',
                      'check2'])
    try:
        ClM1=assigned[assigned['ML_score']==0]
        ClM=ClM1[ClM1['clusters_dt2']!=-1]    
        ClM['check2']=[
        'Cluster Miscalssification'
        for x in range(len(ClM))
        ]
    except KeyError or ValueError:
        ClM=pd.DataFrame(np.nan, index=[0], columns = ['index',
                      'X',
                      'item_price_num',
                      'ML_score',
                      'month',
                      'monthday',
                      'offer',
                      #'offercat',
                      'ons_item_name',
                      'ons_item_no',
                      'product_name',
                      'std_price',
                      'store',
                      'timestamp',
                      'unit_price',
                      'week',
                      'trained_cluster_new',
                      'trained_cluster2', 
                      'clusters_dt',
                      'check2'])

    try:
        CoM1=assigned[assigned['ML_score']==0]
        CoM=CoM1[CoM1['clusters_dt2']==-1]
        CoM['check2']=[
        'Correct Misclassification'

        for x in range(len(CoM))
        ]
    except KeyError or ValueError:
        CoM=pd.DataFrame(np.nan, index=[0], columns = ['index',
                      'X',
                      'item_price_num',
                      'ML_score',
                      'month',
                      'monthday',
                      'offer',
                      #'offercat',
                      'ons_item_name',
                      'ons_item_no',
                      'product_name',
                      'std_price',
                      'store',
                      'timestamp',
                      'unit_price',
                      'week',
                      'trained_cluster_new',
                      'trained_cluster2', 
                      'clusters_dt',
                      'check2'])

    try: 
        MCR1=assigned[pd.isnull(assigned['ML_score'])==True] 
        MCR=MCR1[MCR1['clusters_dt2']==-1]  
        MCR['check2']=[
        'Manual Check Removed'
        for x in range(len(MCR))
        ]

    except KeyError or ValueError:
        MCR=pd.DataFrame(np.nan, index=[0], columns = ['index',
                      'X',
                      'item_price_num',
                      'ML_score',
                      'month',
                      'monthday',
                      'offer',
                      #'offercat',
                      'ons_item_name',
                      'ons_item_no',
                      'product_name',
                      'std_price',
                      'store',
                      'timestamp',
                      'unit_price',
                      'week',
                      'trained_cluster_new',
                      'trained_cluster2', 
                      'clusters_dt',
                      'check2'])

    try:   
        MCC1=assigned[pd.isnull(assigned['ML_score'])==True]  
        MCC=MCC1[MCC1['clusters_dt2']!=-1]    
        MCC['check2']=[
        'Manual Check Correct'
        for x in range(len(MCC))
        ]

    except KeyError or ValueError:
        MCC=pd.DataFrame(np.nan, index=[0], columns=['index',
                      'X',
                      'item_price_num',
                      'ML_score',
                      'month',

                      'monthday',

                      'offer',

                      #'offercat',

                      'ons_item_name',

                      'ons_item_no',

                      'product_name',

                      'std_price',

                      'store',

                      'timestamp',

                      'unit_price',

                      'week',

                      'trained_cluster_new',

                      'trained_cluster2', 

                      'clusters_dt',

                      'check2'])

    assigned2=CC.append(NP).append(ClM).append(CoM).append(MCR).append(MCC)

    assigned3=assigned2[pd.notnull(assigned2['monthday'])==True]  

    return(assigned3)

#%%

#test3=comparison(test2)

#%% do it all in one function

def complete(x):

    name=pd.unique(x['ons_item_name'])[0]

    numb=pd.unique(x['ons_item_no'])[0]

    if np.floor(numb/100000)==2:

        train=clusterstrain(numb,0.1,5)

        print("Clustering complete on item - ")

        print(name)

    elif np.floor(numb/100000)==3:

        train=clusterstrain(numb,0.1,5)

        print("Clustering complete on item - ")

        print(name)

    allmonths=AllDT(train)

    print("Decision Trees complete on item -")

    print(name)

    comp=comparison(allmonths)

    print("Comparison done on item -")

    print(name)
    print("cleaning completed go onto next item")
    return(comp)

#%%
#wb=df[df['ons_item_no']==310421]
#test4=complete(wb)

#%%

def cleaner():
    global df
    dfa = stacker()

    dfa = dfa.rename(columns={'ml_prediction': 'ML_score'})
    dfb=dfa[pd.notnull(dfa['std_price'])]
    df=dfb[pd.notnull(dfb['item_price_num'])] ####need to sort out this variable######
    list(df.columns)

    del dfa
    del dfb
    gc.collect()

    a =  []
    a.append(df.groupby('ons_item_no').apply(lambda L: complete(L) ))
    Results = np.concatenate(a, axis=0)  # axis = 1 would append things as new columns
    results2=pd.DataFrame(Results)

    results2.columns=['Unnamed: 0',
                      'X',
                      'item_price_num',
                      'ML_score',
                      'month',
                      'monthday',
                      'offer',
                      #'offercat',
                      'ons_item_name',
                      'ons_item_no',
                      'product_name',
                      'std_price',
                      'store',
                      'timestamp',
                      'unit_price',
                      'week',
                      'trained_cluster_new',
                      'trained_cluster2', 
                      'clusters_dt',
                      'check2',
                      'QA']
                      
    results2 = results2[results2["month"] > 201406]
    impute_ready = results2[results2['QA'] == 'Correct Classification']
    return impute_ready

def splitter(impute_ready): 
    item_list = [210111, 210113, 210204, 210213, 210214, 210302, 211305, 211501, 211709, 211710, 211807, 
                 211814, 211901, 212006, 212011, 212016, 212017, 212319, 212360, 212515, 212519, 212717, 
                 212719, 212720, 212722, 310207, 310215, 310218, 310401, 310403, 310405, 310419, 310421, 310427]

    for item in item_list:
        results = impute_ready[impute_ready["ons_item_no"]==item]
        results.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/cleaned/cleaned_chunk" + str(item) + "_main_.csv" , encoding="latin_1",index = False)
        print("Item split and ready for imputation: %s " % (str(item)))



def clean_split():
    impute_ready = cleaner()
    splitter(impute_ready)


clean_split()

#%% EVERYTHING BELOW HERE IS ONLY NEEDED IF PROCESSING ALL OF THE DATA - 

     # If you are doing that, skip the imputation step #

'''



#%%split data if necessary



item_list_1 = [210111, 210113, 210204, 210213, 210214,210302, 211305, 211501, 211709, 211710, 211807, 

              211814, 211901, 212006, 212011, 212016, 212017, 212319, 212360, 212515] 

             

item_list_2 =  [212519, 212717, 212719, 212720, 212722, 310207,310215, 310218, 310401, 310403, 310405, 

              310419, 310421, 310427]

              

              

     

def splitter(data,min_date,current_date):

    global data_split_end

    global original_clusters

    global stacker

    #data = data.transpose()

    #data_split = data[min_date:current_date].transpose() #make sure that his is the 3rd onwards due to pythonic conditions

    global original_clusters

    original_clusters = data[data["month"] == 201406]

    original_clusters.to_csv('/Users/tomhuntersmith/Desktop/ONS/Index Numbers/WebScrapingPhase2/Data/august24/original_clusters.csv',encoding = "latin-1")



    data_split_end = data[data["monthday"] > 20160803]

    data_split_end.to_csv('/Users/tomhuntersmith/Desktop/ONS/Index Numbers/WebScrapingPhase2/Data/august24/'+ str(current_date) + '_impute_.csv',encoding = "latin-1")

    stacker = pd.concat([original_clusters,data_split_end])

    impute_ready = stacker[stacker['QA'] == 'Correct Classification']

    impute_ready.to_csv('/Users/tomhuntersmith/Desktop/ONS/Index Numbers/WebScrapingPhase2/Data/august24/'+ str(current_date) + '_7dayimpute_.csv',encoding = "latin-1",index = False)

    





results2.to_csv("/Users/tomhuntersmith/Desktop/ONS/Index Numbers/WebScrapingPhase2/Data/august24/cleaned" + todays_date + "_main_.csv" , encoding="latin_1",index = False)

                      

                   

#%%    #####this section merges on previously annotated data to assist removing incorrect classifications ########



cleaned=pd.read_csv( #readfile

           '/Users/tomhuntersmith/Desktop/ONS/Index Numbers/WebScrapingPhase2/Data/august24/cleaned' + todays_date + '_main_.csv' ,encoding="latin_1"

        ).sort( #sort the dataframe

          ['ons_item_no','product_name']

          )

        

                      

unique=pd.read_csv( #readfile

           '/Users/tomhuntersmith/Desktop/ONS/Index Numbers/WebScrapingPhase2/Data/Unique_product_labels.csv' ,encoding="latin_1"

        ).sort( #sort the dataframe

          ['idvar']

        )    

        

        

cleaned["idvar"]=cleaned["product_name"]+"_"+cleaned["store"]

 

unique = unique.drop_duplicates("idvar")

 

#%%

 

cleaned2=pd.merge(cleaned,unique, left_on='idvar', right_on='idvar', how='outer')

cleaned2.to_csv("/Users/tomhuntersmith/Desktop/ONS/Index Numbers/WebScrapingPhase2/Data/august24/alldata" + todays_date + "_main_.csv" , encoding="latin_1",index = False)

                      

  

   

#%%

   

#cleaned2 = pd.read_csv("/Users/tomhuntersmith/Desktop/ONS/Index Numbers/WebScrapingPhase2/Data/august24/alldata" + todays_date + "_main_.csv" , encoding="latin_1")

    

before = cleaned2[cleaned2["monthday"] < 20160300 ]

prepared_before = before[before["label"] == 1]







#%%

after = cleaned2[cleaned2["monthday"] > 20160229 ]

#after.QA.unique()

prepared_after = after[after['QA'] == 'Correct Classification']





#%%



frames = [prepared_before,prepared_after]

cleaned_data = pd.concat(frames)



#%%

cleaned_data.to_csv("/Users/tomhuntersmith/Desktop/ONS/Index Numbers/WebScrapingPhase2/Data/august24/cleaned_data" + todays_date + "_main_.csv" , encoding="latin_1",index = False)

                      

 '''





















