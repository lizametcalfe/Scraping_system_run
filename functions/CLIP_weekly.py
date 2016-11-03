"""
Created on Friday 20th May

@author: Liz

Date:   20/05/2016
Verion: 1

Inputs:  Price data 
outputs: DataFrame with chained index for all time periods
changes - 
    added updated aggregation code
    
"""
from sklearn.cluster import DBSCAN
import pandas as pd
import numpy as np
from sklearn.tree import DecisionTreeClassifier
from scipy.stats import gmean 
from sklearn.cluster import DBSCAN
from sklearn import metrics
from sklearn.preprocessing import StandardScaler
import fuzzywuzzy
from fuzzywuzzy import fuzz

import pandas as pd
import numpy as np
import math

import os

from scipy.stats import gmean 

import numpy as np
from sklearn.cluster import MeanShift, estimate_bandwidth
from sklearn.datasets.samples_generator import make_blobs

try:
    os.remove("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIPweek2014.csv")
    os.remove("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIPweek2015.csv")
    os.remove("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIPweek2016.csv")
except:
    pass
#%%
#Import Dataset
#
#os.chdir('D:/webscraped/New_Data')


#This function sets the neccesary dataframe and removes empty prices
def man(data,datevar):
    tidata = pd.DataFrame()
    #create dummy variable for store and offer type
    shops = pd.get_dummies(data["store"])
    offercat = pd.get_dummies(data["offer_cat"])
    tidata["product_name"]=data["idvar"]
    tidata["ons_item_name"]=data["ons_item_name"]
    #set freq variable
    tidata["monthday"] = data[datevar]
    #set price type
    tidata["price"] = data["item_price_num"]
    #remove 0 or non-numeric prices
    tidata = tidata[np.isfinite(tidata['price'])]
    #group together the dataframe for later use
    tidata = pd.concat([tidata,shops,offercat],axis=1)
    zscore = lambda x: gmean(x) # Note that it does not reference anything outside of 'x' and for transform 'x' is one column.
    tidata["price"]=tidata.groupby(['product_name','monthday'])["price"].transform(zscore)
    print(tidata.head(2))
    tidata=tidata.drop_duplicates(["product_name","monthday"])
    print(len(tidata))
    return tidata

#The basetree function takes a sub-set of the data to the base month. reduces the dataframe to the varaibles that will be used within the clustering step of the CLIP, and uses DBScan clustering on this data.
# It then uses the DBScan assignments of clusters as training data within a decision tree classifier to find the underlying structure of the clusters.
#This returns the decision tree
def basetree(data, basedate):
    #just look at baseperiod
    data2 = data[data["monthday"].astype(int)==basedate]
    data3 = data2
    #reduce dataframe to the variables that will be used within the dbscan clustering (price is included here)
    del data3["ons_item_name"]
    del data3["product_name"]
    del data3["monthday"]    
    #remove duplicates so that clustering is only on unique products. This stops grouping togethers of the same product
    data3 = data3.drop_duplicates()
    data5 = np.array(data3)
    #run dbscan clustering
#    print("len_unique_prod_base")
#this seems to work well! min_bin_freq = 2, clust = True, min_sample_lead = 5
    ward = MeanShift(cluster_all=True, n_jobs=-1).fit(data5)
    if len(data5) < 100:
        dlen = 6
    else:
        dlen = len(data5)/15  
    dt = DecisionTreeClassifier(criterion = "gini", min_samples_leaf=20)
    data4 = data3
    #criterion = "entropy",
    #set up decision tree clustering on the same data used for the dbscan clustering minus price. Price is removed so that price can vary between clusters over time.
    del data4["price"]
    #fit decision tree clusters
    dt.fit(np.array(data4),ward.labels_+1)
    # return decision tree
#    print(dt.n_classes_)
    return dt
#algorithm  auto 
#The get_lineage function determines the underlying structure of the decision tree computed by the basetree function. Returning a list of rules which make up product classification rules
def get_lineage(tree, feature_names):
     left      = tree.tree_.children_left
     right     = tree.tree_.children_right
     threshold = tree.tree_.threshold
     features  = [feature_names[i] for i in tree.tree_.feature]

     # get ids of child nodes
     idx = np.argwhere(left == -1)[:,0]     

     def recurse(left, right, child, lineage=None):          
          if lineage is None:
               lineage = []
          if child in left:
               parent = np.where(left == child)[0].item()
               split = 'l'
          else:
               parent = np.where(right == child)[0].item()
               split = 'r'

          lineage.append((parent, split, threshold[parent], features[parent]))

          if parent == 0:
               lineage.reverse()
               return lineage
          else:
               return recurse(left, right, parent, lineage)
     results = []
     for child in idx:
          for node in recurse(left, right, child):
               results.append(node)
     return results

#the clus splits the data by the rules uncovered in get_lineage above'e'
def clus(data, choice):
    for i in range(0,len(choice)):
        if choice.iat[i,1] == "l":
            data = data[data[choice.iat[i,3]]<choice.iat[i,2]]
        else:
            data = data[data[choice.iat[i,3]]>choice.iat[i,2]]
    return data

#the thes function applies the decision tree to all time periods
#function to apply the desision tree to the data by time period
def thes(the,fulldata,data):
    results=pd.DataFrame()
    cluss=pd.DataFrame()
    for i in range(0,(len(the))):
        if i == len(the)-1:
            clust = data.loc[the[i]:]
            cluss = clus(fulldata, clust)
            cluss["cluster"]=i
            results = results.append(cluss)
        elif i != (len(the)-1):
            clust = data.loc[the[i]:the[i+1]-1]
            cluss = clus(fulldata, clust)
            cluss["cluster"]=i
            results = results.append(cluss)
    return results

#The baseclust function applies clus and thes to the appropriate time periods (every month separately) then returns the data with the related classifications from the decision tree rules
#The baseclust function applies clus and thes to the appropriate time periods (every month separately) then returns the data with the related classifications from the decision tree rules
def baseclust(bt, data,basedate):
    #take a subset of basedate data
    fulldata = data[data["monthday"].astype(int)==basedate]
    #set up dataframe without period variable
    del fulldata["monthday"]
    fulldata = fulldata.reset_index()
    fulldata = fulldata.drop("index",1)
    #get structure of the decision tree in a table
    structure = get_lineage(bt, fulldata.columns[1:])
    #put structure of the decision tree in a dataframe
    structure=pd.DataFrame(structure,columns=("level","dir","value","var"))
    return structure

def applydecisiontree(data,date,structure):
    #take subset of data for date of interest
    fulldata = data[data["monthday"].astype(int)==date]
    #set up dataframe how you want it
    fulldata = fulldata.drop("monthday",1)
    fulldata = fulldata.reset_index()
    fulldata = fulldata.drop("index",1)
    #find index numbers for the start of each cluster within the structure of the dataframe. Found in the baseclust
    the = np.where(structure["level"]==0)[0]
    del structure["Unnamed: 0"]
    #run the structure of the decision tree classifier (calculated on the data for the base period) over the new data for the date of interest 
    results = thes(the,fulldata,structure)
    #print(baseclust)
    return results

# "Unnamed: 0","sainsbury","tesco","waitrose","Discount","For","add more","prod_no","cluster"

#The geobase function takes the data returned from the baseclust function for the basemonth which contains the clustering assignments. Then calcualtes the geometric mean of each cluster, and returns a table of the clusters and there related geometric means.
def geobase(results):
    from scipy.stats import gmean  
    #group by clusters
    grouped = results.groupby(["cluster"])
    #fing geometric mean of the price for each cluster, and the number of observations within each cluster
    table2015 = grouped['price'].agg({'gmean':gmean,'count':len})
    table2015 = table2015.reset_index()
    #sort table by the number of the cluster, not strictly neccesary but speeds up the merging later on
    table2015.sort_values("cluster",inplace=True, axis = 0)
    #print(geobase)
    return table2015

#The geo function uses the data from the month of interest and takes the geometric mean of the prices for each cluster. 
#It then merges together the geobase results (geometric means in the base month) and the geometric means for this month and calculates a price relative across the each cluster.
#It then weights together these clusters using the size of each cluster (in the comparison time period) as it's weight and returns the price index for that item (COICOP4 level)
def geo(results,base):
    grouped = results.groupby(["cluster"])
    tables = grouped['price'].agg({'gmean':gmean,'count':len})
    table = tables.reset_index()
    table.sort_values("cluaster", inplace=True, axis = 0)
#    print("base clusters")
#    print(base)
#    print("comparison clusters")
#    print(table)
    table = pd.merge(base, table, how='left', on='cluster')
#    print("number of clusters")
#    print(len(table["cluster"]))
    table["pr"] = table.loc[:,'gmean_y']/table.loc[:,'gmean_x']
    table["pr"] = table["pr"].fillna(1)
    table["pr"] = table.apply(lambda x: 1 if x["count_y"] < (x["count_x"]/4) else x["pr"],axis=1)
    print("merge results")
    print(table)
    table.reset_index()
    weights = np.asarray((table["count_x"]).T)
    tablepr = np.asarray(table["pr"].T)
    name1 = tablepr.dot(weights)/sum(weights)*100 
    #print(geo)
    return float(name1)

#The Jevons is only used it only one cluster is formed by the function basetree. This implies either there is not enough data to use the CLIP approach, or that the data is already homogeneous and therefore the CLIP approach is not neccesary.
# The Jevons is the same approach as the unit price index. This computes price relatives between the same products for each month and the base month (January). Then a geometric average of the price relatives in each COICOP4 level item is computed. This is the unit price index for each item. 
# It returns the price index at item level (COICOP4 level)
def jevons(data,date,basedate):
    data_base_prices = data.loc[data['monthday'].astype(int) == basedate]
    data_comp_prices = data.loc[data['monthday'].astype(int) == date]
    data_base_prices=data_base_prices[['product_name','price']]
    data_base_prices.loc[:,'base_prices'] = data_base_prices.loc[:,'price']
    data_base_prices_1=data_base_prices[['product_name','base_prices']]
    groupedbase = data_base_prices_1.groupby(['product_name'])
    groupbase = groupedbase['base_prices'].agg({'gmean':gmean})
    groupedcomp = data_comp_prices.groupby('product_name')
    groupcomp = groupedcomp['price'].agg({'gmean':gmean})
    groupcomp.reset_index(inplace= True)
    groupbase.reset_index(inplace =True)
    groupbase["base_price"] = groupbase["gmean"]
    groupbase=groupbase.drop("gmean",1)
    try:
        datamerged=pd.merge(groupbase,groupcomp, how='inner', on='product_name')
        datamerged.loc[:,'price_relative'] = datamerged.loc[:,'gmean']/datamerged.loc[:,'base_price']
        datamerged['pr_log'] = datamerged['price_relative'].apply(math.log)
        datamerged["groups"] = 1
        test1 = datamerged.groupby('groups')
        lopp = test1['pr_log'].apply(np.mean).apply(np.exp)*100
        lopp =np.array(lopp)
        return lopp
    except:
        print("no product_name")
        pass

def clipped(data, date, basedate,classvalue):
    bbb = pd.DataFrame()
    bbb.loc[:,"index"] = range(0,1)
    bbb.loc[:,"ons_name"]= classvalue
    basedate = int(basedate)
    date = int(date)
    datau = data[data["monthday"].astype(int)==basedate]
    dataa = data[data["monthday"].astype(int)==date]
    if len(dataa) == 0:
        new = 100
        bbb["type"] = "No data, new = 100"
        print("No data at all for this period")
##    print("len basedata")
##    print(len(datau))
    #reduce dataframe to the variables that will be used within the dbscan clustering (price is included here)
    del datau["ons_item_name"]
    del datau["product_name"]
    del datau["monthday"]    
##    print("reducedbase data")
##    print(len(datau))
    uniquedata = datau.drop_duplicates()
 ##   print("unique basedata")
##    print(len(uniquedata))
##    print("percent of unqiue")
##    print(len(uniquedata)/100)*nn
    if len(uniquedata) < 20:
        print("criteria")
        print((len(uniquedata)/100)*10)
        # new=pd.DataFrame()
        # new=pd.DataFrame()
        try:
            new = jevons(pd.DataFrame(data),date,basedate)
            new = float(new)
        except:
            new =100
            print("not enough data to cluster!")
        print("jevons")
        bbb["type"] = "Jevons"
    elif len(uniquedata) >=20:
        data.loc[:, 'prod_no'] = data["product_name"].apply(lambda x: fuzz.ratio(classvalue,x))
        data.loc[:,"prod_no"] = (data["prod_no"] - data["prod_no"].mean()) / (data["prod_no"].max() - data["prod_no"].min())
        bt = basetree(data,basedate)
        if bt.n_classes_ <=2:
            try:
                new = jevons(pd.DataFrame(data),date,basedate)
                new = float(new)
            except:
                new =100
                print("not enough data!")
                print("jevons")
            bbb["type"] = "jevons"
        elif  bt.n_classes_ > 2:
            try:
                data2=data.drop("product_name",1)
                data2 = data2.drop("ons_item_name",1)
                if date == basedate:
                    print("basemonth")
                    basestructure = baseclust(bt, data2,basedate)
                    basestructure.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/basestructureweekly")
                    basedclust = applydecisiontree(data2,basedate,pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/basestructureweekly"))
                    basedclust.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/basedclustweekly")
                clust = applydecisiontree(data2,date,pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/basestructureweekly"))
                new = pd.DataFrame()
                new = geo(clust, geobase(pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/basedclustweekly")))
                bbb["type"] = "CLIP"
            except:
                try:
                    new = jevons(pd.DataFrame(data),date,basedate)
                    new = float(new)
                except:
                    new =100
                    print("not enough data!")
                print("jevons")
            bbb["type"] = "CLIP"
        else:
            new = "wrong"
            print("wrong")
    ab= pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/metadataweek.csv")
    ab = ab.append(bbb)
    print(ab)
    del ab["Unnamed: 0"]
    ab.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/metadataweek.csv")
    print(new)
    return new

def CLIPPEDdate(data, idvar,classvar,pricevar,datevar,basedate):
    classvalue = np.unique(data[classvar])[0]
    print(classvalue)
    date = pd.DataFrame(np.unique(data[datevar]),columns=["dates"]).dropna()
    date.sort_values(by="dates")
    dates = date["dates"]
    T = len(date)
    CLIP = date["dates"].apply(lambda x:clipped(data,x,basedate,classvalue))
    df1 = pd.DataFrame({"i" : range(0,len(date)),"period":dates,"CLIP":CLIP, "ons_item_name":classvalue})
    df1.index = range(T)
    return df1

####monthly (updated) ########
x = pd.read_csv('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/averages/_week_.csv',encoding="latin-1")
x.offer = x.offer.str.encode('utf-8')

#change format of the dicount/offer category
def offer_check(dd):
    terms = ["buy","purchase","the"]
    for j in terms:
        if j in dd: 
            return "BOGO"         
    terms = ["off","was","save","half","now","only"] # perhaps add,only??
    for j in terms:
        if j in dd:
            return "Discount"
    terms = ["special purchase"]
    for j in terms:
        if j in dd: 
            return "Special Purchase"
    terms = ["add","get"]
    for j in terms:
        if j in dd: 
            return "add more"
    terms = ["clear","reduced"]
    for j in terms:
        if j in dd:
            return "reduced to clear"

def produce_features(df):
    df["offer"] =df["offer"].apply(lambda x: str(x))
    df["offer"] =df["offer"].apply(lambda x: x.lower())
    df["offer_cat"] =df["offer"].apply(lambda x: offer_check(x))
    return df

x = produce_features(x)

x = x[x["item_price_num"]>0]
x["ons_item_name"] = x["ons_item_name"].apply(lambda x: x.strip())
x["idvar"]=x["product_name"]+"_"+x["store"]

df=x[x["week"]>201422]

df["month"]=df["week"]
#del df["yearweekno"]

df = man(df,"month")
print(df.head(3))

df14 = df[df["monthday"]<201502]
df15 = df[df["monthday"]>=201501]
df15 = df15[df15["monthday"]<201602]
df16 = df[df["monthday"]>=201601]

df14 = df14[df14["ons_item_name"]!="potatoes, baking"]
df15 = df15[df15["ons_item_name"]!="potatoes, baking"]
df16 = df16[df16["ons_item_name"]!="potatoes, baking"]

df14 = df14[df14["ons_item_name"]!="fizzy bottled drink"]
df15 = df15[df15["ons_item_name"]!="fizzy bottled drink"]
df16 = df16[df16["ons_item_name"]!="fizzy bottled drink"]



def runthrough(data,basedate):
    a = []
    for i in np.unique(data["ons_item_name"]):
        a.append(CLIPPEDdate(data[data["ons_item_name"] == i], 'idvar', 'ons_item_name','item_price_num', 'monthday', basedate))
    aa = np.concatenate(a, axis=0)  # axis = 1 would append things as new columns
    aa=pd.DataFrame(aa)
    aa.columns=["CLIP","i","ons_item_name","period"]

    os.remove("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/basestructureweekly")
    os.remove("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/basedclustweekly")
 
    return aa
    
adf = runthrough(df14, 201423)
adf.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIPweek2014.csv")

bdf = runthrough(df15, 201501)
bdf.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIPweek2015.csv")

cdf = runthrough(df16, 201601)
cdf.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIPweek2016.csv")

abweek = pd.DataFrame()
abweek.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/metadataweek.csv")
