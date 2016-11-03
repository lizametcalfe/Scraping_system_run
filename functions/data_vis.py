# -*- coding: utf-8 -*-
"""
Created on Wed Oct 20

@author: Mint

Verion: 0.1

Inputs:  Price index results
outputs: Data visualisation: Index_v1.html
"""

import pandas as pd
import numpy as np
import datetime
import time

todays_date = str(datetime.date.today())

#The names of the products need to fit the html code, this function takes the ons_item_no and changes it to the neccessary product name
def changeup(dci, level):
    dci[level][dci[level]==212717] = "010106Apples Dessert Per Kg"
    dci[level][dci[level]==212719] = "010106Bananas Per Kg"
    dci[level][dci[level]==210213] ="010101Breakfast Cereal 1"
    dci[level][dci[level]==210214] ="010101Breakfast Cereal 2"
    dci[level][dci[level]==211501] ="010104Cheddar Home Produced Per Kg"
    dci[level][dci[level]==212011] ="010202Cola Flavoured Drink 2 Ltr Btl"
    dci[level][dci[level]==210204] ="010101Dry Spaghetti Or Pasta 500G"
    dci[level][dci[level]==212519] ="010107Fresh Veg Onions Per Kg"
    dci[level][dci[level]==212515] ="010107Fresh Veg Tomatoes Per Kg"
    dci[level][dci[level]==212016] ="010202Fresh_Chilled Orange Juice 1L"
    dci[level][dci[level]==212006] ="010202Fruit Juice Not Orange 1L"
    dci[level][dci[level]==212722] ="010106Grapes Per Kg"
    dci[level][dci[level]==210302] ="010101Plain Biscuits 200-300G"
    dci[level][dci[level]==212319] ="010107Potatoes New Per Kg"
    dci[level][dci[level]==212360] ="010107Potatoes Old White Per Kg"
    dci[level][dci[level]==211709] ="010104Shop Milk Whole Milk 4Pt_2Ltr"
    dci[level][dci[level]==211710] ="010104Shop Milk Semi Skimmed"
    dci[level][dci[level]==211305] ="010105Spreadable Butter 40-70% Butter"
    dci[level][dci[level]==212720] ="010106Strawberries Per Kg Or Punnet"
    dci[level][dci[level]==211901] ="010201Tea Bags 1 Packet Of 80 (250G)"
    dci[level][dci[level]==210111] ="010101White Sliced Loaf Branded 800G"
    dci[level][dci[level]==210113] ="010101Wholemeal Sliced Loaf Branded"
    dci[level][dci[level]==211814] ="010104Yoghurt Small Individual"
    dci[level][dci[level]==211807] ="010104Yoghurt_Fromage 4-6Pk"
    dci[level][dci[level]==310218] ="020102Apple Cider 500-750Ml 4.5-5.5%"
    dci[level][dci[level]==310207] ="020103Bitter 4 Cans 440-500Ml"
    dci[level][dci[level]==310405] ="020101Brandy 68-70 Cl Bottle"
    dci[level][dci[level]==310215] ="020103Lager 4 Bottles Premium"
    dci[level][dci[level]==310421] ="020102Red Wine European 75Cl"
    dci[level][dci[level]==310427] ="020101Rum White Bottle"
    dci[level][dci[level]==310403] ="020101Vodka 70 Cl Bottle"
    dci[level][dci[level]==310401] ="2020101Whisky 70 Cl Bottle"
    dci[level][dci[level]==310419] ="020102White Wine European 75Cl"
    dci=dci.rename(columns = {level:'category'})
    return dci

#price monthly agg
##This function changes the format of the monthyl aggregated price indices so that they fit the data visualisation
def monthlyagg(infile, outfile, index,ind, level, period,value, alc):
    infile = pd.read_csv(infile)
    #take subset in neccessary order using names of variables within the file
    infile = infile[[ind,level, period,value]]
    #rename the variables
    infile.columns=["index","category","date","value"]
    #add index to be used by the data visualisation
    infile["index"] = index
    infile = infile[["category","index","value","date"]]
    # change format of data
    infile["date"] = infile["date"].apply(lambda x: "01/"+str(x)[4:6]+"/"+str(x)[2:4])
    #change name of the category inline with the data visualisation
    infile["category"] = infile["category"].apply(lambda x: "020000Alcoholic Beverages" if x == alc else "010000Food & Non-Alcoholic Beverages")
    infile.to_csv(outfile)

#price monthly itemlevel
##This function changes the format of the monthly itemlevel price indices so that they fit the data visualisation
def monthlyitem(infile,outfilef, outfilea, priceindex, index, period, level, alc):
    infile = pd.read_csv(infile)
    infile = infile[[priceindex, period, level]]
    #import file which has the items and there class level in
    join = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Weights/join.csv")
    #merge together items and join file so that the class level of each item is known
    infile = pd.merge(infile,join,left_on=level, right_on="join", how="left")
    #change the names of the products to those used by the data vis
    infile = changeup(infile, level)
    #add index
    infile["index"] = index
    #change variables names and order
    infile=infile[["category","index",priceindex,period,"level_3"]]
    infile.columns=["category","index","value","date","level_3"]
    #change date format
    infile["date"] = infile["date"].apply(lambda x: "01/"+str(x)[4:6]+"/"+str(x)[2:4])
    #split dataframe into alcohol, and food items
    infilef = infile[infile["level_3"]!= alc]
    infilea = infile[infile["level_3"]==alc]
    infilef.to_csv(outfilef)
    infilea.to_csv(outfilea)

#price weekly agg
##This function changes the format of the weekly aggregated price indices so that they fit the data visualisation
def weekagg(infile, outfile, index, ind, level, period,value, alc):
    infile = pd.read_csv(infile)
    #subset and change variables names
    infile = infile[[ind,level, period,value]]
    infile.columns=["index","category","date","value"]
    #add index
    infile["index"] = index
    infile=infile[["category","index","value","date"]]
    #change date format (this is tricky as the initial date format is in Yearweek number and needs to be in d/m/y)
    infile["date"] = infile["date"].apply(lambda x: "20"+str(x)[2:4]+"-"+"W"+str(x)[4:6])
    infile["date"]=infile["date"].apply(lambda x: datetime.datetime.strptime(x + '-1', "%Y-W%W-%w"))
    infile["date"] = infile["date"].apply(lambda x: str(x)[8:10]+"/"+str(x)[5:7]+"/"+str(x)[2:4])
    #change cateogry names
    infile["category"] = infile["category"].apply(lambda x: "020000Alcoholic Beverages" if x == alc else "010000Food & Non-Alcoholic Beverages")
    infile.to_csv(outfile)

#price weekly itemlevel
##This function changes the format of the weekly itemlevel price indices so that they fit the data visualisation
def weekitem(infile, outfilef, outfilea, priceindex, index, period, level, alc):
    infile = pd.read_csv(infile)
    #import file which has the items and there class level in
    join = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Weights/join.csv")
        #merge together items and join file so that the class level of each item is known
    infile = pd.merge(infile,join,left_on=level, right_on="join", how="left")
    #change the names of the products to those used by the data vis
    infile = changeup(infile, level)
    #add index
    infile["index"] = index
    infile=infile[["category","index",priceindex,period,"level_3"]]
    infile.columns=["category","index","value","date","level_3"]
    #change date format
    infile["date"] = infile["date"].apply(lambda x: "20"+str(x)[2:4]+"-"+"W"+str(x)[4:6])
    infile["date"]=infile["date"].apply(lambda x: datetime.datetime.strptime(x + '-1', "%Y-W%W-%w"))
    infile["date"] = infile["date"].apply(lambda x: str(x)[8:10]+"/"+str(x)[5:7]+"/"+str(x)[2:4])
    #take subsets of food and alcohol
    infilef = infile[infile["level_3"]!=alc]
    infilea = infile[infile["level_3"]==alc]
    infilef.to_csv(outfilef)
    infilea.to_csv(outfilea)

##run Geks, unit price and daily changed monthly agg, itemlevel, weekly aggregation and weekly itemlevel. Because the variables names are different in the different price index files a large number of inputs need to be put in. Format:
#To add another price index write use these four functions.

#Geks monthly agg
gma = monthlyagg("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/month_"+todays_date+"_GEKS_agg.csv"
,"/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/geks_monthagg.csv", 
                 "Index0", "Unnamed: 0", "Top", "period","weighted_index","Alcoholic Drinks")

#Geks monthly item level
gmi = monthlyitem("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/month_"+todays_date+"_GEKS_item.csv",
                  "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/geks_monthly_itemaf.csv",
            "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/geks_monthly_itema.csv","GEKS", "Index0", "period", "ons_item_no","Alcoholic Drinks")

#Geks weekly agg
gwa = weekagg("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/week_"+todays_date+"_GEKS_agg.csv",
              "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/geks_weekly_agg.csv", "Index0",
              "Unnamed: 0", "Top", "period","weighted_index","Alcoholic Drinks")

# Geks weekly item level
gwi = weekitem("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/week_"+todays_date+"_GEKS_item.csv",
               "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/geks_weekly_itemf.csv", "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/geks_weekly_itema.csv",
               "GEKS", "Index0", "period", "ons_item_no", "Alcoholic Drinks")

#Unit monthly agg
uma = monthlyagg("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/unitdoublechained_"+todays_date+"_.csv"
,"/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/unit_monthagg.csv", "Index1"
                , "Unnamed: 0", "level_3", "period","weighted_index","alcoholic_drinks ")

#Unit monthly item level
umi = monthlyitem("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/unitdoublechaineditemlevel_"+todays_date+"_.csv",
                  "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/unit_monthly_itemaf.csv",
            "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/unit_monthly_itema.csv","unit", "Index1", "period", "ons_item_number","alcoholic_drinks ")

#Unit weekly agg
uwa = weekagg("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/unitdoublechainedweek_"+todays_date+"_.csv",
              "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/unit_weekly_agg.csv", "Index1"
              , "Unnamed: 0", "level_3", "period","weighted_index","alcoholic_drinks ")

# Unit weekly item level
uwi = weekitem("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/unitdoublechaineditemlevelweek_"+todays_date+"_.csv",
               "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/unit_weekly_itemf.csv", "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/unit_weekly_itema.csv",
               "unit", "Index1","period", "ons_item_number","alcoholic_drinks ")

#Daily chained monthly agg
dma = monthlyagg("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/DC_monthly_agg_"+todays_date+"_.csv"
,"/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/dc_monthagg.csv", "Index2"
                 , "Unnamed: 0", "level", "month","Chainedaverage","alcoholic_drinks ")

#Daily chained monthly item level
dmi = monthlyitem("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/DC_monthly_itemlevel_"+todays_date+"_.csv",
                  "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/dc_monthly_itemaf.csv",
            "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/dc_monthly_itema.csv","Chainedaverage", "Index2", "month","level","alcoholic_drinks ")


#Daily chained weekly agg
dwa = weekagg("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/DC_weekly_agg_"+todays_date+"_.csv",
              "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/dc_weekly_agg.csv", "Index2"
             , "Unnamed: 0", "level", "month","Chainedaverage","alcoholic_drinks ")

#Daily chained weekly item level
dwi = weekitem("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/DC_weekly_itemlevel_"+todays_date+"_.csv",
               "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/dc_weekly_itemf.csv", "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/dc_weekly_itema.csv",
               "Chainedaverage", "Index2", "month", "level","alcoholic_drinks ")


#CLIP monthly agg
cma = monthlyagg("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIPdoublechained"+todays_date+".csv"
,"/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/clip_monthagg.csv", "Index3"
                 , "Unnamed: 0", "level_3", "period","weighted_index","alcoholic_drinks ")

join = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Weights/join.csv")
join["ons_item_name"] = join["ons_item_name"].apply(lambda x: str(x).lower())
cpmonthitem = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIPdoublechaineditemlevel"+todays_date+".csv")
cpmonthitem = pd.merge(cpmonthitem, join, on="ons_item_name", how="left")
del cpmonthitem["level_3"]
cpmonthitem=cpmonthitem.rename(columns = {'join':'category'})
cpmonthitem.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIPdoublechaineditemlevel"+todays_date+"2.csv")

#CLIP chained monthly item level
cmi = monthlyitem("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIPdoublechaineditemlevel"+todays_date+"2.csv",
                  "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/clip_monthly_itemaf.csv",
            "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/clip_monthly_itema.csv","CLIP", "Index3", "period","category","alcoholic_drinks ")


#CLIP chained weekly agg
cwa = weekagg("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIPdoublechainedweek"+todays_date+".csv",
              "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/clip_weekly_agg.csv", "Index3"
             , "Unnamed: 0", "level_3", "period","weighted_index","alcoholic_drinks ")

join = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Weights/join.csv")
join["ons_item_name"] = join["ons_item_name"].apply(lambda x: str(x).lower())
cpweekitem = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIPdoublechaineditemlevelweek"+todays_date+".csv")
cpweekitem = pd.merge(cpweekitem, join, on="ons_item_name", how="left")
del cpweekitem["level_3"]
cpweekitem=cpweekitem.rename(columns = {'join':'category'})
cpweekitem.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIPdoublechaineditemlevelweek"+todays_date+"2.csv")


#weekitem = weekitem[["ons_item_name", "period", "CLIP", "join_x"]]

#CLIP chained weekly item level
cwi = weekitem("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIPdoublechaineditemlevelweek"+todays_date+"2.csv","/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/clip_weekly_itemf.csv", "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/clip_weekly_itema.csv",
               "CLIP", "Index3", "period", "category","alcoholic_drinks ")



##Monthly bring together, read in data, join it together and save
gm = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/geks_monthagg.csv")
um = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/unit_monthagg.csv")
dm = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/dc_monthagg.csv")
cm = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/clip_monthagg.csv")


gmm = pd.concat([gm,um,dm,cm])

del gmm["Unnamed: 0"]
gmm.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/working/month.csv")

#Put the different price indices monthly aggregates into the neccessary json format
import csv
import json
outputm = {'Overview': []}
with open("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/working/month.csv") as csv_file:
    for person in csv.DictReader(csv_file):
        outputm['Overview'].append({
                'category': person['category'],
                'index': person['index'],
                'value': person['value'],
                'date': person['date']
        })

## Monthly item level, load data, join, save
ggmf =pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/geks_monthly_itemaf.csv")
ggma=pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/geks_monthly_itema.csv")
                  
uumf = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/unit_monthly_itemaf.csv")
uuma = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/unit_monthly_itema.csv")

dcmf=pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/dc_monthly_itemaf.csv")
dcma=pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/dc_monthly_itema.csv")
      
ccmf=pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/clip_monthly_itemaf.csv")
ccma=pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/clip_monthly_itema.csv")
   
allmf = pd.concat([dcmf, uumf,ggmf, ccmf])
#allmf = pd.concat([dcmf, uumf])
allmf.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/working/monthfood.csv")

allma = pd.concat([dcma, uuma,ggma, ccma])
#allma = pd.concat([dcma, uuma])
allma.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/working/monthalc.csv")

#Put the different price indices monthly item level into the neccessary json format (a mean alcohol, f means food)
import csv
import json
outputmf = {'Food': []}
with open('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/working/monthfood.csv') as csv_file:
    for person in csv.DictReader(csv_file):
        outputmf['Food'].append({
            'category': person['category'],
            'index': person['index'],
            'value': person['value'],
            'date': person['date']
        })

outputma = {'Alcoholic': []}
with open('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/working/monthalc.csv') as csv_file:
    for person in csv.DictReader(csv_file):
        outputma['Alcoholic'].append({
            'category': person['category'],
            'index': person['index'],
            'value': person['value'],
            'date': person['date']
        })

##Weekly agg level
## Monthly item level, load data, join, save
gww = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/geks_weekly_agg.csv")
uww = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/unit_weekly_agg.csv")
dww = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/dc_weekly_agg.csv")
cww = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/clip_weekly_agg.csv")
       
allwa = pd.concat([gww, uww,dww, cww])
#allwa = pd.concat([uww,dww])

allwa.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/working/week.csv")


#Put the different price indices weekly aggregate into the neccessary json format
import csv
import json
outputw = {'Overview': []}
with open("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/working/week.csv") as csv_file:
    for person in csv.DictReader(csv_file):
        outputw['Overview'].append({
                'category': person['category'],
                'index': person['index'],
                'value': person['value'],
                'date': person['date']
        })

##Weekly item level
## Monthly item level, load data, join, save
dcif= pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/dc_weekly_itemf.csv")
dcia = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/dc_weekly_itema.csv")

uwif = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/unit_weekly_itemf.csv")
uwia = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/unit_weekly_itema.csv")               

gwf = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/geks_weekly_itemf.csv")
gwa = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/geks_weekly_itema.csv")
     
cwf = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/clip_weekly_itemf.csv")
cwa = pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/working_data/clip_weekly_itema.csv")
    

allf = pd.concat([dcif,uwif, gwf, cwf])
allf.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/working/weekfood.csv")

alla = pd.concat([dcia,uwia, gwa, cwa])
alla.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/working/weekalc.csv")


#Put the different price indices weekly item level into the neccessary json format (a mean alcohol, f means food)
import csv
import json
outputwf = {'Food': []}
with open('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/working/weekfood.csv') as csv_file:
    for person in csv.DictReader(csv_file):
        outputwf['Food'].append({
            'category': person['category'],
            'index': person['index'],
            'value': person['value'],
            'date': person['date']
        })

outputwa = {'Alcoholic': []}
with open('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/working/weekalc.csv') as csv_file:
    for person in csv.DictReader(csv_file):
        outputwa['Alcoholic'].append({
            'category': person['category'],
            'index': person['index'],
            'value': person['value'],
            'date': person['date']
        })

##Pull all together and save
def merge_two_dicts(x, y):
    '''Given two dicts, merge them into a new dict as a shallow copy.'''
    z = x.copy()
    z.update(y)
    return z

#Pull all of the different price indices together in the correct json format
pi = {"Monthly": {"0": outputm,"1": merge_two_dicts(outputmf, outputma)},"Weekly":{"0": outputw,"1": merge_two_dicts(outputwf, outputwa)}}

#save to file
import json
with open("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/data/test.json", 'w') as outfile:
    json.dump(pi, outfile)

import json
with open("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/test.json", 'w') as outfile:
    json.dump(pi, outfile)
#Open /media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/PricesTool/index_v1.html to see data vis