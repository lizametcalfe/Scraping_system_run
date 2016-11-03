import pandas as pd
import numpy as np
import datetime

unit2014= pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIP2014.csv")
unit2015= pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIP2015.csv")
unit2016= pd.read_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIP2016.csv")

unit2014 = unit2014.fillna(100)
unit2015 = unit2015.fillna(100)
unit2016 = unit2016.fillna(100)

#create todays date
todays_date = str(datetime.date.today())

#rebase to June 2014 if neccessary
def rebasetojune(data,datevar,basedate,index):
    date = np.unique(data[datevar])
    basedate = data[data[datevar]==basedate][index]
    data["CLIP"] = data["CLIP"].apply(lambda x: (x/basedate)*100)
    return data    

unit14=[]
unit14.append(unit2014.groupby('ons_item_name').apply(lambda L: rebasetojune(L,"period",201406,"CLIP")))

unit14 = np.concatenate(unit14, axis=0)  # axis = 1 would append things as new columns
unit14=pd.DataFrame(unit14)
unit2014.columns=["Unnamed: 0",  "CLIP","i", "ons_item_name", "period"]

#double chain link in two parts
#chain jan to dec at coicop4 level
def itemchain(df,baseperiod,indices):
    df=df.reset_index()
    df=df.reset_index()
    index=df['level_0']
    for x in index:
        period = df["period"]
        if period[x] > baseperiod:
            df.loc[x,indices] = float(df.loc[x,indices])*float(df.loc[df["period"]==baseperiod,indices])/100
                #        df.loc[len(df.index)-1,"weighted_index"] = df.loc[len(df.index)-1,"weighted_index"]*df.loc[len(df.index)-2,"weighted_index"]/100
#        df["weighted_index"] = df.apply(lambda row: float(row["weighted_index"])*float(df.loc[df["period"]==201501,"weighted_index"])/100 if row["year"]=="2015" else row["weighted_index"], axis=1)
    return df

def singlechainlink(originalyear, chainedyear,chainedyear2, basedate,basedate2,freq):
    #single chain link for new year added
    unit2015 = chainedyear[chainedyear["period"]!=basedate]
    unit2016 = chainedyear2[chainedyear2["period"]!=basedate2]
    aaa1415=pd.concat([originalyear,unit2015],axis=0)
    aaa15=[]
    aaa15.append(aaa1415.groupby('ons_item_name').apply(lambda L: itemchain(L,basedate,'CLIP')))
    aaa15 = np.concatenate(aaa15, axis=0)
    aaa15 = pd.DataFrame(aaa15)
    aaa15.columns=["Unnamed: 0", "Unnamed: 1", "Unnamed: 2","CLIP","i","ons_item_name","period"]
    aaa1516=pd.concat([aaa15,unit2016],axis=0)
    aaa16=[]
    aaa16.append(aaa1516.groupby('ons_item_name').apply(lambda L: itemchain(L,basedate2,"CLIP")))
    aaa16 = np.concatenate(aaa16, axis=0)
    aaa16 = pd.DataFrame(aaa16)
    aaa16.columns=["Unnamed: 0", "Unnamed: 1","CLIP", "Unnamed: 3","Unnamed: 4","Unnamed: 5","Unnamed: 6", "ons_item_name", "period"]
    return aaa16

upto2016 = singlechainlink(unit2014, unit2015,unit2016, 201501,201601,"month")

upto2016.to_csv("/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices/CLIPdoublechaineditemlevel"+todays_date+".csv")