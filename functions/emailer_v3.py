# -*- coding: utf-8 -*-
"""
Created on Wed May  4 13:07:39 2016

@author: Mint

Date:   07/04/2016
Verion: 0.2

Inputs:  WPrice indices
outputs: Email to peole who want one
 """

#%%

import smtplib

import schedule 
import datetime
from datetime import timedelta
import time
import pandas as pd
import schedule 
import csv
import json
import datetime
import time
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient
import smtplib
import base64
import os


#%%
#set path
os.chdir('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/price_indices')
#create variable which contains todays date
todays_date = str(datetime.date.today())
    
import smtplib
import csv
from email.mime.multipart import MIMEMultipart
from email.message import Message
from email.mime.text import MIMEText

from email import encoders
from email.mime.base import MIMEBase
import os

#create and send email containing the different, finished price indicies and a JSON file that can be used with Alessandra's code in the github account: 
# https://github.com/ONSPrices/PricesTool
# password: importio2015
#here is the tool https://onsprices.github.io/PricesTool/index.html

def emailer():
    COMMASPACE = ', '
    #SET MESSAGE FOR THE EMAIL
    msg = MIMEMultipart()
    msg['Subject'] =  "Good morning! Here are your experimental Indices"
    #set where the email goes from
    emailfrom = "onswebscraping@gmail.com" #// Giving proper outlook mail id 
    #Set who the email goes to (add here if you would like an email)
    emailto = ['lizametcalfe@gmail.com', 'eddrowland@gmail.com', 'matthewmayhew1@gmail.com']# // // Giving proper outlook mail id lists
    todays_date = str(datetime.date.today()) 
    #set message
    msg['From'] = emailfrom
    msg['To'] = COMMASPACE.join(emailto)
    msg.preamble = "Good morning! Here are your experimental Indices"
    #select files to be uploaded to the email
    csvfiles = ['month_'+ todays_date + '_GEKS_item.csv','week_'+ todays_date + '_GEKS_item.csv', 
                'month_'+ todays_date + '_GEKS_agg.csv', 'week_'+ todays_date + '_GEKS_agg.csv',
                'DC_daily_itemlevel_'+ todays_date +'_.csv', 'DC_weekly_itemlevel_'+ todays_date +'_.csv',
                'DC_monthly_itemlevel_'+ todays_date +'_.csv', 'DC_daily_agg_'+todays_date+'_.csv','DC_weekly_agg_'+ todays_date +'_.csv',
                'DC_monthly_agg_'+ todays_date +'_.csv', 'unitdoublechained_'+ todays_date +'_.csv',
                'unitdoublechaineditemlevel_'+ todays_date +'_.csv', 'unitdoublechaineditemlevelweek_'+ todays_date +'_.csv',
                'unitdoublechainedweek_'+ todays_date +'_.csv','CLIPdoublechained'+todays_date+'.csv', 'CLIPdoublechaineditemlevel'+todays_date+'.csv',
                'CLIPdoublechainedweek'+todays_date+'.csv','CLIPdoublechaineditemlevelweek'+todays_date+'.csv','test.json'] 
# leave out daily GEKS atm as process time is substantial 'monthday_'+ todays_date + '_GEKS_agg.csv',,'monthday_'+ todays_date + '_GEKS_item.csv',
    for csv in csvfiles:
        #add csv's to the email
        print csv
        with open(csv) as fp:
            record = MIMEBase('application', 'octet-stream')
            record.set_payload(fp.read())
            encoders.encode_base64(record)
            record.add_header('Content-Disposition', 'attachment',
                                  filename=os.path.basename(csv))
        msg.attach(record)

    print "Message sucessfully delievered"
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login(emailfrom, 'ScrapeMyWeb')
    server.sendmail(emailfrom, emailto, msg.as_string())
    server.quit()
    
#emailer()
#%%
#emailer()

#####Can choose from any of these options#########
#schedule.every().monday.do(job)
#schedule.every().wednesday.at("13:15").do(job)
#schedule.every(1).minutes.do(emailer)
#schedule.every().hour.do(job)
#schedule.every().day.at("08:58").do(emailer)

#while True:
#    schedule.run_pending()
#    time.sleep(1)


#%%  OLDER VERSION OF EMAILER (WHICH DOES WORK)
'''

def mail_send():

    to = 'thomas.smith@ons.gsi.gov.uk'
    gmail_user = 'tomhuntersmith@gmail.com'
    gmail_pwd = 'Twitter123456'
    smtpserver = smtplib.SMTP("smtp.gmail.com",587)
    smtpserver.ehlo()
    smtpserver.starttls()
    smtpserver.ehlo
    smtpserver.login(gmail_user, gmail_pwd)
    header = 'To:' + to + '\n' + 'From: ' + gmail_user + '\n' + 'Subject:testing \n'
    print header
    msg = header + '\n this is test msg from mkyong.com \n\n'
    smtpserver.sendmail(gmail_user, to, msg)
    print 'done!'
    smtpserver.close()
    
mail_send()
#schedule.every(1).minutes.do(mail_send)
#schedule.every().hour.do(job)
#schedule.every().day.at("08:58").do(mail_send)
#schedule.every().monday.do(job)
#schedule.every().wednesday.at("13:15").do(job)

#while True:
 #   schedule.run_pending()
  #  time.sleep(1)

'''















