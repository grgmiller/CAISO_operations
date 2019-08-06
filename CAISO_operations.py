
#   Developed by Greg Miller, grmiller@ucdavis.edu
#   Version 3.04
#   Last Updated August 5, 2019

#   Purpose: To compile publicly-available CAISO system-wide electricity demand, supply, and emissions data into a csv file
#   Currently configured to coninue downloading data until the most recent data has been downloaded.

#   All directories and files will be created the first time you run the script
#   Run in unbuffered mode to make sure time.sleep() works: $ python -u 
#%%
from bs4 import BeautifulSoup
import csv
from datetime import datetime, timedelta
from functools import reduce
import math
import numpy as np
import openpyxl
import os 
import pandas as pd
from pathlib import Path
import pytz
import requests
import selenium
from selenium import webdriver 
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC 
from selenium.common.exceptions import ElementNotVisibleException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
import shelve
import sys
import time

start_i = time.time() #start initialization timer

#environment variables
demandURL = "http://www.caiso.com/TodaysOutlook/Pages/default.aspx"
supplyURL = "http://www.caiso.com/TodaysOutlook/Pages/supply.aspx"
emissionsURL = "http://www.caiso.com/TodaysOutlook/Pages/emissions.aspx"
curtailURL = "http://www.caiso.com/informed/Pages/ManagingOversupply.aspx#dailyCurtailment"
downloads = Path.cwd() / 'downloads'
curtailments = Path.cwd() / 'curtailments'
dataFile = Path.cwd() / "outputs/CAISOdata.csv"
dataFile_dtypes = {'month': 'uint8', 'day': 'uint8', 'weekday': 'uint8', 'hour': 'uint8', 'interval': 'uint8', \
'demand_DayAF': 'uint16', 'demand_HourAF': 'uint16', 'demand_actual': 'uint16', 'demand_net': 'uint16', \
'wind_curtail_MW': 'float32', 'solar_curtail_MW': 'float32', 'solar_MW': 'uint16', 'wind_MW': 'uint16', 'geothermal_MW': 'uint16', \
'biomass_MW': 'uint16', 'biogas_MW': 'uint16', 'sm_hydro_MW': 'uint16', 'battery_MW': 'int8', 'renewable_MW': 'uint16', 'natgas_MW': \
'uint16', 'lg_hydro_MW': 'uint16', 'imports_MW': 'int16', 'nuclear_MW': 'uint16', 'coal_MW': 'uint8', 'other_MW': 'uint8', 'imports_co2': \
'int16', 'natgas_co2': 'uint16', 'biogas_co2': 'uint16', 'biomass_co2': 'uint8', 'geothermal_co2': 'uint8', 'coal_co2': 'uint8'}
ct_dtypes = {'Hour': 'uint8', 'Interval': 'uint8', 'Wind Curtailment': 'float32', 'Solar Curtailment': 'float32'}
shelf = Path.cwd() / 'shelf.db'

def main():
    #----- start initialization -----#
    print('  Initializing...')
    directories = ['outputs','downloads','curtailments']
    for d in directories: #if the directories don't exist, create them
        directory = Path.cwd() / d
        if not directory.exists():
            os.makedirs(d)
            print('  '+str(d)+' directory created.')
    if not Path(shelf.stem+'.db.dat').exists():
        with shelve.open(str(shelf)) as s:
            s['caiso'] = {
                'latestDate': '',
                'postDate': '',
                'ct_latestDate': '',
            }
    user_initialized = 0 #track whether the start date is inputted by the user (1) or read from an existing output file (0)
    if not dataFile.exists():
        with open(dataFile, 'w+', newline=''):
            pass
        print('  New CSV output file created.\n  Please check the date dropdown menu for one of the charts at http://www.caiso.com/TodaysOutlook/Pages/default.aspx \n  and enter an available date to start data collection (formatted as "MM/DD/YYYY"):')
        latestDate = input('  >')
        user_initialized += 1
        while True:
            try:
                latestDate = datetime.strptime(latestDate, '%m/%d/%Y')
                break
            except:
                print('  Date format not recognized.\n  Please enter a date formatted as "MM/DD/YYYY":')
                latestDate = input('  >')
        latestDate = datetime.strftime(latestDate - timedelta(days=1), '%m/%d/%Y')
        with shelve.open(str(shelf), writeback=True) as s:
            s['caiso']['latestDate'] = latestDate
    #----- end initialization -----#
    latest = checkLatest()
    latestDate_dt = latest[0]
    dataDate = latest[1]
    browser = webdriverConfig() #configure the webdriver that will be used for data collection
    yesterday = datetime.now() - timedelta(days=1) #create a datetime object for yesterday's date
    count = 1
    end_i = time.time() #end initialization timer
    print('Initialization time = '+str(end_i-start_i)+' seconds') #timer 
    curtail_df = downloadCurtailment(browser, user_initialized) #only needs to run once for each time the code runs
    while latestDate_dt.date() < yesterday.date(): #continue downloading and appending data until the most recent data has been added
        start = time.time()
        tmpDelete('downloads')
        downloadDemand(browser, dataDate)
        downloadSupply(browser, dataDate)
        downloadEmissions(browser, dataDate)
        dataQuality()
        copyData(latestDate_dt, curtail_df)
        latest = checkLatest()
        latestDate_dt = latest[0]
        dataDate = latest[1]
        print('  Data for '+str(datetime.strftime(latestDate_dt, '%m/%d/%Y'))+' appended to data file.')
        end = time.time() 
        print('Loop # '+str(count)+' time = '+str(end-start)+' seconds') #loop timer 
        count += 1
    browser.close()
    print('Data file up to date with most recent data')

def checkLatest(): #check dataFile for date of most recent data
    with shelve.open(str(shelf)) as s:
        latestDate = s['caiso']['latestDate']
    latestDate_dt = datetime.strptime(latestDate, '%m/%d/%Y') #parse the date as a date object
    unixts = latestDate_dt.timestamp() #convert date to epoch/unix time
    pst = pytz.timezone('America/Los_Angeles') #need to account for daylight savings
    offset = int(pst.localize(datetime.fromtimestamp(unixts)).strftime('%z')[2]) #return the number of hours behind UTC
    dataDate = math.trunc((unixts - (3600 * offset)) * 1000 + 86400000) #this is the data attribute that the website uses to identify dates in the datepicker dropdown #subtracting 28,000 sec converts to PST, convert to millisec, add one day
    return latestDate_dt, dataDate

def webdriverConfig(): #configure the webdriver
    options = webdriver.ChromeOptions()
    #options.add_argument('--headless') #disabled: downloading files does not work in headless mode
    options.add_argument('log-level=1') #ignore any info warnings
    prefs = {"download.default_directory" : str(downloads)} 
    options.add_experimental_option("prefs",prefs)
    browser = webdriver.Chrome(options=options)
    return browser

def download_wait(f): #wait for files to finish downloading before continuing
    seconds = 0
    dl_wait = True
    while dl_wait and seconds < 20:
        time.sleep(1) #check every sec
        dl_wait = False
        for fname in os.listdir(Path.cwd() / f):
            if fname.endswith('.crdownload'): #incomplete chrome downloads end in .crdownload
                dl_wait = True
            seconds += 1
    time.sleep(1) #allow 1 sec after downloading

def downloadCurtailment(browser, user_initialized): #download curtailment data (updated monthly)
    print('  Checking for new curtailment data...')
    browser.get(curtailURL) #open webdriver
    time.sleep(1) #wait for page to load
    soup = BeautifulSoup(browser.page_source, 'lxml') #use beautifulsoup to parse html
    postDate = soup.find_all('span', class_='postDate')[0].get_text() #get current postDate from site
    with shelve.open(str(shelf)) as s:
        prevPostDate = s['caiso']['postDate']
    if postDate==prevPostDate: #compare current and previous postdate
        print('  Latest curtailment data already downloaded.') #do nothing if they match; we already have the most current file
        curtail_read = pd.read_csv(curtailments / 'curtailment_data.csv', dtype=ct_dtypes) #load the csv into a dataframe
        curtail_read.columns = (['date','hour', 'interval','wind_curtail_MW','solar_curtail_MW']) #rename columns
        ct_date = curtail_read['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')) #parse times from each row of data
        curtail_read['date'] = ct_date.apply(lambda x: datetime.strftime(x, '%m/%d/%Y')) #set date for each row
        curtail_read.astype({'date':str,'hour':'uint8','interval':'uint8'}, copy=False)
        return curtail_read
    else: #download new curtailment file if more recent data is available
        tmpDelete('downloads') #clear downloads folder
        tmpDelete('curtailments') #delete existing file in curtailments folder
        browser.find_elements_by_partial_link_text('Production and Curtailments Data')[0].click() #download file
        if user_initialized==0: #only notify of new curtailment download if not initiatied by the user
            print('  New curtailment data available!')
        print('  Downloading curtailment Excel file...')
        download_wait('downloads')         #wait for download to finish
        curtailFile = os.listdir(downloads)[0]
        os.rename(downloads / curtailFile, curtailments / curtailFile)  #move file to curtailments directory
        print('  Converting Excel file to CSV. This may take several minutes...')
        wb = openpyxl.load_workbook('curtailments/'+curtailFile) #this step takes a couple minutes to fully load
        sh = wb['Curtailments'] 
        with open(curtailments / 'curtailment_data.csv', 'w', newline='') as f:  #convert xlsx to csv file for faster reading in future
            c = csv.writer(f)
            for r in sh.rows:
                if r[0].value is not None:
                    c.writerow([cell.value for cell in r])
                else: 
                    continue
        time.sleep(1) #pause 1 sec after csv file created
        os.remove(curtailments / curtailFile) #once the new csv file is created, delete the xlsx file
        curtail_read = pd.read_csv(curtailments / 'curtailment_data.csv', dtype=ct_dtypes) #load the csv into a dataframe
        curtail_read.columns = (['date','hour', 'interval','wind_curtail_MW','solar_curtail_MW']) #rename columns
        ct_dateList = curtail_read.date.tolist()
        ct_latestDate = ct_dateList[len(ct_dateList)-1] #find the date of the most recent data available
        ct_date = curtail_read['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')) #parse times from each row of data
        curtail_read['date'] = ct_date.apply(lambda x: datetime.strftime(x, '%m/%d/%Y')) #set date for each row
        curtail_read.astype({'date':str,'hour':'uint8','interval':'uint8'}, copy=False)
        with shelve.open(str(shelf), writeback=True) as s:
            s['caiso']['postDate'] = postDate
            s['caiso']['ct_latestDate'] = ct_latestDate
        if user_initialized==0: #if this is not the first time the program has been run, fill missing values from previous month in dataFile
            fillMissingCurtail(curtail_read)  
        return curtail_read

def tmpDelete(f): #delete any temporary files in folder (f)
    dirPath = Path.cwd() / f
    fileList = os.listdir(dirPath)
    if not fileList:
        pass
    else:
        for fileName in fileList:
            os.remove(dirPath / fileName)

def downloadDemand(browser, dataDate): #download demand data
    print('  Downloading demand data...')
    browser.get(demandURL) #open webdriver
    time.sleep(1) #wait for page to load
    ActionChains(browser).move_to_element(browser.find_element_by_id('demand')).perform()
    browser.find_element(By.CSS_SELECTOR, '.form-control.date.demand-date').click() #click on date dropdown
    #time.sleep(1)
    while True:
        try: #try to find the date in the currently selected month
            browser.find_element(By.CSS_SELECTOR, "[data-date='{}']".format(dataDate)).click() #select date 
            break
        except NoSuchElementException:   #if dataDate not found, then click on previous month button and search again
            while True: #sometimes selenium has a hard time clicking the 'prev' button, so tried to give it multiple tries to do so
                try:
                    browser.find_element_by_class_name('prev').click()
                    break
                except NoSuchElementException: #if it cannot find the prev button
                    browser.find_element(By.CSS_SELECTOR, '.form-control.date.demand-date').click() #click on date dropdown again
                except ElementNotVisibleException: #if it reaches the end of the list for the available months
                    print('  Data for the selected date is not available to download. \n  Please restart the script and try a more recent date.')
                    sys.exit() #exit the script
    time.sleep(1) #wait for chart to load before downloading
    browser.find_elements_by_id('dropdownMenu1')[0].click() #click on download dropdown
    browser.find_element_by_id('downloadDemandCSV').click() #download CSV file
    download_wait('downloads')
    #download net demand data
    print('  Downloading net demand data...')
    ActionChains(browser).move_to_element(browser.find_element_by_id('netDemand')).perform()
    browser.find_element(By.CSS_SELECTOR, '.form-control.date.net-demand-date').click() #click on date dropdown
    #time.sleep(1)
    while True:
        try:
            browser.find_element(By.CSS_SELECTOR, "[data-date='{}']".format(dataDate)).click() #select date 
            break
        except NoSuchElementException:   #if dataDate not found, then click on previous month button and search again
            while True:
                try:
                    browser.find_element_by_class_name('prev').click()
                    break
                except NoSuchElementException:
                    browser.find_element(By.CSS_SELECTOR, '.form-control.date.net-demand-date').click() #click on date dropdown again
    time.sleep(1) #wait for chart to load before downloading
    browser.find_elements_by_id('dropdownMenu1')[2].click() #click on download dropdown
    browser.find_element_by_id('downloadNetDemandCSV').click() #download CSV file
    download_wait('downloads')

def downloadSupply(browser, dataDate): #download csv files from supply page
    print('  Downloading supply data...')
    browser.get(supplyURL) #open webdriver
    time.sleep(1) #wiat for page to load
    ActionChains(browser).move_to_element(browser.find_element_by_id('supplyTrend')).perform()
    browser.find_element(By.CSS_SELECTOR, '.form-control.date.supply-trend-date').click() #click on date dropdown
    #time.sleep(1)
    while True:
        try:
            browser.find_element(By.CSS_SELECTOR, "[data-date='{}']".format(dataDate)).click() #select date 
            break
        except NoSuchElementException:   #if dataDate not found, then click on previous month button and search again
            while True:
                try:
                    browser.find_element_by_class_name('prev').click()
                    break
                except NoSuchElementException:
                    browser.find_element(By.CSS_SELECTOR, '.form-control.date.supply-trend-date').click() #click on date dropdown again
    time.sleep(1) #wait for chart to load before downloading
    browser.find_element_by_id('dropdownMenuSupply').click() #click on download dropdown
    browser.find_element_by_id('downloadSupplyCSV').click() #download CSV file
    download_wait('downloads')
    #download renewables data
    print('  Downloading renewables data...')
    ActionChains(browser).move_to_element(browser.find_element_by_id('renewables')).perform()
    browser.find_element(By.CSS_SELECTOR, '.form-control.date.renewables-date').click() #click on date dropdown
    #time.sleep(1)
    while True:
        try:
            browser.find_element(By.CSS_SELECTOR, "[data-date='{}']".format(dataDate)).click() #select date 
            break
        except NoSuchElementException:   #if dataDate not found, then click on previous month button and search again
            while True:
                try:
                    browser.find_element_by_class_name('prev').click()
                    break
                except NoSuchElementException:
                    browser.find_element(By.CSS_SELECTOR, '.form-control.date.renewables-date').click() #click on date dropdown again
    time.sleep(1) #wait for chart to load before downloading
    browser.find_element_by_id('dropdownMenuRenewables').click() #click on download dropdown
    browser.find_element_by_id('downloadRenewablesCSV').click() #download CSV file
    download_wait('downloads')

def downloadEmissions(browser, dataDate): #download csv files from emissions page
    print('  Downloading emissions data...')
    browser.get(emissionsURL) #open webdriver
    time.sleep(1) #wait for page to load
    ActionChains(browser).move_to_element(browser.find_element_by_id('co2Breakdown')).perform()
    browser.find_element(By.CSS_SELECTOR, '.form-control.date.co2-breakdown-date').click() #click on date dropdown
    #time.sleep(1)
    while True:
        try:
            browser.find_element(By.CSS_SELECTOR, "[data-date='{}']".format(dataDate)).click() #select date 
            break
        except NoSuchElementException:   #if dataDate not found, then click on previous month button and search again
            while True:
                try:
                    browser.find_element_by_class_name('prev').click()
                    break
                except NoSuchElementException:
                    browser.find_element(By.CSS_SELECTOR, '.form-control.date.co2-breakdown-date').click() #click on date dropdown again
    time.sleep(1) #wait for chart to load before downloading
    browser.find_element_by_id('dropdownMenuCO2Breakdown').click() #click on download dropdown
    browser.find_element_by_id('downloadCO2BreakdownCSV').click() #download CSV file
    download_wait('downloads')

def dataQuality(): #after downloading all files, check quality
    files = os.listdir(downloads)
    status = 0
    status_list = []
    for f in files:
        df_quality = pd.read_csv(downloads / f)
        df_quality.drop(df_quality.columns[289:], axis=1, inplace=True)
        if df_quality.isnull().any().any(): #check for missing values (NaN)
            if df_quality.columns[-1]=='23:55': #check for incomplete timeseries (fewer than 288 5-min periods)
                status_list.append('  > '+f+': MISSING DATA')
                status += 1
            else:
                status_list.append('  > '+f+': INCOMPLETE TIMESERIES & MISSING DATA')
                status += 1
        else:
            if df_quality.columns[-1]=='23:55':
                status_list.append('  > '+f+': OK')
            else:
                status_list.append('  > '+f+': INCOMPLETE TIMESERIES')
                status += 1
    if status==0: #if no files have issues
        print('  Data quality OK')
    else: #if status_list not okay, pause process, wait for user input to continue
        print('  DATA QUALITY ISSUES DETECTED:')
        print("\n".join(status_list))
        print("\n")
        input("  Press Enter to continue>")
        print("  Resuming...")

def fillMissingCurtail(curtail_read): #since curtailment data is published with a month lag, this function goes back and fills in the dataFile once the data becomes available
    print("  Filling in previous month's curtailment data...")
    with shelve.open(str(shelf)) as s:
        ct_latestDate = s['caiso']['ct_latestDate']
        ct_latestDate_dt = datetime.strptime(ct_latestDate, '%Y-%m-%d %H:%M:%S')
    df_dataFile = pd.read_csv(dataFile, dtype=dataFile_dtypes)
    nulls = df_dataFile[['wind_curtail_MW','solar_curtail_MW']].isnull().any(axis=1) #figure out which rows contain NaNs (which should be only missing curtailment data)
    overwrite_index = nulls[nulls==True].index[0] #note the row number of the first missing curtailment data in the dataFile
    df_prevmonth = df_dataFile[df_dataFile.isnull().any(axis=1)] #extract rows with NaN to new dataframe
    prevmonth_date_dt = df_prevmonth['date'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y'))
    df_prevmonth = df_prevmonth.drop(df_prevmonth[prevmonth_date_dt > ct_latestDate_dt].index) #remove rows with date greater than available in curtailFile
    df_prevmonth.drop(['wind_curtail_MW','solar_curtail_MW'], axis=1, inplace=True) #delete last two columns
    #re-merge new curtailment data
    df_prevmonth.astype({'date':str,'hour':'uint8','interval':'uint8'}, copy=False)
    print(curtail_read.head()) ###
    df_prevmonth_ct = pd.merge(df_prevmonth, curtail_read, on=['date', 'hour', 'interval'], how='left', left_index=True) #merge curtailment data with prevmonth dataframe
    df_prevmonth_ct.fillna(0, inplace=True) #fill all empty values with '0'
    #merge prevmonth data into df_dataFile
    df_dataFile.reset_index(inplace=True) #reset index of df dataframe
    df_dataFile.set_index('index', inplace=True) #set index = index
    df_prevmonth_ct.reset_index(drop=True, inplace=True) #reset index
    df_prevmonth_ct.reset_index(inplace=True) #reset index
    df_prevmonth_ct['index'] += overwrite_index #add overwrite index to all values of index
    df_prevmonth_ct.set_index('index', inplace=True) #set index to index
    df_dataFile.update(df_prevmonth_ct)
    #overwrite dataFile (csv) with updated df_dataFile
    with open(dataFile,'w', newline='') as f:    
        df_dataFile.to_csv(f, header=True, index=False)


def copyData(latestDate_dt, curtail_df): #clean up data from downloaded CSVs, merge into a single dataframe
    files = os.listdir(downloads)
    #emissions data
    co2_read = pd.read_csv(downloads / files[0]) ##need to update to dynamically match filename
    df_co2 = co2_read.transpose() #transpose data
    df_co2.columns = df_co2.iloc[0] #change first row to header and drop
    df_co2.drop(df_co2.index[0], inplace=True) #drop old header
    df_co2.drop(df_co2.index[288:], inplace=True) #drop extra rows
    df_co2.reset_index(inplace=True, drop=True) #reset the index
    df_co2.columns = ['imports_co2','natgas_co2','biogas_co2','biomass_co2','geothermal_co2','coal_co2']
    #demand data
    demand_read = pd.read_csv(downloads / files[1]) ##need to update to dynamically match filename
    df_demand = demand_read.transpose() #transpose data
    df_demand.columns = df_demand.iloc[0] #change first row to header and drop
    df_demand.drop(df_demand.index[0], inplace=True) #drop old header
    df_demand.drop(df_demand.index[288:], inplace=True) #drop extra rows
    df_demand.reset_index(inplace=True) #reset the index
    df_demand.columns = ['5min_ending','demand_DayAF','demand_HourAF','demand_actual']
    #net demand data
    netdemand_read = pd.read_csv(downloads / files[2]) ##need to update to dynamically match filename
    df_netdemand = netdemand_read.transpose() #transpose data
    df_netdemand.columns = df_netdemand.iloc[0] #change first row to header and drop
    df_netdemand.drop(df_netdemand.index[0], inplace=True) #drop old header
    df_netdemand.drop(df_netdemand.index[288:], inplace=True) #drop extra rows
    df_netdemand.reset_index(inplace=True, drop=True) #reset the index
    df_netdemand.columns = ['A','B','demand_net']
    df_netdemand.drop(['A','B'], axis=1, inplace=True)
    #renewables data
    renew_read = pd.read_csv(downloads / files[3]) ##need to update to dynamically match filename
    df_renew = renew_read.transpose()
    df_renew.columns = df_renew.iloc[0] #change first row to header
    df_renew.drop(df_renew.index[0], inplace=True) #drop old header
    df_renew.drop(df_renew.index[288:], inplace=True) #drop extra rows
    df_renew.reset_index(inplace=True, drop=True)
    df_renew.columns = ['solar_MW','wind_MW','geothermal_MW','biomass_MW','biogas_MW','sm_hydro_MW','battery_MW']
    #supply data
    supply_read = pd.read_csv(downloads / files[4]) ##need to update to dynamically match filename
    df_supply = supply_read.transpose() #transpose data
    df_supply.columns = df_supply.iloc[0] #change first row to header and drop
    df_supply.drop(df_supply.index[0], inplace=True) #drop old header
    df_supply.drop(df_supply.index[288:], inplace=True) #drop extra rows
    df_supply.reset_index(inplace=True, drop=True) #reset the index
    df_supply.columns = ['renewable_MW','natgas_MW','lg_hydro_MW','imports_MW','nuclear_MW','coal_MW','other_MW']
    #create timestamp data   
    df_ts = pd.DataFrame(index=range(0,288),columns=['date','month','day','weekday','hour','interval']) #create an empty dataframe
    time_object = df_demand['5min_ending'].apply(lambda x: datetime.strptime(x, '%H:%M')) #parse times from each row of data
    downloadedDate = datetime.strftime(latestDate_dt + timedelta(days=1), '%m/%d/%Y') #set date for downloaded data
    df_ts['date'] = downloadedDate
    df_ts['month'] = datetime.strftime(latestDate_dt + timedelta(days=1), '%m') #set month number for downloaded data
    df_ts['day'] = datetime.strftime(latestDate_dt + timedelta(days=1), '%d') #set day number for downloaded data
    df_ts['weekday'] = datetime.strftime(latestDate_dt + timedelta(days=1), '%w') #set weekday (0=sun ... 6=sat) for downloaded data
    df_ts['hour'] = time_object.apply(lambda x: datetime.strftime(x, '%H')) #set hour number for each row
    df_ts['interval'] = time_object.apply(lambda x: datetime.strftime(x, '%M')) #set 5-min number for each row
    intervalMap = {'00': '1', '05': '2', '10': '3', '15': '4', '20': '5', '25': '6', '30': '7', '35': '8', '40': '9', '45': '10', '50': '11', '55': '12'} #12 intervals per hour
    df_ts['interval'].replace(intervalMap, inplace=True) #replace 5-min values with interval values
    #curtailment data
    with shelve.open(str(shelf)) as s:
        ct_latestDate = s['caiso']['ct_latestDate']
    ct_latestDate_dt = datetime.strptime(ct_latestDate, '%Y-%m-%d %H:%M:%S')
    if ct_latestDate_dt > latestDate_dt: #if the curtailment file contains curtailment data for the date of the supply/emissions data just downloaded
        df_ts.astype({'hour':np.uint8,'interval':np.uint8})
        df_ts['hour'] = pd.to_numeric(df_ts['hour'],downcast='unsigned')
        df_ts['interval'] = pd.to_numeric(df_ts['interval'],downcast='unsigned')
        df_curtail = pd.merge(df_ts, curtail_df, on=['date', 'hour', 'interval'], how='left', left_index=True) #merge curtailment data with timestamp dataframe
        df_curtail.fillna(0, inplace=True) #fill all empty values with '0'
        df_curtail.drop(['date', 'month', 'day', 'weekday', 'hour', 'interval'], axis=1, inplace=True) #get rid of all columns except for data
        df_curtail.reset_index(drop=True, inplace=True)
        print('  Curtailment data added...')
    else:
        df_curtail = pd.DataFrame(np.NaN, index=pd.RangeIndex(start=0,stop=288), columns=['wind_curtail_MW','solar_curtail_MW']) #create dataframe with 2 columns and 288 rows, filled with NaN
        print('  Curtailment data not yet available, creating placeholders...')
    #merge dataframes
    data_frames = [df_ts, df_demand, df_netdemand, df_curtail, df_renew, df_supply, df_co2] #list of dataframes to merge
    df_merged = reduce(lambda left,right: pd.merge(left,right,left_index=True,right_index=True), data_frames) #merge the dateframes
    with open(dataFile,'a', newline='') as f:    #append dataframe to dataFile CSV
        if os.stat(dataFile).st_size == 0: #if dataFile empty, header=True, otherwise header=False
            df_merged.to_csv(f, header=True, index=False)
        else:
            df_merged.to_csv(f, header=False, index=False)
    with shelve.open(str(shelf), writeback=True) as s:
        s['caiso']['latestDate'] = downloadedDate

if __name__== "__main__":
    main()

#%%
