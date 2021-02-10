#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Created on Tue Aug  4 13:19:28 2020

@author: tristanmeuross
'''
# %% Import modules, Google Sheets authorisation and functions

import pandas as pd
import pygsheets
import requests
from bs4 import BeautifulSoup as bs
import re
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import numpy as np
from functools import reduce

# Authorizes uploads to Google Sheets
client_secret = r'C:\Users\meurost\Documents\Python Projects\Auckland Index\client_secret.json'
gc = pygsheets.authorize(service_file=client_secret)

# header used for requests module authorisation
header = {
'User-Agent': 
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36'
}

# To upload to google sheets, sheet needs to share to email:
# auckland-index-update@auckland-index-update.iam.gserviceaccount.com


# Functions
def upload_gsheets(credentials, workbook_name, dataframes, sheets=[0], range_start=(1,1)):
    """
    Uploads chosen dataframes to selected workbook_name via pygsheets. Note 
    both dataframes and sheets variables need to be a list.

    Parameters
    ----------
    credentials : TYPE
        JSON credentials to authenticate upload.
    workbook_name : TYPE
        Name of Google Sheets file to upload to.
    dataframes : TYPE
        The dataframe/s to upload. Must be a list.
    sheets : TYPE, optional
        The worksheets to upload to (0 is the first). Must be a list. The default is [0].
    range_start : TYPE, optional
        The upper left cell where data will be uploaded to. The default is (1,1).

    Returns
    -------
    None.

    """
    gc = pygsheets.authorize(service_file=credentials)
    sh = gc.open(workbook_name)
    for i, x in zip(sheets, dataframes):
        sh[i].set_dataframe(x,range_start)
        
def format_gsheets(credentials, workbook_name, range_start, range_end, 
                   type_of_format, format_pattern, sheets=[0], model_cell='A1'):
    """
    Formats chosen cells as described format.

    Parameters
    ----------
    credentials : TYPE
         JSON credentials to authenticate formatting.
    workbook_name : TYPE
        Name of Google Sheets file to format.
    range_start : TYPE
        The start cell for the format range. Must be in string format and letter/number (i.e 'A' or 'A1', etc.)
    range_end : TYPE
        The end cell for the format range. Must be in string format and letter/number (i.e 'A' or 'A1', etc.).
    type_of_format : TYPE
        The type of format to be set in the range. Types include PERCENT, DATE, etc. Must be string.
    format_pattern : TYPE
        Pattern of format to be applied. Types include dd-mmm, 0%, etc. Must be string.
    sheets : TYPE, optional
        The worksheets to upload to (0 is the first). Must be a list. The default is [0].
    model_cell : TYPE
        The target cell which which the formatting will be based off. The default is 'A1'.

    Returns
    -------
    None.

    """
    gc = pygsheets.authorize(service_file=credentials)
    sh = gc.open(workbook_name)
    mc = pygsheets.Cell(model_cell)
    mc.set_number_format(
        format_type = eval('pygsheets.FormatType.' + type_of_format),
        pattern = format_pattern)
    for i in sheets:
        pygsheets.DataRange(
            start=range_start, end=range_end, worksheet = sh[i]
          ).apply_format(mc)

# %% Create stats portal data object

URL = 'https://statisticsnz.shinyapps.io/covid_19_dashboard/'
options = Options()
options.headless = True #This setting stops a browser window from opening
driver = webdriver.Chrome(executable_path=r'C:\windows\chromedriver',
                          options=options)
driver.get(URL)

try:
    element = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="download_data-show"]'))
        )
    element.click()
    
    element = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="download_data-downloadData"]'))
        )
    
    time.sleep(5)
    
    stats_download = element.get_attribute('href')

finally:
    driver.quit()    

stats_df = pd.read_csv(stats_download,
                        dtype={'parameter':'object',
                              'value':'object'})

# Need to filter for needed datasets as the 'parameter' column has non-date values
stats_df = stats_df.loc[(stats_df['series_name']=='Auckland - Heavy vehicles') |
                        (stats_df['series_name']=='Auckland - Light vehicles') |
                        (stats_df['indicator_name']=='Daily border crossings - arrivals') |
                        (stats_df['indicator_name']=='Jobseeker support by MSD region') |
                        (stats_df['indicator_name']=='Number of recipients of CIRP') |
                        (stats_df['indicator_name']=='Jobs online measure by region') |
                        (stats_df['indicator_name']=='Monthly filled jobs (by region)') |
                        (stats_df['indicator_name']=='Tests per day')]

stats_df['parameter'] = pd.to_datetime(stats_df['parameter'], format='%Y/%m/%d')



#%% COVID CASES

# Create cases dataframe from ESR nz covid dashboard
# csv_file is downloaded from 'csv download' link in the 'source' section of dashboard
csv_file = r'C:\Users\meurost\Documents\Python Projects\Auckland Index\Data Files\source_case_curve.csv'
cases_df = (pd.read_csv(csv_file,
                       skiprows=3)).dropna(how='any')
cases_df['Imported or import-related'] = (cases_df['Daily imported cases']
                                          + cases_df['Daily import-related cases'])
cases_df['Locally acquired'] = (cases_df['Daily locally acquired cases  epidemiologically linked']
                                + cases_df['Daily locally acquired cases  unknown source'])
cases_df['Date'] = pd.to_datetime(cases_df['Date'], format='%d/%m/%Y')
cases_df = cases_df.iloc[::-1].reset_index() #Reverse date order from oldest to newest

import_df = pd.pivot_table(cases_df,
                          index='Date',
                          values='Imported or import-related',
                          aggfunc='sum').reset_index()
local_df = pd.pivot_table(cases_df,
                          index='Date',
                          values='Locally acquired',
                          aggfunc='sum').reset_index()
investigation_df = pd.pivot_table(cases_df,
                                  index='Date',
                                  values='Daily under investigation',
                                  aggfunc='sum').reset_index()
total_df = pd.pivot_table(cases_df,
                          index='Date',
                          values='Daily total cases',
                          aggfunc='sum').reset_index()

dataframes = [import_df,
              local_df,
              investigation_df,
              total_df]

# Merge all dataframes together
cases_df = reduce(lambda  left,right: pd.merge(left,right,on=['Date'],
                                            how='outer'), dataframes)
cases_df['Number of cases'] = cases_df['Daily total cases'].cumsum()
daily_df = cases_df[['Date',
                    'Imported or import-related',
                    'Locally acquired',
                    'Daily under investigation',
                    'Daily total cases']].copy()

daily_df.rename(columns={'Daily under investigation':'Under investigation',
                         'Daily total cases':'Total cases'},
                inplace=True)
cumulative_df = cases_df[['Date',
                         'Number of cases']].copy()

# Create tests per day dataframe from stats_df
tests_df = ((stats_df.loc[(stats_df['indicator_name']=='Tests per day') &
                          (stats_df['series_name']=='Tests by day'),
                            ['parameter',
                             'value']]).reset_index(drop=True)).copy()
tests_df['value'] = tests_df['value'].astype(float)
tests_df.dropna(inplace=True)
tests_df.rename(columns={'parameter':'Date',
                         'value':'Tests per day'},
                inplace=True)

# Upload to Google Sheets
cases_dataframes = [daily_df, cumulative_df, tests_df]
workbook_name = '1. Auckland-index-covid-dashboard-covid-cases'
upload_gsheets(client_secret,
               workbook_name,
               cases_dataframes, 
               sheets=[0, 1, 2])

# Format cells
format_gsheets(client_secret, 
               workbook_name, 
               'A', 
               'A', 
               'DATE', 
               'dd-mmm-yy', 
               sheets=[0, 1, 2])

format_gsheets(client_secret, 
               workbook_name, 
               'B', 
               'E', 
               'NUMBER', 
               '0', 
               sheets=[0, 1, 2])

#%% UNEMPLOYMENT BENEFITS AND PAYMENTS

# Create Jobseeker dataframe from stats_df
jobseeker_df = (stats_df.loc[(stats_df['indicator_name']=='Jobseeker support by MSD region') &
                            (stats_df['sub_series_name']=='Auckland metro'),
                            ['parameter',
                             'value']]).reset_index(drop=True)
jobseeker_df['value'] = jobseeker_df['value'].astype(float)

#  2020 CIRP dataframe from MSD datasheet
excel_path = ('https://www.msd.govt.nz/documents/about-msd-and-our-work/publications-resources/statistics/benefit/2020/income-support-and-wage-subsidy-weekly-update/data-file-income-support-and-wage-subsidy-weekly-update-25-december-2020.xlsx')

cirp_df_20 = pd.read_excel(excel_path, 
                    sheet_name='1. Timeseries-MainBenefits-CIRP', 
                    skiprows=59,
                    nrows=1).dropna(axis=1) #dropna removes empty values
cirp_df_20 = cirp_df_20.iloc[:,1:].transpose().reset_index()

# Create 2021 CIRP dataframe from MSD datasheet
URL = 'https://www.msd.govt.nz/about-msd-and-our-work/publications-resources/statistics/weekly-reporting/index.html'
options = Options()
options.headless = True #This setting stops a browser window from opening
driver = webdriver.Chrome(executable_path=r'C:\windows\chromedriver',
                          options=options)
driver.get(URL) #opens URL on chrome to activate javascript
soup = bs(driver.page_source, 'html.parser') #uses bs to get data from browser
driver.quit() #quits browser
                     
link = soup.find('a', href=re.compile(
    'data-file-income-support-and-wage-subsidy-weekly-update-')).get('href')

excel_path = ('https://www.msd.govt.nz/' + link)

cirp_df_21 = pd.read_excel(excel_path, 
                    sheet_name='1. Timeseries-MainBenefits-CIRP', 
                    skiprows=59,
                    nrows=1).dropna(axis=1) #dropna removes empty values
cirp_df_21 = cirp_df_21.iloc[:,1:].transpose().reset_index()

# Concat 2020 and 2021 CIRP dataframes
cirp_df = pd.concat([cirp_df_20, cirp_df_21])
cirp_df.reset_index(inplace=True,
                    drop=True)

# As MSD only released national data, Auckland was calculated as approximately 39.5% of 
# total up to 7th August and 43% from 14th August to 11th Sept and 49% from 18th Sept onwards
cirp_df.iloc[:,1] = pd.concat([(cirp_df.iloc[0:9,1]*0.395).round(0),
                               (cirp_df.iloc[9:14,1]*0.43).round(0),
                               (cirp_df.iloc[14:,1]*0.49).round(0)])

# rename columns
jobseeker_df.columns = ['Date',
                        'Jobseeker Support']
cirp_df.columns = ['Date',
                   'COVID-19 Income Relief Payment']

# Merge jobseeker and CIRP datasets
auckland_df = pd.merge(jobseeker_df,
                        cirp_df,
                        on='Date',
                        how='left').fillna(0) #replaces NaN's with zeros

#Create total column and remove zero's
auckland_df['Total'] = pd.to_numeric(auckland_df.sum(axis=1))
auckland_df['COVID-19 Income Relief Payment']  = auckland_df[['COVID-19 Income Relief Payment']].replace(to_replace=0,
                                                                                                         value='')

#Upload to Google Sheets
workbook_name = '2. Auckland-index-covid-dashboard-unemployment-benefits-and-payments'

upload_gsheets(client_secret, 
               workbook_name, 
               [auckland_df])

#Format Google Sheet cells
format_gsheets(client_secret, 
               workbook_name, 
               'A', 
               'A', 
               'DATE', 
               'dd-mmm-yy')

#%% CONSUMER SPENDING
URL = 'https://mbienz.shinyapps.io/card_spend_covid19/'
options = Options()
options.headless = True #This setting stops a browser window from opening
driver = webdriver.Chrome(executable_path=r'C:\windows\chromedriver',
                          options=options)
driver.get(URL)

# Accept caveats pop up
element = WebDriverWait(driver, 120).until(
    EC.presence_of_element_located((By.XPATH, '//*[@id="shiny-modal"]/div/div/div[3]/button'))
    )
element.click()

# Copy data from national - xpath seems to change, program will iterate through xpaths
try: 
    element = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="DataTables_Table_0_wrapper"]/div[2]/button[1]'))
        )
except:
    element = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="DataTables_Table_0_wrapper"]/div[2]/a[1]'))
        )    

time.sleep(10)
element.click()
time.sleep(1)

national_df = pd.read_clipboard(skiprows=2).transpose()
national_df.reset_index(inplace=True)
national_df['year'] = '2020' #Create year column to convert date column (date column has no year)
national_df['Date'] = pd.to_datetime(national_df[['index','year']].astype(str).apply('-'.join, 1), format='%b-%d-%Y') #convert to datetime
national_df.drop(['Domestic', 
                  'International', 
                  'index', 
                  'year', 
                  'Date', 
                  'Weekly'], 
                  axis=1, inplace=True)
national_df['Total'] = (national_df['Total'].astype(float))/100 # convert to percentages
national_df.rename(columns={'Total':'New Zealand'},
                        inplace=True)


# Navigate to regional section
element = WebDriverWait(driver, 30).until(
    EC.presence_of_element_located((By.XPATH, '//*[@id="sidebarItemExpanded"]/ul/li[2]/a'))
    )
element.click()

# Copy data from regional - xpath seems to change, program will iterate through xpaths
try:
    element = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="DataTables_Table_1_wrapper"]/div[2]/button[1]'))
        )
except:
    element = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="DataTables_Table_1_wrapper"]/div[2]/a[1]'))
        )
time.sleep(10)
element.click()
time.sleep(1)

regional_df = pd.read_clipboard(skiprows=2, sep='\t').set_index('Unnamed: 0').transpose() #create dataframe from copied data
regional_df.reset_index(inplace=True)
regional_df.dropna(thresh=3, inplace=True)
regional_df.dropna(axis=1, how='all', inplace=True)
regional_df['year'] = '2020' #Create year column to convert date column (date column has no year)
regional_df['Date'] = pd.to_datetime(regional_df[['index','year']].astype(str).apply('-'.join, 1), format='%b-%d-%Y') #convert to datetime
regional_df = regional_df[['Date', 'Auckland', 'Wellington']]
regional_df[['Auckland','Wellington']] = regional_df[['Auckland','Wellington']]/100 # convert to percentages

card_df = regional_df.reset_index(drop=True).join(national_df) #create combined dataframe

driver.quit() #quit driver

# Upload to Google Sheets
workbook_name = '3. Auckland-index-covid-dashboard-consumer-spending'

upload_gsheets(client_secret, 
               workbook_name, 
               [card_df])

format_gsheets(client_secret, 
               workbook_name, 
               'B', 
               'D', 
               'PERCENT', 
               '0.0%')

format_gsheets(client_secret, 
               workbook_name, 
               'A', 
               'A', 
               'DATE', 
               'dd-mmm-yy')

#%% GOOGLE MOBILITY DATA

#Download CSV data
csv_path = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'

#Create pandas dataframe from CSV data
df = pd.read_csv(csv_path, parse_dates=['date'], index_col='date', 
        dtype={'country_region_code':'string', 
               'country_region':'string', 
               'sub_region_1':'string', 
               'sub_region_2':'string', 
               'metro_area':'string', 
               'iso_3166_2_code':'string', 
               'census_fips_code':'float',  
               'retail_and_recreation_percent_change_from_baseline':'float', 
               'grocery_raw_and_pharmacy_percent_change_from_baseline':'float', 
               'parks_raw_percent_change_from_baseline':'float', 
               'transit_stations_raw_percent_change_from_baseline':'float', 
               'workplaces_raw_percent_change_from_baseline':'float', 
               'residential_raw_percent_change_from_baseline':'float'})

#Rename columns
df.columns = ['country_region_code', 
                'country_region', 
                'sub_region_1', 
                'sub_region_2', 
                'metro_area', 
                'iso_3166_2_code', 
                'census_fips_code', 
                'retail_rec_raw', 
                'grocery_raw', 
                'parks_raw', 
                'transit_stations_raw', 
                'workplaces_raw', 
                'residential_raw']

#Slice dataframe by Auckland
auckland_df = df.loc[df['sub_region_1'] == 'Auckland',
            ['retail_rec_raw', 
             'grocery_raw', 
             'parks_raw',
             'transit_stations_raw', 
             'workplaces_raw', 
             'residential_raw']]

raw = ['retail_rec_raw',
       'grocery_raw', 
       'parks_raw', 
       'transit_stations_raw', 
       'workplaces_raw', 
       'residential_raw']

ma = ['Retail & Recreation', 
      'Grocery & Pharmacy', 
      'Parks', 
      'Transit Stations', 
      'Workplaces', 
      'Residential']

#Create moving averages
for i, x in zip(raw, ma):
    auckland_df[x] = (auckland_df[i].rolling(7).mean())/100

#Reset index for Google Sheets so date is a column and in correct format
auckland_df = auckland_df.reset_index()
auckland_df['date'] = auckland_df['date'].dt.strftime('%m/%d/%Y')

#Select relevant columns for G Sheets in correct order
auckland_df = auckland_df.loc[6:,
                ['date', 
                 'Retail & Recreation', 
                 'Grocery & Pharmacy', 
                 'Parks', 
                 'Transit Stations', 
                 'Workplaces', 
                 'Residential']]


#Upload to Google Sheets
workbook_name = '4. Auckland-index-covid-dashboard-mobility'

upload_gsheets(client_secret, 
               workbook_name, 
               [auckland_df])

#Format cells
format_gsheets(client_secret, 
               workbook_name, 
               'B', 
               'G', 
               'PERCENT', 
               '0%')

format_gsheets(client_secret, 
               workbook_name, 
               'A', 
               'A', 
               'DATE', 
               'dd-mmm-yy')


#%% TRANSPORT

#AT excel file download name changes, so needs to be scraped
URL_at = 'https://at.govt.nz/about-us/reports-publications/at-metro-patronage-report/'
page = requests.get(URL_at)
soup = bs(page.content, 'html.parser')
link = soup.find('a',href=re.compile('daily-patronage-for-at-web')).get('href')

at_download = requests.get(('https://at.govt.nz/' + link), headers=header)
    

at_df = pd.concat((pd.read_excel(at_download.content, 
                                sheet_name=None, 
                                skiprows=4,
                                parse_dates=['Business Date'])), 
                  ignore_index=True)

at_df = at_df.iloc[::-1] #Reverse date order from oldest to newest
at_df['Rolling'] = at_df['Total'].rolling(7).mean().round(0)

#Create daily public transport patronage dataframe - NOTE that at_df file starts at July 2019
pt_df_21 = (at_df.loc[(at_df['Business Date']>='2021-01-01') &
                  (at_df['Business Date']<='2021-12-31'),
                  ['Business Date',
                   'Rolling']]).reset_index(drop=True)
pt_df_21.rename(columns={'Rolling':'2021'},
                         inplace=True)

nan_df = pd.DataFrame([[np.nan] * len(pt_df_21.columns)],
                      index=[0,1,2],
                      columns=pt_df_21.columns)

pt_df_21 = nan_df.append(pt_df_21, ignore_index=True)

# Download data already in Google Sheet (starts Jan 2019)
workbook_name = '6. Auckland-index-covid-dashboard-transport'
sh = gc.open(workbook_name)

# select the third sheet 
wks = sh[2]

# update date format for python
model_cell = pygsheets.Cell("A2")

model_cell.set_number_format(
    format_type = pygsheets.FormatType.DATE,
    pattern = "yyyy-mm-dd"
)

pygsheets.DataRange(
    start='A', end='A', worksheet = wks
  ).apply_format(model_cell)

# download google sheet into dataframe
download_df = wks.get_as_df()
download_df['Date'] = pd.to_datetime(download_df['Date'], format='%Y-%m-%d')
download_df.columns = download_df.columns.astype(str) # Convert column headers to string
download_df.drop(columns=['2021'], inplace=True)
download_df['2020'] = download_df['2020'].str.replace(',','').astype(np.float32)
download_df['2019'] = download_df['2019'].str.replace(',','').astype(np.float32)
download_df = download_df.iloc[:,:3]

# Join dataframes, adds the 2021 column to 
pt_df = download_df.join(pt_df_21['2021'])

print(pt_df)

#%%
# Create light and heavy traffic dataframes
light_df_20 = (stats_df.loc[(stats_df['series_name']=='Auckland - Light vehicles') &
                            (stats_df['parameter']>='2020-01-01'),
                            ['parameter',
                             'value']]).reset_index(drop=True)

light_df_19 = (stats_df.loc[(stats_df['series_name']=='Auckland - Light vehicles') & 
                            (stats_df['parameter']>='2019-01-02') &
                            (stats_df['parameter']<='2019-12-31'),
                            ['parameter',
                             'value']]).reset_index(drop=True)

heavy_df_20 = (stats_df.loc[(stats_df['series_name']=='Auckland - Heavy vehicles') & 
                            (stats_df['parameter']>='2020-01-01'),
                            ['parameter',
                             'value']]).reset_index(drop=True)

heavy_df_19 = (stats_df.loc[(stats_df['series_name']=='Auckland - Heavy vehicles') & 
                            (stats_df['parameter']>='2019-01-02') &
                            (stats_df['parameter']<='2019-12-31'),
                            ['parameter',
                             'value']]).reset_index(drop=True)

light_df_20.columns = ['Date',
                        '2020']

light_df_19.columns = ['Date',
                        '2019']

heavy_df_20.columns = ['Date',
                        '2020']

heavy_df_19.columns = ['Date',
                        '2019']

#Join 2020 and 2019 into one dataframe
light_df = light_df_20[['Date','2020']].join(light_df_19['2019'])
heavy_df = heavy_df_20[['Date','2020']].join(heavy_df_19['2019'])

#Upload to Google Sheets
workbook_name = 'Auckland Index Data Upload Test'

transport_dataframes = [light_df, heavy_df, pt_df]

upload_gsheets(client_secret,
               workbook_name,
               transport_dataframes,
               sheets=[0,1,2])

#Format cells
format_gsheets(client_secret, 
               workbook_name, 
               'A', 
               'A', 
               'DATE', 
               'dd-mmm', 
               sheets=[0,1,2])
# %% JOBS

# Create filled jobs dataframe via stats nz covid portal data
filledjobs_df = stats_df.loc[(stats_df['indicator_name']=='Monthly filled jobs (by region)') &
                             (stats_df['series_name']=='Auckland')]

filledjobs_df = filledjobs_df[['parameter',
                               'value']]
filledjobs_df.rename(columns={'parameter':'Month',
                              'value':'Auckland'},
                     inplace=True)


# Create dataframe for jobs online via stats nz covid portal data
jobsonline_df = (stats_df.loc[(stats_df['indicator_name']=='Jobs online measure by region') &
                            (stats_df['parameter']>='2017-01-01'),
                            ['parameter',
                             'series_name',
                             'value']]).reset_index(drop=True)
jobsonline_df['value'] = jobsonline_df['value'].astype(float)

jobsonline_df = (pd.pivot_table(jobsonline_df, 
                               values='value', 
                               columns='series_name',
                               index='parameter')).reset_index()

regions = ['Auckland', 
           'Canterbury',
           'Wellington',
           'North Island (Other)',
           'South Island (Other)']

# Rebase index to Jan 2017
for i in regions:
    jobsonline_df[i] = (jobsonline_df[i]/jobsonline_df.loc[0,i]*100).round(1)

jobsonline_df.rename(columns={'parameter':'Date'},
                     inplace=True)
regions.insert(0,'Date') #add date column to list
jobsonline_df = jobsonline_df[regions]

#Upload to Google Sheets
workbook_name = '7. Auckland-index-covid-dashboard-jobs'
jobs_dataframes = [filledjobs_df, jobsonline_df]

upload_gsheets(client_secret, 
               workbook_name, 
               jobs_dataframes, 
               sheets=[0,1])

format_gsheets(client_secret,
               workbook_name,
               'A',
               'A',
               'DATE',
               'mmm yyyy',
               sheets=[0,1])


#%% ARRIVALS

#Create arrivals dataframe
arrivals_df_19 = (stats_df.loc[(stats_df['parameter']>='2019-01-01') &
                            (stats_df['parameter']<='2019-12-31') &
                            (stats_df['series_name']=='Total'),
                            ['parameter',
                              'value']]).reset_index(drop=True)


arrivals_df_20 = (stats_df.loc[(stats_df['parameter']>='2020-01-01') &
                            (stats_df['series_name']=='Total'),
                            ['parameter',
                              'value']]).reset_index(drop=True)

#Add blank data line for 'missing' 29th Feb value in 2019
line = pd.DataFrame({'parameter': '2020-03-01', 'value': ''}, index=[3])
arrivals_df_19 = pd.concat([arrivals_df_19.iloc[:59], line, arrivals_df_19.iloc[59:]]).reset_index(drop=True)

#Rename columns
arrivals_df_20.columns = ['Date',
                          '2020']
arrivals_df_19.columns = ['Date',
                          '2019']

arrivals_df = arrivals_df_20[['Date','2020']].join(arrivals_df_19['2019'])


workbook_name = '8. Auckland-index-covid-dashboard-arrivals'
upload_gsheets(client_secret,
               workbook_name,
               [arrivals_df])

format_gsheets(client_secret,
               workbook_name,
               'A',
               'A',
               'DATE',
               'dd-mmm')

#%% TRADE

# Download filled jobs csv file
URL = 'https://www.stats.govt.nz/large-datasets/csv-files-for-download/'
options = Options()
options.headless = True #This setting stops a browser window from opening
driver = webdriver.Chrome(executable_path=r'C:\windows\chromedriver',
                          options=options)
driver.get(URL) #opens URL on chrome to activate javascript
stats_soup = bs(driver.page_source, 'html.parser') #uses bs to get data from browser
driver.quit() #quits browser

# Find link for trade CSV file    
link = stats_soup.find('a', href=re.compile(
    'Effects-of-COVID-19-on-trade')).get('href')

csv_download = ('https://www.stats.govt.nz' + link)

#Create dataframe
df = pd.read_csv(csv_download)
df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
df['Current_Match'] = pd.to_datetime(df['Current_Match'], format='%d/%m/%Y')

#Create 2020 and 2019 dataframes
df_2020 = df.loc[(df['Direction']=='Exports') & 
              (df['Year']==2020) & 
              (df['Country']=='All') &
              (df['Transport_Mode']=='All') &
              (df['Measure']=='$')].reset_index(drop=True)

df_2019 = df.loc[(df['Direction']=='Exports') & 
              (df['Year']==2019) & 
              (df['Country']=='All') &
              (df['Transport_Mode']=='All') &
              (df['Measure']=='$')].reset_index(drop=True)

#Create moving average
df_2020['moving_average'] = df_2020['Value'].rolling(28).mean()
df_2019['moving_average'] = df_2019['Value'].rolling(28).mean()


#Create pivots
df_2020 = pd.pivot_table(df_2020, 
                          index='Current_Match', 
                          values='moving_average',
                          columns='Commodity').dropna()

df_2019 = pd.pivot_table(df_2019, 
                          index='Current_Match', 
                          values='moving_average',
                          columns='Commodity').dropna()

#Create dataframe of year on year percentage change
trade_df = (df_2020.div(df_2019)).sub(1).reset_index()
trade_df.rename(columns={'All':'Total',
                          'Current_Match':'Date'}, 
                inplace = True)

#Upload to Google Sheets
workbook_name = '9. Auckland-index-covid-dashboard-trade'
upload_gsheets(client_secret,
                workbook_name,
                [trade_df])

format_gsheets(client_secret,
                workbook_name,
                'A',
                'A',
                'DATE',
                'dd-mmm')

format_gsheets(client_secret,
                workbook_name,
                'B',
                'H',
                'PERCENT',
                '0.0%')

