# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 16:01:07 2020

@author: meurost
"""

# %% Import modules, Google Sheets authorisation and functions
import pandas as pd
from bs4 import BeautifulSoup as bs
import re
from selenium import webdriver
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import zipfile
import urllib.request
import os
import quandl
import time

from Python import stats_odata as odata
from modules.my_modules import upload_gsheets, format_gsheets, delete_file

# Location of RIMU Datasheet
rimu_file = 'data_files/Economic Update Key Charts.xlsx'
economic_file = 'data_files/Economic Indicators Database - Economic.xlsm'

# header used for requests module authorisation
header = {
'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36'
}

# Setting proxies for gspread, requests and APIs
proxies = {}
# proxies = {'http':os.environ['HTTP_PROXY2'],
#            'https':os.environ['HTTPS_PROXY2']}

# os.environ['HTTPS_PROXY'] = os.environ['HTTPS_PROXY2']

#%% 
# To upload to google sheets, sheet needs to share to email:
# auckland-index-update@auckland-index-update.iam.gserviceaccount.com

# Download HLFS csv file
URL = 'https://www.stats.govt.nz/large-datasets/csv-files-for-download/'
options = Options()
options.headless = False
driver = webdriver.Chrome(executable_path=r'C:\windows\chromedriver',
                          options=options)
driver.get(URL) #opens URL on chrome to activate javascript
stats_soup = bs(driver.page_source, 'html.parser') #uses bs to get data from browser
driver.quit() #quits browser

# Find link for Labour Market Statistics CSV file
link = stats_soup.find('a', href=re.compile(
    'Labour-market-statistics-')).get('href')
csv_download = ('https://www.stats.govt.nz' + link)

# Get zipfile download and unzip hlfs csv
# Downloads zip file
urllib.request.urlretrieve(
    csv_download,
    'labour-market-statistics-september-2020-quarter-csv.zip'
)
# Unzips file
compressed_file = zipfile.ZipFile(
    'labour-market-statistics-september-2020-quarter-csv.zip'
)

# Searches for hlfs file
filename = [s for s in compressed_file.namelist() if 'hlfs' in s] 
# returns string of hlfs file
csv_file = compressed_file.open(filename[0]) 

# Open hlfs csv file as dataframe
hlfs_df = pd.read_csv(
    csv_file,
    encoding='latin',
    dtype='object',
    index_col=False
)

hlfs_df['Period'] = pd.to_datetime(hlfs_df['Period'], format='%Y.%m')
hlfs_df['Quarter'] = hlfs_df['Period'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q
hlfs_df['Data_value'] = pd.to_numeric(hlfs_df['Data_value'], errors='coerce')



# %% 2. PEOPLE

# -----LABOUR FORCE-----
# Create labour force dataframe from hlfs dataframe
labourforce_df = hlfs_df.loc[
    (hlfs_df['Series_reference']=='HLFQ.S2C3QB') |
    (hlfs_df['Series_reference']=='HLFQ.S2D3QB') |
    (hlfs_df['Series_reference']=='HLFQ.S2E3QB') |
    (hlfs_df['Series_reference']=='HLFQ.S2Z3QB')
]
labourforce_df = pd.pivot_table(
    labourforce_df,
    index='Quarter',
    columns='Persons Employed, Unemployed, Not in Labour Force (for current quarter)',
    values='Data_value'
)

labourforce_df[
    'Labour force growth rate (LHS, %)'
] = labourforce_df['Total Labour Force'].pct_change(periods=4)

labourforce_df[
    'Labour force growth level (RHS, #)'
] = (labourforce_df['Total Labour Force'].diff(periods=4))*1000

lf_growth_df = labourforce_df[[
    'Labour force growth rate (LHS, %)',
    'Labour force growth level (RHS, #)'
]]
participation_df = (labourforce_df['Labour Force Participation Rate']/100)
labourforce_df = labourforce_df[[
    'Total Labour Force',
    'Not in Labour Force',
    'Working Age Population'
]]*1000

# Slice from 2001 onwards
labourforce_df = (labourforce_df.loc['2000 Q1':]).reset_index()
participation_df = (participation_df.loc['2000 Q1':]).reset_index()
lf_growth_df = (lf_growth_df.loc['2000 Q1':]).reset_index()

# Rename columns inline with Auckland Index
labourforce_df.rename(
    columns={
        'Total Labour Force': 'Active labour force (#)',
        'Not in Labour Force': 'Not in labour force (#)',
        'Working Age Population': 'Working age population (#)'
    }, inplace=True
)
participation_df.rename(
    columns={
        'Labour Force Participation Rate': 'Labour force participation rate (%)'
    }, inplace=True
)

# Upload to G Sheets
workbook_name = '2-Auckland-index-people-labour-force'
labourforce_dataframes = [
    labourforce_df,
    participation_df,
    lf_growth_df
]

upload_gsheets(
    workbook_name,
    labourforce_dataframes,
    sheets=[0,1,2]
)

format_gsheets(
    workbook_name,
    'B:D',
    'NUMBER',
    '#,##0',
    sheets=[0]
)

format_gsheets(
    workbook_name,
    'B',
    'PERCENT',
    '0.0%',
    sheets=[1,2]
)

format_gsheets(
    workbook_name,
    'C',
    'NUMBER',
    '#,##0',
    sheets=[2]
)

time.sleep(10) # Slow down google API requests to not exceed limit

#-----EMPLOYMENT AND UNEMPLOYMENT-----
# Create unemployment datafram from RIMU monthly datasheet
unemp_rate_df = (
    pd.read_excel(
        rimu_file,
        # engine='openpyxl', # needed for newer versions of pandas to support xlsx files
        sheet_name='Unemployment',
        skiprows=3,
        usecols='A:C'
    )
).dropna()
unemp_rate_df.rename(
    columns={
        'Unnamed: 0': 'Quarter',
        'Rest of NZ': 'Rest of New Zealand'
    }, inplace=True
)
unemp_rate_df['Quarter'] = unemp_rate_df['Quarter'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q

# Create Employment Status dataframe
emp_status_df = hlfs_df.loc[
    (hlfs_df['Period']>='1999-01-01')
    & (
       (hlfs_df['Series_reference']=='HLFQ.S2A3QB')
       | (hlfs_df['Series_reference']=='HLFQ.S2A3QZ')
       | (hlfs_df['Series_reference']=='HLFQ.S2B3QB')
       | (hlfs_df['Series_reference']=='HLFQ.S2B3QZ')
   )
]
emp_status_df = pd.pivot_table(
    emp_status_df,
    index='Quarter',
    values='Data_value',
    columns=[
        'Regional Council',
        'Persons Employed, Unemployed, Not in Labour Force (for current quarter)'
    ]
)*1000
emp_status_df.columns = [f'{i}{j}' for i,j in emp_status_df.columns] # Combine 'Region Council' and 'Persons Employed, Unemployed..' dataframe headers
emp_status_df.rename(
    columns={
        'AucklandPersons Employed in Labour Force':'Auckland employed',
        'AucklandPersons Unemployed in Labour Force':'Auckland unemployed',
        'Total All Regional CouncilsPersons Employed in Labour Force':'NZ employed',
        'Total All Regional CouncilsPersons Unemployed in Labour Force':'NZ unemployed'
    }, inplace=True
)
emp_status_df['Rest of NZ employed'] = (
    emp_status_df['NZ employed']
    - emp_status_df['Auckland employed']
)
emp_status_df['Rest of NZ unemployed'] = (
    emp_status_df['NZ unemployed']
    - emp_status_df['Auckland unemployed']
)
emp_growth_df = (
    emp_status_df[[
        'Auckland employed', 'Rest of NZ employed'
    ]].pct_change(periods=4)
).reset_index() # Create growth dataframe

emp_status_df = (
    emp_status_df[[
        'Auckland employed',
        'Auckland unemployed',
        'Rest of NZ employed',
        'Rest of NZ unemployed'
    ]]
).reset_index()

# Remove 1999 data and rename columns appropriately
emp_status_df = emp_status_df.iloc[4:]
emp_growth_df = emp_growth_df.iloc[4:]

emp_growth_df.rename(
    columns={
        'Auckland employed': 'Auckland',
        'Rest of NZ employed': 'Rest of New Zealand'
    }, inplace=True
)

# Download jobseeker data and set the variables for Jobseeker by region data
service = "https://api.stats.govt.nz/opendata/v1/"
endpoint = "Covid-19Indicators"
entity = "Observations"
query_option = """$filter=(
                        ResourceID eq 'CPBEN7' and
                        Geo eq 'Auckland metro')
                &$select=Period,Value"""
api_key = os.environ['STATS_KEY']

# Call the service
jobseeker_df = odata.get_odata(
    service,
    endpoint,
    entity,
    query_option,
    api_key,
    proxies
)

# Sort by date order
jobseeker_df.sort_values(by='Period', inplace=True)

# Set the variables for CIRP data
query_option = """$filter=(
                        ResourceID eq 'CPINC5' and
                        Label1 eq 'Total number of recipients of CIRP')
                &$select=Period,Value"""

# Call the service
cirp_df = odata.get_odata(
    service,
    endpoint,
    entity,
    query_option,
    api_key,
    proxies
)

cirp_df.sort_values(by='Period', inplace=True)

# As MSD only released national data, Auckland was calculated as approximately 39.5% of
# total up to 7th August and 43% from 14th August to 11th Sept and 49% from 18th Sept onwards
cirp_df.iloc[:, 1] = pd.concat(
    [
        (cirp_df.iloc[0:9, 1] * 0.395).round(0),
        (cirp_df.iloc[9:14, 1] * 0.43).round(0),
        (cirp_df.iloc[14:, 1] * 0.49).round(0)
    ]
)

# Rename columns
jobseeker_df.columns = ['Date', 'Jobseeker Support']
cirp_df.columns = ['Date', 'COVID-19 Income Relief Payment']

# Merge jobseeker and CIRP datasets
benefits_df = pd.merge(
    jobseeker_df,
    cirp_df,
    on='Date',
    how='left'
).fillna(0)  # Replaces NaN's with zeros

# Create total column and remove zero's
benefits_df['Total'] = pd.to_numeric(benefits_df.sum(axis=1))
benefits_df['COVID-19 Income Relief Payment'] = benefits_df[
    ['COVID-19 Income Relief Payment']
].replace(to_replace=0, value='')

# Upload to G Sheets
workbook_name = '1vLqh4tIbh70tKNOurJyGFaI3Vt_27V1PCvQ-n8GYg-c'
employment_dataframes = [
    unemp_rate_df,
    emp_growth_df,
    emp_status_df,
    benefits_df
]

upload_gsheets(
    workbook_name,
    employment_dataframes,
    sheets=[0,1,2,3]
)

format_gsheets(
    workbook_name,
    'B:C',
    'PERCENT',
    '0.0%',
    sheets=[0,1]
)

format_gsheets(
    workbook_name,
    'B:E',
    'NUMBER',
    '#,##0',
    sheets=[2,3]
)

time.sleep(10) # Slow down google API requests to not exceed limit

#-----YOUTH-----
# NEET rate by location
neet_location_df = hlfs_df.loc[
    (hlfs_df['Period']>='2007-01-01')
    & (
       (hlfs_df['Series_reference']=='HLFQ.S8CJQZ9')
       | (hlfs_df['Series_reference']=='HLFQ.S8CJQB9')
   )
]
neet_location_df = pd.pivot_table(
    neet_location_df,
    index='Quarter',
    values='Data_value',
    columns='Regional Council'
)/100
neet_location_df.rename(
    columns={'Total All Regional Councils':'New Zealand'},
    inplace=True
)
neet_location_df.reset_index(inplace=True)

# NEET rate by age
neet_age_df = hlfs_df.loc[
    (hlfs_df['Period']>='2007-01-01')
    & (
       (hlfs_df['Series_reference']=='HLFQ.S8CJQB1')
       | (hlfs_df['Series_reference']=='HLFQ.S8CJQB2')
       | (hlfs_df['Series_reference']=='HLFQ.S8CJQB9')
   )
]
neet_age_df = pd.pivot_table(
    neet_age_df,
    index='Quarter',
    values='Data_value',
    columns=['Youth age groups']
)/100
neet_age_df.reset_index(inplace=True)

# Upload to G Sheets
workbook_name = '5- Auckland-index-people-youth'
employment_dataframes = [
    neet_location_df,
    neet_age_df,
]

upload_gsheets(
    workbook_name,
    employment_dataframes,
    sheets=[0,1]
)

format_gsheets(
    workbook_name,
    'B:D',
    'PERCENT',
    '0.0%',
    sheets=[0,1]
)

time.sleep(10) # Slow down google API requests to not exceed limit



# %% BUSINESS AND ECONOMY

#-----CONFIDENCE INDICIES-----
# Create dataframe from RIMU monthly file
confidence_df = (
    pd.read_excel(
        rimu_file,
        sheet_name='Confidence',
        skiprows=6,
        usecols='A:C'
    )
)

confidence_df.dropna(thresh=2, inplace=True)
confidence_df.columns = [
    'Period',
    'Consumer Confidence Index (LHS)',
    'Business confidence (RHS, %)'
]
confidence_df = confidence_df.loc[confidence_df['Period']>='2000-01-01']
confidence_df['Period'] = confidence_df['Period'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q

# Upload to G Sheets
# Note gspread_pandas was not accepting full sheet name (was throwing an error),
# so sheet ID has been used instead
workbook_name = '1LoCgPcHk31wzrIZYtTDsiuLMwacWuk6TyjNpaycqpwI'
upload_gsheets(
    workbook_name,
    [confidence_df]
)

format_gsheets(
    workbook_name,
    'B',
    'NUMBER',
    '0.0'
)

format_gsheets(
    workbook_name,
    'C',
    'PERCENT',
    '0.0%'
)

time.sleep(10) # Slow down google API requests to not exceed limit

#-----CONSTRUCTION AND HOUSING-----
# Download filled jobs csv file
URL = 'https://www.stats.govt.nz/large-datasets/csv-files-for-download/'
options = Options()
options.headless = False
driver = webdriver.Chrome(
    executable_path=r'C:\windows\chromedriver',
    options=options
)
driver.get(URL) #opens URL on chrome to activate javascript
stats_soup = bs(driver.page_source, 'html.parser') #uses bs to get data from browser
driver.quit() #quits browser

# Find link for trade CSV file (Note: using soup object from employment rates code)
link = stats_soup.find(
    'a',
    href=re.compile('building-consents-issued-')
).get('href')
csv_download = ('https://www.stats.govt.nz' + link)
urllib.request.urlretrieve(csv_download, 'building-consents-issued.zip')
compressed_file = zipfile.ZipFile('building-consents-issued.zip')
filename = [
    s for s in compressed_file.namelist() if 'Building consents by region (Quarterly).csv' in s
] # searches for consents file
csv_file = compressed_file.open(filename[0]) # returns string of hlfs file

# Create consents dataframe
consents_df = pd.read_csv(csv_file, dtype='object')
consents_df['Period'] = pd.to_datetime(consents_df['Period'], format='%Y.%m')
consents_df['Quarter'] = consents_df['Period'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q
consents_df['Data_value'] = pd.to_numeric(
    consents_df['Data_value'],
    errors='coerce'
)
consents_df = consents_df.loc[
    (consents_df['Period']>='2006-01-01') & (
        (consents_df['Series_reference']=='BLDQ.SF021000A1A') |
        (consents_df['Series_reference']=='BLDQ.SF022000A1A')
    )
]
consents_df = pd.pivot_table(
    consents_df,
    index='Quarter',
    columns='Series_title_2',
    values='Data_value'
)
consents_df.reset_index(inplace=True)
consents_df['Residential consents issued'] = consents_df['Residential buildings'].rolling(4).sum()
consents_df['Non-residential consents issued'] = consents_df['Non-residential buildings'].rolling(4).sum()
consents_df['Residential consents growth'] = consents_df['Residential consents issued'].pct_change(periods=4)
consents_df['Non-residential consents growth'] = consents_df['Non-residential consents issued'].pct_change(periods=4)

# Split consents dataframe to number and growth
consents_num_df = consents_df.loc[
    8:, [
        'Quarter',
        'Residential consents issued',
        'Non-residential consents issued'
    ]
]

consents_growth_df = consents_df.loc[
    8:, [
        'Quarter',
        'Residential consents growth',
        'Non-residential consents growth'
    ]
]

# Upload to G Sheets
workbook_name = '15AEHTZDasK6921dJCs9q3SCEMpPWRsb1AobcrMKZqcI'
consents_dataframes = [
    consents_num_df,
    consents_growth_df
]

upload_gsheets(
    workbook_name,
    consents_dataframes,
    sheets=[3,4]
)

format_gsheets(
    workbook_name,
    'B:C',
    'NUMBER',
    '#,##0',
    sheets=[3]
)

format_gsheets(
    workbook_name,
    'B:C',
    'PERCENT',
    '0.0%',
    sheets=[4]
)

time.sleep(10) # Slow down google API requests to not exceed limit


#-----EXPORTS AND IMPORTS-----
service = "https://api.stats.govt.nz/opendata/v1/"
endpoint = "OverseasCargo"
entity = "Observations"
api_key = os.environ['STATS_KEY']

# Exports
query_option = """$filter=(
                        ResourceID eq 'OSC1.1' and
                        Period ge 2006-04-01 and
                        Label1 eq 'Exports' and
                        Measure eq 'Free on board value' and
                        (Label2 eq 'Auckland (sea)' or
                         Label2 eq 'Auckland Airport' or
                         Label2 eq 'Total All Cargo')
                        )
                &$select=Period,Label2,Value"""

# call the service
export_df = odata.get_odata(
    service,
    endpoint,
    entity,
    query_option,
    api_key,
    proxies
)

export_df = pd.pivot_table(
    export_df,
    index='Period',
    values='Value',
    columns='Label2'
).reset_index()

export_df['Auckland Seaport'] = export_df['Auckland (sea)'].rolling(12).sum().mul(1000)
export_df['Auckland Airport2'] = export_df['Auckland Airport'].rolling(12).sum().mul(1000)
export_df['All New Zealand ports'] = export_df['Total All Cargo'].rolling(12).sum().mul(1000)
export_df['All Auckland ports'] = (
    export_df['Auckland Seaport']
    + export_df['Auckland Airport2']
)
export_df['Rest of New Zealand ports'] = (
    export_df['All New Zealand ports']
    - export_df['All Auckland ports']
)
export_df['Period'] = pd.to_datetime(export_df['Period'], format='%Y-%m-%d')
export_df['Quarter'] = export_df['Period'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q
export_df.dropna(inplace=True) # Drop rows with less than 12 sum values
export_df = export_df.iloc[0::3,:] # Retain end of quarter month only
export_df = export_df[[
    'Quarter',
    'Auckland Seaport',
    'Auckland Airport2',
    'All Auckland ports',
    'Rest of New Zealand ports',
    'All New Zealand ports'
]]
export_df.rename(
    columns={
        'Auckland Airport2': 'Auckland Airport',
        'Quarter': 'Date'
    }, inplace=True
)

export_df.set_index(
    'Date',
    inplace=True
) # Set date as index for percentage change method

export_growth_df = export_df.pct_change(periods=4) # Create growth dataframe
export_growth_df.dropna(inplace=True) # Drop na columns
export_growth_df.reset_index(inplace=True) # Remove date from index for upload to G sheets
export_df = (export_df.iloc[4:,:]).reset_index() # Remove date from index for upload to G sheets


# Upload to G Sheets
workbook_name = '1d5rjwXQPFfSRiG4k08qNKDvM1ZS3eV_uaW14KE9P3gQ'
dataframes = [export_df, export_growth_df]
upload_gsheets(
    workbook_name,
    dataframes,
    sheets=[0,1]
)

format_gsheets(
    workbook_name,
    'B:F',
    'NUMBER',
    '$#,###',
    sheets=[0]
)

format_gsheets(
    workbook_name,
    'B:F',
    'PERCENT',
    '#0.0%',
    sheets=[1]
)

time.sleep(10) # Slow down google API requests to not exceed limit

# Imports
query_option = """$filter=(
                        ResourceID eq 'OSC1.1' and
                        Period ge 2006-04-01 and
                        Label1 eq 'Imports' and
                        Measure eq 'Cost, insurance and freight value' and
                        (Label2 eq 'Auckland (sea)' or
                         Label2 eq 'Auckland Airport' or
                         Label2 eq 'Total All Cargo')
                        )
                &$select=Period,Label2,Value"""

# call the service
import_df = odata.get_odata(
    service,
    endpoint,
    entity,
    query_option,
    api_key,
    proxies
)

import_df = pd.pivot_table(
    import_df,
    index='Period',
    values='Value',
    columns='Label2'
).reset_index()

import_df['Auckland Seaport'] = import_df['Auckland (sea)'].rolling(12).sum().mul(1000)
import_df['Auckland Airport2'] = import_df['Auckland Airport'].rolling(12).sum().mul(1000)
import_df['All New Zealand ports'] = import_df['Total All Cargo'].rolling(12).sum().mul(1000)
import_df['All Auckland ports'] = (
    import_df['Auckland Seaport']
    + import_df['Auckland Airport2']
)
import_df['Rest of New Zealand ports'] = (
    import_df['All New Zealand ports']
    - import_df['All Auckland ports']
)
import_df['Period'] = pd.to_datetime(import_df['Period'], format='%Y-%m-%d')
import_df['Quarter'] = import_df['Period'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q
import_df.dropna(inplace=True)
import_df = import_df.iloc[0::3,:]
import_df = import_df[[
    'Quarter',
    'Auckland Seaport',
    'Auckland Airport2',
    'All Auckland ports',
    'Rest of New Zealand ports',
    'All New Zealand ports'
]]
import_df.rename(
    columns={
        'Auckland Airport2':'Auckland Airport',
        'Quarter':'Date'
    }, inplace=True
)

import_df.set_index(
    'Date',
    inplace=True
) # Set date as index for percentage change method

import_growth_df = import_df.pct_change(periods=4) # Create growth dataframe
import_growth_df.dropna(inplace=True) # Drop na columns
import_growth_df.reset_index(inplace=True) # Remove date from index for upload to G sheets
import_df = (import_df.iloc[4:,:]).reset_index() # Remove date from index for upload to G sheets

# Upload to G Sheets
workbook_name = '1d5rjwXQPFfSRiG4k08qNKDvM1ZS3eV_uaW14KE9P3gQ'
dataframes = [import_df, import_growth_df]
upload_gsheets(
    workbook_name,
    dataframes,
    sheets=[3,4]
)

format_gsheets(
    workbook_name,
    'B:F',
    'NUMBER',
    '$#,###',
    sheets=[3]
)

format_gsheets(
    workbook_name,
    'B:F',
    'PERCENT',
    '#0.0%',
    sheets=[4]
)

time.sleep(10) # Slow down google API requests to not exceed limit



