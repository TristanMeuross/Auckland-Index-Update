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
proxies = {'http':os.environ['HTTP_PROXY2'],
           'https':os.environ['HTTPS_PROXY2']}

os.environ['HTTPS_PROXY'] = os.environ['HTTPS_PROXY2']

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

# %% 1. QUARTERLY SNAPSHOT

# -----CONFIDENCE INDICIES-----
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
workbook_name = '1OAolOtOahSo-xmTqOcpAqy3gNSjXBA6d3dxa9WbZkLk'
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

# -----RETAIL SALES-----
# Create dataframe from RIMU monthly file
retail_df = pd.read_excel(
    rimu_file,
    sheet_name='Retail',
    skiprows=3,
    usecols='A:C'
).dropna()
retail_df.rename(columns={'Unnamed: 0':'Period'}, inplace=True)
retail_df = retail_df.loc[retail_df['Period']>='2008-01-01']
retail_df['Period'] = retail_df['Period'].dt.to_period('Q').dt.strftime('%Y Q%q')

#Upload to G Sheets
workbook_name = '1y5AXwdRX_IPDVSP9xjpFyv80G61WC2xqPI7pxCxcpks'

upload_gsheets(
    workbook_name,
    [retail_df]
)

format_gsheets(
    workbook_name,
    'B:C',
    'PERCENT',
    '0.0%'
)

time.sleep(10) # Slow down google API requests to not exceed limit

# -----EMPLOYMENT RATES-----
lfs_df = hlfs_df.loc[
    (hlfs_df['Period']>='2007-01-01') &
    (hlfs_df['Regional Council']=='Auckland')
].copy()

# Create Employment Levels dataframe
emp_levels_df = lfs_df.loc[
    (hlfs_df['Sex']=='Total Both Sexes') & (
        (lfs_df['Series_reference']=='HLFQ.S2A3QB') |
        (lfs_df['Series_reference']=='HLFQ.S2B3QB')
    )
]

emp_levels_df = pd.pivot_table(
    emp_levels_df,
    index='Quarter',
    values='Data_value',
    columns='Persons Employed, Unemployed, Not in Labour Force (for current quarter)'
)

emp_levels_df = (
    emp_levels_df[[
        'Persons Employed in Labour Force','Persons Unemployed in Labour Force'
    ]]*1000
).reset_index()

# Create Employment Rates dataframes
emp_rates_df = lfs_df.loc[
    (lfs_df['Series_reference']=='HLFQ.S2F3QB') &
    (hlfs_df['Sex']=='Total Both Sexes')
]
emp_rates_df = emp_rates_df[['Quarter', 'Data_value']].reset_index(drop=True)
emp_rates_df['Data_value'] = emp_rates_df['Data_value']/100
emp_rates_df2 = emp_levels_df['Persons Employed in Labour Force'].pct_change(periods=4)
emp_rates_df = emp_rates_df.join(emp_rates_df2)


# Create NEET rate dataframe
neet_df = lfs_df.loc[
    (lfs_df['Series_reference']=='HLFQ.S8CJQB9') |
    (lfs_df['Series_reference']=='HLFQ.S8CJQB1') |
    (lfs_df['Series_reference']=='HLFQ.S8CJQB2')
]

neet_df = (
    pd.pivot_table(
        neet_df,
        index='Quarter',
        values='Data_value',
        columns='Youth age groups'
    )/100
).reset_index()

neet_df.rename(
    columns={
        'Aged 15-19 years':'15-19 years only (%)',
        'Aged 20-24 years':'20-24 years only (%)',
        'Total aged 15-24 years':'Total Auckland NEET rate (%)'
    }, inplace=True
)


# Remove 2017 data (needed for growth rates)
emp_levels_df = emp_levels_df.iloc[4:]
emp_rates_df = emp_rates_df.iloc[4:]
neet_df = neet_df.iloc[4:]

# Rename and reorder columns
emp_levels_df.rename(
    columns={
        'Persons Employed in Labour Force':'Persons employed in labour force',
        'Persons Unemployed in Labour Force':'Persons unemployed in labour force'
    }, inplace=True
)
emp_rates_df.rename(
    columns={
        'Data_value':'Unemployment rate (RHS, %)',
        'Persons Employed in Labour Force':'Annual employment growth rate (LHS, %)'
    }, inplace=True
)
emp_rates_df = emp_rates_df[[
    'Quarter',
    'Annual employment growth rate (LHS, %)',
    'Unemployment rate (RHS, %)'
]]

# Upload to G Sheets
workbook_name = '1a3KZbjkS-_KkbL7KsShlNrm4MJpKMPPIeGS4omewPos'

lfs_dataframes = [
    emp_levels_df,
    emp_rates_df,
    neet_df
]

upload_gsheets(
    workbook_name,
    lfs_dataframes,
    sheets=[0,1,2]
)

format_gsheets(
    workbook_name,
    'B:C',
    'NUMBER',
    '#,##0',
    sheets=[0]
)

format_gsheets(
    workbook_name,
    'B:D',
    'PERCENT',
    '0.0%',
    sheets=[1,2]
)

time.sleep(10) # Slow down google API requests to not exceed limit

# -----EARNINGS-----
# Create earnings dataframe from RIMU Economic Indicators Database
wages_df = pd.read_excel(
    economic_file,
    sheet_name='Pop, Employ, Incomes',
    skiprows=10,
    usecols='A,CV:CW'
).dropna()
wages_df.rename(
    columns={
        'Unnamed: 0':'Quarter',
        'Auckland.9':'Auckland',
        'RoNZ.8':'Rest of New Zealand'
    }, inplace=True
)
wages_df = wages_df.loc[wages_df['Quarter']>='2008-01-01']
wages_df['Quarter'] = wages_df['Quarter'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q

# Create growth dataframe from RIMU monthly file
earnings_df = pd.read_excel(
    rimu_file,
    sheet_name='Wages',
    skiprows=3,
    usecols='A:C'
)
earnings_df.dropna(inplace=True)
earnings_df.rename(
    columns={
        'Unnamed: 0':'Date',
        'Auckland':'Auckland growth (%)',
        'Rest of New Zealand': 'Rest of NZ growth (%)'
    }, inplace=True
)
earnings_df = earnings_df.loc[earnings_df['Date']>='2008-01-01']
earnings_df['Quarter'] = earnings_df['Date'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q
earnings_df = earnings_df[[
    'Quarter',
    'Auckland growth (%)',
    'Rest of NZ growth (%)'
]]

# Upload to G Sheets
workbook_name = '1zRP5jgoP5KWCImZg9mKoTdK5u0FQvbCVwptUvE_UHl4'
earnings_dataframes = [
    wages_df,
    earnings_df
]

upload_gsheets(
    workbook_name,
    earnings_dataframes,
    sheets=[0,1]
)

format_gsheets(
    workbook_name,
    'B:C',
    'CURRENCY',
    '$#,##0.00',
    sheets=[0]
)

format_gsheets(
    workbook_name,
    'B:C',
    'PERCENT',
    '0.0%',
    sheets=[1]
)

time.sleep(10) # Slow down google API requests to not exceed limit

# -----CONSTRUCTION-----
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
workbook_name = '1wtNht14rRWIoHCNAPBb4_lZvMP5J_iVx3tr4YNa5LmQ'
consents_dataframes = [
    consents_num_df,
    consents_growth_df
]

upload_gsheets(
    workbook_name,
    consents_dataframes,
    sheets=[0,1]
)

format_gsheets(
    workbook_name,
    'B:C',
    'NUMBER',
    '#,##0',
    sheets=[0]
)

format_gsheets(
    workbook_name,
    'B:C',
    'PERCENT',
    '0.0%',
    sheets=[1]
)

time.sleep(10) # Slow down google API requests to not exceed limit

#%%
# -----TRANSPORT-----
#AT excel file download name changes, so needs to be scraped
URL = 'https://at.govt.nz/about-us/reports-publications/at-metro-patronage-report/'
options = Options()
options.headless = False #This setting stops a browser window from opening
driver = webdriver.Chrome(executable_path=r'C:\windows\chromedriver',
                          options=options)
driver.get(URL) #opens URL on chrome to activate javascript

at_soup = bs(driver.page_source, 'html.parser') #uses bs to get data from browser
driver.quit() #quits browser
link = at_soup.find(
    'a',
    href=re.compile('web-pax-')
).get('href')
xlsx_file = 'https://at.govt.nz/' + link

# Create patronage dataframe
patronage_df = pd.read_excel(
    xlsx_file,
    skiprows=4,
    thousands=','
)
patronage_df.dropna(
    thresh=4,
    inplace=True
)
patronage_df['Month-Year'] = pd.to_datetime(
    patronage_df['Month-Year'],
    format='%b-%y'
)
patronage_df['Quarter'] = patronage_df['Month-Year'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q
patronage_df['Bus'] = patronage_df['Bus Total'].rolling(12).sum()*1000 # add rolling 12 month column
patronage_df['Train'] = patronage_df['Train - (Rapid)'].rolling(12).sum()*1000
patronage_df['Ferry'] = patronage_df['Ferry'].rolling(12).sum()*1000
patronage_df['Bus Growth'] = patronage_df['Bus'].pct_change(periods=12) # Growth columns
patronage_df['Train Growth'] = patronage_df['Train'].pct_change(periods=12)
patronage_df['Ferry Growth'] = patronage_df['Ferry'].pct_change(periods=12)

# Slice by months at the end of each quarter (i.e. quarterly figures)
patronage_df = patronage_df.loc[patronage_df['Month-Year']>='2008-03-01'] #filter for periods after march 2008
patronage_df = patronage_df.iloc[::3]

# Create numbers and growth dataframes
pat_num_df = patronage_df[[
    'Quarter',
    'Bus',
    'Train',
    'Ferry'
]].copy()
pat_growth_df = patronage_df[[
    'Quarter',
    'Bus Growth',
    'Train Growth',
    'Ferry Growth'
]].copy()
pat_growth_df.rename(
    columns={
        'Bus Growth':'Bus',
        'Train Growth':'Train',
        'Ferry Growth':'Ferry'
    }, inplace=True
)

# Upload to G Sheets
workbook_name = '122UI9Dqx15B23V7ALDxFhAFXid4T848TelXl-CXzTPU'

patronage_dataframes = [
    pat_num_df,
    pat_growth_df
]

upload_gsheets(
    workbook_name,
    patronage_dataframes,
    sheets=[0,1]
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
    'B:D',
    'PERCENT',
    '0.0%',
    sheets=[1]
)

time.sleep(10) # Slow down google API requests to not exceed limit

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

# Create Auckland/NZ Neet rate datasheet
neet_df = hlfs_df.loc[
    (hlfs_df['Period']>='2008-01-01')
    & (
       (hlfs_df['Series_reference']=='HLFQ.S8CJQB9')
       | (hlfs_df['Series_reference']=='HLFQ.S8CJQZ9')
   ) 
]

neet_df = (
    pd.pivot_table(
        neet_df,
        index='Quarter',
        values='Data_value',
        columns='Regional Council'
    ) / 100
).reset_index()

neet_df.rename(
    columns={
        'Total All Regional Councils': 'Total New Zealand NEET rate',
        'Auckland': 'Total Auckland NEET rate'
        }, inplace=True
)

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

# Upload to G Sheets
workbook_name = '1vLqh4tIbh70tKNOurJyGFaI3Vt_27V1PCvQ-n8GYg-c'
employment_dataframes = [
    unemp_rate_df,
    neet_df,
    emp_growth_df,
    emp_status_df
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
    sheets=[0,1,2]
)

format_gsheets(
    workbook_name,
    'B:E',
    'NUMBER',
    '#,##0',
    sheets=[3]
)

time.sleep(10) # Slow down google API requests to not exceed limit

#-----YOUTH-----


# %% BUSINESS AND ECONOMY

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
workbook_name = '1mjdmQvubkMaVtzRYrIvhlXoYCz74hd_bqIjjN5jIAjg'
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

#%%
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
workbook_name = '1LOldkkOq4C9XVcPhdX8CKVPtZHctCbOD3A2s2PjJQhQ'
dataframes = [import_df, import_growth_df]
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

# %% 5. MARKETS

# -----INFLATION-----
# Create inflation dataframe from RIMU Economic Indicators Database
inflation_df = pd.read_excel(
    economic_file,
    sheet_name='Inflation, Current AC',
    skiprows=8,
    usecols=['Unnamed: 0', 'CPI Total']
).dropna()
inflation_df.rename(columns={
    'Unnamed: 0': 'Quarter',
    'CPI Total': 'New Zealand'
    }, inplace=True
)
inflation_df = inflation_df.loc[inflation_df['Quarter']>='2008-03-01']
inflation_df['New Zealand'] = inflation_df['New Zealand'] * 100
inflation_df['Quarter'] = inflation_df['Quarter'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q

# Upload to G Sheets
workbook_name = '1-Auckland-index-markets-inflation-CPI'
upload_gsheets(
    workbook_name,
    [inflation_df],
    sheets=[0]
)

format_gsheets(
    workbook_name,
    'B',
    'NUMBER',
    '0.0',
    sheets=[0]
)

time.sleep(10) # Slow down google API requests to not exceed limit

# -----INTEREST RATES-----
# Create inflation dataframe from RIMU Economic Indicators Database
interest_df = pd.read_excel(
    economic_file,
    sheet_name='Exchange rates, Interest rates',
    skiprows=9,
    usecols=['Unnamed: 0', 'Official Cash Rate']
).dropna()
interest_df.rename(columns={
    'Unnamed: 0': 'Quarter',
    'Official Cash Rate': 'Official Cash Rate (OCR)'
    }, inplace=True
)
interest_df = interest_df.loc[interest_df['Quarter']>='2008-03-01']
interest_df = interest_df.iloc[0::3,:]
interest_df['Official Cash Rate (OCR)'] = interest_df['Official Cash Rate (OCR)'] * 100
interest_df['Quarter'] = interest_df['Quarter'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q

# Upload to G Sheets
workbook_name = '1HYDvqGzJVI89ozonrWQZk0o2cCL6ZjFhjXSouC52evM'
upload_gsheets(
    workbook_name,
    [interest_df],
    sheets=[0]
)

format_gsheets(
    workbook_name,
    'B',
    'NUMBER',
    '0.00',
    sheets=[0]
)

time.sleep(10) # Slow down google API requests to not exceed limit

# -----EXCHANGE RATES-----
api_key = os.environ['QUANDL_KEY']

# Call quandl api to get currencies exchange data (to USD)
usd_df = quandl.get(
    "FED/RXI_US_N_M_NZ",
    authtoken=api_key,
    start_date="2000-01-01"
)

aud_df = quandl.get(
    "FED/RXI_US_N_M_AL",
    authtoken=api_key,
    start_date="2000-01-01"
)

eur_df = quandl.get(
    "FED/RXI_US_N_M_EU",
    authtoken=api_key,
    start_date="2000-01-01"
)

jpy_df = quandl.get(
    "FED/RXI_N_M_JA",
    authtoken=api_key,
    start_date="2000-01-01"
)

gbp_df = quandl.get(
    "FED/RXI_US_N_M_UK",
    authtoken=api_key,
    start_date="2000-01-01"
)

cny_df = quandl.get(
    "FED/RXI_N_M_CH",
    authtoken=api_key,
    start_date="2000-01-01"
)

aud_df = (usd_df / aud_df).reset_index()
eur_df = (usd_df / eur_df).reset_index()
gbp_df = (usd_df / gbp_df).reset_index()
cny_df = (usd_df * cny_df).reset_index() # Note exchange data from quandl is reversed
jpy_df = (usd_df * jpy_df).reset_index() # Note exchange data from quandl is reversed
usd_df = usd_df.reset_index()

# Upload to G Sheets
workbook_name = '1MNVhaCFelJVwnc5_4fpJOGYulWBeEBMDwFNJ8N1CqNY'
forex_dataframes = [aud_df, eur_df, gbp_df, usd_df, cny_df, jpy_df]

upload_gsheets(
    workbook_name,
    forex_dataframes,
    sheets=[0,1,2,3,4,5]
)

format_gsheets(
    workbook_name,
    'B',
    'NUMBER',
    '0.0000',
    sheets=[0,1,2,3,4]
)

format_gsheets(
    workbook_name,
    'B',
    'NUMBER',
    '0.00',
    sheets=[5]
)

format_gsheets(
    workbook_name,
    'A',
    'DATE',
    'Mmm yyyy',
    sheets=[0,1,2,3,4,5]
)

time.sleep(10) # Slow down google API requests to not exceed limit

# -----NZ STOCK EXCHANGE-----

URL = 'https://www.spglobal.com/spdji/en/idsexport/file.xls?hostIdentifier=48190c8c-42c4-46af-8d1a-0cd5db894797&redesignExport=true&languageId=1&selectedModule=PerformanceGraphView&selectedSubModule=Graph&yearFlag=oneYearFlag&indexId=92029427'
options = Options()
data_folder = os.path.join(
    os.getenv('USERPROFILE'), 'Auckland-Index-Update\data_files'
)  # Create's path for operating user
prefs = {'download.default_directory': data_folder}  # Download's to project folder path as above
options.add_experimental_option('prefs', prefs)
options.headless = False
driver = webdriver.Chrome(executable_path=r'C:\windows\chromedriver',
                          options=options)

# Delete previously downloaded file
delete_file(data_folder, 'PerformanceGraphExport.xls')

# Download new NZX index file
driver.get(URL)
time.sleep(10) # wait for file to download
driver.quit()

#%%

#Create NZX50 dataframe from S&P excel sheet (10 year period only)
nzx50_df = pd.read_excel('data_files/PerformanceGraphExport.xls',
                          skiprows=6).dropna()

nzx50_df.columns = ['Date',
                    'Monthly average spot price (NZ$)']
nzx50_df['Date'] = pd.to_datetime(nzx50_df['Date'], format='%Y/%m/%d %H:%M:%S')
nzx50_df = nzx50_df.set_index('Date')
nzx50_df = (nzx50_df.resample('M').mean()).reset_index()
nzx50_df = nzx50_df[1:-1] # Removes first and last months, which are incomplete

#Upload to Google Sheets
workbook_name = '5-Auckland-index-markets-NZX50'
upload_gsheets(
    workbook_name,
    [nzx50_df]
)

#Format Google Sheet cells
format_gsheets(
    workbook_name,
    'A',
    'DATE',
    'yyyy mmm'
)

format_gsheets(
    workbook_name,
    'B',
    'CURRENCY',
    '$#,##0.00'
)

time.sleep(10) # Slow down google API requests to not exceed limit

#%% Currently getting blocked by RBNZ website

# # -----GOVERNMENT BONDS-----
# # Create bond yield dataframe from rbnz excel sheet
# URL = 'https://www.rbnz.govt.nz/statistics/b2'
# options = Options()
# data_folder = os.path.join(
#     os.getenv('USERPROFILE'), 'Auckland-Index-Update\data_files'
# )  # Create's path for operating user
# prefs = {'download.default_directory': data_folder}  # Download's to project folder path as above
# options.add_experimental_option('prefs', prefs)
# options.headless = False #This setting stops a browser window from opening
# driver = webdriver.Chrome(executable_path=r'C:\windows\chromedriver',
#                           options=options)
# driver.get(URL) #opens URL on chrome to activate javascript
# element = WebDriverWait(driver, 30).until(
#     EC.presence_of_element_located(
#         (By.XPATH, '//*[@id="RbnzContent"]/div[3]/div[5]/div[2]/a[5]')
#     )
# )
# element.click()
# #%%
# headers = ['Month',
#             '10-year government bond yield (monthly)']
# bond_df = pd.read_excel(
#     xlsx_file,
#     skiprows=5,
#     header=None,
#     usecols='A,J',
#     names=headers
# )
# driver.quit()
# bond_df['Quarter'] = bond_df['Month'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q
# bond_df['10-year government bond yield'] = bond_df['10-year government bond yield (monthly)'].rolling(3).mean()
# bond_df = bond_df[['Quarter',
#                             '10-year government bond yield']].loc[bond_df['Month']>='2008-03-01']
# bond_df = bond_df.iloc[::3] #Data comes as monthly, need to extract end of quarter month
# print(bond_df)
# #%%
# # Upload to G Sheets
# workbook_name = '4-Auckland-index-markets-10y-govt-bond-yield'
# upload_gsheets(
#     workbook_name,
#     [bond_df],
#     sheets=[0]
# )

# format_gsheets(
#     workbook_name,
#     'B',
#     'NUMBER',
#     '0.00',
#     sheets=[0]
# )


