#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Created on Tue Aug  4 13:19:28 2020

@author: tristanmeuross
'''
# %% Import modules, Google Sheets authorisation and functions

import pandas as pd
from bs4 import BeautifulSoup as bs
import re
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import os

from Python import stats_odata as odata
from modules.my_modules import upload_gsheets, download_gsheets, format_gsheets, delete_file

# header used for requests module authorisation
header = {
    'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'
}

# Setting proxies for gspread, requests and APIs
proxies = {}
# proxies = {'http':os.environ['HTTP_PROXY2'],
#            'https':os.environ['HTTPS_PROXY2']}

# os.environ['HTTPS_PROXY'] = os.environ['HTTPS_PROXY2']   

# To upload to google sheets, sheet needs to share to email:
# auckland-index-update@auckland-index-update.iam.gserviceaccount.com

# %% CITY CENTRE RECOVERY


# %% CONFIDENCE INDICIES

rimu_file = 'data_files/Economic Update Key Charts.xlsx'

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
confidence_df = confidence_df.loc[confidence_df['Period']>='2021-01-01']
confidence_df['Period'] = confidence_df['Period'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q

# Upload to G Sheets
# Note gspread_pandas was not accepting full sheet name (was throwing an error),
# so sheet ID has been used instead
workbook_name = '3. Auckland-index-covid-dashboard-confidence-indices'
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


# %% GOOGLE MOBILITY DATA

# Download CSV Data
URL = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'
options = Options()
# options.headless = False
data_folder = os.path.join(
    os.getenv('USERPROFILE'), 'Auckland-Index-Update\\data_files'
)  # Create's path for operating user
prefs = {'download.default_directory': data_folder}  # Download's to project folder path as above
options.add_experimental_option('prefs', prefs)
options.add_argument("start-maximized")
# options.add_argument("--headless")  # This setting stops a browser window from opening
driver = webdriver.Chrome(
    executable_path=r'C:\windows\chromedriver', options=options
)

# Remove previously downloaded MBIE data files
delete_file(data_folder, 'Global_Mobility_Report.csv')

driver.get(URL)

file_path = 'data_files/Global_Mobility_Report.csv'

while not os.path.exists(file_path):
    time.sleep(10)

if os.path.isfile(file_path):
    driver.quit()

csv_path = ('data_files/Global_Mobility_Report.csv')

# Create pandas dataframe from CSV data
df = pd.read_csv(
    csv_path,
    parse_dates=['date'],
    index_col='date',
    dtype={
        'country_region_code': 'string',
            'country_region': 'string',
           'sub_region_1': 'string',
            'sub_region_2': 'string',
            'metro_area': 'string',
            'iso_3166_2_code': 'string',
            'census_fips_code': 'float',
           'retail_and_recreation_percent_change_from_baseline': 'float',
           'grocery_and_pharmacy_percent_change_from_baseline': 'float',
           'parks_percent_change_from_baseline': 'float',
           'transit_stations_percent_change_from_baseline': 'float',
           'workplaces_percent_change_from_baseline': 'float',
           'residential_percent_change_from_baseline': 'float'}
)

# Rename columns
df.rename(
    columns={'retail_and_recreation_percent_change_from_baseline': 'retail_rec_raw',
             'grocery_and_pharmacy_percent_change_from_baseline': 'grocery_raw',
             'parks_percent_change_from_baseline': 'parks_raw',
             'transit_stations_percent_change_from_baseline': 'transit_stations_raw',
             'workplaces_percent_change_from_baseline': 'workplaces_raw',
             'residential_percent_change_from_baseline': 'residential_raw'},
    inplace=True
)

# Slice dataframe by Auckland
auckland_df = df.loc[df['sub_region_1'] == 'Auckland',
                     ['retail_rec_raw',
                      # 'grocery_raw',
                      # 'parks_raw',
                      # 'transit_stations_raw',
                      'workplaces_raw',
                      'residential_raw']
                     ]


raw = ['retail_rec_raw',
       # 'grocery_raw',
       # 'parks_raw',
       # 'transit_stations_raw',
       'workplaces_raw',
       'residential_raw']

ma = ['Retail & Recreation',
      # 'Grocery & Pharmacy',
      # 'Parks',
      # 'Transit Stations',
      'Workplaces',
      'Residential']

# Create moving averages
for i, x in zip(raw, ma):
    auckland_df[x] = (auckland_df[i].rolling(7).mean()) / 100

# Reset index for Google Sheets so date is a column and in correct format
auckland_df = auckland_df.reset_index()
auckland_df['date'] = pd.to_datetime(auckland_df['date'], format='%m/%d/%Y')


# Select relevant columns for G Sheets in correct order
auckland_df = auckland_df.loc[
    auckland_df['date']>='2021-01-01',
    ['date',
     'Retail & Recreation',
     # 'Grocery & Pharmacy',
     # 'Parks',
     # 'Transit Stations',
     'Workplaces',
     'Residential']
]

# Upload to Google Sheets
workbook_name = '4. Auckland-index-covid-dashboard-mobility'

upload_gsheets(
    workbook_name,
    [auckland_df]
)

# Format cells
format_gsheets(
    workbook_name,
    'B:G',
    'PERCENT',
    '0%'
)

format_gsheets(
    workbook_name,
    'A',
    'DATE',
    'dd-mmm-yy'
)


# %% TRANSPORT

# AT excel file download name changes, so needs to be scraped
URL = 'https://at.govt.nz/about-us/reports-publications/at-metro-patronage-report/'
options = Options()
options.headless = False  # This setting stops a browser window from opening
driver = webdriver.Chrome(executable_path=r'C:\windows\chromedriver',
                          options=options)
driver.get(URL)  # Opens URL on chrome to activate javascript
at_soup = bs(driver.page_source, 'html.parser')  # Uses bs to get data from browser
driver.quit()  # Quits browser
link = at_soup.find('a', href=re.compile('daily-patronage-for-at')).get('href')
excel_file = 'https://at.govt.nz/' + link

at_df = pd.concat(
    pd.read_excel(excel_file,
                  sheet_name=None,
                  skiprows=4,
                  parse_dates=['Business Date']),
    ignore_index=True
)  # Concat is to join both sheets in the excel file together

at_df = at_df.iloc[::-1]  # Reverse date order from oldest to newest
at_df['Rolling'] = at_df['Total'].rolling(7).mean().round(0)

# Create daily public transport patronage dataframe - NOTE that at_df file starts at July 2020
pt_df_22 = (
    at_df.loc[
        (at_df['Business Date'] >= '2022-01-01')
        & (at_df['Business Date'] <= '2022-12-31'),  # Ends 28th to line up with 2019-20 year's weekdays
        ['Business Date', 'Rolling']
    ]
).reset_index(drop=True)

pt_df_22.rename(columns={'Rolling': '2022'}, inplace=True)

# Download patronage data data already in Google Sheet (starts Jan 2019)
workbook_name = '5. Auckland-index-covid-dashboard-transport'
format_gsheets(
    workbook_name,
    'A',
    'DATE',
    'yyyy-mm-dd',
    sheets=[2]
)
download_df = download_gsheets(
    workbook_name,
    sheet=2
)

download_df['Date'] = pd.to_datetime(download_df['Date'], format='%Y-%m-%d')
download_df.columns = download_df.columns.astype(str)  # Convert column headers to string
download_df.drop(columns=['2022'], inplace=True)
download_df['2019'] = pd.to_numeric(download_df['2019'].str.replace(',', ''), errors='coerce')

# Join dataframes, adds the 2021 column to
pt_df = download_df.join(pt_df_22['2022'])

# Create light traffic dataframes from stats api
service = "https://api.stats.govt.nz/opendata/v1/"
endpoint = "Covid-19Indicators"
entity = "Observations"
query_option = """$filter=(
                        ResourceID eq 'CPTRA5' and
                        Geo eq 'Auckland' and
                        Label1 eq 'Light vehicles')
                &$select=Period,Value"""
api_key = os.environ['STATS_KEY']

# Call the service
light_df = odata.get_odata(
    service,
    endpoint,
    entity,
    query_option,
    api_key,
    proxies
)

light_df.sort_values(by='Period', inplace=True)

light_df['Rolling'] = light_df['Value'].rolling(7).mean().round(0)

light_df_19 = (
    light_df.loc[
        (light_df['Period'] >= '2019-01-01')
        & (light_df['Period'] <= '2019-12-31'),
        ['Period', 'Rolling']
    ]
).reset_index(drop=True)

light_df_22 = (
    light_df.loc[
        (light_df['Period'] >= '2022-01-01')
        & (light_df['Period'] <= '2022-12-31'),
        ['Period', 'Rolling']
    ]
).reset_index(drop=True)

# Create heavy traffic dataframes from stats api
service = "https://api.stats.govt.nz/opendata/v1/"
endpoint = "Covid-19Indicators"
entity = "Observations"
query_option = """$filter=(
                        ResourceID eq 'CPTRA5' and
                        Geo eq 'Auckland' and
                        Label1 eq 'Heavy vehicles')
                &$select=Period,Value"""
api_key = os.environ['STATS_KEY']

# Call the service
heavy_df = odata.get_odata(
    service,
    endpoint,
    entity,
    query_option,
    api_key,
    proxies
)

heavy_df.sort_values(by='Period', inplace=True)

heavy_df['Rolling'] = heavy_df['Value'].rolling(7).mean().round(0)

heavy_df_19 = (
    heavy_df.loc[
        (heavy_df['Period'] >= '2019-01-01')
        & (heavy_df['Period'] <= '2019-12-31'),
        ['Period', 'Rolling']
    ]
).reset_index(drop=True)

heavy_df_22 = (
    heavy_df.loc[
        (heavy_df['Period'] >= '2022-01-01')
        & (heavy_df['Period'] <= '2022-12-31'),
        ['Period', 'Rolling']
    ]
).reset_index(drop=True)

# Rename columns
light_df_19.columns = ['Date', '2019']
light_df_22.columns = ['Date', '2022']
heavy_df_19.columns = ['Date', '2019']
heavy_df_22.columns = ['Date', '2022']


# Join 2022, 2021 and 2020 into one dataframe
light_df = light_df_19.join(
    light_df_22['2022']
)

heavy_df = heavy_df_19.join(
    heavy_df_22['2022']
)

# Upload to Google Sheets
workbook_name = '5. Auckland-index-covid-dashboard-transport'

transport_dataframes = [light_df, heavy_df, pt_df]

upload_gsheets(
    workbook_name,
    transport_dataframes,
    sheets=[0, 1, 2]
)

# Format cells
format_gsheets(
    workbook_name,
    'A',
    'DATE',
    'dd-mmm',
    sheets=[0, 1, 2]
)


# %% ARRIVALS

# Set the variables for arrivals data
service = "https://api.stats.govt.nz/opendata/v1/"
endpoint = "Covid-19Indicators"
entity = "Observations"
query_option = """$filter=(
                        Measure eq 'Border crossings - arrivals' and
                        Period ge 2019-01-01 and
                        Label1 eq 'Total')
                &$select=Period,Value"""
api_key = os.environ['STATS_KEY']

# call the service
arrivals_df = odata.get_odata(
    service,
    endpoint,
    entity,
    query_option,
    api_key,
    proxies
)
arrivals_df.sort_values(by='Period', inplace=True)  # Arrivals data not in order

# Create arrivals dataframe
arrivals_19_df = arrivals_df.loc[
    (arrivals_df['Period'] >= '2019-01-01')
    & (arrivals_df['Period'] <= '2019-12-31')
].reset_index(drop=True)

arrivals_22_df = arrivals_df.loc[
    (arrivals_df['Period'] >= '2022-01-01')
    & (arrivals_df['Period'] <= '2022-12-31')
].reset_index(drop=True)

# Rename columns
arrivals_19_df.columns = ['Date', '2019']
arrivals_22_df.columns = ['Date', '2022']

# Join three dataframes together
arrivals_df = arrivals_19_df.join(
    arrivals_22_df['2022']
)

# Upload to Google sheets
workbook_name = '6. Auckland-index-covid-dashboard-arrivals'

upload_gsheets(
    workbook_name,
    [arrivals_df]
)

format_gsheets(
    workbook_name,
    'A',
    'DATE',
    'dd-mmm'
)


# %% CONSUMER SPENDING
URL = 'https://mbienz.shinyapps.io/card_spend_covid19/'
options = Options()
# options.headless = False
data_folder = os.path.join(
    os.getenv('USERPROFILE'), 'Auckland-Index-Update\\data_files'
)  # Create's path for operating user
prefs = {'download.default_directory': data_folder}  # Download's to project folder path as above
options.add_experimental_option('prefs', prefs)
options.add_argument("start-maximized")
# options.add_argument("--headless")  # This setting stops a browser window from opening
driver = webdriver.Chrome(
    executable_path=r'C:\windows\chromedriver', options=options
)
driver.get(URL)

# Accept caveats pop up
element = WebDriverWait(driver, 120).until(
    EC.presence_of_element_located(
        (By.XPATH, '//*[@id="shiny-modal"]/div/div/div[3]/button')
    )
)
driver.execute_script("arguments[0].click();", element)

# Remove previously downloaded MBIE data files
delete_file(data_folder, 'MBIE - COVID19 Response.csv')

# Download national data file
element = WebDriverWait(driver, 30).until(
    EC.presence_of_element_located(
        (By.XPATH, '//*[@id="DataTables_Table_0_wrapper"]/div[2]/button[2]')
    )
)

time.sleep(10)  # Wait extra time for data to load
element.click()  # Download CSV
time.sleep(15)  # Wait for file to download

national_df = pd.read_csv(
    'data_files/MBIE - COVID19 Response.csv', index_col=(0)
).transpose()
national_df.drop(national_df.index[0], inplace=True)
national_df.reset_index(inplace=True)
national_df['year'] = '2020'  # Create year column to convert date column (date column has no year)
national_df.loc[48:99, 'year'] = '2021'  # Update 2021 year
national_df.loc[100:, 'year'] = '2022'  # Update 2022 year
national_df['Date'] = pd.to_datetime(
    national_df[['index', 'year']].astype(str).apply('-'.join, 1),
    format='%b-%d-%Y'
)  # Convert to datetime
national_df.drop(
    ['Domestic', 'International', 'index', 'year', 'Date'],
    axis=1,
    inplace=True
)
national_df.rename(columns={'Total': 'New Zealand'}, inplace=True)
national_df = national_df / 100  # Convert to percentage

# Navigate to regional section
element = WebDriverWait(driver, 30).until(
    EC.presence_of_element_located(
        (By.XPATH, '//*[@id="sidebarItemExpanded"]/ul/li[2]/a')
    )
)
element.click()

# Remove previously downloaded MBIE data files
delete_file(data_folder, 'MBIE - COVID19 Response.csv')

# Copy data from regional - xpath seems to change, program will iterate through xpaths
try:
    element = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located(
            (By.XPATH, '//*[@id="DataTables_Table_1_wrapper"]/div[2]/button[2]')
        )
    )
except:
    element = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located(
            (By.XPATH, '//*[@id="DataTables_Table_1_wrapper"]/div[2]/button[2]')
        )
    )
time.sleep(10)  # Wait extra time for data to load
element.click()  # Download CSV (note previous file overwritten)
time.sleep(15)  # Wait for file to download

regional_df = pd.read_csv(
    'data_files/MBIE - COVID19 Response.csv', index_col=(0)
).transpose()
regional_df.drop(regional_df.index[0], inplace=True)
regional_df.reset_index(inplace=True)
regional_df['year'] = '2020'  # Create year column to convert date column (date column has no year)
regional_df.loc[48:99, 'year'] = '2021'  # Update 2021 year
regional_df.loc[100:, 'year'] = '2022'  # Update 2022 year
regional_df['Date'] = pd.to_datetime(
    regional_df[['index', 'year']].astype(str).apply('-'.join, 1),
    format='%b-%d-%Y'
)  # Convert to datetime
regional_df = regional_df[['Date', 'Auckland', 'Wellington']]
regional_df[['Auckland', 'Wellington']] = regional_df[
    ['Auckland', 'Wellington']
] / 100  # Convert to percentages

card_df = regional_df.reset_index(drop=True).join(national_df)

driver.quit()  # Quit driver

# Upload to Google Sheets
workbook_name = '2. Auckland-index-covid-dashboard-consumer-spending'

upload_gsheets(
    workbook_name,
    [card_df]
)

format_gsheets(
    workbook_name,
    'B:D',
    'PERCENT',
    '0.0%'
)

format_gsheets(
    workbook_name,
    'A',
    'DATE',
    'dd-mmm-yy'
)
