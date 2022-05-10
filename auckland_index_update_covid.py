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
import numpy as np
import os

from Python import stats_odata as odata
from modules.my_modules import upload_gsheets, download_gsheets, format_gsheets, delete_file

# header used for requests module authorisation
header = {
    'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'
}

# Setting proxies for gspread, requests and APIs
proxies = {'http':os.environ['HTTP_PROXY2'],
           'https':os.environ['HTTPS_PROXY2']}

os.environ['HTTPS_PROXY'] = os.environ['HTTPS_PROXY2']   

# To upload to google sheets, sheet needs to share to email:
# auckland-index-update@auckland-index-update.iam.gserviceaccount.com

# %% COVID CASES

# Download cases data from MOH github repository
cases_df = pd.read_csv('https://raw.githubusercontent.com/minhealthnz/nz-covid-data/main/cases/covid-cases.csv',
                       dtype={'Report Date': object,
                              'Case Status': object,
                              'Sex': object,
                              'Age group': object,
                              'DHB': object,
                              'Overseas Travel': object,
                              'Historical': object})
cases_df.loc[(cases_df['DHB'] == 'Auckland') |
             (cases_df['DHB'] == 'Counties Manukau') |
             (cases_df['DHB'] == 'Waitemata'), 'DHB'] = 'Auckland'
cases_df.loc[cases_df['DHB'] != 'Auckland', 'DHB'] = 'Rest of New Zealand'

daily_df = pd.pivot_table(
    cases_df,
    values='Case Status',
    columns='DHB',
    index='Report Date',
    aggfunc='count').fillna(0)
daily_df.index = pd.to_datetime(daily_df.index)
daily_df['Total'] = daily_df.sum(axis=1)
daily_df = daily_df.sort_index().asfreq(freq='D', fill_value=0)
cumulative_df = daily_df.cumsum()
daily_df.reset_index(inplace=True)
cumulative_df.reset_index(inplace=True)

# Create tests per day dataframe from stats api
service = "https://api.stats.govt.nz/opendata/v1/"
endpoint = "Covid-19Indicators"
entity = "Observations"
query_option = """$filter=(
                        ResourceID eq 'CPCOV1')
                &$select=Period,Value"""
api_key = os.environ['STATS_KEY']

# Call the service
tests_df = odata.get_odata(
    service,
    endpoint,
    entity,
    query_option,
    api_key,
    proxies
)

# Sort by date
tests_df.sort_values(by='Period', inplace=True)

tests_df.rename(
    columns={'Period': 'Date', 'Value': 'Tests per day'}, inplace=True
)

# Create vaccines per day dataframe from stats api
service = "https://api.stats.govt.nz/opendata/v1/"
endpoint = "Covid-19Indicators"
entity = "Observations"
query_option = """$filter=(
                        ResourceID eq 'CPCOV9')
                &$select=Period,Label1,Value"""
api_key = os.environ['STATS_KEY']

# Call the service
vaccines_daily_df = odata.get_odata(
    service,
    endpoint,
    entity,
    query_option,
    api_key,
    proxies
)

# Pivot 'dose type' into columns
vaccines_daily_df = pd.pivot_table(
    vaccines_daily_df,
    values='Value',
    columns='Label1',
    index='Period'
).reset_index()

# Sort by date
vaccines_daily_df.sort_values(by='Period', inplace=True)

# Organise columns
vaccines_daily_df = vaccines_daily_df[[
    'Period',
    'First dose administered',
    'Second dose administered',
    'Third primary administered',
    'Boosters administered'
]]

# Create share of population vaccinated dataframe from ourworldindata.org
csv_file = 'https://github.com/owid/covid-19-data/raw/master/public/data/vaccinations/vaccinations.csv'
vacc_share_df = pd.read_csv(csv_file)
vacc_share_df = (
    vacc_share_df.loc[
        vacc_share_df['location'] == 'New Zealand',
        ['date',
         'people_vaccinated',
         'people_fully_vaccinated']
    ]
).dropna()
vacc_share_df['date'] = pd.to_datetime(vacc_share_df['date'], format='%Y-%m-%d')
vacc_share_df['% of population with one dose'] = vacc_share_df['people_vaccinated'] / 4209057 # HSU +12 population from MOH
vacc_share_df['% of population with two or more doses'] = vacc_share_df['people_fully_vaccinated'] / 4209057

#
vacc_share_df = vacc_share_df[['date',
                               '% of population with one dose',
                               '% of population with two or more doses']]

# Upload to Google Sheets
cases_dataframes = [
    daily_df,
    cumulative_df,
    tests_df,
    vaccines_daily_df,
    vacc_share_df
]

workbook_name = '1. Auckland-index-covid-dashboard-covid-cases'

upload_gsheets(
    workbook_name,
    cases_dataframes,
    sheets=[0, 1, 2, 3, 4]
)

# Format cells
format_gsheets(
    workbook_name,
    'A',
    'DATE',
    'dd-mmm-yy',
    sheets=[0, 1, 2, 3, 4]
)

format_gsheets(
    workbook_name,
    'B:E',
    'NUMBER',
    '0',
    sheets=[0, 1, 2, 3]
)

format_gsheets(
    workbook_name,
    'B:E',
    'PERCENT',
    '0.0%',
    sheets=[4]
)

# %% UNEMPLOYMENT BENEFITS AND PAYMENTS

# Set the variables for Jobseeker by region data
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
auckland_df = pd.merge(
    jobseeker_df,
    cirp_df,
    on='Date',
    how='left'
).fillna(0)  # Replaces NaN's with zeros

# Create total column and remove zero's
auckland_df['Total'] = pd.to_numeric(auckland_df.sum(axis=1))
auckland_df['COVID-19 Income Relief Payment'] = auckland_df[
    ['COVID-19 Income Relief Payment']
].replace(to_replace=0, value='')

# Upload to Google Sheets
workbook_name = '2. Auckland-index-covid-dashboard-unemployment-benefits-and-payments'

upload_gsheets(
    workbook_name,
    [auckland_df]
)

# Format Google Sheet cells
format_gsheets(
    workbook_name,
    'A',
    'DATE',
    'dd-mmm-yy'
)


# %% GOOGLE MOBILITY DATA

# Download CSV data
csv_path = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'

# Create pandas dataframe from CSV data
df = pd.read_csv(
    csv_path,
    parse_dates=['date'],
    index_col='date',
    dtype={'country_region_code': 'string',
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
                      'grocery_raw',
                      'parks_raw',
                      'transit_stations_raw',
                      'workplaces_raw',
                      'residential_raw']
                     ]

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

# Create moving averages
for i, x in zip(raw, ma):
    auckland_df[x] = (auckland_df[i].rolling(7).mean()) / 100

# Reset index for Google Sheets so date is a column and in correct format
auckland_df = auckland_df.reset_index()
auckland_df['date'] = auckland_df['date'].dt.strftime('%m/%d/%Y')

# Select relevant columns for G Sheets in correct order
auckland_df = auckland_df.loc[
    6:,
    ['date',
     'Retail & Recreation',
     'Grocery & Pharmacy',
     'Parks',
     'Transit Stations',
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

# Creates dataframe with NaN values
nan_df_22 = pd.DataFrame(
    [[np.nan] * len(pt_df_22.columns)],
    index=[58.5],
    columns=pt_df_22.columns
)

# Adds empty row for missing leap year day to line up with 2020
pt_df_22 = nan_df_22.append(pt_df_22).sort_index().reset_index(drop=True)

# Download patronage data data already in Google Sheet (starts Jan 2019)
workbook_name = '6. Auckland-index-covid-dashboard-transport'
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
download_df['2021'] = pd.to_numeric(download_df['2021'].str.replace(',', ''), errors='coerce')
download_df['2020'] = pd.to_numeric(download_df['2020'].str.replace(',', ''), errors='coerce')
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

light_df_20 = (
    light_df.loc[
        (light_df['Period'] >= '2020-01-01')
        & (light_df['Period'] <= '2020-12-31'),
        ['Period', 'Rolling']
    ]
).reset_index(drop=True)

light_df_21 = (
    light_df.loc[
        (light_df['Period'] >= '2021-01-01')
        & (light_df['Period'] <= '2021-12-31'),
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

heavy_df_20 = (
    heavy_df.loc[
        (heavy_df['Period'] >= '2020-01-01')
        & (heavy_df['Period'] <= '2020-12-31'),
        ['Period', 'Rolling']
    ]
).reset_index(drop=True)

heavy_df_21 = (
    heavy_df.loc[
        (heavy_df['Period'] >= '2021-01-01')
        & (heavy_df['Period'] <= '2021-12-31'),
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
light_df_20.columns = ['Date', '2020']
light_df_21.columns = ['Date', '2021']
light_df_22.columns = ['Date', '2022']
heavy_df_19.columns = ['Date', '2019']
heavy_df_20.columns = ['Date', '2020']
heavy_df_21.columns = ['Date', '2021']
heavy_df_22.columns = ['Date', '2022']

# # Add empty cells to line up weekdays across 2019-21
# nan_df_20 = pd.DataFrame(
#     [[np.nan] * len(light_df_20.columns)],
#     index=[0],
#     columns=light_df_20.columns
# )

# nan_df_21 = pd.DataFrame(
#     [[np.nan] * len(light_df_21.columns)],
#     index=[0, 1, 2],
#     columns=light_df_21.columns
# )

# # Adds empty rows to line weekdays up for 2019-20 year's with 2021
# light_df_20 = nan_df_20.append(light_df_20, ignore_index=True)
# light_df_21 = nan_df_21.append(light_df_21, ignore_index=True)
# heavy_df_20 = nan_df_20.append(heavy_df_20, ignore_index=True)
# heavy_df_21 = nan_df_21.append(heavy_df_21, ignore_index=True)

# Join 2022, 2021 and 2020 into one dataframe
light_df = light_df_19.join([
    light_df_20['2020'],
    light_df_21['2021'],
    light_df_22['2022']
])

heavy_df = heavy_df_19.join([
    heavy_df_20['2020'],
    heavy_df_21['2021'],
    heavy_df_22['2022']
])

# Upload to Google Sheets
workbook_name = '6. Auckland-index-covid-dashboard-transport'

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

# %% JOBS
# Set the variables for filled jobs data
service = "https://api.stats.govt.nz/opendata/v1/"
endpoint = "EmploymentIndicators"
entity = "Observations"
query_option = """$filter=(
                        ResourceID eq 'MEI4.1' and
                        Geo eq 'Auckland Region')
                &$select=Period,Value"""
api_key = os.environ['STATS_KEY']

# Call the service
filledjobs_df = odata.get_odata(
    service,
    endpoint,
    entity,
    query_option,
    api_key,
    proxies
)

# Sort to date order
filledjobs_df.sort_values(by='Period', inplace=True)

filledjobs_df.rename(
    columns={'Period': 'Month',
             'Value': 'Auckland'},
    inplace=True
)

# Set the variables for the jobs online data
endpoint = "Covid-19Indicators"
query_option = """$filter=(
                        ResourceID eq 'CPEMP5' and
                        Period ge 2017-01-01)
                &$select=Period,Geo,Value"""

# Call the service
jobsonline_df = odata.get_odata(
    service,
    endpoint,
    entity,
    query_option,
    api_key,
    proxies
)

# Sort to date order
jobsonline_df.sort_values(by='Period', inplace=True)

jobsonline_df = pd.pivot_table(
    jobsonline_df,
    values='Value',
    columns='Geo',
    index='Period'
).reset_index()

# Rebase index to Jan 2017
regions = [
    'Auckland',
    'Canterbury',
    'Wellington',
    'North Island (Other)',
    'South Island (Other)'
]

for i in regions:
    jobsonline_df[i] = (
        jobsonline_df[i] / jobsonline_df.loc[0, i] * 100
    ).round(1)

jobsonline_df.rename(columns={'Period': 'Date'}, inplace=True)
regions.insert(0, 'Date')  # add date column to list

jobsonline_df = jobsonline_df[regions]

# Upload to Google Sheets
workbook_name = '7. Auckland-index-covid-dashboard-jobs'
jobs_dataframes = [filledjobs_df, jobsonline_df]

upload_gsheets(
    workbook_name,
    jobs_dataframes,
    sheets=[0, 1]
)

format_gsheets(
    workbook_name,
    'A',
    'DATE',
    'mmm yyyy',
    sheets=[0, 1]
)

format_gsheets(
    workbook_name,
    'B:F',
    'NUMBER',
    '#.0',
    sheets=[1]
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

arrivals_20_df = arrivals_df.loc[
    (arrivals_df['Period'] >= '2020-01-01')
    & (arrivals_df['Period'] <= '2020-12-31')
].reset_index(drop=True)

arrivals_21_df = arrivals_df.loc[
    (arrivals_df['Period'] >= '2021-01-01')
    & (arrivals_df['Period'] <= '2021-12-31')
].reset_index(drop=True)

arrivals_22_df = arrivals_df.loc[
    (arrivals_df['Period'] >= '2022-01-01')
    & (arrivals_df['Period'] <= '2022-12-31')
].reset_index(drop=True)

# Rename columns
arrivals_19_df.columns = ['Date', '2019']
arrivals_20_df.columns = ['Date', '2020']
arrivals_21_df.columns = ['Date', '2021']
arrivals_22_df.columns = ['Date', '2022']

# Drop 29th Feb line from 2020 dataframe
i = arrivals_20_df[(arrivals_20_df['Date'] == '2020-02-29')].index
arrivals_20_df.drop(i, inplace=True)
arrivals_20_df.reset_index(drop=True, inplace=True)

# Join three dataframes together
arrivals_df = arrivals_19_df.join([
    arrivals_20_df['2020'],
    arrivals_21_df['2021'],
    arrivals_22_df['2022']
])

# Upload to Google sheets
workbook_name = '8. Auckland-index-covid-dashboard-arrivals'

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

# %% TRADE

# Download filled jobs csv file
URL = 'https://www.stats.govt.nz/large-datasets/csv-files-for-download/'
options = Options()
options.headless = False  # This setting stops a browser window from opening
driver = webdriver.Chrome(
    executable_path=r'C:\windows\chromedriver',
    options=options
)
driver.get(URL)  # Opens URL on chrome to activate javascript
stats_soup = bs(driver.page_source, 'html.parser')  # Uses bs to get data from browser
driver.quit()  # Quits browser

# Find link for trade CSV file
link = stats_soup.find(
    'a', href=re.compile('Effects-of-COVID-19-on-trade')
).get('href')

csv_download = ('https://www.stats.govt.nz' + link)

# Create dataframe
df = pd.read_csv(csv_download)
df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')

# Filter to relevant data and pivot so that categories are in columns
filtered_df = df.loc[
    (df['Direction'] == 'Exports')
    & (df['Country'] == 'All')
    & (df['Transport_Mode'] == 'All')
    & (df['Measure'] == '$')
].reset_index(drop=True)

# Pivot so that commodities are in columns
pivot_df = pd.pivot_table(
    filtered_df,
    index='Date',
    values='Value',
    columns='Commodity'
).dropna()

# Update values to rolling 28 day average
pivot_df = pivot_df.rolling(28).mean()

# Create df's of relevant date ranges
df_2019 = pivot_df.loc['2019-01-01':'2019-12-31']
df_2020 = pivot_df.loc['2020-01-01':'2020-12-31']
df_2021 = pivot_df.loc['2021-01-01':'2021-12-31']
df_2022 = pivot_df.loc['2022-01-01':'2022-12-31']
df_2019_ly = (df_2019.append(df_2019.iloc[-1:]))  # Create 366 line 2019 dataframe (last day repeated) to compare with 2020 leap year

# Calculate 2020 percentage change
pct_df = df_2019_ly.append(df_2020)
trade_df_20 = (pct_df.pct_change(periods=366)).dropna()

# Calculate 2021 percentage change
pct_df = df_2019.append(df_2021)
trade_df_21 = (pct_df.pct_change(periods=365)).dropna()

# Calculate 2022 percentage change
pct_df = df_2019.append(df_2022)
trade_df_22 = (pct_df.pct_change(periods=365)).dropna()

# Combine df's
trade_df = pd.concat([trade_df_20, trade_df_21, trade_df_22])
trade_df.reset_index(inplace=True)
trade_df.rename(
    {'All': 'Total'},
    inplace=True
)

# Upload to Google Sheets
workbook_name = '9. Auckland-index-covid-dashboard-trade'

upload_gsheets(
    workbook_name,
    [trade_df]
)

format_gsheets(
    workbook_name,
    'A',
    'DATE',
    'dd-mmm-yy'
)

format_gsheets(
    workbook_name,
    'B:H',
    'PERCENT',
    '0.0%'
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
workbook_name = '3. Auckland-index-covid-dashboard-consumer-spending'

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
