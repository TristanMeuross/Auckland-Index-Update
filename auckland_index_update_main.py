# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 16:01:07 2020

@author: meurost
"""

# %% Import modules, Google Sheets authorisation and functions
import pandas as pd
import pygsheets
from bs4 import BeautifulSoup as bs
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import zipfile
import urllib.request

# Location of RIMU Datasheet
rimu_file = 'data_files/Economic Update Key Charts.xlsx'

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

# Download HLFS csv file
URL = 'https://www.stats.govt.nz/large-datasets/csv-files-for-download/'
options = Options()
options.headless = True
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
urllib.request.urlretrieve(csv_download, 'labour-market-statistics-september-2020-quarter-csv.zip') #downloads zip file
compressed_file = zipfile.ZipFile('labour-market-statistics-september-2020-quarter-csv.zip') #unzips file
filename = [s for s in compressed_file.namelist() if 'tables/hlfs' in s] # searches for hlfs file
csv_file = compressed_file.open(filename[0]) # returns string of hlfs file

# Open hlfs csv file as dataframe
hlfs_df = pd.read_csv(csv_file, encoding='latin', dtype='object')
hlfs_df['Period'] = pd.to_datetime(hlfs_df['Period'], format='%Y.%m')
hlfs_df['Quarter'] = hlfs_df['Period'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q
hlfs_df['Data_value'] = pd.to_numeric(hlfs_df['Data_value'], errors='coerce')


# %% 1. QUARTERLY SNAPSHOT

# -----CONFIDENCE INDICIES-----
# Create dataframe from RIMU monthly file
confidence_df = (pd.read_excel(rimu_file,
                               # engine='openpyxl', # needed for newer versions of pandas to support xlsx files
                               sheet_name='Confidence',
                               skiprows=6,
                               usecols='A:C'))

confidence_df.dropna(thresh=2, inplace=True)
confidence_df.columns = ['Period',
                         'Consumer Confidence Index (LHS)',
                         'Business confidence (RHS, %)']
confidence_df = confidence_df.loc[confidence_df['Period']>='2000-01-01']
confidence_df['Period'] = confidence_df['Period'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q


# Upload to G Sheets
workbook_name = '2-Auckland-index-quarterly-snapshot-confidence-indices-2019-q3'


upload_gsheets(client_secret,
               workbook_name,
               [confidence_df])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'B',
               'NUMBER',
               '0.0')

format_gsheets(client_secret,
               workbook_name,
               'C',
               'C',
               'PERCENT',
               '0.0%')

# -----RETAIL SALES-----
# Create dataframe from RIMU monthly file
retail_df = pd.read_excel(rimu_file,
                          # engine='openpyxl', # needed for newer versions of pandas to support xlsx files
                          sheet_name='Retail',
                          skiprows=3,
                          usecols='A:C').dropna()
retail_df.rename(columns={'Unnamed: 0':'Period'},
                 inplace=True)
retail_df = retail_df.loc[retail_df['Period']>='2008-01-01']
retail_df['Period'] = retail_df['Period'].dt.to_period('Q').dt.strftime('%Y Q%q')

#Upload to G Sheets
workbook_name = '3-Auckland-index-quarterly-snapshot-retail-sales-growth-rate-2019-q3'


upload_gsheets(client_secret,
               workbook_name,
               [retail_df])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'C',
               'PERCENT',
               '0.0%')

# -----EMPLOYMENT RATES-----
lfs_df = hlfs_df.loc[(hlfs_df['Period']>='2007-01-01') &
                     (hlfs_df['Regional Council']=='Auckland')].copy()

# Create Employment Levels dataframe
emp_levels_df = lfs_df.loc[(hlfs_df['Sex']=='Total Both Sexes') &
                           ((lfs_df['Series_reference']=='HLFQ.S2A3QB') |
                           (lfs_df['Series_reference']=='HLFQ.S2B3QB'))]

emp_levels_df = pd.pivot_table(emp_levels_df,
                          index='Quarter',
                          values='Data_value',
                          columns='Persons Employed, Unemployed, Not in Labour Force (for current quarter)')

emp_levels_df = (emp_levels_df[['Persons Employed in Labour Force',
                        'Persons Unemployed in Labour Force']]*1000).reset_index()

# Create Employment Rates dataframes
emp_rates_df = lfs_df.loc[(lfs_df['Series_reference']=='HLFQ.S2F3QB') &
                          (hlfs_df['Sex']=='Total Both Sexes')]
emp_rates_df = emp_rates_df[['Quarter', 'Data_value']].reset_index(drop=True)
emp_rates_df['Data_value'] = emp_rates_df['Data_value']/100 # Convert to percentage
emp_rates_df2 = emp_levels_df['Persons Employed in Labour Force'].pct_change(periods=4)
emp_rates_df = emp_rates_df.join(emp_rates_df2)


# Create NEET rate dataframe
neet_df = lfs_df.loc[(lfs_df['Series_reference']=='HLFQ.S8CJQB9') |
                     (lfs_df['Series_reference']=='HLFQ.S8CJQB1') |
                     (lfs_df['Series_reference']=='HLFQ.S8CJQB2')]

neet_df = ((pd.pivot_table(neet_df,
                         index='Quarter',
                         values='Data_value',
                         columns='Youth age groups'))/100).reset_index()

neet_df.rename(columns={'Aged 15-19 years':'15-19 years only (%)',
                        'Aged 20-24 years':'20-24 years only (%)',
                        'Total aged 15-24 years':'Total Auckland NEET rate (%)'},
               inplace=True)


# Remove 2017 data (needed for growth rates)
emp_levels_df = emp_levels_df.iloc[4:] 
emp_rates_df = emp_rates_df.iloc[4:]
neet_df = neet_df.iloc[4:]

# Rename and reorder columns
emp_levels_df.rename(columns={'Persons Employed in Labour Force':'Persons employed in labour force',
                              'Persons Unemployed in Labour Force':'Persons unemployed in labour force'},
                     inplace=True)
emp_rates_df.rename(columns={'Data_value':'Unemployment rate (RHS, %)',
                             'Persons Employed in Labour Force':'Annual employment growth rate (LHS, %)'},
                    inplace=True)
emp_rates_df = emp_rates_df[['Quarter',
                              'Annual employment growth rate (LHS, %)',
                              'Unemployment rate (RHS, %)']]

# Upload to G Sheets
workbook_name = '5-Auckland-index-quarterly-snapshot-employment-rates'

lfs_dataframes = [emp_levels_df,
                  emp_rates_df,
                  neet_df]

upload_gsheets(client_secret,
                workbook_name,
                lfs_dataframes,
                sheets=[0,1,2])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'C',
               'NUMBER',
               '#,##0',
               sheets=[0])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'D',
               'PERCENT',
               '0.0%',
               sheets=[1,2])


# -----EARNINGS-----
# Create earnings dataframe from RIMUM Economic Indicators Database
excel_file = r'U:\CityWide\Permanent\Research Information\Economic indicators\Economic Indicators Database - Economic.xlsm'
wages_df = pd.read_excel(excel_file,
                         # engine='openpyxl', # needed for newer versions of pandas to support xlsx files
                         sheet_name='Pop, Employ, Incomes',
                         skiprows=10,
                         usecols='A,CV:CW').dropna()
wages_df.rename(columns={'Unnamed: 0':'Quarter',
                         'Auckland.9':'Auckland',
                         'RoNZ.8':'Rest of New Zealand'},
                inplace=True)
wages_df = wages_df.loc[wages_df['Quarter']>='2008-01-01']
wages_df['Quarter'] = wages_df['Quarter'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q

# Create growth dataframe from RIMU monthly file
earnings_df = (pd.read_excel(rimu_file,
                             # engine='openpyxl', # needed for newer versions of pandas to support xlsx files
                               sheet_name='Wages',
                               skiprows=3,
                               usecols='A:C'))
earnings_df.dropna(inplace=True)
earnings_df.rename(columns={'Unnamed: 0':'Date',
                            'Auckland':'Auckland growth (%)',
                            'Rest of New Zealand': 'Rest of NZ growth (%)'},
                   inplace=True)
earnings_df = earnings_df.loc[earnings_df['Date']>='2008-01-01']
earnings_df['Quarter'] = earnings_df['Date'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q
earnings_df = earnings_df[['Quarter', 'Auckland growth (%)', 'Rest of NZ growth (%)']]

# Upload to G Sheets
workbook_name = '6-Auckland-index-quarterly-snapshot-earnings'
earnings_dataframes = [wages_df,
                       earnings_df]

upload_gsheets(client_secret,
               workbook_name,
               earnings_dataframes,
               sheets=[0,1])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'C',
               'CURRENCY',
               '$#,##0.00',
               sheets=[0])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'C',
               'PERCENT',
               '0.0%',
               sheets=[1])

# -----CONSTRUCTION-----
# Download filled jobs csv file
URL = 'https://www.stats.govt.nz/large-datasets/csv-files-for-download/'
options = Options()
options.headless = True
driver = webdriver.Chrome(executable_path=r'C:\windows\chromedriver',
                          options=options)
driver.get(URL) #opens URL on chrome to activate javascript
stats_soup = bs(driver.page_source, 'html.parser') #uses bs to get data from browser
driver.quit() #quits browser

# Find link for trade CSV file (Note: using soup object from employment rates code)         
link = stats_soup.find('a', href=re.compile(
    'building-consents-issued-')).get('href')
csv_download = ('https://www.stats.govt.nz' + link)

urllib.request.urlretrieve(csv_download, 'building-consents-issued.zip')
compressed_file = zipfile.ZipFile('building-consents-issued.zip')

filename = [s for s in compressed_file.namelist() if 'Building consents by region (Quarterly).csv' in s] # searches for consents file
csv_file = compressed_file.open(filename[0]) # returns string of hlfs file

# Create consents dataframe
consents_df = pd.read_csv(csv_file, dtype='object')
consents_df['Period'] = pd.to_datetime(consents_df['Period'], format='%Y.%m')
consents_df['Quarter'] = consents_df['Period'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q
consents_df['Data_value'] = pd.to_numeric(consents_df['Data_value'], errors='coerce')


consents_df = consents_df.loc[(consents_df['Period']>='2006-01-01') &
                              ((consents_df['Series_reference']=='BLDQ.SF021000A1A') |
                               (consents_df['Series_reference']=='BLDQ.SF022000A1A'))]

consents_df = pd.pivot_table(consents_df,
                             index='Quarter',
                             columns='Series_title_2',
                             values='Data_value')
consents_df.reset_index(inplace=True)
consents_df['Residential consents issued'] = consents_df['Residential buildings'].rolling(4).sum()
consents_df['Non-residential consents issued'] = consents_df['Non-residential buildings'].rolling(4).sum()
consents_df['Residential consents growth'] = consents_df['Residential consents issued'].pct_change(periods=4)
consents_df['Non-residential consents growth'] = consents_df['Non-residential consents issued'].pct_change(periods=4)

# Split consents dataframe to number and growth
consents_num_df = consents_df.loc[8:,
                                  ['Quarter', 
                                   'Residential consents issued', 
                                   'Non-residential consents issued']]

consents_growth_df = consents_df.loc[8:,
                                     ['Quarter',
                                      'Residential consents growth',
                                      'Non-residential consents growth']]

# Upload to G Sheets
workbook_name = '8-Auckland-index-quarterly-snapshot-construction'
consents_dataframes = [consents_num_df,
                       consents_growth_df]

upload_gsheets(client_secret,
                workbook_name,
                consents_dataframes,
                sheets=[0,1])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'C',
               'NUMBER',
               '#,##0',
               sheets=[0])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'C',
               'PERCENT',
               '0.0%',
               sheets=[1])


# -----TRANSPORT-----
csv_file = 'https://at.govt.nz/media/1983939/public-transport-patronage-data-september-2020-csv.csv'

# Create patronage dataframe
patronage_df = pd.read_csv(csv_file,
                           encoding = "ISO-8859-1",
                           skiprows=4,
                           thousands=',')
patronage_df.dropna(thresh=4,
                    inplace=True)
patronage_df['Month-Year'] = pd.to_datetime(patronage_df['Month-Year'], format='%b-%y')
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
pat_num_df = patronage_df[['Quarter',
                           'Bus',
                           'Train',
                           'Ferry']].copy()
pat_growth_df = patronage_df[['Quarter',
                              'Bus Growth',
                              'Train Growth',
                              'Ferry Growth']].copy()
pat_growth_df.rename(columns={'Bus Growth':'Bus',
                              'Train Growth':'Train',
                              'Ferry Growth':'Ferry'},
                     inplace=True)

# Upload to G Sheets
workbook_name = '9-Auckland-index-quarterly-snapshot-transport'
patronage_dataframes = [pat_num_df,
                       pat_growth_df]

upload_gsheets(client_secret,
                workbook_name,
                patronage_dataframes,
                sheets=[0,1])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'D',
               'NUMBER',
               '#,##0',
               sheets=[0])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'D',
               'PERCENT',
               '0.0%',
               sheets=[1])

# %% 2. PEOPLE

# -----LABOUR FORCE-----
# Create labour force dataframe from hlfs dataframe
labourforce_df = hlfs_df.loc[(hlfs_df['Series_reference']=='HLFQ.S2C3QB') |
                             (hlfs_df['Series_reference']=='HLFQ.S2D3QB') |
                             (hlfs_df['Series_reference']=='HLFQ.S2E3QB') |
                             (hlfs_df['Series_reference']=='HLFQ.S2Z3QB')]
labourforce_df = pd.pivot_table(labourforce_df,
                                index='Quarter',
                                columns='Persons Employed, Unemployed, Not in Labour Force (for current quarter)',
                                values='Data_value')
labourforce_df['Labour force growth rate (LHS, %)'] = labourforce_df['Total Labour Force'].pct_change(periods=4)
labourforce_df['Labour force growth level (RHS, #)'] = (labourforce_df['Total Labour Force'].diff(periods=4))*1000

lf_growth_df = labourforce_df[['Labour force growth rate (LHS, %)',
                            'Labour force growth level (RHS, #)']]
participation_df = (labourforce_df['Labour Force Participation Rate']/100)
labourforce_df = (labourforce_df[['Total Labour Force',
                                 'Not in Labour Force',
                                 'Working Age Population']]*1000)

# Slice from 2001 onwards
labourforce_df = (labourforce_df.loc['2000 Q1':]).reset_index()
participation_df = (participation_df.loc['2000 Q1':]).reset_index()
lf_growth_df = (lf_growth_df.loc['2000 Q1':]).reset_index()

# Rename columns inline with Auckland Index
labourforce_df.rename(columns={'Total Labour Force':'Active labour force (#)',
                               'Not in Labour Force':'Not in labour force (#)',
                               'Working Age Population':'Working age population (#)'},
                      inplace=True)
participation_df.rename(columns={'Labour Force Participation Rate':'Labour force participation rate (%)'},
                        inplace=True)

# Upload to G Sheets
workbook_name = '2-Auckland-index-people-labour-force'
labourforce_dataframes = [labourforce_df,
                          participation_df,
                          lf_growth_df]

upload_gsheets(client_secret,
                workbook_name,
                labourforce_dataframes,
                sheets=[0,1,2])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'D',
               'NUMBER',
               '#,##0',
               sheets=[0])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'B',
               'PERCENT',
               '0.0%',
               sheets=[1,2])

format_gsheets(client_secret,
               workbook_name,
               'C',
               'C',
               'NUMBER',
               '#,##0',
               sheets=[2])

#-----EMPLOYMENT AND UNEMPLOYMENT-----
# Create unemployment datafram from RIMU monthly datasheet
unemp_rate_df = (pd.read_excel(rimu_file,
                               # engine='openpyxl', # needed for newer versions of pandas to support xlsx files
                               sheet_name='Unemployment',
                               skiprows=3,
                               usecols='A:C')).dropna()
unemp_rate_df.rename(columns={'Unnamed: 0':'Quarter',
                              'Rest of NZ':'Rest of New Zealand'},
                    inplace=True)
unemp_rate_df['Quarter'] = unemp_rate_df['Quarter'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q

# Create Auckland/NZ Neet rate datasheet
neet_df = hlfs_df.loc[((hlfs_df['Series_reference']=='HLFQ.S8CJQB9') |
                       (hlfs_df['Series_reference']=='HLFQ.S8CJQZ9')) &
                      (hlfs_df['Period']>='2008-01-01')]

neet_df = ((pd.pivot_table(neet_df,
                         index='Quarter',
                         values='Data_value',
                         columns='Regional Council'))/100).reset_index()
neet_df.rename(columns={'Total All Regional Councils':'Total New Zealand NEET rate',
                        'Auckland':'Total Auckland NEET rate'},
               inplace=True)

# Create Employment Status dataframe
emp_status_df = hlfs_df.loc[((hlfs_df['Series_reference']=='HLFQ.S2A3QB') |
                             (hlfs_df['Series_reference']=='HLFQ.S2A3QZ') |
                             (hlfs_df['Series_reference']=='HLFQ.S2B3QB') |
                             (hlfs_df['Series_reference']=='HLFQ.S2B3QZ')) &
                            (hlfs_df['Period']>='1999-01-01')]
emp_status_df = pd.pivot_table(emp_status_df,
                               index='Quarter',
                               values='Data_value',
                               columns=['Regional Council',
                                        'Persons Employed, Unemployed, Not in Labour Force (for current quarter)'])*1000
emp_status_df.columns = [f'{i}{j}' for i,j in emp_status_df.columns] # Combine 'Region Council' and 'Persons Employed, Unemployed..' dataframe headers 
emp_status_df.rename(columns={'AucklandPersons Employed in Labour Force':'Auckland employed',
                              'AucklandPersons Unemployed in Labour Force':'Auckland unemployed',
                              'Total All Regional CouncilsPersons Employed in Labour Force':'NZ employed',
                              'Total All Regional CouncilsPersons Unemployed in Labour Force':'NZ unemployed'},
                     inplace=True)

emp_status_df['Rest of NZ employed'] = emp_status_df['NZ employed'] - emp_status_df['Auckland employed']
emp_status_df['Rest of NZ unemployed'] = emp_status_df['NZ unemployed'] - emp_status_df['Auckland unemployed']

emp_growth_df = (emp_status_df[['Auckland employed',
                               'Rest of NZ employed']].pct_change(periods=4)).reset_index() # Create growth dataframe

emp_status_df = (emp_status_df[['Auckland employed',
                                'Auckland unemployed',
                                'Rest of NZ employed',
                                'Rest of NZ unemployed']]).reset_index()

# Remove 1999 data and rename columns appropriately
emp_status_df = emp_status_df.iloc[4:]
emp_growth_df = emp_growth_df.iloc[4:]

emp_growth_df.rename(columns={'Auckland employed':'Auckland',
                              'Rest of NZ employed':'Rest of New Zealand'},
                     inplace=True)

# Upload to G Sheets
workbook_name = '3-Auckland-index-employment-and-unemployment-auckland-vs-nz'
employment_dataframes = [unemp_rate_df,
                         neet_df,
                         emp_growth_df,
                         emp_status_df]

upload_gsheets(client_secret,
                workbook_name,
                employment_dataframes,
                sheets=[0,1,2,3])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'C',
               'PERCENT',
               '0.0%',
               sheets=[0,1,2])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'E',
               'NUMBER',
               '#,##0',
               sheets=[3])

# %% 5. MARKETS

# -----INFLATION-----
# Create inflation dataframe from rbnz excel file
excel_file = 'https://www.rbnz.govt.nz/-/media/ReserveBank/Files/Statistics/tables/m1/hm1.xlsx'
headers = ['Date',
           'New Zealand']
inflation_df = pd.read_excel(excel_file,
                             # engine='openpyxl', # needed for newer versions of pandas to support xlsx files
                             skiprows=5,
                             usecols='A,D',
                             header=None,
                             names=headers)
inflation_df['Quarter'] = inflation_df['Date'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q
inflation_df = inflation_df[['Quarter', 'New Zealand']].loc[inflation_df['Date']>='2008-01-01']


# Upload to G Sheets
workbook_name = '1-Auckland-index-markets-inflation-CPI'
upload_gsheets(client_secret,
                workbook_name,
                [inflation_df],
                sheets=[0])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'B',
               'NUMBER',
               '#.0',
               sheets=[0])

# -----INTEREST RATES-----
# Create interest rates dataframe from rbnz excel file
excel_file = 'https://www.rbnz.govt.nz/-/media/ReserveBank/Files/Statistics/tables/b2/hb2-monthly.xlsx'
headers = ['Month',
           'Official Cash Rate (OCR)']
interest_df = pd.read_excel(excel_file,
                            # engine='openpyxl', # needed for newer versions of pandas to support xlsx files
                            skiprows=5,
                            header=None,
                            usecols='A:B',
                            names=headers)
interest_df['Quarter'] = interest_df['Month'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q
interest_df = interest_df[['Quarter',
                           'Official Cash Rate (OCR)']].loc[interest_df['Month']>='2008-03-01']
interest_df = interest_df.iloc[::3] #Data comes as monthly, need to extract end of quarter month

# Upload to G Sheets
workbook_name = '2-Auckland-index-markets-monthly-cash-interest-rate'
upload_gsheets(client_secret,
                workbook_name,
                [interest_df],
                sheets=[0])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'B',
               'NUMBER',
               '0.00',
               sheets=[0])

# -----EXCHANGE RATES-----
# Create exchange rates dataframe from rbnz excel file
excel_file = 'https://www.rbnz.govt.nz/-/media/ReserveBank/Files/Statistics/tables/b1/hb1-monthly.xlsx'
headers = ['Month',
           'NZD/USD',
           'NZD/GBP',
           'NZD/AUD',
           'NZD/JPY',
           'NZD/EUR',
           'NZD/CNY']
forex_df = pd.read_excel(excel_file,
                         # engine='openpyxl', # needed for newer versions of pandas to support xlsx files
                         skiprows=5,
                         header=None,
                         usecols='A,D:H,K',
                         names=headers)
forex_df = forex_df.loc[forex_df['Month']>='2000-01-01']

usd_df = forex_df[['Month',
                    'NZD/USD']]
gbp_df = forex_df[['Month',
                    'NZD/GBP']]
aud_df = forex_df[['Month',
                    'NZD/AUD']]
jpy_df = forex_df[['Month',
                    'NZD/JPY']]
eur_df = forex_df[['Month',
                    'NZD/EUR']]
cny_df = forex_df[['Month',
                    'NZD/CNY']]

# Upload to G Sheets
workbook_name = '3-Auckland-index-markets-RBNZ-exchange-rates'
forex_dataframes = [aud_df, eur_df, gbp_df, usd_df, cny_df, jpy_df]

upload_gsheets(client_secret,
                workbook_name,
                forex_dataframes,
                sheets=[0,1,2,3,4,5])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'B',
               'NUMBER',
               '0.0000',
               sheets=[0,1,2,3,4])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'B',
               'NUMBER',
               '0.00',
               sheets=[5])

format_gsheets(client_secret,
                workbook_name,
                'A',
                'A',
                'DATE',
                'Mmm yyyy',
                sheets=[0,1,2,3,4,5])

# -----GOVERNMENT BONDS-----
# Create bond yield dataframe from rbnz excel sheet
excel_file = 'https://www.rbnz.govt.nz/-/media/ReserveBank/Files/Statistics/tables/b2/hb2-monthly.xlsx'
headers = ['Month',
           '10-year government bond yield (monthly)']
interest_df = pd.read_excel(excel_file,
                            # engine='openpyxl', # needed for newer versions of pandas to support xlsx files
                            skiprows=5,
                            header=None,
                            usecols='A,J',
                            names=headers)
interest_df['Quarter'] = interest_df['Month'].dt.to_period('Q').dt.strftime('%Y Q%q') #format for YYYY Q
interest_df['10-year government bond yield'] = interest_df['10-year government bond yield (monthly)'].rolling(3).mean()
interest_df = interest_df[['Quarter',
                           '10-year government bond yield']].loc[interest_df['Month']>='2008-03-01']
interest_df = interest_df.iloc[::3] #Data comes as monthly, need to extract end of quarter month

# Upload to G Sheets
workbook_name = '4-Auckland-index-markets-10y-govt-bond-yield'
upload_gsheets(client_secret,
                workbook_name,
                [interest_df],
                sheets=[0])

format_gsheets(client_secret,
               workbook_name,
               'B',
               'B',
               'NUMBER',
               '0.00',
               sheets=[0])

# -----NZ STOCK EXCHANGE----- 
#Create NZX50 dataframe from S&P excel sheet (10 year period only)
excel_file = 'https://www.spglobal.com/spdji/en/idsexport/file.xls?hostIdentifier=48190c8c-42c4-46af-8d1a-0cd5db894797&redesignExport=true&languageId=1&selectedModule=PerformanceGraphView&selectedSubModule=Graph&yearFlag=tenYearFlag&indexId=92029429'
nzx50_df = pd.read_excel(excel_file,
                         skiprows=6).dropna()

nzx50_df.columns = ['Date',
                    'Monthly average spot price (NZ$)']
nzx50_df['Date'] = pd.to_datetime(nzx50_df['Date'], format='%Y/%m/%d %H:%M:%S')
nzx50_df = nzx50_df.set_index('Date')
nzx50_df = (nzx50_df.resample('M').mean()).reset_index()
nzx50_df = nzx50_df[1:-1] # Removes first and last months, which are incomplete

#Upload to Google Sheets
workbook_name = '5-Auckland-index-markets-NZX50'
upload_gsheets(client_secret, 
               workbook_name, 
               [nzx50_df])

#Format Google Sheet cells
format_gsheets(client_secret, 
               workbook_name, 
               'A', 
               'A', 
               'DATE', 
               'yyyy mmm')

format_gsheets(client_secret, 
               workbook_name, 
               'B', 
               'B', 
               'CURRENCY', 
               '$#,##0.00')
