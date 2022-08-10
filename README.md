# Auckland Index Update
## Overview
This program was designed to automate the data processing and uploading for the [Auckland Index](https://www.aucklandnz.com/auckland-index) - a publicly available, digital repository of Auckland-centric economic data.

## Development
The program was written in Python and the data is stored on Google Sheets, where Infogram (data visualisation tool) connects to it for presentation on the Auckland Index. The following packages have been used:
- Requests for connecting to the Stats New Zealand API
- Selenium and beautifulsoup to scrape data sheets from data sources without a formal API
- Pandas for transforming the data into a form that is acceptable for Google Sheets
- Gspread and gspread_pandas to upload and store data on Google Sheets

## Usage
The program has been split into two python files due to different running frequencies: [auckland_index_update_covid.py](auckland_index_update_covid.py) and [auckland_index_update_main.py](auckland_index_update_main.py). The covid file is run daily as it is largely uploading daily data, whereas the main file is mostly quarterly/monthly and is run on a weekly basis. This helps to limit the number of times the api's are called while still keeping the data as up to date as possible. The modules folder contains the functions that process the uploading to Google Sheets.

Batch files were used for the automated running of the script ([batch_files](batch_files) folder), which was processed through Windows Task Scheduler.
