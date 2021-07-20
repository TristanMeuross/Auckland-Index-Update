# -*- coding: utf-8 -*-
"""
Created on Fri May  7 15:52:18 2021

@author: meurost
"""

from gspread_pandas import Spread
import gspread
import os

def upload_gsheets(workbook_name, dataframes, sheets=[0], range_start=(1,1)):
    """
    Uploads chosen dataframes to selected workbook_name via gspread_pandas. Note 
    both dataframes and sheets variables need to be a list.

    Parameters
    ----------
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
    sh = Spread(workbook_name)
    for i, x in zip(sheets, dataframes):
        sh.df_to_sheet(x, index=False, sheet=i, start=range_start)

def download_gsheets(workbook_name, sheet=0, index=None):
    """
    Uploads chosen dataframes to selected workbook_name via gspread_pandas. Note 
    both dataframes and sheets variables need to be a list.

    Parameters
    ----------
    workbook_name : TYPE
        Name of Google Sheets file to upload to.
    sheet : TYPE, optional
        The worksheets to download (0 is the first). 
    index : TYPE, optional
        Column number of index, 0 or None for no index.

    Returns
    -------
    None.

    """
    sh = Spread(workbook_name)
    sh.open_sheet(sheet)
    df = sh.sheet_to_df(index=index)
    return df

def format_gsheets(workbook_name, format_range, type_of_format, format_pattern, sheets=[0]):
    """
    Formats chosen cells as described format.

    Parameters
    ----------
    workbook_name : TYPE
        Name or ID of Google Sheets file to format.
    format_range : TYPE
        The range of cells to be formatted. Must be in string format and letter/number (i.e 'A' or 'A1', etc.)
    type_of_format : TYPE
        The type of format to be set in the range. Types include PERCENT, DATE, etc. Must be string.
    format_pattern : TYPE
        Pattern of format to be applied. Types include dd-mmm, 0%, etc. Must be string.
    sheets : TYPE, optional
        The worksheets to upload to (0 is the first). Must be a list. The default is [0].

    Returns
    -------
    None.

    """
    gc = gspread.service_account()
    try:
        sh = gc.open(workbook_name)
        for i in sheets:
            worksheet = sh.get_worksheet(i)
            worksheet.format(format_range, {
                'numberFormat': {
                    'type': type_of_format,
                    'pattern': format_pattern
                }
            })
    # If using workbook ID instead of name, function will try to open_by_key
    except gspread.client.SpreadsheetNotFound:
        sh = gc.open_by_key(workbook_name)
        for i in sheets:
            worksheet = sh.get_worksheet(i)
            worksheet.format(format_range, {
                'numberFormat': {
                    'type': type_of_format,
                    'pattern': format_pattern
                }
            })

def clear_gsheets(workbook_name, sheets=[0]):
    gc = gspread.service_account()
    sh = gc.open(workbook_name)
    for i in sheets:
        worksheet = sh.get_worksheet(i)
        worksheet.clear()
    

def delete_file(folder_path, filename):
    """    
    Parameters
    ----------
    folder_path : TYPE
        The folder path where the file to be deleted is located.
    filename : TYPE
        The name of the file, including the extension.

    Returns
    -------
    None.

    """
    filepath = os.path.join(folder_path, filename)
    if os.path.exists(filepath):
        os.remove(filepath)
