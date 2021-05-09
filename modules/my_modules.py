# -*- coding: utf-8 -*-
"""
Created on Fri May  7 15:52:18 2021

@author: meurost
"""

import pygsheets
import os

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
