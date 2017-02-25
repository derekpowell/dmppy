#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 11:10:23 2016

@author: derekpowell
"""
def filenames(fpath, extension = "*.*"):
    import glob

    flist = glob.glob(fpath+extension)
    return flist
    
# functions to read in and combine json.gz files
import json
import gzip

def gzip_json_load(filename):
    '''loads gzipped json file specified by filename
    filename: string'''
    with gzip.open(filename, "rb") as f:
        data = json.loads(f.read())
    f.close()
    return data

def gzip_json_dump(obj, filename):
    '''writes json-ready object to gzipped json file specified by filename
    obj: json-ready object (e.g., dictionary or list of dictionaries)
    filename: string '''

    jsonObj = json.dumps(obj, indent=4, sort_keys=True)#, ensure_ascii=False).encode('utf8')
    with gzip.open(filename, 'wb') as f:
            f.write(jsonObj)
    f.close()
    return None


# easy functions to read and write csv
import csv

def csv_dump(obj, filename, header = None):
    ''' writes obj (list of lists) to filename
        Optionally writes header as first row

        obj: list of lists
        filename: string specifying filename ending in .csv
        header: list for header row'''

    with open(filename, "wb") as f:
        writer = csv.writer(f)
        if header != None:
            writer.writerow(header)
        writer.writerows(obj)

def csv_load(filename, mode = 'list'):
    '''loads csv file specified by filename
        filename: string specifying file
        mode = 'list': loads as list of lists
        mode = 'list_skip': loads as list of lists skipping header
        mode = 'df': load in style of dataframe, dictionary of lists w/ header as keys
        mode = 'dict': load as dictionary (first col assumed to specify items, header specifies keys)
        mode = 'dict_skipID': load as dict (but do not include ID field in each item)'''

    if mode == 'list':
        output=[]
        with open(filename, 'rb') as f:
            reader = csv.reader(f)
            for row in reader:
                output.append(row)

    if mode == 'list_header':
        output=[]
        with open(filename, 'rb') as f:
            reader = csv.reader(f)
            reader.next()
            for row in reader:
                output.append(row)

    if 'dict' in mode:
        with open(filename, 'rb') as f:
            reader = csv.reader(f)
            header = reader.next()
            output = {}
            for row in reader:
                item = {}
                if mode == 'dict':
                    ind = 0
                elif mode =='dict_skipID':
                    ind = 1
                for h in header[ind:]:
                    item[h]=row[ind]
                    ind += 1
                output[row[0]]=item

    if mode =='df':
        with open(filename, 'rb') as f:
            reader = csv.reader(f)
            header = reader.next()
            output = {}
            for h in header:
                output[h] = []
            for row in reader:
                for i in range(0,len(header)):
                    output[header[i]].append(row[i])

    return output

## This was taken from urschrei/excel_things.py

import os
import xlrd


def open_csv(to_read):
    """
    Open a text file for reading, then iterate over it with a csv reader,
    returning a list of lists, each containing 1 row.
    """
    def unicode_csv_reader(unicode_csv_data, dialect = csv.excel, **kwargs):
        """ Encode utf-8 strings from CSV as Unicode """
        csv_reader = csv.reader(unicode_csv_data,
                                dialect = dialect, **kwargs)
        for row in csv_reader:
            # decode UTF-8 back to Unicode, cell by cell:
            yield [unicode(cell.strip(), 'utf-8') for cell in row if cell]
    try:
        with open(to_read, 'rU') as got_a_file:
            return [line for line in unicode_csv_reader(got_a_file)]
    except (IOError, csv.Error):
        print "Couldn't read from file %s. Exiting." % (to_read)
        raise

def open_excel(to_read):
    """
    Open an Excel workbook and read rows from first sheet into sublists
    """
    def read_lines(workbook):
        """ decode strings from each row into unicode lists """
        sheet = workbook.sheet_by_index(0)
        for row in range(sheet.nrows):
            yield [sheet.cell(row, col).value for col in range(sheet.ncols)]
    try:
        workbook = xlrd.open_workbook(to_read)
        return [line for line in read_lines(workbook)]
    except (IOError, ValueError):
        print "Couldn't read from file %s. Exiting" % (to_read)
        raise

def save_as_xls(lines_to_write, output_filename):
    """
    Write a list of lists to an Excel (xls) sheet, one row per nested list
    """
    # initialise a new Excel workbook object, and a worksheet
    from xlwt import Workbook
    from xlwt import XFStyle
    book = Workbook(encoding = 'utf-8')
    sheet = book.add_sheet('Sheet 1')
    style = XFStyle()
    style.num_format_str = 'general'
    # iterate through the nested lists
    for row_index, row_contents in enumerate(lines_to_write):
        for column_index, cell_value in enumerate(row_contents):
            sheet.write(row_index, column_index, cell_value, style)
    # write the file to the current working directory
    book.save(os.path.join(os.getcwd(), output_filename))
