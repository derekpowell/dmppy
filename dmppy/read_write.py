#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 11:10:23 2016

@author: derekpowell
"""

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
        