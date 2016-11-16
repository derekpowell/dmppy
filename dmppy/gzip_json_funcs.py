#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 11:10:23 2016

@author: derekpowell
"""

# script to read in and combine json.gz files

def filenames(fpath):
    import glob
    
    flist = glob.glob(fpath+'*.*')
    
#    fnames = [f[len(fpath):] for f in flist]
    return flist

def gzip_json_load(filename): 
    
    import json
    import gzip
    
    with gzip.open(filename, "rb") as f:
        data = json.loads(f.read())
    f.close()
    return data

def gzip_json_dump(obj, filename):
    
    import json
    import gzip
    
    jsonObj = json.dumps(obj, indent=4, sort_keys=True)#, ensure_ascii=False).encode('utf8')    
    with gzip.open(filename, 'wb') as f:
            f.write(jsonObj)
    f.close()
    return None