# -*- coding: utf-8 -*-
"""
Created on Sat Oct 22 11:06:21 2016

@author: User
"""


import os
import sys

def get_script_path():
    return os.path.dirname(os.path.realpath(sys.argv[0]))

def list_filenames(fpath):
    import glob
    
    flist = glob.glob(fpath+'*.*')
    
    #    fnames = [f[len(fpath):] for f in flist]
    return flist