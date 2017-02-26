# -*- coding: utf-8 -*-
"""
Created on Fri Oct 21 18:15:37 2016

@author: User
"""

import csv_unicode_handler
import read_write

from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count

def parallel_process(function, array, n_jobs=-1, use_kwargs=False, front_num=3, progressbar = False):
    """
        A parallel version of the map function with a tqdm progress bar.

        Args:
            function (function): A python function to apply to the elements of array
            array (array-like): An array to iterate over.
            n_jobs (int, default= -1): The number of cores to use (-1 = use all cores)
            use_kwargs (boolean, default=False): Whether to consider the elements of array as dictionaries of
                keyword arguments to function
            front_num (int, default=3): The number of iterations to run serially before kicking off the parallel job.
                Useful for catching bugs
        Returns:
            [function(array[0]), function(array[1]), ...]
    """


    if n_jobs == -1:
        n_jobs = cpu_count()

    #We run the first few iterations serially to catch bugs
    if front_num > 0:
        front = [function(**a) if use_kwargs else function(a) for a in array[:front_num]]
    #If we set n_jobs to 1, just run a list comprehension. This is useful for benchmarking and debugging.

    if progressbar == True:
        if n_jobs==1:
            return front + [function(**a) if use_kwargs else function(a) for a in tqdm(array[front_num:])]
        #Assemble the workers
        with ProcessPoolExecutor(max_workers=n_jobs) as pool:
            #Pass the elements of array into function
            if use_kwargs:
                futures = [pool.submit(function, **a) for a in array[front_num:]]
            else:
                futures = [pool.submit(function, a) for a in array[front_num:]]
            kwargs = {
                'total': len(futures),
                'unit': 'it',
                'unit_scale': True,
                'leave': True
            }
            #Print out the progress as tasks complete
            for f in tqdm(as_completed(futures), **kwargs):
                pass
        out = []
        #Get the results from the futures.
        for i, future in tqdm(enumerate(futures)):
            try:
                out.append(future.result())
            except Exception as e:
                out.append(e)
    else:
        if n_jobs==1:
            return front + [function(**a) if use_kwargs else function(a) for a in array[front_num:]]
        #Assemble the workers
        with ProcessPoolExecutor(max_workers=n_jobs) as pool:
            #Pass the elements of array into function
            if use_kwargs:
                futures = [pool.submit(function, **a) for a in array[front_num:]]
            else:
                futures = [pool.submit(function, a) for a in array[front_num:]]
            kwargs = {
                'total': len(futures),
                'unit': 'it',
                'unit_scale': True,
                'leave': True
            }

        out = []
        #Get the results from the futures.
        for i, future in enumerate(futures):
            try:
                out.append(future.result())
            except Exception as e:
                out.append(e)
    return front + out
