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
from itertools import partial

def func_kwstar(kwargs,func):
    """Convert `f({"x":1,"y":2})` to `f(1,2)` call."""
    return func(**kwargs)

def p_map(func, iterable, use_kwargs = False, n_jobs = -1):
    """
    Parallelized map function. More readable than parallel_process.

    func (function): function taking 1 argument to apply to iterable
    iterable (iterable): list or iterable of objects to be modified by func
    use_kwargs (bool, default = False): treat iterable items as keyword dicts
    n_jobs (int, default = -1): number of processes (-1 for # CPUs)

    """

    from multiprocessing import Pool, cpu_count

    if n_jobs == -1:
        n_jobs = cpu_count()

    pool = Pool(n_jobs)

    if __name__=="__main__":

        if use_kwargs == False:
            output = pool.map(func, iterable)

        elif use_kwargs == True:

            func_kwargs = partial(func_kwstar, func=func)
            output = pool.map(func_kwargs, iterable)

        pool.close()
        pool.join()

        return output


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
            progressbar (bool, default=False): Show tqdm progress bar
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
