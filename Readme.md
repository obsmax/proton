# PROTON

[![Build Status](https://travis-ci.com/obsmax/proton.svg?branch=master)](https://travis-ci.com/obsmax/proton)
[![Build Status](https://travis-ci.com/obsmax/proton.svg?branch=dev)](https://travis-ci.com/obsmax/proton) 

Parallelization helper for python based on python-multiprocessing

* Threads input/output exchanged using python generators  
* provide tools to handle the exceptions raised by the threads
* Allow controling the affinity/priority of the threads (linux systems)
* Provides on time execution statistics
* Provides thread safe tools for random numbers applications
* ...

## Install
```
# cd installation/path
git clone https://github.com/obsmax/proton
conda create -n py3 python=3.7  
conda activate py3
conda install --yes --file requirements.txt
pip install -e .
```

## Basic usage
```python
import time
from proton import *


# ============= DEFINE THE JOB GENERATOR
def job_generator():
    for i in range(10):
        # returns job arguments on demand (as soon as a thread is availble)
        yield Job(i, j=3 * i)  # yield only Job objects as if it where arguments of function fun


# ============= DEFINE THE TARGET FUNCTION TO CALL
def fun(i, j):
    """
    works for 3 sec and returns i**2 + j
    """
    start = time.time()
    while time.time() - start < 3.:
        0 + 0
    return i ** 2 + j


# ============= START THE PROCESS
with MapAsync(
        # target function
        function_or_instance=fun,
        # The job generator (provide input args if needed, else use "()")
        job_generator=job_generator(),
        # List of non-fatal exceptions
        ignore_exceptions=[ValueError],
        # Number of virtual threads
        nworkers=8,
        # Affinity (passed to "taskset -pc" linux systems only)
        affinity="0-3",
        # prompt messages
        verbose=True,
        # run threads with low priority (linux systems only)
        lowpriority=True) as ma:
    # ma is a generator, which will return process outputs on your demand
    print(ma)

    for worker_output in ma:
        print(worker_output)

        # ==== the worker_output is the result of one execution
        # the id of the job (ordered as provided by the job_generator)
        print(worker_output.jobid)

        # the time needed to generate the input job
        print(worker_output.generator_time[1] - worker_output.generator_time[0])

        # the time needed to process the job
        print(worker_output.processor_time)

        # the output of fun for the current job
        print(worker_output.answer)
```