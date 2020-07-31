import time
from proton import *
from proton.workers import Worker

'''
pass the worker to the target function
'''


# ============= DEFINE THE JOB GENERATOR
def job_generator():
    for jobid in range(10):
        yield Job(jobid)  # do not pass a worker here


# ============= DEFINE THE TARGET FUNCTION TO CALL
def fun(worker, jobid):
    # add a worker argument at first position to require the worker to be passed to the target function
    worker: Worker

    # ==== worker has some useful properties attached to it like :
    # getting thread safe random numbers
    t = worker.rand() * 3.0

    # communicating through the message queue (visible only if verbose=True)
    worker.communicate(
        f'Job {jobid} is beeing processed by worker "{worker.name}" of type {type(worker)}')
    worker.communicate(
        f'Job {jobid} will now work for {t:.2f}s')

    start = time.time()
    while time.time() - start < t: 0. + 0.

    # and more... see later examples

    return t


# ============= START THE PROCESS
with MapAsync(
        function_or_instance=fun,
        job_generator=job_generator(),
        nworkers=4,
        verbose=True) as ma:

    for worker_output in ma:
        print(worker_output)
