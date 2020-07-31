import time
from proton import *
from proton.workers import Worker
from multiprocessing import Lock

'''
use locks
'''


# ============= DEFINE THE JOB GENERATOR
def job_generator():
    for jobid in range(10):
        yield Job(jobid)  # do not pass a worker here


# ============= DEFINE THE TARGET FUNCTION TO CALL
def fun(worker, jobid):
    # add a worker argument at first position to require the worker to be passed to the target function
    worker: Worker
    t = worker.rand() * 3.0

    worker.communicate(f'worker {worker.name} (job {jobid}) is waiting for the lock')
    # =========================== protected section :
    worker.acquire()
    worker.communicate(f'worker {worker.name} (job {jobid}) acquired the lock for {t:.2f}s')

    # do operations here that should not be performed in parallel, like database accesses, ...
    # all other threads that need the lock will be paused util this thread has finished

    start = time.time()
    while time.time() - start < t: 0. + 0.

    worker.release()
    worker.communicate(f'worker {worker.name} (job {jobid}) released the lock')

    return t


# ============= START THE PROCESS
with MapAsync(
        function_or_instance=fun,
        job_generator=job_generator(),
        nworkers=4,
        verbose=True,
        lock=Lock()) as ma:

    for worker_output in ma:
        print(worker_output)
