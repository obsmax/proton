import time
from proton import *

'''
play with the number of virtual threads, affinity or priority
'''


# ============= DEFINE THE JOB GENERATOR
def job_generator():
    for i in range(10):
        yield Job(i, j=3 * i)


# ============= DEFINE THE TARGET FUNCTION TO CALL
def fun(i, j):
    start = time.time()
    while time.time() - start < 3.:
        0. + 0.
    return i ** 2 + j


# ============= START THE PROCESS
with MapAsync(
        # target function
        function_or_instance=fun,
        # The job generator (provide input args if needed, else use "()")
        job_generator=job_generator(),
        # Number of virtual threads
        nworkers=4,
        # Affinity (passed to "taskset -pc" linux systems only)
        affinity="0-3",
        # prompt messages
        verbose=True,
        # run threads with low priority (linux systems only)
        lowpriority=True) as ma:

    for worker_output in ma:
        print(worker_output)
