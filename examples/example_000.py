import time
from proton import *

'''
The simplest possible usage: run 10 jobs in parallel 
'''


# ============= DEFINE THE JOB GENERATOR
def job_generator():
    for i in range(10):
        # provides jobs on demand (as soon as a thread is available)
        # please only yield Job initiated with the arguments you want to pass to function fun
        yield Job(i, j=3 * i)


# ============= DEFINE THE TARGET FUNCTION TO CALL
def fun(i, j):
    """
    works for 3 sec and returns i**2 + j
    """
    start = time.time()
    while time.time() - start < 3.:
        0. + 0.  # pointless operation to see the CPU activity raising (top, htop, ...)
    return i ** 2 + j


# ============= START THE PROCESS
with MapAsync(
        # target function
        function_or_instance=fun,
        # The job generator (provide input args if needed, else use "()")
        job_generator=job_generator()) as ma:

    # ma is a generator, which will return process outputs on your demand
    print(ma)

    for worker_output in ma:
        print(worker_output)

        # ==== the worker_output is the result of one execution
        # the id of the job (ordered as provided by the job_generator)
        # worker_output.jobid

        # the time needed to generate the input job
        # worker_output.generator_time[1] - worker_output.generator_time[0]

        # the time needed to process the job
        # worker_output.processor_time[1] - worker_output.processor_time[0]

        # the output of fun for the current job
        # worker_output.answer

        # WARNING : with MapAsync, the job order is not preserved, use jobid to reorder

    # New : Use MapSync instead of MapAsync to preserve the input job order
