from proton import *
import time


def job_generator():
    for i in range(10):
        yield Job(i, j=3*i)


def fun(i, j):
    start = time.time()
    while time.time() - start < 3.:
        0 + 0
    return i ** 2 + j


with MapAsync(
    function_or_instance=fun,
    job_generator=job_generator(),
    ignore_exceptions=[ValueError],
    nworkers=8,
    affinity="0-3",
    verbose=True,
    lowpriority=True) as ma:

    print(ma)
    for worker_output in ma:
        print(worker_output)
