from proton import *


def job_generator():
    for i in range(10):
        yield Job(i, j=3*i)


def fun(i, j):
    return i ** 2 + j


with MapAsync(
    function_or_instance=fun,
    job_generator=job_generator(),
    ignore_exceptions=[ValueError],
    nworkers=8,
    taskset=None, #"0-4",
    verbose=True,
    lowpriority=True) as ma:

    print(ma)
    for worker_output in ma:
        print(worker_output)
