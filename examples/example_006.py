import time
from proton import *
import numpy as np

'''
Parallelize a cumulative processing using StackAsync
In this example I show how to compute the mean of a list of numbers 
split over several threads
'''


class MyStackableObject(object):
    def __init__(self, value=0.):
        self.value = value
        self.count = 1

    def __iadd__(self, other):
        """define here how two objects must be stacked together"""
        assert isinstance(other, self.__class__)
        self.value = (self.count * self.value + other.count * other.value) / float(self.count + other.count)
        self.count += other.count
        return self

    def __str__(self):
        return f"mean:{self.value}, count:{self.count}"


# ============= DEFINE THE JOB GENERATOR
def job_generator():
    for v in np.arange(10):
        # the v value will be passed to the first available stacker
        # and stacked internally
        yield Job(v)


# ============= Use the target function to initiate the stackable instance
def fun(v):
    return MyStackableObject(value=v)


# ============= START THE PROCESS
with StackAsync(
        # target function
        function_or_instance=fun,
        # The job generator (provide input args if needed, else use "()")
        job_generator=job_generator(),
        nworkers=2,
        verbose=True) as sa:

    # sa is a generator exactly like MapAsync
    print(sa)

    # use the stack method to collect the partial stack of all threads and stack them
    total_stack = sa.stack()

print(total_stack)

# make sure the parallelized mean is correct :
print(np.mean(np.arange(10)))
