import time
from proton import *
from proton.workers import Worker
import numpy as np

'''
Use a callable class for target 
'''


# ============= DEFINE THE JOB GENERATOR
def job_generator():
    for jobid in range(10):
        yield Job(jobid)  # do not pass a worker here


# ============= DEFINE THE TARGET FUNCTION TO CALL
class MyCallableObject(object):
    def __init__(self, data):
        """
        :param data: some heavy read only data used by all jobs
                     placing big data here avoids sharing it through the pipes
                     which can slow down the processes a lot
        """
        self.data = data

    def __call__(self, worker, jobid):
        # if worker is passed place it right after self
        worker: Worker
        t = worker.rand() * 3.0

        start = time.time()
        while time.time() - start < t: 0. + 0.

        # you may use the data attached to this thread do process this job
        # but remember that data is readonly, do not try to change data like this :
        # self.data *= 0.  # this may have no effect or lead to pickling errors

        return t + jobid + sum(self.data)


# ============= START THE PROCESS
inst = MyCallableObject(
    data=np.arange(10))

with MapAsync(
        function_or_instance=inst,
        job_generator=job_generator(),
        nworkers=4,
        verbose=True) as ma:

    for worker_output in ma:
        print(worker_output)
