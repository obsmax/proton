import time
from proton import *

'''
handle fatal / non-fatal exceptions
'''


class NonFataleError(ValueError):
    pass


class FataleError(ValueError):
    pass


# ============= DEFINE THE JOB GENERATOR
def job_generator():
    for i in range(10):
        yield Job(i, j=3 * i)


# ============= DEFINE THE TARGET FUNCTION TO CALL
def fun(i, j):
    start = time.time()
    while time.time() - start < 3.:
        0. + 0.

    if i in [2, 3, 4]:
        # this thread will crash but not the whole process
        raise NonFataleError(f'i must not be {i}')

    if i == 5:
        # this thread will crash and make all other threads crash
        raise FataleError(f'i must not be {i}')

    return i ** 2 + j


# ============= START THE PROCESS
with MapAsync(
        function_or_instance=fun,
        job_generator=job_generator(),
        # list of exception types to ignore (all other exceptions will make the whole process crash)
        ignore_exceptions=[NonFataleError],
        nworkers=4,
        affinity="0-3",
        verbose=True,
        lowpriority=True) as ma:

    for worker_output in ma:
        print(worker_output)
