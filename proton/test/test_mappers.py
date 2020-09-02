from proton import Job, MapAsync, MapSync, StackAsync, StackerOutput
import time
import numpy as np


def test_mapasync():

    def job_generator():
        for i in range(10):
            yield Job(i, j=2*i)

    def fun(i, j):
        return i, j

    with MapAsync(function_or_instance=fun,
                  job_generator=job_generator()) as ma:
        for worker_output in ma:
            i, j = worker_output.answer
            assert worker_output.jobid == i
            assert worker_output.jobid == j / 2


def test_mapsync():

    runtimes = np.linspace(0.2, 0.1, 10)
    ordered_jobids = np.arange(len(runtimes))

    def job_generator():
        for jobid, runtime in zip(ordered_jobids, runtimes):
            yield Job(runtime)

    def fun(runtime):
        time.sleep(runtime)
        return runtime

    with MapAsync(function_or_instance=fun,
                  job_generator=job_generator()) as ma:

        async_jobids = np.asarray([worker_output.jobid for worker_output in ma], int)

    with MapSync(function_or_instance=fun,
                  job_generator=job_generator()) as ma:

        sync_jobids = np.asarray([worker_output.jobid for worker_output in ma], int)

    assert np.all(async_jobids == ordered_jobids[::-1])
    assert np.all(sync_jobids == ordered_jobids)


def test_stacker():
    values = np.arange(10) * 100

    def job_generator():
        for i, v in enumerate(values):
            yield Job(i, v)

    def fun(i, v):
        time.sleep(i / 100.)
        return v

    with StackAsync(function_or_instance=fun,
                    job_generator=job_generator(),
                    nworkers=2,
                    verbose=True) as sa:
        ans = sa.stack()

    assert ans.answer == values.sum()
    assert len(ans.jobids) == len(values)
