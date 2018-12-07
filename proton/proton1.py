from __future__ import print_function
from multiprocessing import Process, Lock, RLock, Pipe, Value, cpu_count
from signalcatcher import SignalCatcher
from timelimiter import TimeLimiter
import random
import sys, traceback
import warnings
import numpy as np
import time
import os, signal


def workfor(t):
    start = time.time()
    while time.time() - start < t:
        0. + 0.


class NoResult(object):
    pass


class Job(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class MainThreadInterrupt(Exception):
    pass


class WorkerInterrupt(Exception):
    pass


class Worker(Process):
    def __init__(self, job,
                 lock=None,
                 seed=None,
                 ppid=None,
                 verbose=False):

        Process.__init__(self)
        self.job = job
        self.parent_conn, self.child_conn = Pipe()

        self.lock = lock
        self.seed = seed
        self.verbose = verbose
        if ppid is None:
            self.ppid = os.getppid()

        self.rand1 = None
        if self.seed is not None:
            self.rand1 = random.Random(self.seed).random
        self.interruption_signal_sent = Value('i', 0)  # 0 = False, 1 = True

    def rand(self, n=1):
        if self.seed is None:
            raise ValueError('the worker was not initiated with a seed')
        if n == 1:
            return self.rand1()
        else:
            return np.array([self.rand1() for _ in range(n)], float)

    def fetch(self):
        return self.parent_conn.recv()

    def _handle_sigusr1(self, signum, frame):
        """
        how the worker should behave if it gets SIGUSR1 signal (sent by the main thread)
        :param signum:
        :param frame:
        :return:
        """
        if self.verbose:
            print(self.name, "got interruption order from the main thread")
        raise MainThreadInterrupt("{} got the interruption signal from the main thread".format(self.name))

    def cancel(self, *args, **kwargs):
        """
        :param args:
        :param kwargs:
        :return:
        """
        print(self.name, 'cancel section got', args, kwargs, "do some stuff before raising")
        return

    def __call__(self, *args, **kwargs):
        """usage demonstration, please subclass"""
        try:
            print(self.name, "got", args, kwargs)

            # =============== Protected area
            print(self.name, "waiting")
            self.lock.acquire()
            print(self.name, "locked")

            workfor(1.)

            self.lock.release()
            print(self.name, "released")
            # =============== End of protected area

            r = self.rand()  # thread safe random numbers (need a seed at initiation)
            if r <= 0.5:
                print(self.name, "first encountered an error")
                raise Exception('1')

            ans = 10 * r
            print(self.name, "done", ans)

        except MainThreadInterrupt as e:
            # ========== got the interruption order, an error has occured in another workspace
            self.cancel(e)
            raise e

        except BaseException as e:
            # ========== an error has occured in this workspace
            self.send_interruption_signal()
            self.cancel(e)
            raise e

    def send_interruption_signal(self):
        """
        send the interruption signal to the main thread so that it can interrupt the other workers
        :return:
        """
        if self.interruption_signal_sent.value:
            # signal already sent, maybe by the user
            return

        if self.ppid is None:
            # parent process not known
            return

        if self.verbose:
            print(self.name, "sent the interruption signal to the main thread")
        os.kill(self.ppid, signal.SIGUSR2)
        self.interruption_signal_sent.value = 1

    def run(self):
        result = NoResult()

        # save the default interruption signal handler, set the temporary one instead
        defaulf_sigusr1_handler = signal.signal(signal.SIGUSR1, self._handle_sigusr1)

        try:
            result = self(*self.job.args, **self.job.kwargs)

        except MainThreadInterrupt as e:
            # the worker has raised because the main thread told it to
            # no need to send the interruption signal
            message = traceback.format_exc()
            e.args = (message,)
            raise e

        except BaseException as e:
            # the error is coming from this worker
            self.send_interruption_signal()  # hopefully the user sent it before starting the cancellation
            message = traceback.format_exc()
            e.args = (message, )
            raise e

        finally:
            # send the result to the main workspace
            self.child_conn.send(result)

            # restore the default signal handler
            signal.signal(signal.SIGUSR1, defaulf_sigusr1_handler)


class WorkerGroup(object):
    def __init__(self, job_list, userworker, verbose=False):

        if not isinstance(userworker, type):
            raise TypeError('userworker must be a class, got {}'.format(type(userworker)))

        if len(job_list) >= 2 * cpu_count():
            raise ValueError('too many jobs')

        self.workers = []
        lock = RLock()
        for njob, job in enumerate(job_list):
            if not isinstance(job, Job):
                raise TypeError('job must be an instance of Job class, got {}'.format(type(job)))

            worker = userworker(job=job, seed=njob, verbose=verbose, lock=lock)

            if not isinstance(worker, Worker):
                raise TypeError('userworker must derive from the Worker class, got {}'.format(type(worker)))

            self.workers.append(worker)

        self.ppid = os.getpid()
        self.pids = []
        self.verbose = verbose

        for worker in self.workers:
            worker.ppid = self.ppid

    def _handle_sigusr2(self, signum, frame):
        print ('main thread', ' got the interruption signal from a worker')
        raise WorkerInterrupt

    def __enter__(self):
        self.defaulf_sigusr2_handler = signal.signal(signal.SIGUSR2, self._handle_sigusr2)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):

        for worker in self.workers:
            if worker.is_alive():
                if worker.interruption_signal_sent.value:
                    # the worker sent the interruption signal,
                    # no need to send it back
                    if self.verbose:
                        print("do not send the interruption signal to ",
                              worker.name, worker.pid, "(interruption was coming from it)")
                    continue
                if self.verbose:
                    print("send the interruption signal to ", worker.name, worker.pid)
                os.kill(worker.pid, signal.SIGUSR1)

        self.join()
        signal.signal(signal.SIGUSR2, self.defaulf_sigusr2_handler)

    def start(self):
        for worker in self.workers:
            worker.start()

    def join(self):
        for worker in self.workers:
            worker.join()

    def fetch(self):
        for worker in self.workers:
            yield worker.fetch()


if __name__ == '__main__':
    import signal

    class UserWorker(Worker):
        def cancel(self):
            pass

        def __call__(self, x, y):
            try:
                ans = x + y
                workfor(10.)

            except MainThreadInterrupt as e:
                self.cancel()
                raise e

            except BaseException as e:
                self.send_interruption_signal()
                self.cancel()
                raise e

            return ans

    job_list = [Job(*np.random.randn(2)) for _ in xrange(8)]
    try:
        with WorkerGroup(job_list, UserWorker, verbose=True) as wg:
            wg.start()
            #time.sleep(0.5)
            #raise ValueError('test error in the main workspace => workers will raise a MainThreadInterrupt')
            wg.join()
    except Exception as e:
        print("***", type(e))
        raise

    for ans in wg.fetch():
        print (ans)


