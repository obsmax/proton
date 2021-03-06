from multiprocessing import Lock, cpu_count
from proton.workers import Worker, WorkerOutput, Stacker, StackerOutput
from proton.errors import GeneratorError, EndingSignal, WorkerError
from proton.messages import Message, MessageQueue, BasicPrinter
from proton.ioqueue import InputQueue, OutputQueue
from proton.processingtarget import ProcessingTarget
from proton.jobs import JobFeeder, Job
from proton.waitingqueue import WaitingQueue
import time, random
import os


ERRORLOGFILE = "protonerrors.log"


class Mapper(object):
    def __iter__(self):
        return self


class MapAsync(Mapper):
    whichworker = Worker

    def __init__(self, function_or_instance, job_generator,
                 ignore_exceptions=None,
                 nworkers=None, affinity=None,
                 lock=None,
                 verbose=False,
                 lowpriority=False):

        self.ignore_exceptions = \
            ignore_exceptions if ignore_exceptions is not None else []

        for exception in self.ignore_exceptions:
            assert isinstance(exception, type)
            assert issubclass(exception, Exception)

        self.nworkers = nworkers if nworkers is not None else cpu_count()
        self.affinity = affinity
        self.verbose = verbose
        self.lowpriority = lowpriority
        self.lock = lock
        self.ppid = os.getpid()

        # ----------- message queue and message printer thread
        # needed even in non-verbose mode (for worker.communicate)
        self.message_queue = MessageQueue(maxsize=1000)
        self.printer = BasicPrinter(self.message_queue)

        # ----------- create the input and output queues
        self.input_queue = InputQueue(maxsize=self.nworkers)
        self.output_queue = OutputQueue(maxsize=self.nworkers)
        # do not increase maxsize, there is always a better explanation
        # if the code is slow

        # -----------
        self.pids = []  # for now

        # -----------
        self.job_feeder = JobFeeder(
            job_generator=job_generator,
            input_queue=self.input_queue,
            message_queue=self.message_queue,
            verbose=self.verbose)

        # ----------- determine if each worker will have a distinct target or not
        self.workers = []
        self.nactive = self.nworkers

        seedstmp = [random.random() * 100000
                    for _ in range(self.nworkers)]

        target = ProcessingTarget(function_or_instance=function_or_instance)  # will be deep copied !! times self.nworkers

        for i in range(self.nworkers):

            worker = self.whichworker(
                target=target,
                inputqueue=self.input_queue,
                outputqueue=self.output_queue,
                messagequeue=self.message_queue,
                ignore_exceptions=self.ignore_exceptions,
                seed=seedstmp[i],  # in case two mapasync run at the same time
                parent=self,
                lock=self.lock,
                verbose=self.verbose)
            worker.name = "Worker_{:04d}".format(i + 1)
            self.workers.append(worker)

    def __str__(self):
        s = "----------------------------\n" \
            "Parent pid = {ppid}\n" \
            "    Generator pid = {generator_pid}\n" \
            "    Printer pid = {printer_pid}\n".format(
            ppid=self.ppid,
            generator_pid=self.job_feeder.pid,
            printer_pid=self.printer.pid if self.printer is not None else -1)

        for worker in self.workers:
            s += "    {}  pid = {}; seed = {}\n".format(worker.name, worker.pid, worker.seed)
        return s

    def __enter__(self):

        for worker in self.workers:
            worker.start()
        self.job_feeder.start()
        if self.printer is not None:
            self.printer.start()

        self.pids = [worker.pid for worker in self.workers]
        self.pids.append(self.job_feeder.pid)
        if self.printer is not None:
            self.pids.append(self.printer.pid)

        self.set_affinity()
        self.renice()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # case 1 : no error, all outputs have been extracted from Out => join the workers
        # case 2 : no error, not all outputs extracted from Out => terminate
        # case 3 : some errors => terminate
        if exc_type is None and self.nactive == 0:

            for worker in self.workers:
                worker.join()  # this might be blocking
            self.job_feeder.join()

            self.input_queue.close()
            self.output_queue.close()
            self.message_queue.close()

            if self.printer is not None:
                self.printer.join()
        else:
            # either an error has occured or the user leaves too soon
            # if self.verbose:print "killing workers and queues"

            self.input_queue.close()
            if self.message_queue is not None:  # better test than self.verbose
                self.message_queue.close()
            self.output_queue.close()

            for worker in self.workers:
                worker.terminate()
            self.job_feeder.terminate()
            if self.printer is not None:
                self.printer.terminate()

    def affinity_to_taskset_command(self):
        if "-" in self.affinity:

            corestart, coreend = self.affinity.split('-')
            corestart = int(corestart)
            coreend = int(coreend)
            if not coreend > corestart >= 0:
                raise ValueError('taskset command not understood {}'.format(self.affinity))

            cmd = "taskset -pca %d-%d %%d" % (corestart, coreend)
            cmd = "\n".join([cmd % pid for pid in self.pids])

        else:
            corestart = coreend = int(self.affinity)
            if not coreend == corestart >= 0:
                raise ValueError('taskset command not understood {}'.format(self.affinity))
            cmd = "taskset -pca %d %%d" % corestart
            cmd = "\n".join([cmd % pid for pid in self.pids])
        return cmd

    def set_affinity(self):
        if self.affinity is None:
            return
        cmd = self.affinity_to_taskset_command()
        os.system(cmd)

    def renice(self):
        if self.lowpriority:
            cmd = "renice -n 10 -g %d" % self.ppid
            os.system(cmd)

    def communicate(self, *args, **kwargs):
        self.printer.communicate(*args, **kwargs)

    def __iter__(self):
        return self

    def __next__(self):
        if not self.nactive:
            raise StopIteration

        while self.nactive:
            packet = self.output_queue.get()

            if isinstance(packet, EndingSignal):
                self.nactive -= 1
                continue

            elif isinstance(packet, GeneratorError):
                raise packet

            elif isinstance(packet, WorkerError):
                with open(ERRORLOGFILE, 'a') as fid:
                    fid.write(str(packet) + "\n")

                if self.verbose:
                    self.communicate(str(packet))

                message, errtype, errvalue = packet.args
                if errtype not in self.ignore_exceptions:
                    # fatal error
                    raise packet

                # non fatal
                continue

            elif isinstance(packet, WorkerOutput):
                return packet

            else:
                raise TypeError(type(packet))

        if self.verbose:
            message = Message(
                sender_name="Mapper",
                time_value=time.time(),
                message="got EndingSignal",
                jobid=None)
            self.message_queue.put(message)

        if self.message_queue is not None:
            # not related to verbose mode
            self.message_queue.put(EndingSignal())

        raise StopIteration


class MapSync(MapAsync):

    def __init__(self, *args, **kwargs):
        if 'ignore_exceptions' in kwargs.keys() and len(kwargs['ignore_exceptions']):
            raise NotImplementedError("ignore_exceptions not handled by MapSync")

        super(MapSync, self).__init__(*args, **kwargs)

    def __iter__(self):
        # jobs that come up too soon are kept in a waiting queue to preserve the input order
        return WaitingQueue(self, verbose=self.verbose, message_queue=self.message_queue)


class StackAsync(MapAsync):
    whichworker = Stacker

    def stack(self):
        ans = StackerOutput(stacker_name="StackAsync")
        for stacker_output in self:

            if self.verbose:
                message = Message(
                    sender_name="StackAsync",
                    time_value=time.time(),
                    # message=f"added {len(stacker_output.jobids)} more prestack job(s) "
                    #         f"from {stacker_output.stacker_name} "
                    #         f"to the grand total")
                    message=f"got the prestack result from {stacker_output.stacker_name} \n"
                            f"{stacker_output} ")

                self.message_queue.put(message)

            ans += stacker_output

        return ans


if __name__ == '__main__':
    import numpy as np

    def job_generator():
        for n in range(32):
            running_time = np.random.rand() * 3.
            yield Job(running_time=running_time)

    def fun(running_time):
        start = time.time()
        while time.time() - start < running_time:
            0 + 0
        return running_time

    with MapSync(
        function_or_instance=fun,
        job_generator=job_generator(),
        nworkers=8,
        verbose=True) as ma:

        print(ma)
        for _ in ma:
            pass

    exit()

    # =================
    def job_generator():

        for n in range(32):
            yield Job(t=1.)
        yield 1.0

    def fun(worker, t):
        start = time.time()
        while time.time() - start < t:
            0 + 0
        worker.communicate('I am fine, ' + str(t))
        if worker.rand() < 0.1:
            raise ValueError('skip me')
        if worker.rand() < 0.1:
            raise NameError('skip me')
        if worker.rand() < 0.1:
            raise TypeError('skip me')
        return t

    with MapAsync(
        function_or_instance=fun,
        job_generator=job_generator(),
        ignore_exceptions=(ValueError, NameError, TypeError),
        nworkers=8,
        verbose=True) as ma:

        print(ma)
        for _ in ma:
            pass
