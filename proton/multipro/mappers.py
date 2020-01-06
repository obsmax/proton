from proton.multipro.workers import Worker, WorkerOutput
from proton.multipro.errors import GeneratorError, EndingSignal, WorkerError
from proton.multipro.messages import Message, MessageQueue, BasicPrinter
from proton.multipro.ioqueue import InputQueue, OutputQueue
from proton.multipro.target import Target
from proton.multipro.jobs import JobFeeder, Job
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
                 nworkers=12, taskset=None,
                 verbose=False, lowpriority=False):

        self.ignore_exceptions = \
            ignore_exceptions if ignore_exceptions is not None else []

        for exception in self.ignore_exceptions:
            assert isinstance(exception, type)
            assert issubclass(exception, Exception)

        self.nworkers = nworkers
        self.taskset = taskset
        self.verbose = verbose
        self.lowpriority = lowpriority
        self.ppid = os.getpid()

        # -----------
        self.message_queue = None
        self.printer = None

        if self.verbose:
            self.message_queue = MessageQueue(maxsize=1000)
            self.printer = BasicPrinter(self.message_queue)

        # ----------- create the input and output queues
        self.input_queue = InputQueue(maxsize=self.nworkers)
        self.output_queue = OutputQueue(maxsize=self.nworkers)
        # do not increase maxsize, there is always a better explaination
        # if the code is slow

        # -----------
        self.pids = []  # for now

        # -----------
        self.job_feeder = JobFeeder(
            job_generator=job_generator,
            input_queue=self.input_queue,
            message_queue=self.message_queue)

        # ----------- determine if each worker will have a distinct target or not
        self.workers = []
        self.nactive = self.nworkers

        seedstmp = [random.random() * 100000
                    for _ in range(self.nworkers)]

        target = Target(function_or_instance=function_or_instance)  # will be deep copied !! times self.nworkers

        for i in range(self.nworkers):

            worker = self.whichworker(
                target=target,
                inputqueue=self.input_queue,
                outputqueue=self.output_queue,
                messagequeue=self.message_queue,
                ignore_exceptions=self.ignore_exceptions,
                seed=seedstmp[i],  # in case two mapasync run at the same time
                parent=self,
                lock=None)  # self.lock)
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

        self.settask()
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
            if self.verbose:
                self.message_queue.close()
            if self.printer is not None:
                self.printer.join()
        else:
            # either an error has occured or the user leaves too soon
            # if self.verbose:print "killing workers and queues"

            self.input_queue.close()
            if self.verbose:
                self.message_queue.close()
            self.output_queue.close()

            for worker in self.workers:
                worker.terminate()
            self.job_feeder.terminate()
            if self.printer is not None:
                self.printer.terminate()

    def settask(self):
        if self.taskset is None:
            return

        if "-" in self.taskset:
            corestart, coreend = [int(x) for x in self.taskset.split('-')]
            assert coreend > corestart >= 0
            cmd = "taskset -pca %d-%d %%d" % (corestart, coreend)
            cmd = "\n".join([cmd % pid for pid in self.pids])

        else:
            corestart = coreend = int(self.taskset)
            assert coreend == corestart >= 0
            cmd = "taskset -pca %d %%d" % (corestart)
            cmd = "\n".join([cmd % pid for pid in self.pids])

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
            self.message_queue.put(EndingSignal())
        raise StopIteration


if __name__ == '__main__':

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
