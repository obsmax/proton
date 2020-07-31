import random
import time
import traceback
import sys
from multiprocessing import Process
from proton.jobs import Job
from proton.messages import Message, MessageQueue
from proton.processingtarget import ProcessingTarget
from proton.errors import EndingSignal, GeneratorError, WorkerError


class WorkerOutput(object):
    def __init__(self, jobid=None, answer=None, generator_time=None, processor_time=None):
        self.jobid = jobid
        self.answer = answer
        self.generator_time = generator_time
        self.processor_time = processor_time

    def elapsed_generator_time(self):
        try:
            return self.generator_time[1] - self.generator_time[0]
        except (IndexError, KeyError):
            return -1.0
        except:
            raise Exception('unexpected case')

    def elapsed_processor_time(self):
        try:
            return self.processor_time[1] - self.processor_time[0]
        except (IndexError, KeyError):
            return -1.0
        except:
            raise Exception('unexpected case')

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "{classname}: " \
               "job:{jobid}\n\t" \
               "gentime:{elapsed_generator_time:.2f}ms\n\t" \
               "protime:{elapsed_processor_time:.2f}ms\n\t" \
               "answer:{answer}" \
               "".format(
            classname=self.__class__.__name__,
            jobid=self.jobid,
            answer=str(self.answer).split('\n')[0],
            elapsed_generator_time=self.elapsed_generator_time() * 1.e6,
            elapsed_processor_time=self.elapsed_processor_time() * 1.e6)


class Worker(Process):
    def __init__(self, target, inputqueue, outputqueue, messagequeue, ignore_exceptions=None, seed=None,
                 parent=None, lock=None):
        """
        :param target: a Target object, the function or object to call inside the dedicated workspaces
        :param inputqueue: a InputQueue object, the queue that transmit jobs from the main workspace inside the dedicated workspaces
        :param outputqueue:
        :param messagequeue:
        :param raiseiferror:
        :param seed:
        :param parent: the parent object, may be MapAsync, MapSync, StackAsync, ...
        :param lock:
        """
        Process.__init__(self)
        self.inputqueue = inputqueue
        self.outputqueue = outputqueue
        self.messagequeue = messagequeue
        self.target = target

        self.ignore_exceptions = ignore_exceptions if ignore_exceptions is not None else []
        for exception in self.ignore_exceptions:
            assert isinstance(exception, type)
            assert issubclass(exception, Exception)

        self.seed = seed
        self.verbose = messagequeue is not None  # not NoPrinter
        self.parent = parent
        self.is_locked = False
        self.lock = lock

        # ------ attach random functions to the worker
        if self.seed is None:
            # seedtmp    = self.pid + 10 * int((time.time() * 1.e4) % 10.)
            # randfuntmp = random.Random(seedtmp).random
            # self.seed = int(1000. * randfuntmp())
            # time.sleep(0.1)
            raise Exception('')
        self.rand_ = random.Random(self.seed).random
        # ------

    def acquire(self):
        if self.lock is None:
            raise Exception('cannot acquire the lock, since no lock was provided when initating the mapper')

        if self.is_locked:
            raise Exception('{} is already locked'.format(self.name))
        self.is_locked = True
        self.lock.acquire()

    def release(self):
        if not self.is_locked:
            raise Exception('{} is not locked'.format(self.name))
        self.is_locked = False
        self.lock.release()

    def rand(self, N=1):
        if N == 1:
            return self.rand_()
        else:
            return [self.rand_() for i in range(N)]

    def communicate(self, message):  # !#
        message = Message(
            sender_name=self.name,
            time_value=time.time(),
            message=message,
            jobid=None)
        self.messagequeue.put(message)

    def run(self):
        """gets jobs from the inputqueue and runs it until
           it gets the ending signal
        """

        # ----- for statistics
        ngot, nput, nfail = 0, 0, 0
        inittime = time.time()
        # -----

        while True:
            packet = self.inputqueue.get()
            if isinstance(packet, EndingSignal):  # got the ending signal
                if self.verbose:
                    message = Message(
                        sender_name=self.name,
                        time_value=time.time(),
                        message="got EndingSignal",
                        jobid=None)
                    self.messagequeue.put(message)

                self.inputqueue.put(packet)  # resend the ending signal for the other workers
                self.outputqueue.put(packet)  # ending signal
                return

            elif isinstance(packet, GeneratorError):  # the generator has failed
                self.inputqueue.put(EndingSignal())  # send the ending signal to the other workers
                self.outputqueue.put(packet)  # transmit the GeneratorError
                return

            elif not isinstance(packet, Job):
                raise TypeError('got object of type {} out of the input queue'.format(str(type(packet))))

            job = packet
            ngot += 1  # count only jobs, not signals or errors

            if self.verbose:
                message = Message(
                    sender_name=self.name,
                    time_value=time.time(),
                    message="got job {}".format(job._jobid),
                    jobid=job._jobid)
                self.messagequeue.put(message)

            try:
                start = time.time()

                if self.target.passworker:
                    # pass self (i.e. the worker to self.target as first argument)
                    answer = self.target(
                        self, *job.args, **job.kwargs)

                else:
                    # call the target function here!!!
                    answer = self.target(
                        *job.args, **job.kwargs)

                jobtime = (start, time.time())

            except Exception:
                nfail += 1
                errtype, errvalue, errtrace = sys.exc_info()
                message = "Worker {} failed while executing job {}\n".format(self.name, job._jobid)
                message += "    " + "    ".join(traceback.format_exception(
                    etype=errtype, value=errvalue, tb=errtrace, limit=10))

                output = WorkerError(
                    message=message,
                    errtype=errtype,
                    errvalue=errvalue)

                self.outputqueue.put(output)

                if errtype not in self.ignore_exceptions:
                    self.inputqueue.put(EndingSignal())  # send the ending signal for the other workers
                    return  # stop the execution of this thread

                if self.verbose:
                    message = Message(
                        sender_name=self.name,
                        time_value=time.time(),
                        message="failed",
                        jobid=job._jobid)
                    self.messagequeue.put(message)
                continue  # ignore the error and continue getting tasks

            ouptut = WorkerOutput(
                jobid=job._jobid,
                answer=answer,
                generator_time=job._gentime,
                processor_time=jobtime)

            self.outputqueue.put(ouptut)
            nput += 1  # count only jobs, not signals or errors

            if self.verbose:
                message = Message(
                    sender_name=self.name,
                    time_value=time.time(),
                    message="put job {}".format(job._jobid),
                    jobid=job._jobid)
                self.messagequeue.put(message)
