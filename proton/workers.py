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
    def __init__(self, jobid=None, answer=None,
                 generator_time=None,
                 processor_time=None):
        self.jobid = jobid
        self.answer = answer
        self.generator_time = generator_time
        self.processor_time = processor_time

    def elapsed_generator_time(self):
        try:
            return self.generator_time[1] - self.generator_time[0]
        except (IndexError, KeyError):
            return -1.0
        except Exception as e:
            e.args = (str(e), 'unexpected error, please define what to do', )
            raise e

    def elapsed_processor_time(self):
        try:
            return self.processor_time[1] - self.processor_time[0]
        except (IndexError, KeyError):
            return -1.0
        except Exception as e:
            e.args = (str(e), 'unexpected error, please define what to do',)
            raise e

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        ans_cut = str(self.answer).split('\n')[0]
        return f"{self.__class__.__name__}:\n\t" \
               f"job:{self.jobid}\n\t" \
               f"gentime:{self.elapsed_generator_time() * 1e6:.2f}ms\n\t" \
               f"protime:{self.elapsed_processor_time() * 1e6:.2f}ms\n\t" \
               f"answer:{ans_cut}"


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


class StackerOutput(WorkerOutput):
    def __init__(self, jobids=None, answer=None,
                 generator_time=0., processor_time=0.,
                 stacker_name=None):
        super(StackerOutput, self).__init__()

        self.stacker_name = stacker_name
        self.jobids = jobids if jobids is not None else []
        self.answer = answer  # cumulative answer
        self.generator_time = generator_time  # cumulative generator time
        self.processor_time = processor_time  # cumulative processor time

    def elapsed_generator_time(self):
        return self.generator_time

    def elapsed_processor_time(self):
        return self.processor_time

    def __str__(self):
        ans_cut = str(self.answer).split('\n')[0]
        jobids_cut = str(self.jobids)[:50] + ("..." if len(str(self.jobids)) > 50 else "")

        return f"{self.__class__.__name__}: \n\t" \
               f"stacker:{self.stacker_name}\n\t" \
               f"jobs:{jobids_cut}\n\t" \
               f"gentime:{self.generator_time * 1e6:.2f}ms\n\t" \
               f"protime:{self.processor_time * 1e6:.2f}ms\n\t" \
               f"answer:{ans_cut}"

    def __iadd__(self, other):
        assert isinstance(other, StackerOutput)
        self.jobids += other.jobids  # must be both lists
        self.generator_time += other.generator_time  # must be both floats
        self.processor_time += other.processor_time  # must be both floats

        if self.answer is None:
            self.answer = other.answer

        elif other.answer is not None:
            self.answer += other.answer

        return self


class Stacker(Worker):
    """
    same as Worker, but do not put results into the output queue, unless the ending signal has been received
    see tutorials
    """

    def run(self):
        """gets jobs from the inputqueue and runs it until
           it gets the ending signal
        """

        # ----- for statistics
        ngot, nput, nfail = 0, 0, 0
        jobids, stackanswer = [], None  # list of jobs stacked by this thread, stack result
        Tgen = 0.  # cumulative generation time
        Tpro = 0.  # cumulative processing time

        while True:
            packet = self.inputqueue.get()

            if isinstance(packet, EndingSignal):
                # got the ending signal

                if self.verbose:
                    message = Message(
                        sender_name=self.name,
                        time_value=time.time(),
                        message="got EndingSignal",
                        jobid=None)
                    self.messagequeue.put(message)

                if stackanswer is not None:
                    # this threads stacked something
                    ouptut = StackerOutput(
                        stacker_name=self.name,
                        jobids=jobids,
                        answer=stackanswer,
                        generator_time=Tgen,
                        processor_time=Tpro)

                    self.outputqueue.put(ouptut)

                self.inputqueue.put(packet)  # resend the ending signal for the other workers
                self.outputqueue.put(packet)  # ending signal
                return

            elif isinstance(packet, GeneratorError):  # the generator has failed
                if stackanswer is not None:  # this threads stacked something
                    ouptut = StackerOutput(
                        stacker_name=self.name,
                        jobids=jobids,
                        answer=stackanswer,
                        generator_time=Tgen,
                        processor_time=Tpro)

                    self.outputqueue.put(ouptut)

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

                # assert hasattr(answer, "__iadd__")
                if stackanswer is None:
                    stackanswer = answer
                else:
                    stackanswer += answer

                jobtime = (start, time.time())

                Tgen += job._gentime[1] - job._gentime[0]  # cumulate the generation time
                Tpro += jobtime[1] - jobtime[0]  # cumulate the processing time
                jobids.append(job._jobid)  # add this job to the list stacked by this thread

            except Exception:
                nfail += 1
                errtype, errvalue, errtrace = sys.exc_info()
                message = "Worker {} failed while executing job {}\n".format(self.name, job._jobid)
                message += "    " + "    ".join(traceback.format_exception(
                    etype=errtype, value=errvalue, tb=errtrace, limit=10))

                if errtype not in self.ignore_exceptions:
                    # put worker error in the output list only if it is fatal
                    output = WorkerError(
                        message=message,
                        errtype=errtype,
                        errvalue=errvalue)
                    self.outputqueue.put(output)

                    self.inputqueue.put(EndingSignal())  # send the ending signal for the other workers
                    return  # stop the execution of this thread

                if self.verbose:
                    message = Message(
                        sender_name=self.name,
                        time_value=time.time(),
                        message="failed",
                        jobid=job._jobid)
                    self.messagequeue.put(message)

                continue  # ignore the error and continue getting packets