from multiprocessing import Process
from typing import Union
from proton.multipro.messages import MessageQueue, Message
from proton.multipro.ioqueue import InputQueue
from proton.multipro.errors import EndingSignal
import time


class Job(object):
    args, kwargs = (), {}

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self._jobid = None  # protected attr, will be set latter on
        self._gentime = None
        self._process_time = None


class JobFeeder(Process):
    def __init__(
            self, job_generator, input_queue: InputQueue,
            message_queue: Union[MessageQueue, None] = None):
        super().__init__()
        self.job_generator = job_generator
        self.input_queue = input_queue
        self.message_queue = message_queue
        self.verbose = self.message_queue is not None

    def run(self):
        gen_begin = time.time()
        # operates in the generator workspace
        for jobid, job in enumerate(self.job_generator):
            gen_end = time.time()
            assert isinstance(job, Job)
            job._jobid = jobid   # attribute the jobid now and once for all
            job._gentime = (gen_begin, gen_end)

            self.input_queue.put(job)

            if self.verbose:
                message = Message(
                    sender_name="job feeder",
                    time_value=time.time(),
                    message="put job",
                    jobid=job._jobid)

                self.message_queue.put(message)

        self.input_queue.put(EndingSignal())
