from multiprocessing import Process
from proton.multipro.ioqueue import BasicQueue
from proton.communication.printcolors import printblue
from proton.multipro.errors import EndingSignal
import time

printer = printblue


class Message(object):
    def __init__(self, sender_name=None, time_value=None, message=None, jobid=None):
        self.sender_name = sender_name
        self.time_value = time_value
        self.message = message
        self.jobid = jobid

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        s = "{time:8s}: {sender_name:>30s}(job:{jobid:4d}): {message}".format(
            sender_name=self.sender_name,
            time=time.ctime(self.time_value).split()[3],
            jobid=self.jobid,
            message=self.message)
        return s


class MessageQueue(BasicQueue):

    def put(self, message, **kwargs):
        assert isinstance(message, Message)
        super().put(message, **kwargs)


class Printer(Process):
    pass


class BasicPrinter(Printer):
    """standard printing to stdout"""

    def __init__(self, messagequeue):
        Process.__init__(self)
        self.messagequeue = messagequeue

    def communicate(self, message):
        print(message)

    def run(self):
        while True:
            message = self.messagequeue.get()
            if isinstance(message, Message):
                print(str(message))
            elif isinstance(message, EndingSignal):
                return
            else:
                raise TypeError(type(message))
