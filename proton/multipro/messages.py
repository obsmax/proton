from multiprocessing import Process
from proton.multipro.ioqueue import BasicQueue
from proton.communication.printcolors import printblue
from proton.multipro.errors import EndingSignal
import time


class Message(object):
    def __init__(self, sender_name=None, time_value=None, message=None, jobid=None):
        self.sender_name = sender_name
        self.time_value = time_value
        self.message = message
        self.jobid = jobid

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        s = "{time:8s}: {sender_name:<12s}(job:{jobid:4s}): {message}".format(
            sender_name=self.sender_name,
            time=time.ctime(self.time_value).split()[3],
            jobid=str(self.jobid),
            message=self.message)
        s = s.replace('(job:None)', '          ')
        return s


class MessageQueue(BasicQueue):

    def put(self, message, **kwargs):
        assert isinstance(message, Message) or isinstance(message, EndingSignal)
        super(MessageQueue, self).put(message, **kwargs)


class Printer(Process):
    pass


class BasicPrinter(Printer):
    """standard printing to stdout"""

    printer = printblue

    def __init__(self, messagequeue):
        Process.__init__(self)
        self.messagequeue = messagequeue

    def communicate(self, *args, **kwargs):
        self.printer(*args, **kwargs)

    def run(self):
        while True:
            message = self.messagequeue.get()
            if isinstance(message, Message):
                self.communicate(str(message))
            elif isinstance(message, EndingSignal):
                return
            else:
                raise TypeError(type(message))
