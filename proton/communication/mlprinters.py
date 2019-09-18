from multiprocessing import Process, Queue
from multiprocessing.queues import Empty as EmptyQueueError
from proton.communication.multiline import MultiLine
import curses
import numpy as np
import time


def hhmmss(t):
    """return time at format hh:mm:ss"""
    s = time.ctime(t)
    return s.split()[3]


class ExitedMultilinePrinterError(Exception):
    pass


class InterruptionSignal(object):
    pass


class InteractivePrinter(Process):
    """
    a multiline printer which take its messages from queuing system
    """

    def __init__(self, maxlines, message_queue=None):
        Process.__init__(self)
        if message_queue is None:
            self.message_queue = Queue()  # default queue
        else:
            self.message_queue = message_queue  # user defined queue
        self.maxlines = maxlines

    def write(self, line, message):
        if not self.is_alive():
            raise ExitedMultilinePrinterError('')
        self.message_queue.put((line, message))

    def communicate(self, message):
        """to be customized, put a message that must be understood by interpretor so that the output of interpretor will be : -1, message
        message "exit iso" forces the printer to leave (equivalent to pressing "q")
        """
        self.message_queue.put((-1, message))

    def interpretor(self, tup):
        """to be customized, tell me how to convert the message_queue outputs into a tuple like (line, message)"""
        line, message = tup
        return line, message

    def run(self):
        with MultiLine(maxlines=self.maxlines) as ml:
            ml.win.nodelay(True)
            autofollow = False
            while True:
                ml.reset_termsize()
                # ------------------------
                try:
                    line, message = self.interpretor(self.message_queue.get_nowait())
                    if message.lower() == "exit iso":
                        lines = ml.lines
                        break  # ending signal from outside

                    if line == -1:
                        ml.communicate(message)
                    else:
                        ml.write(line, message)
                        if autofollow:
                            if line - ml.line0 >= ml.nmax:
                                ml.move(np.min([np.max([0, line - 2]), ml.maxlines - ml.nmax - 1]))

                except EmptyQueueError:
                    pass
                except KeyboardInterrupt:
                    raise
                except Exception as Detail:
                    ml.communicate("%s" % Detail)

                # ------------------------
                ch = ml.win.getch()
                # cursor up
                if ch in (ord('k'), ord('K'), curses.KEY_UP):
                    ml.move(max([0, ml.line0 - 1]))
                    continue
                # cursor down
                elif ch in (ord('j'), ord('j'), curses.KEY_DOWN):
                    ml.move(min([ml.maxlines - ml.nmax - 1, ml.line0 + 1]))
                    continue
                    # page previous
                elif ch in (curses.KEY_PPAGE, curses.KEY_BACKSPACE, 0x02):
                    ml.move(max([0, ml.line0 - ml.nmax]))
                    continue
                # page next
                elif ch in (curses.KEY_NPAGE, ord(' '), 0x06):  # Ctrl-F
                    ml.move(min([ml.maxlines - ml.nmax - 1, ml.line0 + ml.nmax]))
                    continue
                # home
                elif ch in (curses.KEY_HOME, 0x01):
                    ml.move(0)
                    continue
                # end
                elif ch in (curses.KEY_END, 0x05):
                    ml.move(ml.maxlines - ml.nmax - 1)
                    continue
                # enter
                elif ch in (10, 13):
                    ml.move(min([ml.maxlines - ml.nmax - 1, ml.line0 + ml.nmax]))
                    continue
                # resize
                elif ch == curses.KEY_RESIZE:
                    ml.reset_termsize()
                    ml.move(ml.line0)
                    continue
                # cursor left
                # elif ch == curses.KEY_LEFT: continue
                # cursor right
                # elif ch == curses.KEY_RIGHT: continue
                # toggle .dot-files
                elif ch == 0x08:  # Ctrl-H
                    autofollow = not autofollow  # toggle autofollow mode
                    ml.communicate('autofollow : %s' % str(autofollow))

                # quit
                elif ch in (ord('q'), ord('Q')):
                    lines = ml.lines
                    break  # , curses.KEY_F10, 0x03):
                else:
                    continue
        # recall what was printed
        for l in lines:
            if len(l):
                print(l)


# class NoPrinter(object):
#     """use that printer to shut the processes up"""
#
#     def __init__(self, noqueue):
#         self.pid = -1
#         # messagequeue is None
#
#     def start(self):
#         return
#
#     def join(self):
#         return
#
#     def terminate(self):
#         return
#
#     def communicate(self, message):
#         print(message)
#
#
# class BasicPrinter(Process):
#     """standard printing to stdout"""
#
#     def __init__(self, messagequeue):
#         Process.__init__(self)
#         self.messagequeue = messagequeue
#
#     def communicate(self, message):
#         print(message)
#
#     def run(self):
#         while True:
#             packet = self.messagequeue.get()
#             if isinstance(packet, InterruptionSignal):
#                 break
#             sender, tim, mess, jobid = packet
#             message = "{} at {} : {} ".format(sender + " " * (20 - len(sender)), str(hhmmss(tim)), mess)
#             if jobid is not None:
#                 message += str(jobid)
#             print(message)
#         return
#
#
# class ProcessPrinter(InteractivePrinter):
#     def __init__(self, messagequeue):
#         InteractivePrinter.__init__(self, maxlines=1000, message_queue=messagequeue)
#
#     def communicate(self, message):
#         self.message_queue.put(("User", time.time(), message, None))
#
#     def interpretor(self, tup):
#         if isinstance(tup, InterruptionSignal):
#             return -1, "exit iso"  # send the exit signal to the printer
#
#         sender, tim, mess, jobid = tup
#
#         if sender == "InputQueue":
#             line = 0
#             message = "%s at %s : %s " % (sender + " " * (20 - len(sender)), str(hhmmss(tim)), mess)
#             if jobid is not None: message += str(jobid)
#         elif sender.split('-')[0] == "Worker":
#             line = int(sender.split('-')[-1])
#             message = "%s at %s : %s " % (sender + " " * (20 - len(sender)), str(hhmmss(tim)), mess)
#             if jobid is not None: message += str(jobid)
#         elif sender == "MessageQueue":
#             line = -1
#             message = sender + mess
#         elif sender == "User":
#             line = -1
#             message = mess
#         else:  # raise Exception('message not understood')
#             line = -1
#             message = mess
#         return line, message
#
#
# class JobPrinter(InteractivePrinter):
#     def __init__(self, messagequeue):
#         InteractivePrinter.__init__(self, maxlines=100000, message_queue=messagequeue)
#
#     def communicate(self, message):
#         self.message_queue.put(("User", time.time(), message, None))
#
#     def interpretor(self, tup):
#         if isinstance(tup, InterruptionSignal):
#             return -1, "exit iso"  # send the exit signal to the printer
#
#         sender, tim, mess, jobid = tup
#         if sender == "User":
#             line = -1
#             message = mess
#         elif jobid is None:
#             line = -1
#             message = mess
#         elif isinstance(jobid, int):
#             line = jobid
#             if sender == "InputQueue":
#                 message = "Job%d%s at %s  : %s" % (jobid, " " * (10 - len(str(jobid))), hhmmss(tim), sender)
#             elif "Worker" in sender:
#                 if "got" in mess:
#                     message = message = "Job%d%s at %s  : %s" % (jobid, " " * (10 - len(str(jobid))), hhmmss(tim), sender)
#                 elif "put" in mess:
#                     message = message = "Job%d%s at %s  : %s" % (jobid, " " * (10 - len(str(jobid))), hhmmss(tim), "done")
#                 elif "fail" in mess:
#                     message = message = "Job%d%s at %s  : %s" % (
#                         jobid, " " * (10 - len(str(jobid))), hhmmss(tim), "failed (see /tmp/multiproerrors.log)")
#                 else:
#                     line, message = -1, mess
#             else:
#                 line, message = -1, mess
#         else:
#             line, message = -1, mess
#         return line, message
#
#
# class FakePrinter(object):
#     def communicate(self, message):
#         print(message)


if __name__ == '__main__':
    p = InteractivePrinter(maxlines=3, message_queue=None)

    p.message_queue.put((0, 'hello world[0]'))
    p.message_queue.put((1, 'hello world[1]'))
    p.message_queue.put((2, 'hello world[2]'))
    p.message_queue.put(InterruptionSignal)

    p.start()
    p.join()