import sys
import time
from proton.multiline import MultiLine


def countdown(message, t):
    l = "%s %4.0fs" % (message, t)
    sys.stdout.write(l)
    sys.stdout.flush()

    start = time.time()
    while time.time() - start < t:
        sys.stdout.write("\b" * len(l))                

        l = "{} {:4.0f}s".format(message, t - time.time() + start)
        sys.stdout.write(l)
        sys.stdout.flush()
        time.sleep(0.5)
    print()


class WaitBar(object):
    symbol = '\u2588'

    def __str__(self):
        s = "{:s} {:s} {:6.2f}% {:s}".format(self.title, self.bars(), 100. * self.purcent, self.remain)
        return s 

    def bars(self):
        nbars   = int(round(self.purcent * self.width))
        nspaces = self.width - nbars
        return self.symbol * nbars + " " * nspaces

    def __init__(self, title="", width=40, reevaluatespeed=5.0):
        self.title = title
        self.width = width
        self.purcent = 0.
        self.lastpurcent = 0.
        self.start = time.time()
        self.time = self.start
        self.lasttime = self.start
        self.speed = 0.
        self.remain = "unkn"
        self.string = self.__str__()
        self.reevaluatespeed = reevaluatespeed

        sys.stdout.write(self.string)
        sys.stdout.flush()

    def set_purcent(self, purcent):
        if self.purcent < 0. or self.purcent > 1.:
            self.close()
        # if purcent and purcent - 1. and abs(self.purcent - purcent) < 0.005:
        #   return

        self.purcent, self.time = purcent, time.time()
        if self.reevaluatespeed and self.time - self.lasttime > self.reevaluatespeed:
            # reevaluate speed every X seconds
            self.speed = (self.purcent - self.lastpurcent) / (self.time - self.lasttime) 
            self.lastpurcent, self.lasttime = self.purcent, self.time

        elif self.start == self.lasttime:
            self.speed = (self.purcent - self.lastpurcent) / (self.time - self.lasttime) 

        if self.speed:
            tremain = ((1. - self.purcent) / self.speed)
            d = int(tremain / 24. / 3600.)
            h = int(tremain % (24. * 3600.) / 3600.)
            m = int(tremain % 3600. / 60.)
            s = int(tremain % 60.)
            self.remain = "{:2d}s".format(s)
            if m or h or d:
                self.remain = "{:2d}mn{:s}".format(m, self.remain)
            if h or d:
                self.remain = "{:2d}h{:s}".format(h, self.remain)
            if d:
                self.remain = "{:3d}d{:s}".format(d, self.remain)

        else:
            self.remain = "unkn"

        self.remain = self.remain
        self.remain = self.remain + " " * (25 - len(self.remain))
        sys.stdout.write("\b" * len(self.string))

        self.string = self.__str__()
        sys.stdout.write(self.string)
        sys.stdout.flush()

    def close(self):
        self.purcent = 1.0-1.e-20
        self.set_purcent(self.purcent)
        sys.stdout.write("\n")


class WaitBarPipe(WaitBar):
    symbol = "|"


class MultiWaitbar(MultiLine):
    symbol = '\u2588'
    width = 40

    def __init__(self, linenames):
        super().__init__(maxlines=len(linenames))
        self.linenames = linenames
        self.purcents = [0 for _ in range(self.maxlines)]

    def __enter__(self):
        super().__enter__()
        for line in range(self.maxlines):
            self.set_purcent(line, 0.)
        return self

    def bars(self, purcent):
        nbars   = int(round(purcent * self.width))
        nspaces = self.width - nbars
        return self.symbol * nbars + " " * nspaces

    def set_purcent(self, line_number, purcent):
        message = '{linename} {bars} {tail}'.format(
            linename=self.linenames[line_number],
            bars=self.bars(purcent),
            tail="{:.2f}%".format(purcent))

        self.write(line_number=line_number, message=message)


if __name__ == "__main__":

    with MultiWaitbar(['worker 1:', 'worker 2:', 'worker 3:']) as mwb:
        for linenum in range(3)[::-1]:
            for purcent in range(50):
                mwb.set_purcent(linenum, purcent / 100.)

                time.sleep(0.01)

        for purcent in range(50, 100):
            for linenum in range(3)[::-1]:
                mwb.set_purcent(linenum, purcent / 100.)

                time.sleep(0.01)

        mwb.communicate("bye")
        time.sleep(1.0)

    # =================
    countdown('please wait ', 2)

    # =================
    w = WaitBar('progress')
    for i in range(100):
        time.sleep(0.01)
        if i and not i % 3:
            # set_purcent every 3 iterations
            w.set_purcent(i / 100.)
    w.set_purcent(1.)
    w.close()

    w = WaitBarPipe('progress')
    for i in range(100):
        time.sleep(0.01)
        if i and not i % 3:
            # set_purcent every 3 iterations
            w.set_purcent(i / 100.)
    w.set_purcent(1.)
    w.close()

