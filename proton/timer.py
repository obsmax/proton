import signal, time


class TimeOutError(Exception):
    pass


class TimeLimiter(object):
    """
    contextual manager to interrupt execution after sec seconds
    raises obsmax4.mainexceptions.TimeoutError

    usage :

    with TimeLimiter(3):
        while True:
            pass
    """

    def __init__(self, sec):
        """

        :param sec:
        """
        self.sec = sec

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.raise_timeout)
        signal.alarm(self.sec)

    def __exit__(self, *args):
        signal.alarm(0)

    def raise_timeout(self, *args):
        raise TimeoutError("execution interrupted by TimeLimiter after {}s".format(self.sec))


class Timer(object):
    """
    measure time spent in a block of code with checkpoints

    with Timer() as t:
        start = time.time()
        while time.time() - start < 0.3:
            time.sleep(0.05)

        t.checkpoint('point1')
        start = time.time()
        while time.time() - start < 0.4:
            time.sleep(0.05)

        t.checkpoint('point2')
        start = time.time()
        while time.time() - start < 0.2:
            time.sleep(0.05)
    """
    def __init__(self, message="timer"):
        self.message = message
        self.entertime = None
        self.exittime  = None
        self.checkpointnames = []
        self.checkpointtimes = []

    def checkpoint(self, name):
        self.checkpointnames.append(name)
        self.checkpointtimes.append(time.time())

    def __enter__(self):
        print("%s : start" % self.message)
        self.entertime = time.time()
        self.exittime  = None  # reinitiate exiting time
        self.checkpointnames.append("start")
        self.checkpointtimes.append(self.entertime)
        return self

    def __str__(self):
        out = "%s : done, " % self.message
        if len(self.checkpointtimes) > 2:
            out += "\n"
            for n in range(len(self.checkpointtimes) - 1):
                out += "    %10s -> %10s : %.0fms, %6.2f%%\n" % \
                       (self.checkpointnames[n], self.checkpointnames[n+1],
                        1000. * (self.checkpointtimes[n+1] - self.checkpointtimes[n]),
                        100. * (self.checkpointtimes[n+1] - self.checkpointtimes[n]) / (self.exittime - self.entertime)
                        )
            out += (" " * len(self.message)) + "   "
        out += "elpased time %.0fms" % (1000. * (self.exittime - self.entertime))
        return out

    def __exit__(self, *args, **kwargs):
        self.exittime = time.time()
        self.checkpointnames.append("end")
        self.checkpointtimes.append(self.exittime)
        print(self)

    def __call__(self):
        if self.exittime is None:  # self called inside the with
            return time.time() - self.entertime
        else:  # called after self has been closed
            return self.exittime - self.entertime


class TimeCounter(object):
    """
    count time spent in a peace of code over iterations
    usage :

    t1 = TimeCounter("time counter 1")
    t2 = TimeCounter("time counter 2")
    for i in xrange(10):
        with t1:
            start = time.time()
            while time.time() - start < 0.3:
                time.sleep(0.05)

        with t2:
            start = time.time()
            while time.time() - start < 0.1:
                time.sleep(0.05)
    print t1
    print t2

    """
    def __init__(self, name="time counter"):
        self.elapsed_time = 0.
        self.begin = None
        self.name = name

    def __enter__(self):
        if self.begin is not None:
            raise ValueError('start called before end')
        self.begin = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.begin is None:
            raise ValueError('end called before start')
        self.elapsed_time += time.time() - self.begin
        self.begin = None

    def __str__(self):
        return "{:>20s} : {:.0f}ms".format(self.name, self.elapsed_time * 1000.)


if __name__ == '__main__':

    # ===========
    t1 = TimeCounter("time counter 1")
    t2 = TimeCounter("time counter 2")
    for i in range(2):
        with t1:
            start = time.time()
            while time.time() - start < 0.3:
                time.sleep(0.05)

        with t2:
            start = time.time()
            while time.time() - start < 0.1:
                time.sleep(0.05)
    print(t1)
    print(t2)

    # ===========
    with Timer() as t:
        start = time.time()
        while time.time() - start < 0.3:
            time.sleep(0.05)

        t.checkpoint('point1')
        start = time.time()
        while time.time() - start < 0.4:
            time.sleep(0.05)

        t.checkpoint('point2')
        start = time.time()
        while time.time() - start < 0.2:
            time.sleep(0.05)

    # ===========
    try:
        with TimeLimiter(1):
            while True:
                time.sleep(0.05)
    except TimeoutError as e:
        print(e)

