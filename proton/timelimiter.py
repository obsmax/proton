import time
import signal


class TimeOut(Exception):
    pass


class TimeLimiter(object):

    def __init__(self, sec):
        self.sec = int(sec)
        if self.sec == 0:
            raise ValueError('sec must be >= 1')

    def raise_timeout(self, *args):
        raise TimeOut()

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.raise_timeout)
        signal.alarm(self.sec)

    def __exit__(self, *args):
        signal.alarm(0)


def workfor(t):
    start = time.time()
    while time.time() - start <= t:
        0. + 0.


if __name__ == '__main__':
    with TimeLimiter(3):
        workfor(10.)
