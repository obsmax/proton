import logging
import signal
import sys


class SigIntException(Exception):
    pass


class SigTermException(Exception):
    pass


class SignalCatcher:
    """
    re-route SIGINT and SIGTERM toward user defined exceptions
    """
    def __init__(self, sigint=SigIntException, sigterm=SigTermException):
        """

        :param sigint:
        :param sigterm:
        """
        if not isinstance(sigint, type):
            raise TypeError('sigint must a class derived from Exception')

        if not isinstance(sigterm, type):
            raise TypeError('sigterm must be a class derived from Exception')

        self.sigint = sigint
        self.sigterm = sigterm

    def _sigint_handler(self, signum, frame):
        raise self.sigint()

    def _sigterm_handler(self, signum, frame):
        raise self.sigterm()

    def __enter__(self):
        # save the original handlers replace them by self._sigxxx_handler
        self.backup_sigint_handler = \
            signal.signal(signal.SIGINT, self._sigint_handler)
        self.backup_sigterm_handler = \
            signal.signal(signal.SIGTERM, self._sigterm_handler)

    def __exit__(self, type, value, traceback):
        # reset the original handlers
        signal.signal(signal.SIGINT, self.backup_sigint_handler)
        signal.signal(signal.SIGTERM, self.backup_sigterm_handler)


if __name__ == '__main__':
    print("Try pressing ctrl+c while the sleep is running!")
    from time import sleep
    import os

    with SignalCatcher():
        try:
            print('press ctrl+c to test sigint, or wait 3s to test  sigterm')
            sleep(3)
            os.kill(os.getpid(), signal.SIGTERM)
        except SigIntException as e:
            e.args = ('got sigint signal and redirected it as a SigIntException', )
            raise e
        except SigTermException as e:
            e.args = ('got sigterm signal and redirected it as a SigTermException', )
            raise e
