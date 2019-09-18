import signal


# === exceptions to raise after receiving unix signals
class SignalException(Exception): pass
class SigIntException(SignalException): pass
class SigTermException(SignalException): pass
class SigUsr1Exception(SignalException): pass
class SigUsr2Exception(SignalException): pass


# === exceptions to raise after receiving unix signals
class SignalHandler(object):
    def __init__(self, exception):
        """
        :param exception: a class inherited from SignalException
        :type exception: class
        """
        # if not isinstance(exception, type):
        #     raise ValueError('exception must be a class inherited from SignalException')
        # if not isinstance(exception(), SigIntException):
        #     raise ValueError('exception must be a class inherited from SignalException')
        self.exception = exception

    def __call__(self, signum, frame):
        raise self.exception(signum, frame)


class SignalCatcher(object):
    """
    re-route SIGINT and SIGTERM toward user defined exceptions
    """
    def __init__(self):
        self.backup_sigint_handler = None
        self.backup_sigterm_handler = None
        self.backup_sigusr1_handler = None
        self.backup_sigusr2_handler = None

    def __enter__(self):
        # save the original handlers replace them by self._sigxxx_handler
        self.backup_sigint_handler = \
            signal.signal(signal.SIGINT, SignalHandler(SigIntException))

        self.backup_sigterm_handler = \
            signal.signal(signal.SIGTERM, SignalHandler(SigTermException))

        self.backup_sigusr1_handler = \
            signal.signal(signal.SIGUSR1, SignalHandler(SigUsr1Exception))

        self.backup_sigusr2_handler = \
            signal.signal(signal.SIGUSR2, SignalHandler(SigUsr2Exception))

    def __exit__(self, type, value, traceback):
        # reset the original handlers
        signal.signal(signal.SIGINT, self.backup_sigint_handler)
        signal.signal(signal.SIGTERM, self.backup_sigterm_handler)
        signal.signal(signal.SIGUSR1, self.backup_sigusr1_handler)
        signal.signal(signal.SIGUSR2, self.backup_sigusr2_handler)


if __name__ == '__main__':
    print("Try pressing ctrl+c while the sleep is running!")
    from time import sleep
    import os

    with SignalCatcher():
        try:
            print('press ctrl+c to test sigint, or wait 3s to test  sigterm')
            sleep(3)
            # send a SIGTERM signal to this process, see you in the SigTermException section !
            os.kill(os.getpid(), signal.SIGTERM)
            # os.kill(os.getpid(), signal.SIGUSR1)
            # os.kill(os.getpid(), signal.SIGUSR2)
        except SigIntException as e:
            e.args = ('got sigint signal and redirected it to SigIntException', )
            raise e
        except SigTermException as e:
            e.args = ('got sigterm signal and redirected it to SigTermException', )
            raise e
