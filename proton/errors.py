class ArgumentError(Exception):
    pass


class EndingSignal(Exception):
    pass


class GeneratorError(Exception):
    pass


class WorkerError(Exception):
    def __init__(self, message, errtype, errvalue):
        super(WorkerError, self).__init__()
        self.args = (message, errtype, errvalue)

    def __str__(self):
        message, errtype, errvalue = self.args
        return "{}: {}, {}".format(errtype, errvalue, message)


class WaitingQueueFullError(Exception):
    pass


if __name__ == '__main__':
    w = WorkerError(
        message='toto',
        errtype=Exception,
        errvalue='dfsdf')

