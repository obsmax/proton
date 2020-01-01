class ArgumentError(Exception):
    pass


class EndingSignal(Exception):
    pass


class GeneratorError(Exception):
    pass


class WorkerError(Exception):
    def __init__(self, message, errtype, errvalue, errtrace):
        super(WorkerError, self).__init__()
        self.args = (message, )
        self.message = message
        self.errtype = errtype
        self.errvalue = errvalue
        self.errtrace = errtrace
