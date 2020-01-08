import inspect
from proton.errors import ArgumentError


class ProcessingTarget(object):
    def __init__(self, function_or_instance):
        """
        :param function_or_instance:
            a function or an instance of a callable object
            if it is a function, the signature must look like
            def fun(...):
                ...
            or
            def fun(worker, ...):
                ...


            for an instance the __call__ method must look like
            def __call__(self, ...):
                ...
            or
            def __call__(self, worker, ...):
                ...

        """
        self.function_or_instance = function_or_instance

        args = list(inspect.signature(function_or_instance).parameters.keys())
        self.passworker = False
        if len(args):
            # at least one argument
            if args[0] == "self":
                # this is a method
                if len(args) > 1:
                    # f(self, ...)
                    if args[1] == "worker":
                        # f(self, worker, ...)
                        self.passworker = True
                    elif "worker" in args:
                        # f(self, ..., worker, ...)
                        raise ArgumentError(args)

            elif "self" in args:
                # f(..., self, ...)
                raise ArgumentError(args)

            elif args[0] == "worker":
                # f(worker, ...)
                self.passworker = True

            elif "worker" in args:
                # f(..., worker, ...)
                raise ArgumentError(args)

    def __call__(self, *args, **kwargs):
        return self.function_or_instance(*args, **kwargs)


if __name__ == '__main__':

    def fun0(x):
        print(x)
        return x * 2

    def fun1(worker, x):
        print(worker, x)
        return x * 2

    class Fun2(object):
        def __init__(self, y):
            self.y = y

        def __call__(self, x):
            print(x, self.y)
            return x * 2

    class Fun3(object):
        def __init__(self, y):
            self.y = y

        def __call__(self, worker, x):
            print(worker, x, self.y)
            return x * 2

    t0 = ProcessingTarget(fun0)
    t1 = ProcessingTarget(fun1)
    t2 = ProcessingTarget(Fun2(222))
    t3 = ProcessingTarget(Fun3(333))

    t0(1, worker=1)
    t1(1, worker=1)
    t2(1, worker=1)
    t3(1, worker=1)