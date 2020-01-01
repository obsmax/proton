from multiprocessing import get_context
from multiprocessing.queues import Queue


class BasicQueue(Queue):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('ctx', get_context())
        super().__init__(*args, **kwargs)


class InputQueue(BasicQueue):
    pass


class OutputQueue(BasicQueue):
    pass