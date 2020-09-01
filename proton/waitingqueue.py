import time
import numpy as np
from proton.workers import WorkerOutput
from proton.errors import WaitingQueueFullError
from proton.messages import Message, MessageQueue


class MissingPacket(WorkerOutput):
    def __init__(self, jobid):
        super(MissingPacket, self).__init__(
            jobid=jobid,
            answer=None,
            generator_time=(0., 0.),
            processor_time=(0., 0.))


class WaitingQueue(object):
    """receive outputs from the output queue in a random order
        make them wait until the right ones shows up
        returns them in the correct order

        packet = WorkerOuptut received from MapAsync
                jobid is the number attributed to the job when it was generated,
                we use that number to re-order the packets
        """

    def __init__(self, generator, limit=1e9, verbose=False, message_queue=None):
        """
        generator must return jobid, something
        the jobid list must be exaustive from 0 to N
        """
        self.jobids = []
        self.packets = []
        self.currentjob = 0  # index of the currently expexted jobid
        self.generator = generator  # a generator of packets
        self.limit = limit  # max size
        self.verbose = verbose
        self.message_queue = message_queue

    def __len__(self):
        return len(self.packets)

    def append(self, jobid, packet):
        if len(self) >= self.limit:
            raise WaitingQueueFullError(
                f'the {self.__class__.__name__} was full')

        if not len(self.packets):
            # first packet received
            self.jobids.append(jobid)
            self.packets.append(packet)
            return None

        # place (jobid, packet) at the right place in self.jobids and self.packets
        i = np.searchsorted(self.jobids, jobid)
        self.jobids.insert(i, jobid)
        self.packets.insert(i, packet)

    def pop(self):
        """extract the packet in self.l[0], remove it from self.l"""
        self.jobids.pop(0)
        packet = self.packets.pop(0)
        return packet

    def __iter__(self):
        return self

    def __next__(self):
        if len(self) and self.jobids[0] == self.currentjob:
            # the first item in self.packets is the expected packet,
            # remove it from self.packets and return it
            # increment the expected packet number
            if self.verbose:
                message = Message(
                    sender_name="WaitingQueue",
                    time_value=time.time(),
                    message='picked from the waiting queue',
                    jobid=self.currentjob)
                self.message_queue.put(message)

            self.currentjob += 1
            return self.pop()

        while True:
            try:
                # get the next packet from the generator (i.e. the outputqueue)
                packet = next(self.generator)
                # the first item of the packet must be the jobid
                jobid = packet.jobid  # packet[0]

            except StopIteration:
                break

            if jobid > self.currentjob:
                # this packet came too soon, store it and do not return it yet
                if self.verbose:
                    message = Message(
                        sender_name="WaitingQueue",
                        time_value=time.time(),
                        message='came too soon, placed in the waiting queue',
                        jobid=jobid)
                    self.message_queue.put(message)

                self.append(jobid, packet)

            elif jobid == self.currentjob:
                # got the right packet, move to the net one
                if self.verbose:
                    message = Message(
                        sender_name="WaitingQueue",
                        time_value=time.time(),
                        message='returned right away',
                        jobid=jobid)
                    self.message_queue.put(message)

                self.currentjob += 1
                return packet

        if len(self):
            # may append if some processes have failed
            if self.verbose:
                message = Message(
                    sender_name="WaitingQueue",
                    time_value=time.time(),
                    message='never received',
                    jobid=self.currentjob)
                self.message_queue.put(message)
            self.currentjob += 1

            # replace the missing packet by a dedicated workeroutput
            return MissingPacket(self.currentjob - 1)

        raise StopIteration


if __name__ == '__main__':
    def generator():
        if 0:
            jobids = [0, 1, 2, 3, 4, 5]
            packets = ['a', 'b', 'c', 'd', 'e', 'f']
        elif 0:
            jobids = [1, 2, 3, 4, 5, 0]
            packets = ['a', 'b', 'c', 'd', 'e', 'f']
        elif 0:
            jobids = [1, 0, 2, 3, 4, 5]
            packets = ['a', 'b', 'c', 'd', 'e', 'f']
        elif 1:
            jobids = [1, 6, 2, 3, 4, 5]
            packets = ['a', 'b', 'c', 'd', 'e', 'f']

        for jobid, packet in zip(jobids, packets):
            yield WorkerOutput(jobid=jobid, answer=packet)

    mq = MessageQueue()
    wq = WaitingQueue(generator(), verbose=True, message_queue=mq)

    for _ in wq:
        print(_)

    # print the details of what has happen
    while not mq.empty():
        print(mq.get())
