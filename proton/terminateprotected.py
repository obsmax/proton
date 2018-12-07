from time import sleep
import logging
import signal
import sys


class TerminateProtected:
    """ Protect a piece of code from being killed by SIGINT or SIGTERM.
    It can still be killed by a force kill.

    Example:
        with TerminateProtected():
            run_func_1()
            run_func_2()

    Both functions will be executed even if a sigterm or sigkill has been received.
    """
    killed = False

    def _handler(self, signum, frame):
        if signum == signal.SIGINT:
            logging.error("Received SIGINT. Finishing this block, then exiting.")
        elif signum == signal.SIGTERM:
            logging.error("Received SIGTERM. Finishing this block, then exiting.")
        else:
            raise Exception('programming error')
        self.killed = True

    def __enter__(self):
        # save the original handlers replace them by self._handler
        self.backup_sigint_handler = signal.signal(signal.SIGINT, self._handler)
        self.backup_sigterm_handler = signal.signal(signal.SIGTERM, self._handler)

    def __exit__(self, type, value, traceback):
        if self.killed:
            # the interruption signal has occured inside the contextual manager
            # do not execute operations after leaving it
            sys.exit(0)

        # reset the original handlers
        signal.signal(signal.SIGINT, self.backup_sigint_handler)
        signal.signal(signal.SIGTERM, self.backup_sigterm_handler)


if __name__ == '__main__':
    print("Try pressing ctrl+c while the sleep is running!")
    from time import sleep

    with TerminateProtected():
        sleep(10)
        print("Finished anyway!")

    print("This only prints if there was no sigint or sigterm")
