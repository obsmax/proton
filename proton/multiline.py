import curses


class MultiLine(object):
    """prints stuff dynamically on several lines at the same time"""

    def __init__(self, maxlines):
        self.maxlines = maxlines
        self.win = None
        self.nmax, self.mmax = None, None
        self.last_communication = ""
        self.lines = ["" for _ in range(self.maxlines)]
        self.line0 = 0  # first line printed

    def __enter__(self):
        self.win = curses.initscr()
        curses.noecho()
        curses.cbreak()
        self.win.keypad(True)
        self.win.scrollok(True)

        self.reset_termsize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        curses.echo()
        curses.nocbreak()
        self.win.keypad(0)
        self.win.scrollok(False)
        curses.endwin()

    def reset_termsize(self):
        self.nmax, self.mmax = self.win.getmaxyx()
        self.nmax -= 1
        self.mmax -= 1

    def refresh(self):
        self.win.refresh()

    def communicate(self, message):
        self.last_communication = message
        self.reset_termsize()
        self.win.addstr(self.nmax, 0, message[:self.mmax])
        self.win.clrtoeol()
        self.refresh()

    def write(self, line_number, message, refresh=True):
        self.lines[line_number] = message

        while True:
            if line_number < self.line0:
                return
            elif line_number > self.line0 + self.nmax:
                return
            try:
                self.win.addstr(line_number - self.line0, 0, message[:self.mmax])
                self.win.clrtoeol()
                break
            except curses.error:
                self.reset_termsize()
                continue

        if refresh: self.refresh()

    def move(self, line0):
        self.line0 = line0
        for i in range(self.line0, self.line0 + self.nmax + 1):
            self.write(i, self.lines[i], refresh=False)
        self.communicate(self.last_communication)
        self.refresh()

    def pause(self):
        self.communicate("pause")
        return self.win.getstr()


if __name__ == '__main__':
    import time
    with MultiLine(maxlines=3) as ml:
        ml.write(0, "write to line 0")
        time.sleep(0.3)

        ml.write(1, "write to line 1")
        time.sleep(0.3)

        ml.write(2, "write to line 2")
        time.sleep(0.3)

        ml.write(0, "correct line 0")
        time.sleep(0.3)

        ml.write(1, "correct line 1")
        time.sleep(0.3)

        ml.write(2, "correct line 2")
        ml.communicate("it works !!!")
        time.sleep(1.0)

        ml.communicate("bye")
        time.sleep(1.0)
