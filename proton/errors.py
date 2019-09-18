import sys
import traceback
import logging

def error_message():
    type, value, trace = sys.exc_info()
    msg = ""
    for level, tbb in enumerate(traceback.extract_tb(trace)):
        file, line, fun, idiocy = tbb
        tab  = "".join(['  ' for i in range(level + 1)])
        msg += '#%sIN %s, line %d, in %s : "%s"\n' % (tab, file, line, fun, idiocy)
        level += 1
    msg = '\n#ERROR : %s\n%s' % (value, msg[:-1])
    return msg


logger = logging.getLogger(__name__)

try:
    1 / 0
except ZeroDivisionError as error:
    logger.error(error)
    raise                 # just this!
    # raise AppError      # Don't do this, you'll lose the stack trace!
finally:
    print(logger)