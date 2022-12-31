import logging
from threading import Lock

logging.basicConfig(
    format="%(process)s %(thread)s: %(levelname)s %(asctime)s %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S",
    # comment out to get it to log to the screen
    # filename="example.log",
    # overwrites - comment out to get append
    filemode="w",
    level=logging.DEBUG,
)

logger = logging.getLogger(__name__)

logger_lock = Lock()


def debug_print(*args):
    # lock the lock
    logger_lock.acquire()
    # print out message used for debugging
    logger.debug(" ".join(len(args) * ["%s"]), *args)
    # unlock once message is printed
    logger_lock.release()