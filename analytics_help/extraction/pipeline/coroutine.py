
from functools import wraps
import logging

logger = logging.getLogger()


def coroutine(func):
    @wraps(func)
    def start_coroutine(*args, **kwargs):
        cr = func(*args, **kwargs)
        cr.next()
        return cr
    return start_coroutine
