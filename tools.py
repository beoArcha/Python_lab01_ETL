from functools import wraps
from time import perf_counter
from enum import Enum


def elapsed(func):
    """Decorator for counting elapsed time"""
    @wraps(func)
    def elapse(*args, **kwargs):
        start = perf_counter()
        ret = func(*args, **kwargs)
        elapsed_time = perf_counter() - start
        if str(Elapsed.print_elapsed) in kwargs and kwargs[str(Elapsed.print_elapsed)]:
            print('Time elapsed for function {} is {}'.format(func.__name__, elapsed_time))
            return ret
        elif str(Elapsed.return_elapsed) in kwargs and kwargs[str(Elapsed.return_elapsed)]:
            return ret, elapsed_time
    return elapse


class Elapsed(Enum):
    """Enum of kwargs for elapsed decorator"""
    return_elapsed = 'return_elapsed'
    print_elapsed = 'print_elapsed'

    def __str__(self):
        return self.value
