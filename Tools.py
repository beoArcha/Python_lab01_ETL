from functools import wraps
import time


def elapsed(func):
    """Decorator for counting elapsed time"""
    @wraps(func)
    def elapse(*args, **kwargs):
        start = time.perf_counter()
        ret = func(*args, **kwargs)
        elapsed_time = time.perf_counter() - start
        if 'print_elapsed' in kwargs and kwargs['print_elapsed']:
            print('Time elapsed for function {} is {}'.format(func.__name__, elapsed_time))
            return ret
        elif 'return_elapsed' in kwargs and kwargs['return_elapsed']:
            return ret, elapsed_time

    return elapse
