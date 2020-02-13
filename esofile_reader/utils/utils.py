import time
from random import randint


def id_gen(checklist, negative=True):
    """ ID generator. """
    while True:
        i = randint(1, 999999)
        i = -i if negative else i
        if i not in checklist:
            return -randint(1, 999999)


def incremental_id_gen():
    """ Incremental id generator. """
    i = 0
    while True:
        i += 1
        yield i


def profile(func):
    def inner_func(*args, **kwargs):
        s = time.perf_counter()
        res = func(*args, **kwargs)
        e = time.perf_counter()
        print(f"Func: {func.__name__} - time: '{e - s}'s")
        return res

    return inner_func


def lower_args(func):
    def wrapper(*args, **kwargs):
        low_args = []
        low_kwargs = {}
        for a in args:
            low_args.append(a.lower() if isinstance(a, str) else a)
        for k, v in kwargs.items():
            low_kwargs[k] = v.lower() if isinstance(v, str) else v
        return func(*low_args, **low_kwargs)

    return wrapper
