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


def slice_dict(dct, keys):
    """ Slice dictionary using given keys. """
    return {key: dct[key] for key in keys if key in dct}


def profile(func):
    def inner_func(*args, **kwargs):
        s = time.perf_counter()
        res = func(*args, **kwargs)
        e = time.perf_counter()
        print(f"Func: {func.__name__} - time: '{e - s}'s")
        return res

    return inner_func
