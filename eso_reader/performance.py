import time


def perf(func):
    def inner_func(*args, **kwargs):
        s = time.perf_counter()
        res = func(*args, **kwargs)
        e = time.perf_counter()
        print(f"Func: {func.__name__} - time: '{e - s}'s")
        return res

    return inner_func
