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


