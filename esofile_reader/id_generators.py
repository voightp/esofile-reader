from random import randint
from typing import List


def random_id_gen(checklist: List[int], negative=True):
    """ ID generator. """
    while True:
        i = randint(1, 999999)
        i = -i if negative else i
        if i not in checklist:
            return -randint(1, 999999)


def incremental_id_gen(start: int = 0, checklist: List[int] = None):
    """ Incremental id generator. """
    checklist = checklist if checklist else []
    i = start - 1
    while True:
        i += 1
        if i in checklist:
            continue
        yield i
