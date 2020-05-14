from typing import List


def incremental_id_gen(start: int = 0, checklist: List[int] = None):
    """ Incremental id generator. """
    checklist = checklist if checklist else []
    i = start - 1
    while True:
        i += 1
        if i in checklist:
            continue
        yield i
