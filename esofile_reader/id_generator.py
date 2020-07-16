from typing import List


def incremental_id_gen(start: int = 0, checklist: List[int] = None) -> int:
    """ Incremental id generator. """
    checklist = checklist if checklist else []
    i = start - 1
    while True:
        i += 1
        if i in checklist:
            continue
        yield i


def get_str_identifier(
        base_name: str,
        check_list: List[str],
        delimiter=" ",
        start_i: int = None,
        brackets: bool = True
) -> str:
    """ Create a unique name by adding index number to the base name. """

    def add_num():
        si = f"({i})" if brackets else f"{i}"
        return f"{base_name}{delimiter}{si}"

    i = start_i if start_i else 0
    new_name = base_name

    # add unique number if the file name is not unique
    while new_name in check_list:
        i += 1
        new_name = add_num()

    return new_name
