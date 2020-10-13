import pytest

from esofile_reader.id_generator import incremental_id_gen


@pytest.mark.parametrize(
    "start, checklist, expected_ids",
    [
        (0, [1, 2, 5, 7], [0, 3, 4, 6, 8, 9]),
        (20, None, [20, 21, 22]),
        (3, [1, 2, 5, 7], [3, 4, 6, 8, 9]),
    ],
)
def test_incremental_id_gen_checklist(start, checklist, expected_ids):
    id_gen = incremental_id_gen(checklist=checklist, start=start)
    for expected_id in expected_ids:
        assert expected_id == next(id_gen)
