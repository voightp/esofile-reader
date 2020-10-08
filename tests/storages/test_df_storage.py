import pandas as pd

from esofile_reader.storages.df_storage import DFStorage
from tests.session_fixtures import *


@pytest.fixture(scope="module")
def storage():
    return DFStorage()


def test_store_file_first_file(storage, eplusout1):
    id_ = storage.store_file(eplusout1)
    assert id_ == 0


def test_stored_file_attributes(storage, eplusout1):
    assert storage.files[0].file_name == eplusout1.file_name
    assert storage.files[0].file_path == eplusout1.file_path
    assert storage.files[0].file_created == eplusout1.file_created
    assert storage.files[0].search_tree == eplusout1.search_tree
    assert storage.files[0].file_type == eplusout1.file_type
    for table in eplusout1.table_names:
        pd.testing.assert_frame_equal(
            eplusout1.tables[table], storage.files[0].tables[table],
        )


def test_store_file_second_file(storage, eplusout2):
    id_ = storage.store_file(eplusout2)
    assert id_ == 1


def test_get_all_file_names(storage):
    assert storage.get_all_file_names() == ["eplusout1", "eplusout2"]


def test_delete_first_file(storage):
    storage.delete_file(1)
    with pytest.raises(KeyError):
        _ = storage.files[1]
