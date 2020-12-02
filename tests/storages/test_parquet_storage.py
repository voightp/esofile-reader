import contextlib
import shutil

from esofile_reader.pqt.parquet_storage import ParquetStorage
from esofile_reader.pqt.parquet_tables import ParquetFrame
from tests.session_fixtures import *


@pytest.fixture(scope="module")
def storage(eplusout1, eplusout2, excel_file):
    ParquetFrame.MAX_SIZE = 10
    storage = ParquetStorage()
    storage.store_file(eplusout1)
    storage.store_file(eplusout2)
    storage.store_file(excel_file)
    try:
        yield storage
    finally:
        files = [Path("pqs" + ParquetStorage.EXT), Path("file-0")]
        ParquetFrame.MAX_SIZE = 100
        for f in files:
            with contextlib.suppress(FileNotFoundError):
                f.unlink()
        shutil.rmtree(storage.workdir)


@pytest.fixture(scope="module")
def loaded_storage(storage):
    path = storage.save_as("", "pqs")
    loaded_storage = ParquetStorage.load_storage(path)
    try:
        yield loaded_storage
    finally:
        path.unlink()
        shutil.rmtree(loaded_storage.workdir)


@pytest.fixture(
    params=[pytest.lazy_fixture("storage"), pytest.lazy_fixture("loaded_storage")],
    scope="module",
)
def parametrized_storage(request):
    return request.param


def test_init_storage(storage):
    assert Path(storage.workdir).exists()
    assert storage.path is None


@pytest.mark.parametrize(
    "id_,test_file",
    [
        (0, pytest.lazy_fixture("eplusout1")),
        (1, pytest.lazy_fixture("eplusout2")),
        (2, pytest.lazy_fixture("excel_file")),
    ],
)
def test_stored_file_attributes(parametrized_storage, id_, test_file):
    assert parametrized_storage.files[id_].file_name == test_file.file_name
    assert parametrized_storage.files[id_].file_path == test_file.file_path
    assert parametrized_storage.files[id_].file_created == test_file.file_created
    assert parametrized_storage.files[id_].file_type == test_file.file_type


@pytest.mark.parametrize(
    "id_,test_file",
    [
        (0, pytest.lazy_fixture("eplusout1")),
        (1, pytest.lazy_fixture("eplusout2")),
        (2, pytest.lazy_fixture("excel_file")),
    ],
)
def test_stored_file_tables(parametrized_storage, id_, test_file):
    assert parametrized_storage.files[id_].tables == test_file.tables


def test_get_all_file_names(parametrized_storage):
    assert parametrized_storage.get_all_file_names() == [
        "eplusout1",
        "eplusout2",
        "test_excel_results",
    ]


def test_delete_file(eplusout1):
    storage = ParquetStorage()
    id_ = storage.store_file(eplusout1)
    pqf = storage.files[id_]
    path = pqf.workdir
    assert path.exists()

    storage.delete_file(0)
    assert not path.exists()


def test_save_as_storage_exists(loaded_storage):
    assert loaded_storage.path.exists()


def test_load_storage_with_invalid_extension():
    with pytest.raises(IOError):
        ParquetStorage.load_storage("test.foo")


def test_save_path_not_set(storage):
    with pytest.raises(FileNotFoundError):
        pqs = ParquetStorage()
        pqs.save()


def test_save(loaded_storage):
    assert loaded_storage.save() == Path("pqs.cfs")


def test_merge_storages(storage, loaded_storage):
    storage.merge_with(loaded_storage.path)
    assert storage.get_all_file_names() == [
        "eplusout1",
        "eplusout2",
        "test_excel_results",
        "eplusout1 (1)",
        "eplusout2 (1)",
        "test_excel_results (1)",
    ]


def test_storage_in_path(tmpdir):
    path = Path(tmpdir, "foo")
    pqs = ParquetStorage(path)
    assert pqs.workdir == path
    assert path.exists()
