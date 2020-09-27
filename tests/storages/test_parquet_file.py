import tempfile

from esofile_reader.storages.pqt_storage import ParquetFile
from esofile_reader.tables.pqt_tables import ParquetFrame
from tests.session_scope_fixtures import *


@pytest.fixture(scope="module")
def parquet_file(eplusout_all_intervals):
    ParquetFrame.CHUNK_SIZE = 10
    parquet_file = ParquetFile.from_results_file(0, eplusout_all_intervals)
    try:
        yield parquet_file
    finally:
        ParquetFrame.CHUNK_SIZE = 100
        parquet_file.clean_up()


@pytest.fixture(scope="module")
def saved_parquet_file_path(parquet_file):
    directory = ""
    name = "pqf"
    parquet_file.save_as(directory, name)
    path = Path(directory, f"{name}{ParquetFile.EXT}")
    try:
        yield path.absolute()
    finally:
        path.unlink()


@pytest.fixture(scope="module")
def loaded_parquet_file(saved_parquet_file_path):
    with tempfile.TemporaryDirectory(dir=Path(ROOT_PATH, "storages")) as temp_dir:
        parquet_file = ParquetFile.from_file_system(saved_parquet_file_path, temp_dir)
        try:
            yield parquet_file
        finally:
            parquet_file.clean_up()


@pytest.fixture(scope="module")
def loaded_parquet_file_from_buffer(parquet_file):
    buffer = parquet_file.save_into_buffer()
    with tempfile.TemporaryDirectory(dir=Path(ROOT_PATH, "storages")) as temp_dir:
        parquet_file = ParquetFile.from_buffer(buffer, temp_dir)
        try:
            yield parquet_file
        finally:
            parquet_file.clean_up()


@pytest.fixture(
    params=[
        pytest.lazy_fixture("parquet_file"),
        pytest.lazy_fixture("loaded_parquet_file"),
        pytest.lazy_fixture("loaded_parquet_file_from_buffer"),
    ]
)
def file(request):
    return request.param


def test_saved_parquet_file(saved_parquet_file_path):
    print(saved_parquet_file_path)
    assert saved_parquet_file_path.exists()


def test_parquet_file_attributes(file, eplusout_all_intervals):
    assert str(eplusout_all_intervals.file_path) == str(file.file_path)
    assert eplusout_all_intervals.file_name == file.file_name
    assert eplusout_all_intervals.file_created == file.file_created
    assert ".eso" == file.file_type
    assert str(eplusout_all_intervals.file_path) == str(file.file_path)


def test_parquet_file_tables(file, eplusout_all_intervals):
    assert file.tables == eplusout_all_intervals.tables


def test_load_invalid_parquet_file():
    with pytest.raises(IOError):
        ParquetFile.from_file_system("foo.bar")
