from esofile_reader.pqt.parquet_storage import ParquetStorage
from esofile_reader.pqt.parquet_tables import (
    DfParquetTables,
    VirtualParquetTables,
    ParquetTables,
)
from esofile_reader.processing.progress_logger import TimeLogger, INFO
from tests.session_fixtures import *


class AssertLogger(TimeLogger):
    def __init__(self, name: str):
        super().__init__(name, level=INFO)

    def log_task_finished(self):
        super().log_task_finished()
        if self.max_progress != self.progress:
            pytest.fail(
                f"Maximum progress '{self.max_progress}' is "
                f"not equal to last progress '{self.progress}'."
            )


@pytest.fixture(scope="function")
def logger():
    return AssertLogger("TEST")


@pytest.fixture(scope="function", params=[DfParquetTables, VirtualParquetTables, ParquetTables])
def pqs(eplusout1, eplusout2, eplusout_all_intervals, request):
    pqs = ParquetStorage(tables_class=request.param)
    pqs.store_file(eplusout1)
    pqs.store_file(eplusout2)
    pqs.store_file(eplusout_all_intervals)
    pqs.path = Path(f"test_logger_{DfParquetTables.__name__}" + ParquetStorage.EXT)
    return pqs


@pytest.fixture(scope="function")
def saved_pqs(logger, pqs):
    pqs.save(logger)
    try:
        yield pqs
    finally:
        pqs.path.unlink()


def test_increment_file(logger):
    _ = GenericFile.from_eplus_file(Path(EPLUS_TEST_FILES_PATH, "eplusout1.eso"), logger)


def test_incerement_multienv_eso_file(logger):
    _ = GenericFile.from_eplus_multienv_file(
        Path(EPLUS_TEST_FILES_PATH, "multiple_environments.eso"), logger
    )


def test_increment_sql_file(logger):
    _ = GenericFile.from_eplus_multienv_file(
        Path(EPLUS_TEST_FILES_PATH, "multiple_years.sql"), logger
    )


def test_increment_multienv_sql_file(logger):
    _ = GenericFile.from_eplus_multienv_file(
        Path(EPLUS_TEST_FILES_PATH, "multiple_environments.sql"), logger
    )


def test_increment_xlsx_file(logger):
    _ = GenericFile.from_excel(Path(TEST_FILES_PATH, "test_excel_results.xlsx"), logger=logger)


def test_increment_csv_file(logger):
    _ = GenericFile.from_csv(Path(TEST_FILES_PATH, "test_excel_results.csv"), logger=logger)


def test_increment_totals_file(logger, eplusout1):
    _ = GenericFile.from_totals(eplusout1, logger)


def test_increment_diff_file(logger, eplusout1, eplusout2):
    _ = GenericFile.from_diff(eplusout1, eplusout2, logger)


def test_increment_parquet_storage_save_file(logger, pqs, eplusout1):
    pqs.store_file(eplusout1, logger)


def test_increment_parquet_storage_load_storage(logger, saved_pqs):
    _ = ParquetStorage.load_storage(
        saved_pqs.path, tables_class=saved_pqs._tables_class, logger=logger
    )


def test_increment_parquet_storage_merge_storage(logger, pqs, saved_pqs):
    pqs.merge_with(saved_pqs.path)


@pytest.mark.parametrize(
    "tables_class, new_class",
    [
        (DfParquetTables, VirtualParquetTables),
        (DfParquetTables, ParquetTables),
        (ParquetTables, DfParquetTables),
        (ParquetTables, VirtualParquetTables),
        (VirtualParquetTables, ParquetTables),
        (VirtualParquetTables, DfParquetTables),
    ],
)
def test_increment_parquet_storage_change_tables(logger, tables_class, new_class, eplusout1):
    pqs = ParquetStorage(tables_class=tables_class)
    pqs.store_file(eplusout1)
    pqs.change_tables_class(new_class, logger)
