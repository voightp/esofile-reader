from unittest.mock import patch

from esofile_reader.exceptions import FormatNotSupported
from esofile_reader.processing.progress_logger import GenericLogger
from tests.session_fixtures import *


def test_from_excel():
    rf = GenericFile.from_excel(Path(TEST_FILES_PATH, "test_excel_results.xlsx"))
    assert rf.file_type == GenericFile.XLSX


def test_from_csv():
    rf = GenericFile.from_csv(Path(TEST_FILES_PATH, "test_excel_results.csv"))
    assert rf.file_type == GenericFile.CSV


def test_from_eso_file():
    rf = GenericFile.from_eplus_file(Path(TEST_FILES_PATH, "eplusout1.eso"))
    assert rf.file_type == GenericFile.ESO


def test_from_multienv_eso_file():
    pytest.fail()


def test_from_sql_file():
    pytest.fail()


def test_from_multienv_sql_file():
    pytest.fail()


@pytest.mark.parametrize(
    "path, mock",
    [
        (Path(TEST_FILES_PATH, "test_excel_results.xlsx"), "from_excel"),
        (Path(TEST_FILES_PATH, "test_excel_results.csv"), "from_csv"),
        (Path(TEST_FILES_PATH, "eplusout1.eso"), "from_eplus_file"),
    ],
)
def test_from_path(path, mock):
    with patch(f"esofile_reader.generic_file.GenericFile.{mock}") as mocked_function:
        logger = GenericLogger("logger")
        _ = GenericFile.from_path(path, logger=logger)
        mocked_function.assert_called_with(path, logger=logger)


def test_unsupported_file_type():
    with pytest.raises(FormatNotSupported):
        _ = GenericFile.from_path("some.foo")


def test_from_totals(eplusout1):
    rf = GenericFile.from_totals(eplusout1)
    assert rf.file_type == GenericFile.TOTALS


def test_from_diff(eplusout1, eplusout2):
    rf = GenericFile.from_diff(eplusout1, eplusout2)
    assert rf.file_type == GenericFile.DIFF
