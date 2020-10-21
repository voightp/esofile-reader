from unittest.mock import patch

from esofile_reader.exceptions import FormatNotSupported
from esofile_reader.processing.progress_logger import GenericProgressLogger
from tests.session_fixtures import *


def test_from_excel():
    rf = GenericFile.from_excel(Path(TEST_FILES_PATH, "test_excel_results.xlsx"))
    assert rf.file_type == GenericFile.XLSX


def test_from_csv():
    rf = GenericFile.from_csv(Path(TEST_FILES_PATH, "test_excel_results.csv"))
    assert rf.file_type == GenericFile.CSV


def test_from_eso_file():
    rf = GenericFile.from_eso_file(Path(TEST_FILES_PATH, "eplusout1.eso"))
    assert rf.file_type == GenericFile.ESO


@pytest.mark.parametrize(
    "path, mock",
    [
        (Path(TEST_FILES_PATH, "test_excel_results.xlsx"), "from_excel"),
        (Path(TEST_FILES_PATH, "test_excel_results.csv"), "from_csv"),
        (Path(TEST_FILES_PATH, "eplusout1.eso"), "from_eso_file"),
    ],
)
def test_from_path(path, mock):
    with patch(f"esofile_reader.generic_file.GenericFile.{mock}") as mocked_function:
        logger = GenericProgressLogger("logger")
        _ = GenericFile.from_path(path, progress_logger=logger)
        mocked_function.assert_called_with(path, progress_logger=logger)


def test_unsupported_file_type():
    with pytest.raises(FormatNotSupported):
        _ = GenericFile.from_path("some.foo")


def test_from_totals(eplusout1):
    rf = GenericFile.from_totals(eplusout1)
    assert rf.file_type == GenericFile.TOTALS


def test_from_diff(eplusout1, eplusout2):
    rf = GenericFile.from_diff(eplusout1, eplusout2)
    assert rf.file_type == GenericFile.DIFF
