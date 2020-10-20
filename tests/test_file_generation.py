from unittest.mock import patch

from esofile_reader.processing.progress_logger import GenericProgressLogger
from tests.session_fixtures import *


def test_from_excel():
    rf = ResultsFile.from_excel(Path(ROOT_PATH, "eso_files", "test_excel_results.xlsx"))
    assert rf.file_type == ResultsFile.XLSX


def test_from_csv():
    rf = ResultsFile.from_csv(Path(ROOT_PATH, "eso_files", "test_excel_results.csv"))
    assert rf.file_type == ResultsFile.CSV


def test_from_eso_file():
    rf = ResultsFile.from_eso_file(Path(ROOT_PATH, "eso_files", "eplusout1.eso"))
    assert rf.file_type == ResultsFile.ESO


@pytest.mark.parametrize(
    "path, mock",
    [
        (Path(ROOT_PATH, "eso_files", "test_excel_results.xlsx"), "from_excel"),
        (Path(ROOT_PATH, "eso_files", "test_excel_results.csv"), "from_csv"),
        (Path(ROOT_PATH, "eso_files", "eplusout1.eso"), "from_eso_file"),
    ],
)
def test_from_path(path, mock):
    with patch(f"esofile_reader.results_file.ResultsFile.{mock}") as mocked_function:
        logger = GenericProgressLogger("logger")
        _ = ResultsFile.from_path(path, progress_logger=logger)
        mocked_function.assert_called_with(path, progress_logger=logger)


def test_from_totals(eplusout1):
    rf = ResultsFile.from_totals(eplusout1)
    assert rf.file_type == ResultsFile.TOTALS


def test_from_diff(eplusout1, eplusout2):
    rf = ResultsFile.from_diff(eplusout1, eplusout2)
    assert rf.file_type == ResultsFile.DIFF
