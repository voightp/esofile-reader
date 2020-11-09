from pathlib import Path

import pytest

from esofile_reader import EsoFile, GenericFile

ROOT_PATH = Path(__file__).parent.absolute()
TEST_FILES_PATH = Path(ROOT_PATH, "eso_files")


@pytest.fixture(scope="session")
def eplusout1():
    return EsoFile.from_path(Path(TEST_FILES_PATH, "eplusout1.eso"), year=2002)


@pytest.fixture(scope="session")
def eplusout2():
    return EsoFile.from_path(Path(TEST_FILES_PATH, "eplusout2.eso"), year=2002)


@pytest.fixture(scope="session")
def eplusout_all_intervals():
    return EsoFile.from_path(Path(TEST_FILES_PATH, "eplusout_all_intervals.eso"), year=2002)


@pytest.fixture(scope="session")
def eplusout1_peaks():
    return EsoFile.from_path(
        Path(TEST_FILES_PATH, "eplusout1.eso"), ignore_peaks=False, year=2002
    )


@pytest.fixture(scope="session")
def eplusout2_peaks():
    return EsoFile.from_path(
        Path(TEST_FILES_PATH, "eplusout2.eso"), ignore_peaks=False, year=2002
    )


@pytest.fixture(scope="session")
def eplusout_all_intervals_peaks():
    return EsoFile.from_path(
        Path(TEST_FILES_PATH, "eplusout_all_intervals.eso"), ignore_peaks=False, year=2002
    )


@pytest.fixture(scope="session")
def tiny_eplusout():
    return EsoFile.from_path(Path(TEST_FILES_PATH, "tiny_eplusout.eso"))


@pytest.fixture(scope="session")
def excel_file():
    return GenericFile.from_excel(Path(TEST_FILES_PATH, "test_excel_results.xlsx"))


@pytest.fixture(scope="session")
def multienv_leap_files():
    return EsoFile.from_multienv_path(
        Path(TEST_FILES_PATH, "eplusout_leap_year.eso"), year=None
    )
