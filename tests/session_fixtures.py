from pathlib import Path

import pytest

from esofile_reader import EsoFile, GenericFile

ROOT_PATH = Path(__file__).parent.absolute()
EPLUS_TEST_FILES_PATH = Path(ROOT_PATH, "test_files", "eplus")
TEST_FILES_PATH = Path(ROOT_PATH, "test_files")


@pytest.fixture(scope="session")
def eplusout1():
    return EsoFile.from_path(Path(EPLUS_TEST_FILES_PATH, "eplusout1.eso"), year=2002)


@pytest.fixture(scope="session")
def eplusout2():
    return EsoFile.from_path(Path(EPLUS_TEST_FILES_PATH, "eplusout2.eso"), year=2002)


@pytest.fixture(scope="session")
def eplusout_all_intervals():
    return EsoFile.from_path(
        Path(EPLUS_TEST_FILES_PATH, "eplusout_all_intervals.eso"), year=2002
    )


@pytest.fixture(scope="session")
def eplusout1_peaks():
    return EsoFile.from_path(
        Path(EPLUS_TEST_FILES_PATH, "eplusout1.eso"), ignore_peaks=False, year=2002
    )


@pytest.fixture(scope="session")
def eplusout2_peaks():
    return EsoFile.from_path(
        Path(EPLUS_TEST_FILES_PATH, "eplusout2.eso"), ignore_peaks=False, year=2002
    )


@pytest.fixture(scope="session")
def eplusout_all_intervals_peaks():
    return EsoFile.from_path(
        Path(EPLUS_TEST_FILES_PATH, "eplusout_all_intervals.eso"), ignore_peaks=False, year=2002
    )


@pytest.fixture(scope="session")
def tiny_eplusout():
    return EsoFile.from_path(Path(EPLUS_TEST_FILES_PATH, "tiny_eplusout.eso"))


@pytest.fixture(scope="session")
def excel_file():
    return GenericFile.from_excel(Path(TEST_FILES_PATH, "test_excel_results.xlsx"))


@pytest.fixture(scope="session")
def leap_year_file():
    return EsoFile.from_path(Path(EPLUS_TEST_FILES_PATH, "leap_year.eso"), year=None)


@pytest.fixture(scope="session")
def multienv_file():
    return EsoFile.from_multienv_path(
        Path(EPLUS_TEST_FILES_PATH, "multiple_environments.eso"), year=None
    )


@pytest.fixture(scope="session")
def multiyear_file():
    return EsoFile.from_path(Path(EPLUS_TEST_FILES_PATH, "multiple_years.eso"), year=None)
