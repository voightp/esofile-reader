from pathlib import Path

import pytest

from esofile_reader import EsoFile, ResultsFile

ROOT_PATH = Path(__file__).parent.absolute()


@pytest.fixture(scope="session")
def eplusout1():
    return EsoFile(Path(ROOT_PATH, "eso_files/eplusout1.eso"))


@pytest.fixture(scope="session")
def eplusout2():
    return EsoFile(Path(ROOT_PATH, "eso_files/eplusout2.eso"))


@pytest.fixture(scope="session")
def eplusout_all_intervals():
    return EsoFile(Path(ROOT_PATH, "eso_files/eplusout_all_intervals.eso"))


@pytest.fixture(scope="session")
def eplusout1_peaks():
    return EsoFile(Path(ROOT_PATH, "eso_files/eplusout1.eso"), ignore_peaks=False)


@pytest.fixture(scope="session")
def eplusout2_peaks():
    return EsoFile(Path(ROOT_PATH, "eso_files/eplusout2.eso"), ignore_peaks=False)


@pytest.fixture(scope="session")
def eplusout_all_intervals_peaks():
    return EsoFile(Path(ROOT_PATH, "eso_files/eplusout_all_intervals.eso"), ignore_peaks=False)


@pytest.fixture(scope="session")
def excel_file():
    return ResultsFile.from_excel(Path(ROOT_PATH, "eso_files/test_excel_results.xlsx"))
