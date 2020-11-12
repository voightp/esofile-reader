from tests.session_fixtures import *


@pytest.fixture(scope="module")
def sql_multienv_files():
    return EsoFile.from_multienv_path(Path(EPLUS_TEST_FILES_PATH, "multiple_environments.sql"))


@pytest.fixture(scope="module")
def sql_multiyear_file():
    return EsoFile.from_path(Path(EPLUS_TEST_FILES_PATH, "multiple_years.sql"))


@pytest.fixture(scope="module")
def sql_leap_year():
    return EsoFile.from_path(Path(EPLUS_TEST_FILES_PATH, "leap_year.sql"))


def test_compare_multienv_with_eso(sql_multienv_files, multienv_file):
    for ef, sqf in zip(multienv_file, sql_multienv_files):
        assert ef == sqf


def test_compare_multiyear_with_eso(sql_multiyear_file, multiyear_file):
    assert sql_multiyear_file == multiyear_file


def test_compare_leap_with_eso(sql_leap_year, leap_year_file):
    assert sql_leap_year == leap_year_file
