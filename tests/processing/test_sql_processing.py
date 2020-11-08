from tests.session_fixtures import *


@pytest.fixture(scope="module")
def sql_multienv_leap_files():
    return EsoFile.from_path(Path(TEST_FILES_PATH, "eplusout_leap_year.sql"))


@pytest.fixture(scope="module")
def sql_file():
    return EsoFile.from_path(Path(TEST_FILES_PATH, "multiple_years.sql"))


def test_environment_names(sql_file):
    print(sql_file)
