from tests.session_fixtures import *


@pytest.fixture(scope="module")
def sql_multienv_leap_files():
    return EsoFile.from_multienv_path(Path(EPLUS_TEST_FILES_PATH, "eplusout_leap_year.sql"))


def test_environment_names(sql_multienv_leap_files):
    print(sql_multienv_leap_files)


def test_compare_with_eso(sql_multienv_leap_files):
    efs = EsoFile.from_multienv_path(Path(EPLUS_TEST_FILES_PATH, "eplusout_leap_year.eso"))
    for ef, sqf in zip(efs, sql_multienv_leap_files):
        for table in ef.table_names:
            import pandas as pd

            pd.Series(ef.tables[table].index).to_csv("ef.csv")
            pd.Series(sqf.tables[table].index).to_csv("sqf.csv")
            print(ef.tables[table].index.difference(sqf.tables[table].index))
            print(sqf.tables[table].index.difference(ef.tables[table].index))
            break
        break
