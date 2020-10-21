from copy import copy

import pandas as pd
from pandas.testing import assert_frame_equal

from esofile_reader.constants import *
from esofile_reader.exceptions import NoResults
from esofile_reader.processing.diff import (
    subtract_tables,
    get_shared_special_table,
    process_diff,
)
from tests.session_fixtures import *


@pytest.fixture(scope="module")
def same_file_diff(eplusout1):
    return process_diff(eplusout1, eplusout1)


@pytest.mark.parametrize(
    "df1,df2,expected_df",
    [
        (
            pd.DataFrame(
                {"a": [1, 2, 3], "b": [3, 2, 1], "c": [4, 5, 6]},
                index=pd.date_range("2020-01-01", freq="d", periods=3, name="date"),
            ),
            pd.DataFrame(
                {"a": [1, 2], "b": [3, 2], "d": [4, 5]},
                index=pd.date_range("2020-01-02", freq="d", periods=2, name="date"),
            ),
            pd.DataFrame(
                {"a": [1, 1], "b": [-1, -1]},
                index=pd.date_range("2020-01-02", freq="d", periods=2, name="date"),
            ),
        ),
        (
            pd.DataFrame(
                [[1, 2, 3], [3, 2, 1], [4, 5, 6]],
                index=pd.date_range("2020-01-01", freq="d", periods=3, name="date"),
                columns=pd.MultiIndex.from_tuples(
                    [("a", "b", "c"), ("a", "b", "d"), ("c", "e", "f")]
                ),
            ),
            pd.DataFrame(
                [[1, 2, 3], [3, 2, 1]],
                index=pd.date_range("2020-01-02", freq="d", periods=2, name="date"),
                columns=pd.MultiIndex.from_tuples(
                    [("a", "b", "c"), ("f", "g", "h"), ("c", "e", "f")]
                ),
            ),
            pd.DataFrame(
                [[2, -2], [1, 5]],
                index=pd.date_range("2020-01-02", freq="d", periods=2, name="date"),
                columns=pd.MultiIndex.from_tuples([("a", "b", "c"), ("c", "e", "f")]),
            ),
        ),
    ],
)
def test_subtract_tables(df1, df2, expected_df):
    df = subtract_tables(df1, df2)
    assert_frame_equal(df, expected_df, check_freq=False)


@pytest.mark.parametrize(
    "df1,df2",
    [
        (
            pd.DataFrame(
                {"a": [1, 2, 3], "b": [3, 2, 1], "c": [4, 5, 6]},
                index=pd.date_range("2020-01-01", freq="d", periods=3, name="date"),
            ),
            pd.DataFrame({"a": [1, 2, 3], "b": [3, 2, 1], "c": [4, 5, 6]},),
        ),
        (
            pd.DataFrame(
                {"a": [1, 2, 3], "b": [3, 2, 1], "c": [4, 5, 6]},
                index=pd.date_range("2020-01-01", freq="d", periods=3, name="date"),
            ),
            pd.DataFrame(
                {"d": [1, 2, 3], "e": [3, 2, 1], "f": [4, 5, 6]},
                index=pd.date_range("2020-01-01", freq="d", periods=3, name="date"),
            ),
        ),
    ],
)
def test_subtract_tables_different_index(df1, df2):
    df = subtract_tables(df1, df2)
    assert df.empty


@pytest.mark.parametrize(
    "df1,df2,expected_df",
    [
        (
            pd.DataFrame(
                {"a": ["a", "b", "c"], "b": [10, 20, 30], "c": [4, 5, 6]},
                index=pd.date_range("2020-01-01", freq="d", periods=3, name="date"),
            ),
            pd.DataFrame(
                {"a": ["b", "c"], "b": [0, 0], "d": [4, 5]},
                index=pd.date_range("2020-01-02", freq="d", periods=2, name="date"),
            ),
            pd.DataFrame(
                {"a": ["b", "c"]},
                index=pd.date_range("2020-01-02", freq="d", periods=2, name="date"),
            ),
        ),
        (
            pd.DataFrame(
                [["a", 10, 4], ["b", 20, 5], ["c", 30, 6]],
                index=pd.date_range("2020-01-01", freq="d", periods=3, name="date"),
                columns=pd.MultiIndex.from_tuples(
                    [("a", "b", "c"), ("a", "b", "d"), ("c", "e", "f")]
                ),
            ),
            pd.DataFrame(
                [["b", 10, 4], ["c", 20, 5]],
                index=pd.date_range("2020-01-02", freq="d", periods=2, name="date"),
                columns=pd.MultiIndex.from_tuples(
                    [("a", "b", "c"), ("f", "g", "h"), ("c", "e", "f")]
                ),
            ),
            pd.DataFrame(
                [["b"], ["c"]],
                index=pd.date_range("2020-01-02", freq="d", periods=2, name="date"),
                columns=pd.MultiIndex.from_tuples([("a", "b", "c")]),
            ),
        ),
    ],
)
def test_get_shared_special_table(df1, df2, expected_df):
    df = get_shared_special_table(df1, df2)
    assert_frame_equal(df, expected_df, check_freq=False)


@pytest.mark.parametrize(
    "df1,df2",
    [
        (
            pd.DataFrame(
                {"a": [1, 2, 3], "b": [3, 2, 1], "c": [4, 5, 6]},
                index=pd.date_range("2020-01-01", freq="d", periods=3, name="date"),
            ),
            pd.DataFrame(
                {"d": [1, 2, 3], "e": [3, 2, 1], "f": [4, 5, 6]},
                index=pd.date_range("2020-01-01", freq="d", periods=3, name="date"),
            ),
        ),
        (
            pd.DataFrame(
                {"a": [1, 2, 3], "b": [3, 2, 1], "c": [4, 5, 6]},
                index=pd.date_range("2020-01-01", freq="d", periods=3, name="date"),
            ),
            pd.DataFrame(
                {"a": [1, 2, 3], "b": [3, 2, 1], "c": [4, 5, 6]},
                index=pd.date_range("2020-01-2", freq="d", periods=3, name="date"),
            ),
        ),
    ],
)
def test_get_shared_special_table_empty(df1, df2):
    df = get_shared_special_table(df1, df2)
    assert df.empty


def test_identical_files_diff_tables(same_file_diff):
    for table in same_file_diff.get_table_names():
        df = same_file_diff.get_numeric_table(table)
        bool_df = df == 0
        # check if all calculated values are 0
        assert bool_df.all().all()


def test_identical_files_diff_special_tables(same_file_diff, eplusout1):
    for table in same_file_diff.get_table_names():
        # check if n days and day of week columns are copied
        if table in [TS, H, D]:
            df = same_file_diff.get_special_table(table)
            test_df = eplusout1.tables.get_special_table(table)
            assert_frame_equal(df, test_df, check_freq=False)


def test_process_diff_similar_files(eplusout1, eplusout2):
    diff = GenericFile.from_diff(eplusout1, eplusout2)
    shapes = [(4392, 59), (183, 59), (6, 59)]
    for table, test_shape in zip(diff.table_names, shapes):
        assert diff.tables[table].shape == test_shape


def test_process_diff_different_datetime(eplusout1, eplusout_all_intervals):
    diff = GenericFile.from_diff(eplusout1, eplusout_all_intervals)
    shapes = [(4392, 3), (183, 3), (6, 3)]
    for interval, test_shape in zip(diff.table_names, shapes):
        assert diff.tables[interval].shape == test_shape


def test_no_shared_intervals(eplusout1):
    ef1 = copy(eplusout1)
    ef2 = copy(eplusout1)

    del ef1.tables["hourly"]
    del ef1.tables["daily"]

    del ef2.tables["monthly"]
    del ef2.tables["runperiod"]

    with pytest.raises(NoResults):
        _ = GenericFile.from_diff(ef1, ef2)


def test_simple_and_standard_table_same_name():
    rf1 = GenericFile.from_excel(
        Path(TEST_FILES_PATH, "test_excel_edge_cases.xlsx"), sheet_names=["test"]
    )
    rf2 = GenericFile.from_excel(
        Path(TEST_FILES_PATH, "test_excel_edge_cases.xlsx"), sheet_names=["test-simple"]
    )
    with pytest.raises(NoResults):
        GenericFile.from_diff(rf1, rf2)
