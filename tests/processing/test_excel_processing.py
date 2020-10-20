from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_index_equal

from esofile_reader.exceptions import InsuficientHeaderInfo, NoResults
from esofile_reader.processing.excel import is_data_row, parse_header
from esofile_reader.results_file import ResultsFile
from tests.session_fixtures import TEST_FILES_PATH

RESULTS_PATH = Path(TEST_FILES_PATH, "test_excel_results.xlsx")
EDGE_CASE_PATH = Path(TEST_FILES_PATH, "test_excel_edge_cases.xlsx")
CSV_PATH = Path(TEST_FILES_PATH, "test_excel_results.csv")


@pytest.fixture(scope="module")
def excel_file():
    return ResultsFile.from_excel(RESULTS_PATH)


def test_is_data_row_mixed():
    sr = pd.Series(["Saturday", pd.NaT, 0, 0, 1.23456])
    assert is_data_row(sr)


def test_is_data_row_all_nat():
    sr = pd.Series([pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT])
    assert is_data_row(sr)


def test_is_data_row_all_string():
    sr = pd.Series(["Saturday"] * 5)
    assert not is_data_row(sr)


def test_is_data_row_all_float():
    sr = pd.Series(np.random.rand(5))
    assert is_data_row(sr)


def test_is_data_row_all_int():
    sr = pd.Series(np.random.randint(0, high=100, size=5))
    assert is_data_row(sr)


def test_is_data_row_more_strings():
    sr = pd.Series(["a", "b", pd.NaT, pd.NaT, 0.1])
    assert not is_data_row(sr)


def test_skip_row_header():
    f = "%d/%m/%Y %H:%M"
    df = pd.DataFrame(
        [
            ["eplusout", None, None],
            ["Date/Time", "Air Temperature", "Wet Bulb Temperature"],
            [None, "BLOCK1:ZONE1", "BLOCK1:ZONE1"],
            [None, "C", "C"],
            [datetime.strptime("01/01/2002 01:00", f), -5.998554089, -9.083890143],
            [datetime.strptime("01/01/2002 02:00", f), -6.358205625, -9.466095593],
            [datetime.strptime("01/01/2002 03:00", f), -6.18423747, -9.391278551],
        ]
    )
    test_mi = pd.MultiIndex.from_tuples(
        [
            ("Air Temperature", "BLOCK1:ZONE1", "C"),
            ("Wet Bulb Temperature", "BLOCK1:ZONE1", "C"),
        ],
        names=["key", "type", "units"],
    )
    mi, skip_rows, index_column = parse_header(df)
    assert_index_equal(test_mi, mi)


def test_template_no_index_name():
    f = "%d/%m/%Y %H:%M"
    df = pd.DataFrame(
        [
            ["key", "Air Temperature", "Wet Bulb Temperature"],
            ["units", "C", "C"],
            [datetime.strptime("01/01/2002 01:00", f), -5.998554089, -9.083890143],
            [datetime.strptime("01/01/2002 02:00", f), -6.358205625, -9.466095593],
            [datetime.strptime("01/01/2002 03:00", f), -6.18423747, -9.391278551],
        ]
    )
    test_mi = pd.MultiIndex.from_tuples(
        [("Air Temperature", "C"), ("Wet Bulb Temperature", "C")], names=["key", "units"]
    )
    mi, skip_rows, index_column = parse_header(df)
    assert_index_equal(test_mi, mi)


def test_insufficient_header_info():
    # let's say that there is a lot of text rows
    with pytest.raises(InsuficientHeaderInfo):
        df = pd.DataFrame([["this", "is", "test", "header", "row"]] * 10)
        parse_header(df, force_index=False)


@pytest.mark.parametrize(
    "table,is_simple",
    [
        ("simple-no-template-no-index", True),
        ("simple-no-template-dt-index", True),
        ("monthly-simple", True),
        ("range", True),
    ],
)
def test_is_simple(excel_file, table, is_simple):
    assert excel_file.tables.is_simple(table) is is_simple


@pytest.mark.parametrize(
    "table,shape",
    [
        ("simple-no-template-no-index", (12, 7)),
        ("simple-no-template-dt-index", (12, 7)),
        ("monthly-simple", (12, 8)),
        ("range", (12, 7)),
        ("no-template-full-dt-index", (12, 8)),
        ("hourly", (8760, 8)),
        ("daily", (365, 8)),
        ("monthly", (12, 8)),
        ("runperiod", (1, 20)),
    ],
)
def test_table_shape(excel_file, table, shape):
    df = excel_file.tables[table]
    assert shape == df.shape


@pytest.mark.parametrize(
    "table,index_name,index_type",
    [
        ("simple-no-template-no-index", "range", pd.RangeIndex,),
        ("simple-no-template-dt-index", "timestamp", pd.DatetimeIndex,),
        ("monthly-simple", "timestamp", pd.DatetimeIndex,),
        ("range", "range", pd.RangeIndex,),
        ("no-template-full-dt-index", "timestamp", pd.DatetimeIndex,),
        ("hourly", "timestamp", pd.DatetimeIndex,),
        ("daily", "timestamp", pd.DatetimeIndex,),
        ("monthly", "timestamp", pd.DatetimeIndex,),
        ("runperiod", "timestamp", pd.DatetimeIndex,),
    ],
)
def test_index(excel_file, table, index_name, index_type):
    df = excel_file.tables[table]
    assert index_name == df.index.name
    assert isinstance(df.index, index_type)


@pytest.mark.parametrize(
    "table,column_names",
    [
        ("simple-no-template-no-index", ["id", "table", "key", "units"],),
        ("simple-no-template-dt-index", ["id", "table", "key", "units"],),
        ("monthly-simple", ["id", "table", "key", "units"],),
        ("range", ["id", "table", "key", "units"],),
        ("no-template-full-dt-index", ["id", "table", "key", "type", "units"],),
        ("hourly", ["id", "table", "key", "type", "units"],),
        ("daily", ["id", "table", "key", "type", "units"],),
        ("monthly", ["id", "table", "key", "type", "units"],),
        ("runperiod", ["id", "table", "key", "type", "units"],),
    ],
)
def test_column_names(excel_file, table, column_names):
    df = excel_file.tables[table]
    assert column_names == df.columns.names


def test_drop_blank_lines():
    ef = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["blank-lines"])
    df = ef.tables["blank-lines"]
    assert df.shape == (12, 7)


def test_force_index_generic_column():
    ef = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["force-index"], force_index=True)
    df = ef.tables["force-index"]
    assert df.shape == (12, 6)
    assert df.index.name == "index"
    assert_index_equal(pd.Index(list("abcdefghijkl"), name="index"), df.index)


def test_index_duplicate_values():
    ef = ResultsFile.from_excel(
        EDGE_CASE_PATH, sheet_names=["duplicate-index"], force_index=True
    )
    df = ef.tables["duplicate-index"]
    assert df.shape == (6, 6)
    assert df.index.name == "index"
    assert_index_equal(pd.Index(list("aaadef"), name="index"), df.index)


def test_column_duplicate_values():
    ef = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["duplicate-columns"])
    df = ef.tables["monthly"]
    assert df.shape == (12, 7)


def test_too_few_header_rows():
    with pytest.raises(InsuficientHeaderInfo):
        _ = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["too-few-header-items"])


def test_too_many_header_rows():
    with pytest.raises(InsuficientHeaderInfo):
        _ = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["too-many-header-items"])


def test_too_many_header_rows_template():
    ef = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["too-many-items-template"])
    df = ef.tables["monthly"]
    assert df.shape == (12, 7)
    assert df.columns.names == ["id", "table", "key", "type", "units"]


def test_too_switched_template_levels():
    ef = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["switched-template-levels"])
    df = ef.tables["monthly"]
    assert df.shape == (12, 7)
    assert df.columns.names == ["id", "table", "key", "type", "units"]


def test_template_missing_key_level():
    with pytest.raises(InsuficientHeaderInfo):
        _ = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["missing-key"])


def test_multiple_tables_single_sheet():
    ef = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["multiple-tables"])
    df = ef.tables["table1"]
    assert df.shape == (12, 3)
    assert df.index.name == "timestamp"

    df = ef.tables["table2"]
    assert df.shape == (12, 5)
    assert df.index.name == "timestamp"


def test_all_tables(excel_file):
    assert excel_file.table_names == [
        "simple-no-template-dt-index",
        "simple-no-template-no-index",
        "monthly-simple",
        "daily-simple",
        "range",
        "no-template-full-dt-index",
        "hourly",
        "daily",
        "monthly",
        "runperiod",
    ]


@pytest.mark.parametrize(
    "sheet_names", [["dup-names-table", "dup-names"], ["dup-names", "dup-names-table"]]
)
def test_duplicate_table_names(sheet_names):
    rf = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=sheet_names)
    assert rf.table_names == ["dup-names", "dup-names (2)"]


def test_no_numeric_outputs():
    with pytest.raises(NoResults):
        _ = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["only-special-column"])


def test_blank_sheet():
    with pytest.raises(NoResults):
        ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["blank-sheet"])


def test_csv_file():
    rf = ResultsFile.from_csv(CSV_PATH)
    assert rf.tables["monthly"].shape == (12, 8)
    assert rf.table_names == ["monthly"]


def test_empty_csv_file():
    with pytest.raises(NoResults):
        _ = ResultsFile.from_csv(Path(TEST_FILES_PATH, "empty_csv.csv"))
