import pandas as pd

from esofile_reader import Variable
from esofile_reader.results_processing.table_formatter import TableFormatter
from tests.session_fixtures import *


@pytest.fixture(scope="function")
def test_table(eplusout_all_intervals):
    df = eplusout_all_intervals.tables["monthly"].iloc[:, 1:5]
    return df.copy()


@pytest.fixture(scope="function")
def test_table_with_day(eplusout_all_intervals):
    df = eplusout_all_intervals.tables["monthly"].iloc[:, 1:5]
    df = eplusout_all_intervals.tables.add_day_to_index(df, "monthly")
    return df.copy()


@pytest.mark.parametrize(
    "file_name_position, column_names, index_names",
    [
        ("row", ["key", "type", "units"], ["file", "timestamp"]),
        ("column", ["file", "key", "type", "units"], ["timestamp"]),
        (None, ["key", "type", "units"], ["timestamp"]),
        ("invalid", ["key", "type", "units"], ["file", "timestamp"]),
    ],
)
def test_file_name_position(test_table, file_name_position, column_names, index_names):
    table_formatter = TableFormatter(file_name_position=file_name_position)
    df = table_formatter.format_table(test_table, "foo")
    assert df.index.names == index_names
    assert df.columns.names == column_names


def test_file_name_row(test_table):
    table_formatter = TableFormatter(file_name_position="row")
    df = table_formatter.format_table(test_table, "foo")
    assert df.index.levels[0] == pd.Index(["foo"], name="file")


def test_file_name_column(test_table):
    table_formatter = TableFormatter(file_name_position="column")
    df = table_formatter.format_table(test_table, "foo")
    assert df.columns.levels[0] == pd.Index(["foo"], name="file")


@pytest.mark.parametrize(
    "include, column_names",
    [(True, ["table", "key", "type", "units"]), (False, ["key", "type", "units"]),],
)
def test_include_table_name(test_table, include, column_names):
    table_formatter = TableFormatter(include_table_name=include)
    df = table_formatter.format_table(test_table, "foo")
    assert df.columns.names == column_names


@pytest.mark.parametrize(
    "table, include, index_names",
    [
        (pytest.lazy_fixture("test_table"), True, ["file", "timestamp"]),
        (pytest.lazy_fixture("test_table"), False, ["file", "timestamp"]),
        (pytest.lazy_fixture("test_table_with_day"), True, ["file", "timestamp", "day"]),
        (pytest.lazy_fixture("test_table_with_day"), False, ["file", "timestamp"]),
    ],
)
def test_include_day_no_day_column(table, include, index_names):
    table_formatter = TableFormatter(include_day=include)
    df = table_formatter.format_table(table, "foo")
    assert df.index.names == index_names


@pytest.mark.parametrize(
    "include, column_names",
    [(True, ["id", "key", "type", "units"]), (False, ["key", "type", "units"]),],
)
def test_include_id(test_table, include, column_names):
    table_formatter = TableFormatter(include_id=include)
    df = table_formatter.format_table(test_table, "foo")
    assert df.columns.names == column_names


def test_timestamp_format(test_table):
    table_formatter = TableFormatter(timestamp_format="%Y-%m-%d-%H-%M")
    df = table_formatter.format_table(test_table, "foo")
    formatted_timestamp = [
        "2002-01-01-00-00",
        "2002-02-01-00-00",
        "2002-03-01-00-00",
        "2002-04-01-00-00",
        "2002-05-01-00-00",
        "2002-06-01-00-00",
        "2002-07-01-00-00",
        "2002-08-01-00-00",
        "2002-09-01-00-00",
        "2002-10-01-00-00",
        "2002-11-01-00-00",
        "2002-12-01-00-00",
    ]
    assert df.index.get_level_values("timestamp").tolist() == formatted_timestamp


@pytest.mark.parametrize(
    "table", [pytest.lazy_fixture("test_table"), pytest.lazy_fixture("test_table_with_day"),]
)
def test_default_format(table):
    table_formatter = TableFormatter()
    df = table_formatter.format_table(table, "foo")
    assert df.index.names == ["file", "timestamp"]
    assert df.columns.names == ["key", "type", "units"]


def test_full_index(test_table_with_day):
    table_formatter = TableFormatter(include_day=True, file_name_position="row")
    df = table_formatter.format_table(test_table_with_day, "foo")
    assert df.index.names == ["file", "timestamp", "day"]


def test_full_columns(test_table):
    table_formatter = TableFormatter(
        include_id=True, include_table_name=True, file_name_position="column"
    )
    df = table_formatter.format_table(test_table, "foo")
    assert df.columns.names == ["file", "id", "table", "key", "type", "units"]


def test_timestamp_format_peak_outputs(eplusout_all_intervals_peaks):
    table_formatter = TableFormatter(timestamp_format="%Y-%m-%d-%H-%M")
    df = eplusout_all_intervals_peaks.get_results(
        Variable("monthly", "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
        table_formatter=table_formatter,
        output_type="global_min",
    )
    assert df.iloc[0, 1] == "2002-06-01-00-00"
