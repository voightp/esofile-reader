from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from esofile_reader import GenericFile, Variable
from esofile_reader.constants import SPECIAL, COLUMN_LEVELS
from esofile_reader.exceptions import NoResults
from esofile_reader.processing.totals import process_totals
from esofile_reader.search_tree import Tree
from esofile_reader.df.df_tables import DFTables
from tests.session_fixtures import TEST_FILES_PATH


@pytest.fixture
def test_file():
    daily_variables = [
        (SPECIAL, "daily", "day", "", ""),
        (1, "daily", "BLOCK1:ZONE1", "Zone Temperature", "C"),
        (2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C"),
        (3, "daily", "BLOCK1:ZONE3", "Zone Temperature", "C"),
        (4, "daily", "BLOCK1:ZONE1", "Heating Load", "W"),
        (5, "daily", "BLOCK1:ZONE1_WALL_3_0_0_0_0_0_WIN", "Window Gain", "W"),
        (6, "daily", "BLOCK1:ZONE1_WALL_4_0_0_0_0_0_WIN", "Window Gain", "W"),
        (7, "daily", "BLOCK1:ZONE1_WALL_5_0_0_0_0_0_WIN", "Window Gain", "W"),
        (8, "daily", "BLOCK1:ZONE1_WALL_6_0_0_0_0_0_WIN", "Window Lost", "W"),
        (9, "daily", "BLOCK1:ZONE1_WALL_5_0_0", "Wall Gain", "W"),
        (10, "daily", "BLOCK1:ZONE2_WALL_4_8_9", "Wall Gain", "W"),
        (11, "daily", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"),
        (12, "daily", "Meter", "BLOCK1:ZONE2#LIGHTS", "J"),
        (13, "daily", "Some Flow 1", "Mass Flow", "kg/s"),
        (14, "daily", "Some Curve", "Performance Curve Input Variable 1", "kg/s"),
        (15, "daily", "BAR", "FOO", "W"),
        (16, "daily", "BAZ", "FOO", "W"),
    ]
    names = ["id", "table", "key", "type", "units"]

    daily_columns = pd.MultiIndex.from_tuples(daily_variables, names=names)
    daily_index = pd.DatetimeIndex(
        pd.date_range("2002-1-1", freq="d", periods=3), name="timestamp"
    )
    daily_results = pd.DataFrame(
        [
            ["Sunday", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            ["Monday", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            ["Tuesday", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        ],
        columns=daily_columns,
        index=daily_index,
    )

    monthly_variables = [(15, "monthly", "Some Flow 1", "Mass Flow", "kg/s")]
    monthly_columns = pd.MultiIndex.from_tuples(monthly_variables, names=names)
    monthly_index = pd.DatetimeIndex([datetime(2002, 1, 1)], name="timestamp")
    monthly_results = pd.DataFrame([[1]], columns=monthly_columns, index=monthly_index)

    range_variables = [
        (17, "range", "BLOCK1:ZONE1", "Zone Temperature", "DON'T GROUP"),
        (18, "range", "BLOCK1:ZONE2", "Zone Temperature", "DON'T GROUP"),
        (19, "range", "BLOCK1:ZONE3", "Zone Temperature", "C"),
        (20, "range", "BLOCK1:ZONE3", "Zone Temperature", "W"),
        (21, "range", "BLOCK1:ZONE1", "Heating Load", "W"),
    ]

    range_columns = pd.MultiIndex.from_tuples(range_variables, names=names)
    range_index = pd.RangeIndex(start=0, step=1, stop=2, name="range")
    range_results = pd.DataFrame(
        [[1, 2, 3, 4, 5], [1, 2, 3, 4, 5],], columns=range_columns, index=range_index
    )

    text_columns = pd.MultiIndex.from_tuples(
        [
            (1, "text", "BLOCK1:ZONE1", "Zone Type", ""),
            (2, "text", "BLOCK1:ZONE2", "Zone Type", ""),
            (3, "text", "BLOCK1:ZONE3", "Zone Type", ""),
        ],
        names=COLUMN_LEVELS,
    )
    text_index = pd.RangeIndex(start=0, step=1, stop=2, name="range")
    text_results = pd.DataFrame(
        [["a", "b", 1], ["c", "d", 1],], columns=text_columns, index=text_index
    )
    tables = DFTables()
    tables["daily"] = daily_results
    tables["monthly"] = monthly_results
    tables["range"] = range_results
    tables["text"] = text_results
    tree = Tree.from_header_dict(tables.get_all_variables_dct())
    return GenericFile("dummy/path", "base", datetime.utcnow(), tables, tree, "test")


@pytest.fixture
def totals_file(test_file):
    return GenericFile.from_totals(test_file)


@pytest.fixture
def totals_tables(test_file):
    return GenericFile.from_totals(test_file).tables


def test_file_name(totals_file):
    assert totals_file.file_name == "base - totals"


def test_file_path(totals_file):
    assert totals_file.file_path == "dummy/path"


def test_file_type(totals_file):
    assert totals_file.file_type == "totals"


def test_totals_tables(totals_tables):
    test_columns = pd.MultiIndex.from_tuples(
        [(13, "text", "Zone", "Zone Type", "")], names=COLUMN_LEVELS
    )

    test_index = pd.RangeIndex(start=0, step=1, stop=2, name="range")
    test_df = pd.DataFrame([[1], [1]], columns=test_columns, index=test_index)
    assert_frame_equal(totals_tables["text"], test_df)


def test_search_tree(totals_file):
    ids = totals_file.find_id(
        [
            Variable("daily", "Zone", "Zone Temperature", "C"),
            Variable("daily", "Meter", "LIGHTS", "J"),
            Variable("range", "Heating", "Heating Load", "W"),
        ]
    )
    assert ids == [1, 6, 12]


def test_grouped_variables(totals_tables):
    test_columns = pd.MultiIndex.from_tuples(
        [
            (SPECIAL, "daily", "day", "", ""),
            (1, "daily", "Zone", "Zone Temperature", "C"),
            (2, "daily", "Heating", "Heating Load", "W"),
            (3, "daily", "Windows", "Window Gain", "W"),
            (4, "daily", "Windows", "Window Lost", "W"),
            (5, "daily", "Walls", "Wall Gain", "W"),
            (6, "daily", "Meter", "LIGHTS", "J"),
            (7, "daily", "FOO", "FOO", "W"),
        ],
        names=["id", "table", "key", "type", "units"],
    )
    test_index = pd.DatetimeIndex(
        pd.date_range("2002-1-1", freq="d", periods=3), name="timestamp"
    )
    test_results = pd.DataFrame(
        [
            ["Sunday", 2, 4, 6, 8, 9.5, 23, 15.5],
            ["Monday", 2, 4, 6, 8, 9.5, 23, 15.5],
            ["Tuesday", 2, 4, 6, 8, 9.5, 23, 15.5],
        ],
        columns=test_columns,
        index=test_index,
    )
    pd.testing.assert_frame_equal(totals_tables["daily"], test_results, check_dtype=False)


def test_non_grouped_variables(totals_tables):
    test_columns = pd.MultiIndex.from_tuples(
        [
            (8, "range", "BLOCK1:ZONE1", "Zone Temperature", "DON'T GROUP"),
            (9, "range", "BLOCK1:ZONE2", "Zone Temperature", "DON'T GROUP"),
            (10, "range", "Zone", "Zone Temperature", "C"),
            (11, "range", "Zone", "Zone Temperature", "W"),
            (12, "range", "Heating", "Heating Load", "W"),
        ],
        names=["id", "table", "key", "type", "units"],
    )
    test_index = pd.RangeIndex(start=0, step=1, stop=2, name="range")
    test_results = pd.DataFrame(
        [[1, 2, 3, 4, 5], [1, 2, 3, 4, 5],], columns=test_columns, index=test_index
    )
    pd.testing.assert_frame_equal(totals_tables["range"], test_results)


def test_empty_table(totals_tables):
    with pytest.raises(KeyError):
        _ = totals_tables["monthly"]


def test_filter_non_numeric_columns(totals_tables):
    assert (1, "text", "BLOCK1:ZONE1", "Zone Type", "") not in totals_tables["text"].columns


def test_only_simple_tables():
    rf = GenericFile.from_excel(
        Path(TEST_FILES_PATH, "test_excel_results.xlsx"),
        sheet_names=["simple-template-monthly", "simple-template-daily"],
    )
    with pytest.raises(NoResults):
        _ = GenericFile.from_totals(rf)
