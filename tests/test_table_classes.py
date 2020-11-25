from copy import copy
from datetime import datetime

import numpy as np
import pandas as pd
from pandas.testing import assert_index_equal, assert_frame_equal, assert_series_equal
from pytest import lazy_fixture

from esofile_reader.df.df_functions import (
    slice_series_by_datetime_index,
    slice_df_by_datetime_index,
    sort_by_ids,
)
from esofile_reader.df.level_names import (
    N_DAYS_COLUMN,
    DAY_COLUMN,
    COLUMN_LEVELS,
    SIMPLE_COLUMN_LEVELS,
    PEAK_COLUMN_LEVELS,
    SPECIAL,
)
from esofile_reader.typehints import Variable, SimpleVariable
from esofile_reader.pqt.parquet_storage import ParquetFile
from tests.session_fixtures import *


@pytest.fixture(scope="module")
def df_tables(eplusout_all_intervals):
    return eplusout_all_intervals.tables


@pytest.fixture(scope="module")
def simple_file():
    return GenericFile.from_excel(
        Path(TEST_FILES_PATH, "test_excel_results.xlsx"),
        sheet_names=["simple-template-monthly", "simple-no-template-no-index"],
    )


@pytest.fixture(scope="module")
def parquet_tables(eplusout_all_intervals):
    pqf = ParquetFile.from_results_file(0, eplusout_all_intervals)
    try:
        yield pqf.tables
    finally:
        pqf.clean_up()


@pytest.fixture(
    scope="module", params=[lazy_fixture("df_tables"), lazy_fixture("parquet_tables")]
)
def tables(request):
    return request.param


@pytest.fixture(scope="module")
def simple_df_tables(simple_file):
    return simple_file.tables


@pytest.fixture(scope="module")
def simple_parquet_tables(simple_file):
    pqf = ParquetFile.from_results_file(1, simple_file)
    try:
        yield pqf.tables
    finally:
        pqf.clean_up()


@pytest.fixture(
    scope="module",
    params=[lazy_fixture("simple_df_tables"), lazy_fixture("simple_parquet_tables")],
)
def simple_tables(request):
    return request.param


@pytest.fixture(scope="function")
def copied_tables(request):
    return copy(request.param)


@pytest.mark.parametrize(
    "test_tables, is_simple",
    [(lazy_fixture("tables"), False), (lazy_fixture("simple_tables"), True),],
)
def test_is_simple(test_tables, is_simple):
    table_names = test_tables.get_table_names()
    for table in table_names:
        assert test_tables.is_simple(table) is is_simple


@pytest.mark.parametrize(
    "test_tables, levels",
    [
        (lazy_fixture("tables"), COLUMN_LEVELS),
        (lazy_fixture("simple_tables"), SIMPLE_COLUMN_LEVELS),
    ],
)
def test_get_levels(test_tables, levels):
    table_names = test_tables.get_table_names()
    for table in table_names:
        assert test_tables.get_levels(table) == levels


NAMES = ["timestep", "hourly", "daily", "monthly", "runperiod", "annual"]
SIMPLE_NAMES = ["monthly-simple", "simple-no-template-no-index"]


@pytest.mark.parametrize(
    "test_tables, names",
    [(lazy_fixture("tables"), NAMES), (lazy_fixture("simple_tables"), SIMPLE_NAMES),],
)
def test_get_table_names(test_tables, names):
    assert test_tables.get_table_names() == names


INDEX = pd.DatetimeIndex(
    [
        "2002-01-01",
        "2002-02-01",
        "2002-03-01",
        "2002-04-01",
        "2002-05-01",
        "2002-06-01",
        "2002-07-01",
        "2002-08-01",
        "2002-09-01",
        "2002-10-01",
        "2002-11-01",
        "2002-12-01",
    ],
    dtype="datetime64[ns]",
    name="timestamp",
    freq=None,
)


@pytest.mark.parametrize(
    "test_tables, table, index",
    [
        (lazy_fixture("tables"), "monthly", INDEX),
        (lazy_fixture("simple_tables"), "monthly-simple", INDEX),
    ],
)
def test_get_datetime_index(test_tables, table, index):
    assert_index_equal(test_tables.get_datetime_index(table), index)


SIMPLE_IDS = [1, 2, 3, 4, 5, 6, 7]
IDS = [9, 15, 21, 27, 33, 299, 305, 311, 317, 323, 329, 335, 341, 433, 477, 521, 565, 952, 982]


@pytest.mark.parametrize(
    "test_tables, table, ids",
    [
        (lazy_fixture("tables"), "daily", IDS),
        (lazy_fixture("simple_tables"), "monthly-simple", SIMPLE_IDS),
    ],
)
def test_get_variables_dct(test_tables, table, ids):
    assert list(test_tables.get_variables_dct(table).keys()) == ids


# fmt: off
ALL_SIMPLE_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
ALL_IDS = [
    7, 13, 19, 25, 31, 297, 303, 309, 315, 321, 327, 333, 339, 431, 475, 519, 563,
    950, 956, 8, 14, 20, 26, 32, 298, 304, 310, 316, 322, 328, 334, 340, 432, 476,
    520, 564, 951, 981, 9, 15, 21, 27, 33, 299, 305, 311, 317, 323, 329, 335, 341,
    433, 477, 521, 565, 952, 982, 10, 16, 22, 28, 34, 300, 306, 312, 318, 324, 330,
    336, 342, 434, 478, 522, 566, 953, 983, 11, 17, 23, 29, 35, 301, 307, 313, 319,
    325, 331, 337, 343, 435, 479, 523, 567, 954, 984, 12, 18, 24, 30, 36, 302, 308,
    314, 320, 326, 332, 338, 344, 436, 480, 524, 568, 955, 985,
]


# fmt: on


@pytest.mark.parametrize(
    "test_tables, ids",
    [(lazy_fixture("tables"), ALL_IDS), (lazy_fixture("simple_tables"), ALL_SIMPLE_IDS),],
)
def test_get_all_variable_ids(test_tables, ids):
    assert test_tables.get_all_variable_ids() == ids


@pytest.mark.parametrize(
    "test_tables, table, shape",
    [
        (lazy_fixture("tables"), "daily", (19, 5)),
        (lazy_fixture("simple_tables"), "monthly-simple", (7, 4)),
    ],
)
def test_get_variables_df(test_tables, table, shape):
    assert test_tables.get_variables_df(table).shape == shape


@pytest.mark.parametrize(
    "test_tables, shape",
    [(lazy_fixture("tables"), (114, 5)), (lazy_fixture("simple_tables"), (14, 4)),],
)
def test_get_all_variables_df(test_tables, shape):
    assert test_tables.get_all_variables_df().shape == shape


@pytest.mark.parametrize(
    "test_tables, expected_count",
    [(lazy_fixture("tables"), 114), (lazy_fixture("simple_tables"), 14)],
)
def test_get_variables_count(test_tables, expected_count):
    assert test_tables.get_all_variables_count() == expected_count


@pytest.mark.parametrize(
    "copied_tables, table, id_, new_key, new_type, units",
    [
        (lazy_fixture("tables"), "timestep", 7, "FOO", "BAR", "W/m2"),
        (lazy_fixture("simple_tables"), "monthly-simple", 1, "FOO", None, "W/m2"),
    ],
    indirect=["copied_tables"],
)
def test_rename_variable(copied_tables, id_, table, new_key, new_type, units):
    copied_tables.update_variable_name(table, id_, new_key, new_type)
    variable = (
        (id_, table, new_key, new_type, units) if new_type else (id_, table, new_key, units)
    )
    try:
        _ = copied_tables[table].loc[:, [variable]]
    except KeyError:
        pytest.fail("Test fail! Renamed variable cannot be found!")


@pytest.mark.parametrize(
    "copied_tables, new_variable, new_id",
    [
        (lazy_fixture("tables"), Variable("monthly", "FOO", "BAR", "C"), 100),
        (lazy_fixture("simple_tables"), SimpleVariable("monthly-simple", "FOO", "C"), 100,),
    ],
    indirect=["copied_tables"],
)
def test_insert_variable(copied_tables, new_variable, new_id):
    id_ = copied_tables.insert_column(new_variable, list(range(12)))
    df = copied_tables.get_results_df(new_variable.table, [new_id])
    assert id_ == new_id
    assert df.squeeze().tolist() == list(range(12))


@pytest.mark.parametrize(
    "copied_tables, table, ids",
    [
        (lazy_fixture("tables"), "monthly", [10, 16, 28]),
        (lazy_fixture("simple_tables"), "monthly-simple", [6, 7]),
    ],
    indirect=["copied_tables"],
)
def test_delete_variables(copied_tables, table, ids):
    copied_tables.delete_variables(table, ids)
    assert not any(map(lambda x: x in copied_tables.get_variable_ids(table), ids))


@pytest.mark.parametrize(
    "copied_tables, table, ids",
    [
        (lazy_fixture("tables"), "monthly", [10, 10000]),
        (lazy_fixture("simple_tables"), "monthly-simple", [6, 10000]),
    ],
    indirect=["copied_tables"],
)
def test_delete_variables_invalid(copied_tables, table, ids):
    with pytest.raises(KeyError):
        copied_tables.delete_variables(table, ids)


@pytest.mark.parametrize(
    "copied_tables, table, id_",
    [
        (lazy_fixture("tables"), "monthly", 983),
        (lazy_fixture("simple_tables"), "monthly-simple", 1),
    ],
    indirect=["copied_tables"],
)
def test_update_variable(copied_tables, table, id_):
    copied_tables.update_variable_values(table, id_, list(range(12)))
    values = copied_tables.get_results_df(table, id_).iloc[:, 0].to_list()
    assert values == list(range(12))


@pytest.mark.parametrize(
    "copied_tables, table, id_",
    [
        (lazy_fixture("tables"), "monthly", 983),
        (lazy_fixture("simple_tables"), "monthly-simple", 1),
    ],
    indirect=["copied_tables"],
)
def test_update_variable_invalid(copied_tables, table, id_):
    copied_tables.update_variable_values(table, id_, list(range(11)))
    values = copied_tables.get_results_df(table, id_).iloc[:, 0].to_list()
    assert values != list(range(12))


@pytest.mark.parametrize(
    "copied_tables, table",
    [(lazy_fixture("tables"), "monthly"), (lazy_fixture("simple_tables"), "monthly-simple"),],
    indirect=["copied_tables"],
)
def test_insert_special_column(copied_tables, table):
    array = list("abcdefghijkl")
    copied_tables.insert_special_column(table, "TEST", array)
    variable = (
        (SPECIAL, table, "TEST", "")
        if copied_tables.is_simple(table)
        else (SPECIAL, table, "TEST", "", "")
    )
    values = copied_tables[table].loc[:, [variable]].squeeze().tolist()
    assert values == array


@pytest.mark.parametrize(
    "copied_tables, table",
    [(lazy_fixture("tables"), "monthly"), (lazy_fixture("simple_tables"), "monthly-simple"),],
    indirect=["copied_tables"],
)
def test_insert_special_column_invalid(copied_tables, table):
    array = list("abcdefghij")
    copied_tables.insert_special_column(table, "TEST", array)
    with pytest.raises(KeyError):
        copied_tables.get_special_column(table, "TEST")


@pytest.mark.parametrize(
    "test_tables, table",
    [(lazy_fixture("tables"), "monthly"), (lazy_fixture("simple_tables"), "monthly-simple"),],
)
def test_get_special_column_invalid(test_tables, table):
    with pytest.raises(KeyError):
        test_tables.get_special_column(table, "FOO")


@pytest.mark.parametrize(
    "test_tables, table",
    [(lazy_fixture("tables"), "monthly"), (lazy_fixture("simple_tables"), "monthly-simple"),],
)
def test_get_number_of_days(test_tables, table):
    col = test_tables.get_special_column(table, N_DAYS_COLUMN)
    assert col.to_list() == [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


@pytest.mark.parametrize(
    "test_tables, table", [(lazy_fixture("tables"), "monthly"),],
)
def test_get_days_of_week(test_tables, table):
    col = test_tables.get_special_column("daily", DAY_COLUMN)
    assert col[0] == "Tuesday"
    assert col.size == 365


@pytest.mark.parametrize(
    "test_tables, table, shape",
    [
        (lazy_fixture("tables"), "daily", (365, 19)),
        (lazy_fixture("simple_tables"), "monthly-simple", (12, 7)),
    ],
)
def test_get_all_results(test_tables, table, shape):
    assert test_tables.get_numeric_table(table).shape == shape


TEST_DF = pd.DataFrame(
    [
        [18.948067, 2.582339e08],
        [18.879265, 6.594828e08],
        [20.987345, 1.805162e09],
        [23.129456, 2.573239e09],
        [24.993765, 3.762886e09],
        [26.255885, 3.559705e09],
        [27.007450, 5.093662e09],
        [26.448572, 4.479418e09],
        [24.684673, 3.334583e09],
        [22.725196, 2.615657e09],
        [20.549040, 1.485742e09],
        [18.520034, 1.945721e08],
    ],
    columns=pd.MultiIndex.from_tuples(
        [
            (324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C"),
            (983, "monthly", "CHILLER", "Chiller Electric Energy", "J"),
        ],
        names=COLUMN_LEVELS,
    ),
    index=pd.Index([datetime(2002, i, 1) for i in range(1, 13)], name="timestamp"),
)

TEST_SIMPLE_DF = pd.DataFrame(
    [
        [4.44599391, 19.14850348],
        [4.280304696, 18.99527211],
        [4.059385744, 20.98875615],
        [4.394446155, 22.78142137],
        [4.44599391, 24.3208488],
        [3.99495105, 25.47972495],
        [4.44599391, 26.16745932],
        [4.252689827, 25.68404781],
        [4.194698603, 24.15289436],
        [4.44599391, 22.47691717],
        [4.194698603, 20.58877632],
        [4.252689827, 18.66182101],
    ],
    columns=pd.MultiIndex.from_tuples(
        [
            (2, "monthly-simple", "BLOCK1:ZONE1", ""),
            (6, "monthly-simple", "BLOCK1:ZONE1", "C"),
        ],
        names=SIMPLE_COLUMN_LEVELS,
    ),
    index=pd.Index([datetime(2002, i, 1) for i in range(1, 13)], name="timestamp"),
)


@pytest.mark.parametrize(
    "test_tables, table, ids, test_df",
    [
        (lazy_fixture("tables"), "monthly", [324, 983], TEST_DF),
        (lazy_fixture("simple_tables"), "monthly-simple", [2, 6], TEST_SIMPLE_DF),
    ],
)
def test_get_results(test_tables, table, ids, test_df):
    df = test_tables.get_results_df(table, ids)
    assert_frame_equal(df, test_df, check_column_type=False)


@pytest.mark.parametrize(
    "test_tables, table, ids, test_df",
    [
        (lazy_fixture("tables"), "monthly", [324, 983], TEST_DF),
        (lazy_fixture("simple_tables"), "monthly-simple", [2, 6], TEST_SIMPLE_DF),
    ],
)
def test_get_results_sliced(test_tables, table, ids, test_df):
    df = test_tables.get_results_df(
        table, ids, start_date=datetime(2002, 4, 1), end_date=datetime(2002, 6, 1),
    )
    test_df = test_df.iloc[3:6, :]
    assert_frame_equal(df, test_df, check_column_type=False)


@pytest.mark.parametrize(
    "test_tables, table, ids", [(lazy_fixture("tables"), "daily", [323]),],
)
def test_get_results_include_day(test_tables, table, ids):
    df = test_tables.get_results_df(
        table,
        ids,
        start_date=datetime(2002, 4, 1),
        end_date=datetime(2002, 4, 3),
        include_day=True,
    )
    test_index = pd.MultiIndex.from_arrays(
        [[datetime(2002, 4, i) for i in range(1, 4)], ["Monday", "Tuesday", "Wednesday"]],
        names=["timestamp", "day"],
    )
    assert_index_equal(df.index, test_index)


@pytest.mark.parametrize(
    "test_tables, table, ids",
    [
        (lazy_fixture("tables"), "monthly", [324, 983]),
        (lazy_fixture("simple_tables"), "monthly-simple", [2, 6]),
    ],
)
def test_get_results_include_day_from_date(test_tables, table, ids):
    df = test_tables.get_results_df(
        table,
        ids,
        start_date=datetime(2002, 4, 1),
        end_date=datetime(2002, 6, 1),
        include_day=True,
    )
    test_index = pd.MultiIndex.from_arrays(
        [[datetime(2002, i, 1) for i in range(4, 7)], ["Monday", "Wednesday", "Saturday"]],
        names=["timestamp", "day"],
    )
    assert_index_equal(df.index, test_index)


@pytest.mark.parametrize(
    "test_tables, table",
    [(lazy_fixture("tables"), "monthly"), (lazy_fixture("simple_tables"), "monthly-simple"),],
)
def test_get_results_invalid_ids(test_tables, table):
    with pytest.raises(KeyError):
        _ = test_tables.get_results_df(table, [999999])


TEST_MAX_DF = pd.DataFrame(
    [[27.007450, datetime(2002, 7, 1), 5.093662e09, datetime(2002, 7, 1)],],
    columns=pd.MultiIndex.from_tuples(
        [
            (324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C", "value"),
            (324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C", "timestamp",),
            (983, "monthly", "CHILLER", "Chiller Electric Energy", "J", "value"),
            (983, "monthly", "CHILLER", "Chiller Electric Energy", "J", "timestamp"),
        ],
        names=PEAK_COLUMN_LEVELS,
    ),
)

TEST_MAX_SIMPLE_DF = pd.DataFrame(
    [[4.44599391, datetime(2002, 1, 1), 26.16745932, datetime(2002, 7, 1)]],
    columns=pd.MultiIndex.from_tuples(
        [
            (2, "monthly-simple", "BLOCK1:ZONE1", "", "value"),
            (2, "monthly-simple", "BLOCK1:ZONE1", "", "timestamp",),
            (6, "monthly-simple", "BLOCK1:ZONE1", "C", "value"),
            (6, "monthly-simple", "BLOCK1:ZONE1", "C", "timestamp"),
        ],
        names=["id", "table", "key", "units", "data"],
    ),
)


@pytest.mark.parametrize(
    "test_tables, table, ids, test_df",
    [
        (lazy_fixture("tables"), "monthly", [324, 983], TEST_MAX_DF),
        (lazy_fixture("simple_tables"), "monthly-simple", [2, 6], TEST_MAX_SIMPLE_DF),
    ],
)
def test_get_global_max_results(test_tables, table, ids, test_df):
    df = test_tables.get_global_max_results_df(table, ids)
    assert_frame_equal(df, test_df, check_column_type=False)


TEST_MIN_DF = pd.DataFrame(
    [[18.520034, datetime(2002, 12, 1), 1.945721e08, datetime(2002, 12, 1)],],
    columns=pd.MultiIndex.from_tuples(
        [
            (324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C", "value"),
            (324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C", "timestamp"),
            (983, "monthly", "CHILLER", "Chiller Electric Energy", "J", "value"),
            (983, "monthly", "CHILLER", "Chiller Electric Energy", "J", "timestamp"),
        ],
        names=PEAK_COLUMN_LEVELS,
    ),
)
TEST_MIN_SIMPLE_DF = pd.DataFrame(
    [[3.994951, datetime(2002, 6, 1), 18.661821, datetime(2002, 12, 1)]],
    columns=pd.MultiIndex.from_tuples(
        [
            (2, "monthly-simple", "BLOCK1:ZONE1", "", "value"),
            (2, "monthly-simple", "BLOCK1:ZONE1", "", "timestamp",),
            (6, "monthly-simple", "BLOCK1:ZONE1", "C", "value"),
            (6, "monthly-simple", "BLOCK1:ZONE1", "C", "timestamp"),
        ],
        names=["id", "table", "key", "units", "data"],
    ),
)


@pytest.mark.parametrize(
    "test_tables, table, ids, test_df",
    [
        (lazy_fixture("tables"), "monthly", [324, 983], TEST_MIN_DF),
        (lazy_fixture("simple_tables"), "monthly-simple", [2, 6], TEST_MIN_SIMPLE_DF),
    ],
)
def test_get_global_min_results(test_tables, table, ids, test_df):
    df = test_tables.get_global_min_results_df(table, ids)
    assert_frame_equal(df, test_df, check_column_type=False)


TEST_SLICE_DF = pd.DataFrame(
    {"a": list(range(5))},
    index=pd.DatetimeIndex(pd.date_range("2002-01-01", freq="d", periods=5)),
)


@pytest.mark.parametrize(
    "start_date, end_date, test_df",
    [
        (datetime(2002, 1, 2), None, TEST_SLICE_DF.iloc[1:, :]),
        (None, datetime(2002, 1, 2), TEST_SLICE_DF.iloc[:2, :]),
        (datetime(2002, 1, 2), datetime(2002, 1, 2), TEST_SLICE_DF.iloc[[1], :]),
    ],
)
def test_df_dt_slicer(start_date, end_date, test_df):
    assert_frame_equal(
        slice_df_by_datetime_index(TEST_SLICE_DF, start_date=start_date, end_date=end_date),
        test_df,
    )


TEST_SERIES = pd.Series(
    list(range(5)), index=pd.DatetimeIndex(pd.date_range("2002-01-01", freq="d", periods=5))
)


@pytest.mark.parametrize(
    "start_date, end_date, test_series",
    [
        (datetime(2002, 1, 2), None, TEST_SERIES.iloc[1:]),
        (None, datetime(2002, 1, 2), TEST_SERIES.iloc[:2]),
        (datetime(2002, 1, 2), datetime(2002, 1, 2), TEST_SERIES.iloc[[1]]),
    ],
)
def test_sr_dt_slicer(start_date, end_date, test_series):
    assert_series_equal(
        slice_series_by_datetime_index(TEST_SERIES, start_date=start_date, end_date=end_date),
        test_series,
    )


def test_sort_by_ids():
    columns = pd.MultiIndex.from_tuples(
        [(1, "a", "b", "c"), (2, "d", "e", "f"), (3, "g", "h", "i")],
        names=["id", "table", "key", "units"],
    )
    index = pd.date_range(start="01/01/2020", periods=8760, freq="h", name="datetime")
    df = pd.DataFrame(np.random.rand(8760, 3), index=index, columns=columns)
    expected_df = df.loc[:, [(3, "g", "h", "i"), (1, "a", "b", "c"), (2, "d", "e", "f")]]
    sorted_df = sort_by_ids(df, [3, 1, 2])
    assert_frame_equal(expected_df, sorted_df)


def test_sort_by_ids_na_id():
    columns = pd.MultiIndex.from_tuples(
        [(1, "a", "b", "c"), (2, "d", "e", "f"), (3, "g", "h", "i")],
        names=["id", "table", "key", "units"],
    )
    index = pd.date_range(start="01/01/2020", periods=8760, freq="h", name="datetime")
    df = pd.DataFrame(np.random.rand(8760, 3), index=index, columns=columns)
    expected_df = df.loc[:, [(3, "g", "h", "i"), (1, "a", "b", "c"), (2, "d", "e", "f")]]
    sorted_df = sort_by_ids(df, [4, 3, 1, 2])
    assert_frame_equal(expected_df, sorted_df)


def test_set_table_invalid(df_tables):
    variables = [
        (1, "test", "ZoneA", "Temperature", "C"),
        (2, "test", "ZoneB", "Temperature", "C"),
        (3, "test", "ZoneC", "Temperature", "C"),
        (3, "test", "ZoneD", "Temperature", "C"),
    ]
    columns = pd.MultiIndex.from_tuples(
        variables, names=["id", "WRONG", "key", "type", "units"]
    )
    index = pd.RangeIndex(start=0, stop=3, step=1, name="range")
    df = pd.DataFrame(
        [
            [25.123, 27.456, 14.546, 1000],
            [25.123, 27.456, 14.546, 2000],
            [25.123, 27.456, 14.546, 3000],
        ],
        columns=columns,
        index=index,
    )
    with pytest.raises(TypeError):
        df_tables["test"] = df


def test_file_not_equal(eplusout1, eplusout2):
    assert not eplusout1 == eplusout2
