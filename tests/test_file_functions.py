from copy import copy
from datetime import datetime

import pandas as pd
from pandas.testing import assert_frame_equal
from pytest import lazy_fixture

from esofile_reader import Variable, SimpleVariable
from esofile_reader.df.level_names import SPECIAL, N_DAYS_COLUMN
from esofile_reader.exceptions import CannotAggregateVariables
from esofile_reader.pqt.parquet_file import ParquetFile
from esofile_reader.pqt.parquet_tables import VirtualParquetTables, DfParquetTables
from esofile_reader.processing.eplus import H, M
from tests.session_fixtures import *


@pytest.fixture(scope="module")
def parquet_eso_file(eplusout_all_intervals):
    pqf = ParquetFile.from_results_file(0, eplusout_all_intervals)
    try:
        yield pqf
    finally:
        pqf.clean_up()


@pytest.fixture(scope="module")
def virtual_parquet_eso_file(eplusout_all_intervals):
    pqf = ParquetFile.from_results_file(
        1, eplusout_all_intervals, tables_class=VirtualParquetTables
    )
    try:
        yield pqf
    finally:
        pqf.clean_up()


@pytest.fixture(scope="module")
def df_parquet_eso_file(eplusout_all_intervals):
    pqf = ParquetFile.from_results_file(2, eplusout_all_intervals, tables_class=DfParquetTables)
    try:
        yield pqf
    finally:
        pqf.clean_up()


@pytest.fixture(
    scope="module",
    params=[
        lazy_fixture("eplusout_all_intervals"),
        lazy_fixture("parquet_eso_file"),
        lazy_fixture("virtual_parquet_eso_file"),
        lazy_fixture("df_parquet_eso_file"),
    ],
)
def eso_file(request):
    return request.param


@pytest.fixture(scope="module")
def simple_excel_file():
    return GenericFile.from_excel(
        Path(TEST_FILES_PATH, "test_excel_results.xlsx"),
        sheet_names=["simple-template-monthly", "simple-no-template-no-index"],
    )


@pytest.fixture(scope="module")
def simple_parquet_file(simple_excel_file):
    pqf = ParquetFile.from_results_file(3, simple_excel_file)
    try:
        yield pqf
    finally:
        pqf.clean_up()


@pytest.fixture(scope="module")
def virtual_simple_parquet_file(simple_excel_file):
    pqf = ParquetFile.from_results_file(4, simple_excel_file, tables_class=VirtualParquetTables)
    try:
        yield pqf
    finally:
        pqf.clean_up()


@pytest.fixture(scope="module")
def df_simple_parquet_file(simple_excel_file):
    pqf = ParquetFile.from_results_file(5, simple_excel_file, tables_class=DfParquetTables)
    try:
        yield pqf
    finally:
        pqf.clean_up()


@pytest.fixture(
    scope="module",
    params=[
        lazy_fixture("simple_excel_file"),
        lazy_fixture("simple_parquet_file"),
        lazy_fixture("virtual_simple_parquet_file"),
        lazy_fixture("df_simple_parquet_file"),
    ],
)
def simple_file(request):
    return request.param


@pytest.fixture(scope="function")
def copied_file(request):
    copied_file = copy(request.param)
    try:
        yield copied_file
    finally:
        if isinstance(copied_file, ParquetFile):
            copied_file.clean_up()


@pytest.mark.parametrize(
    "file,table_names",
    [
        (
            pytest.lazy_fixture("eso_file"),
            ["timestep", "hourly", "daily", "monthly", "runperiod", "annual"],
        ),
        (pytest.lazy_fixture("simple_file"), ["monthly-simple", "simple-no-template-no-index"]),
    ],
)
def test_table_names(file, table_names):
    assert file.table_names == table_names


@pytest.mark.parametrize(
    "file,n_ids",
    [(pytest.lazy_fixture("eso_file"), 114), (pytest.lazy_fixture("simple_file"), 14),],
)
def test_all_ids(file, n_ids):
    assert len(file.tables.get_all_variable_ids()) == n_ids


@pytest.mark.parametrize(
    "file", [(pytest.lazy_fixture("eso_file")), (pytest.lazy_fixture("simple_file")),],
)
def test_created(file):
    assert isinstance(file.file_created, datetime)


@pytest.mark.parametrize(
    "file", [(pytest.lazy_fixture("eso_file")), (pytest.lazy_fixture("simple_file")),],
)
def test_complete(file):
    assert file.complete


@pytest.mark.parametrize(
    "file, table, n",
    [
        (pytest.lazy_fixture("eso_file"), H, 19),
        (pytest.lazy_fixture("simple_file"), "monthly-simple", 7),
    ],
)
def test_get_header_dict(file, table, n):
    assert len(file.get_header_dictionary(table)) == n


@pytest.mark.parametrize(
    "file, table, shape",
    [
        (pytest.lazy_fixture("eso_file"), H, (19, 5)),
        (pytest.lazy_fixture("simple_file"), "monthly-simple", (7, 4)),
    ],
)
def test_get_header_df(file, table, shape):
    assert file.get_header_df(table).shape == shape


@pytest.mark.parametrize(
    "file,column_names,n_columns",
    [
        (pytest.lazy_fixture("eso_file"), ["id", "table", "key", "type", "units"], 114,),
        (pytest.lazy_fixture("simple_file"), ["id", "table", "key", "units"], 14),
    ],
)
def test_all_header_df(file, column_names, n_columns):
    assert file.tables.get_all_variables_df().columns.to_list() == column_names
    assert len(file.tables.get_all_variables_df().index) == n_columns


@pytest.mark.parametrize(
    "copied_file",
    [(pytest.lazy_fixture("eso_file")), (pytest.lazy_fixture("simple_file")),],
    indirect=["copied_file"],
)
def test_rename(copied_file):
    copied_file.rename("foo")
    assert copied_file.file_name == "foo"


@pytest.mark.parametrize(
    "file,variable,part_match,test_ids",
    [
        (
            pytest.lazy_fixture("eso_file"),
            Variable(
                table="timestep",
                key="BLOCK1:ZONE1",
                type="Zone People Occupant Count",
                units="",
            ),
            False,
            [13],
        ),
        (
            pytest.lazy_fixture("simple_file"),
            SimpleVariable(table="monthly-simple", key="BLOCK1:ZONE1", units=""),
            False,
            [2],
        ),
        (
            pytest.lazy_fixture("eso_file"),
            Variable(
                table="timestep", key="BLOCK1", type="Zone People Occupant Count", units=""
            ),
            True,
            [13],
        ),
        (
            pytest.lazy_fixture("simple_file"),
            SimpleVariable(table="monthly-simple", key="BLOCK1", units=""),
            True,
            [2, 6, 7],
        ),
        (
            pytest.lazy_fixture("eso_file"),
            Variable(
                table="time", key="BLOCK1:ZONE1", type="Zone People Occupant Count", units=""
            ),
            False,
            [],
        ),
        (
            pytest.lazy_fixture("simple_file"),
            SimpleVariable(table="time", key="BLOCK1:ZONE1", units=""),
            False,
            [],
        ),
    ],
)
def test_find_ids(file, variable, part_match, test_ids):
    ids = file.search_tree.find_ids(variable, part_match=part_match)
    assert ids == test_ids


@pytest.mark.parametrize(
    "file,variable,part_match,test_ids",
    [
        (
            pytest.lazy_fixture("eso_file"),
            Variable(
                table="timestep",
                key="BLOCK1:ZONE1",
                type="Zone People Occupant Count",
                units="",
            ),
            False,
            {"timestep": [13]},
        ),
        (
            pytest.lazy_fixture("simple_file"),
            SimpleVariable(table="monthly-simple", key="BLOCK1:ZONE1", units=""),
            False,
            {"monthly-simple": [2]},
        ),
        (
            pytest.lazy_fixture("eso_file"),
            Variable(
                table="timestep", key="BLOCK1", type="Zone People Occupant Count", units=""
            ),
            True,
            {"timestep": [13]},
        ),
        (
            pytest.lazy_fixture("simple_file"),
            SimpleVariable(table="monthly-simple", key="BLOCK1", units=""),
            True,
            {"monthly-simple": [2, 6, 7]},
        ),
        (
            pytest.lazy_fixture("eso_file"),
            Variable(
                table="timestep", key="BLOCK1", type="Zone People Occupant Count", units=""
            ),
            False,
            {},
        ),
        (
            pytest.lazy_fixture("simple_file"),
            SimpleVariable(table="time", key="BLOCK1:ZONE1", units=""),
            False,
            {},
        ),
        (
            pytest.lazy_fixture("eso_file"),
            [31, 32, 297, 298,],
            False,
            {"timestep": [31, 297], "hourly": [32, 298]},
        ),
        (
            pytest.lazy_fixture("simple_file"),
            [1, 2, 3, 10, 11],
            False,
            {"monthly-simple": [1, 2, 3], "simple-no-template-no-index": [10, 11]},
        ),
    ],
)
def test_find_table_id_map(file, variable, part_match, test_ids):
    ids = file.find_table_id_map(variable, part_match=part_match)
    assert ids == test_ids


def test_find_table_id_map_unexpected_type(eso_file):
    with pytest.raises(TypeError):
        _ = eso_file.find_table_id_map(
            [("timestep", 31), ("hourly", 32), ("timestep", 297), ("hourly", 298)]
        )


@pytest.mark.parametrize(
    "file, table, new_key, new_type, new_units, created_variable",
    [
        (
            pytest.lazy_fixture("eso_file"),
            "timestep",
            "dummy",
            "type",
            "foo",
            Variable(table="timestep", key="dummy", type="type", units="foo"),
        ),
        (
            pytest.lazy_fixture("simple_file"),
            "monthly-simple",
            "dummy",
            None,
            "foo",
            SimpleVariable(table="monthly-simple", key="dummy", units="foo"),
        ),
    ],
)
def test_create_new_header_variable(
    file, table, new_key, new_type, new_units, created_variable
):
    v = file._create_header_variable(table, new_key, new_units, type_=new_type)
    assert v == created_variable


@pytest.mark.parametrize(
    "file, table, new_key, new_type, new_units",
    [
        (pytest.lazy_fixture("eso_file"), "timestep", "dummy", None, "foo",),
        (pytest.lazy_fixture("simple_file"), "monthly-simple", "dummy", "Type", "foo",),
    ],
)
def test_create_new_header_variable_invalid(file, table, new_key, new_type, new_units):
    with pytest.raises(TypeError):
        _ = file._create_header_variable(table, new_key, new_units, type_=new_type)


@pytest.mark.parametrize(
    "copied_file, variable, new_key, new_type, test_variable, test_id",
    [
        (
            pytest.lazy_fixture("eso_file"),
            Variable(
                table="annual", key="BLOCK1:ZONE1", type="Zone People Occupant Count", units="",
            ),
            "NEW3",
            "VARIABLE",
            Variable(table="annual", key="NEW3", type="VARIABLE", units=""),
            18,
        ),
        (
            pytest.lazy_fixture("eso_file"),
            Variable(
                table="annual", key="BLOCK1:ZONE1", type="Zone People Occupant Count", units="",
            ),
            "BLOCK1:ZONE1",
            "VARIABLE",
            Variable(table="annual", key="BLOCK1:ZONE1", type="VARIABLE", units=""),
            18,
        ),
        (
            pytest.lazy_fixture("simple_file"),
            SimpleVariable(table="monthly-simple", key="BLOCK1:ZONE1", units=""),
            "NEW3",
            None,
            SimpleVariable(table="monthly-simple", key="NEW3", units=""),
            2,
        ),
    ],
    indirect=["copied_file"],
)
def test_rename_variable(copied_file, variable, new_key, new_type, test_variable, test_id):
    id_, new_variable = copied_file.rename_variable(
        variable, new_key=new_key, new_type=new_type
    )
    assert id_ == test_id
    assert new_variable == test_variable


@pytest.mark.parametrize(
    "copied_file, variable, new_key, new_type",
    [
        (
            pytest.lazy_fixture("eso_file"),
            Variable(table="timestep", key="foo", type="", units=""),
            "NEW3",
            "VARIABLE",
        ),
        (
            pytest.lazy_fixture("eso_file"),
            Variable(table="timestep", key=None, type=None, units=None),
            "NEW3",
            "VARIABLE",
        ),
        (
            pytest.lazy_fixture("simple_file"),
            SimpleVariable(table="monthly-simple", key="foo", units=""),
            "dummy",
            None,
        ),
    ],
    indirect=["copied_file"],
)
def test_rename_variable_invalid(copied_file, variable, new_key, new_type):
    with pytest.raises(KeyError):
        copied_file.rename_variable(variable, new_key=new_key, new_type=new_type)


@pytest.mark.parametrize(
    "copied_file, variable, new_key, new_type, test_id",
    [
        (
            pytest.lazy_fixture("eso_file"),
            Variable(
                table="annual", key="BLOCK1:ZONE1", type="Zone People Occupant Count", units="",
            ),
            "BLOCK1:ZONE1",
            "Zone People Occupant Count",
            18,
        ),
        (
            pytest.lazy_fixture("simple_file"),
            SimpleVariable(table="monthly-simple", key="BLOCK1:ZONE1", units=""),
            "BLOCK1:ZONE1",
            None,
            2,
        ),
    ],
    indirect=["copied_file"],
)
def test_rename_variable_identical_text(copied_file, variable, new_key, new_type, test_id):
    id_, new_variable = copied_file.rename_variable(
        variable, new_key=new_key, new_type=new_type
    )
    assert id_ == test_id
    assert new_variable == variable


def test_rename_variable_missing_args(eplusout_all_intervals):
    with pytest.raises(ValueError):
        eplusout_all_intervals.rename_variable(
            Variable(table="timestep", key="foo", type="", units=""),
            new_key=None,
            new_type=None,
        )


@pytest.mark.parametrize(
    "copied_file, table, new_key, new_type, new_units, array, test_variable, test_id",
    [
        (
            pytest.lazy_fixture("eso_file"),
            "runperiod",
            "dummy",
            "type",
            "foo",
            [1.123],
            Variable(table="runperiod", key="dummy", type="type", units="foo"),
            100,
        ),
        (
            pytest.lazy_fixture("simple_file"),
            "monthly-simple",
            "new",
            None,
            "C",
            list(range(12)),
            SimpleVariable("monthly-simple", "new", "C"),
            100,
        ),
    ],
    indirect=["copied_file"],
)
def test_add_output(
    copied_file, table, new_key, new_type, new_units, array, test_variable, test_id
):
    id_, var = copied_file.insert_variable(table, new_key, new_units, array, type_=new_type)
    assert id_ == test_id
    assert var == test_variable


@pytest.mark.parametrize(
    "copied_file, table, new_key, new_type, new_units, array, variable1, variable2, id1, id2",
    [
        (
            pytest.lazy_fixture("eso_file"),
            "runperiod",
            "dummy",
            "type",
            "foo",
            [1.123],
            Variable(table="runperiod", key="dummy", type="type", units="foo"),
            Variable(table="runperiod", key="dummy (1)", type="type", units="foo"),
            100,
            101,
        ),
        (
            pytest.lazy_fixture("simple_file"),
            "monthly-simple",
            "new",
            None,
            "C",
            list(range(12)),
            SimpleVariable("monthly-simple", "new", "C"),
            SimpleVariable("monthly-simple", "new (1)", "C"),
            100,
            101,
        ),
    ],
    indirect=["copied_file"],
)
def test_add_two_identical_outputs(
    copied_file, table, new_key, new_type, new_units, array, variable1, variable2, id1, id2
):
    id_, var = copied_file.insert_variable(table, new_key, new_units, array, type_=new_type)
    assert id_ == id1
    assert var == variable1
    id_, var = copied_file.insert_variable(table, new_key, new_units, array, type_=new_type)
    assert id_ == id2
    assert var == variable2


@pytest.mark.parametrize(
    "copied_file, table, new_key, new_type, new_units, array",
    [
        (pytest.lazy_fixture("eso_file"), "runperiod", "dummy", "type", "foo", [1.123],),
        (
            pytest.lazy_fixture("simple_file"),
            "monthly-simple",
            "new",
            None,
            "C",
            list(range(12)),
        ),
    ],
    indirect=["copied_file"],
)
def test_add_output_test_tree(copied_file, table, new_key, new_type, new_units, array):
    id_, var = copied_file.insert_variable(table, new_key, new_units, array, type_=new_type)
    tree_id = copied_file.search_tree.find_ids(var)
    assert [id_] == tree_id


def test_add_output_invalid_array(eso_file):
    out = eso_file.insert_variable("timestep", "new", "type", "C", [1])
    assert out is None


def test_add_output_invalid_table(eso_file):
    with pytest.raises(KeyError):
        _ = eso_file.insert_variable("foo", "new", "type", "C", [1])


@pytest.mark.parametrize(
    "copied_file, variables, expected",
    [
        (
            pytest.lazy_fixture("eso_file"),
            Variable(table="hourly", key=None, type="Zone People Occupant Count", units=""),
            Variable(
                table="hourly",
                key="Custom Key - sum",
                type="Zone People Occupant Count",
                units="",
            ),
        ),
        (
            pytest.lazy_fixture("simple_file"),
            [
                SimpleVariable(table="monthly-simple", key="BLOCK1:ZONE1", units=""),
                SimpleVariable(table="monthly-simple", key="BLOCK2:ZONE1", units=""),
                SimpleVariable(table="monthly-simple", key="BLOCK3:ZONE1", units=""),
            ],
            SimpleVariable(table="monthly-simple", key="Custom Key - sum", units=""),
        ),
    ],
    indirect=["copied_file"],
)
def test_aggregate_variables(copied_file, variables, expected):
    id_, var = copied_file.aggregate_variables(variables, "sum")
    assert var == expected


@pytest.mark.parametrize(
    "copied_file", [pytest.lazy_fixture("eso_file")], indirect=["copied_file"]
)
def test_aggregate_energy_rate(copied_file):
    v1 = Variable("monthly", "CHILLER", "Chiller Electric Power", "W")
    v2 = Variable("monthly", "CHILLER", "Chiller Electric Energy", "J")

    id_, var = copied_file.aggregate_variables([v1, v2], "sum")
    df = copied_file.get_results(var)

    test_mi = pd.MultiIndex.from_tuples(
        [("Custom Key - sum", "Custom Type", "J")], names=["key", "type", "units"]
    )
    test_index = pd.MultiIndex.from_product(
        [["eplusout_all_intervals"], [datetime(2002, i, 1) for i in range(1, 13)]],
        names=["file", "timestamp"],
    )
    test_df = pd.DataFrame(
        [
            [5.164679e08],
            [1.318966e09],
            [3.610323e09],
            [5.146479e09],
            [7.525772e09],
            [7.119410e09],
            [1.018732e10],
            [8.958836e09],
            [6.669166e09],
            [5.231315e09],
            [2.971484e09],
            [3.891442e08],
        ],
        index=test_index,
        columns=test_mi,
    )
    assert_frame_equal(df, test_df)


@pytest.mark.parametrize(
    "copied_file", [pytest.lazy_fixture("eso_file")], indirect=["copied_file"]
)
def test_aggregate_energy_rate_hourly(copied_file):
    v1 = Variable("hourly", "CHILLER", "Chiller Electric Power", "W")
    v2 = Variable("hourly", "CHILLER", "Chiller Electric Energy", "J")
    test_sr = copied_file.get_results([v1, v2], rate_to_energy=True).sum(axis=1)
    test_df = pd.DataFrame(test_sr)
    test_mi = pd.MultiIndex.from_tuples(
        [("Custom Key - sum", "Custom Type", "J")], names=["key", "type", "units"]
    )
    test_df.columns = test_mi
    id_, var = copied_file.aggregate_variables([v1, v2], "sum")
    df = copied_file.get_results(id_)
    assert_frame_equal(test_df, df)


def test_aggregate_invalid_variables(eso_file):
    vars = [
        Variable("hourly", "invalid", "variable1", "units"),
        Variable("hourly", "invalid", "type", "units"),
    ]
    with pytest.raises(CannotAggregateVariables):
        eso_file.aggregate_variables(vars, "sum")


@pytest.mark.parametrize(
    "copied_file", [(pytest.lazy_fixture("eso_file")),], indirect=["copied_file"],
)
def test_aggregate_energy_rate_invalid(copied_file):
    copied_file.tables["monthly"].drop(
        (SPECIAL, M, N_DAYS_COLUMN, "", ""), axis=1, inplace=True,
    )
    v1 = Variable("monthly", "CHILLER", "Chiller Electric Power", "W")
    v2 = Variable("monthly", "CHILLER", "Chiller Electric Energy", "J")
    with pytest.raises(CannotAggregateVariables):
        _ = copied_file.aggregate_variables([v1, v2], "sum")


def test_aggregate_variables_too_much_vars(eplusout_all_intervals):
    v = Variable(table="hourly", key="BLOCK1:ZONE1", type=None, units=None)
    with pytest.raises(CannotAggregateVariables):
        _ = eplusout_all_intervals.aggregate_variables(v, "sum")


def test_aggregate_variables_invalid_too_many_tables(eso_file):
    v = Variable(table=None, key=None, type="Zone People Occupant Count", units="")
    with pytest.raises(CannotAggregateVariables):
        _ = eso_file.aggregate_variables(v, "sum")


def test_aggregate_variables_single_variable(eso_file):
    v1 = Variable("monthly", "CHILLER", "Chiller Electric Power", "W")
    with pytest.raises(CannotAggregateVariables):
        _ = eso_file.aggregate_variables([v1], "sum")


def test_as_df_invalid_table(eso_file):
    with pytest.raises(KeyError):
        _ = eso_file.get_numeric_table("foo")


@pytest.mark.parametrize(
    "copied_file", [(pytest.lazy_fixture("eso_file")),], indirect=["copied_file"],
)
def test_remove_variables(copied_file):
    variable = Variable("monthly", "CHILLER", "Chiller Electric Power", "W")
    copied_file.remove_variables(variable)
    assert not copied_file.search_tree.find_ids(variable)
    assert variable not in copied_file.tables[M].columns


def test_to_excel(tiny_eplusout):
    p = Path("test.xlsx")
    try:
        tiny_eplusout.to_excel(p)
        loaded_ef = GenericFile.from_excel(p)
        assert loaded_ef.tables == tiny_eplusout.tables
        assert p.exists()
    finally:
        p.unlink()


@pytest.mark.parametrize(
    "file, expected_class",
    [
        (pytest.lazy_fixture("tiny_eplusout"), EsoFile),
        (pytest.lazy_fixture("excel_file"), GenericFile),
    ],
)
def test_file_copy(file, expected_class):
    assert isinstance(copy(file), expected_class)
