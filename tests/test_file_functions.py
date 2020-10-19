from copy import deepcopy
from datetime import datetime

import pandas as pd
from pandas.testing import assert_frame_equal

import esofile_reader.results_processing.aggregate_results
from esofile_reader import Variable, SimpleVariable
from esofile_reader.constants import *
from esofile_reader.exceptions import CannotAggregateVariables
from tests.session_fixtures import *


@pytest.fixture(scope="module")
def simple_file():
    return ResultsFile.from_excel(
        Path(ROOT_PATH, "eso_files", "test_excel_results.xlsx"),
        sheet_names=["simple-template-monthly", "simple-no-template-no-index"],
    )


@pytest.fixture(scope="function")
def copied_simple_file(simple_file):
    return deepcopy(simple_file)


@pytest.fixture(scope="function")
def copied_eplusout_all_intervals(eplusout_all_intervals):
    return deepcopy(eplusout_all_intervals)


@pytest.mark.parametrize(
    "file,table_names",
    [
        (
            pytest.lazy_fixture("eplusout_all_intervals"),
            ["timestep", "hourly", "daily", "monthly", "runperiod", "annual"],
        ),
        (pytest.lazy_fixture("simple_file"), ["monthly-simple", "simple-no-template-no-index"]),
    ],
)
def test_table_names(file, table_names):
    assert file.table_names == table_names


@pytest.mark.parametrize(
    "file,table,can_convert",
    [
        (pytest.lazy_fixture("eplusout_all_intervals"), "timestep", True),
        (pytest.lazy_fixture("eplusout_all_intervals"), "hourly", True),
        (pytest.lazy_fixture("eplusout_all_intervals"), "daily", True),
        (pytest.lazy_fixture("eplusout_all_intervals"), "monthly", True),
        (pytest.lazy_fixture("eplusout_all_intervals"), "runperiod", True),
        (pytest.lazy_fixture("eplusout_all_intervals"), "annual", True),
        (pytest.lazy_fixture("simple_file"), "monthly-simple", True),
        (pytest.lazy_fixture("simple_file"), "simple-no-template-no-index", False),
    ],
)
def test_can_convert_rate_to_energy(file, table, can_convert):
    assert can_convert == file.can_convert_rate_to_energy(table)


@pytest.mark.parametrize(
    "file,n_ids",
    [
        (pytest.lazy_fixture("eplusout_all_intervals"), 114),
        (pytest.lazy_fixture("simple_file"), 14),
    ],
)
def test_all_ids(file, n_ids):
    assert len(file.tables.get_all_variable_ids()) == n_ids


@pytest.mark.parametrize(
    "file",
    [(pytest.lazy_fixture("eplusout_all_intervals")), (pytest.lazy_fixture("simple_file")),],
)
def test_created(file):
    assert isinstance(file.file_created, datetime)


@pytest.mark.parametrize(
    "file",
    [(pytest.lazy_fixture("eplusout_all_intervals")), (pytest.lazy_fixture("simple_file")),],
)
def test_complete(file):
    assert file.complete


@pytest.mark.parametrize(
    "file,column_names,n_columns",
    [
        (
            pytest.lazy_fixture("eplusout_all_intervals"),
            ["id", "table", "key", "type", "units"],
            114,
        ),
        (pytest.lazy_fixture("simple_file"), ["id", "table", "key", "units"], 14),
    ],
)
def test_header_df(file, column_names, n_columns):
    assert file.tables.get_all_variables_df().columns.to_list() == column_names
    assert len(file.tables.get_all_variables_df().index) == n_columns


@pytest.mark.parametrize(
    "file",
    [
        (pytest.lazy_fixture("copied_eplusout_all_intervals")),
        (pytest.lazy_fixture("copied_simple_file")),
    ],
)
def test_rename(file):
    file.rename("foo")
    assert file.file_name == "foo"


@pytest.mark.parametrize(
    "file,variable,part_match,test_ids",
    [
        (
            pytest.lazy_fixture("eplusout_all_intervals"),
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
            pytest.lazy_fixture("eplusout_all_intervals"),
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
            pytest.lazy_fixture("eplusout_all_intervals"),
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
    ids = file.find_id(variable, part_match=part_match)
    assert ids == test_ids


@pytest.mark.parametrize(
    "file,variable,part_match,test_ids",
    [
        (
            pytest.lazy_fixture("eplusout_all_intervals"),
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
            pytest.lazy_fixture("eplusout_all_intervals"),
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
            pytest.lazy_fixture("eplusout_all_intervals"),
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
            pytest.lazy_fixture("eplusout_all_intervals"),
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


def test_find_table_id_map_unexpected_type(eplusout_all_intervals):
    with pytest.raises(TypeError):
        _ = eplusout_all_intervals.find_table_id_map(
            [("timestep", 31), ("hourly", 32), ("timestep", 297), ("hourly", 298)]
        )


@pytest.mark.parametrize(
    "file, table, new_key, new_type, new_units, created_variable",
    [
        (
            pytest.lazy_fixture("eplusout_all_intervals"),
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
    v = file.create_header_variable(table, new_key, new_units, type_=new_type)
    assert v == created_variable


@pytest.mark.parametrize(
    "file, table, new_key, new_type, new_units",
    [
        (pytest.lazy_fixture("eplusout_all_intervals"), "timestep", "dummy", None, "foo",),
        (pytest.lazy_fixture("simple_file"), "monthly-simple", "dummy", "Type", "foo",),
    ],
)
def test_create_new_header_variable_invalid(file, table, new_key, new_type, new_units):
    with pytest.raises(TypeError):
        _ = file.create_header_variable(table, new_key, new_units, type_=new_type)


@pytest.mark.parametrize(
    "file, variable, new_key, new_type, test_variable, test_id",
    [
        (
            pytest.lazy_fixture("copied_eplusout_all_intervals"),
            Variable(
                table="timestep",
                key="BLOCK1:ZONE1",
                type="Zone People Occupant Count",
                units="",
            ),
            "NEW3",
            "VARIABLE",
            Variable(table="timestep", key="NEW3", type="VARIABLE", units=""),
            13,
        ),
        (
            pytest.lazy_fixture("copied_simple_file"),
            SimpleVariable(table="monthly-simple", key="BLOCK1:ZONE1", units=""),
            "NEW3",
            None,
            SimpleVariable(table="monthly-simple", key="NEW3", units=""),
            2,
        ),
    ],
)
def test_rename_variable(file, variable, new_key, new_type, test_variable, test_id):
    id_, new_variable = file.rename_variable(variable, new_key=new_key, new_type=new_type)
    assert id_ == test_id
    assert new_variable == test_variable


@pytest.mark.parametrize(
    "file, variable, new_key, new_type",
    [
        (
            pytest.lazy_fixture("copied_eplusout_all_intervals"),
            Variable(table="timestep", key="foo", type="", units=""),
            "NEW3",
            "VARIABLE",
        ),
        (
            pytest.lazy_fixture("copied_simple_file"),
            SimpleVariable(table="monthly-simple", key="foo", units=""),
            "dummy",
            None,
        ),
    ],
)
def test_rename_variable_invalid(file, variable, new_key, new_type):
    out = file.rename_variable(variable, new_key=new_key, new_type=new_type)
    assert out is None


@pytest.mark.parametrize(
    "file, table, new_key, new_type, new_units, array, test_variable, test_id",
    [
        (
            pytest.lazy_fixture("copied_eplusout_all_intervals"),
            "runperiod",
            "dummy",
            "type",
            "foo",
            [1.123],
            Variable(table="runperiod", key="dummy", type="type", units="foo"),
            100,
        ),
        (
            pytest.lazy_fixture("copied_simple_file"),
            "monthly-simple",
            "new",
            None,
            "C",
            list(range(12)),
            SimpleVariable("monthly-simple", "new", "C"),
            100,
        ),
    ],
)
def test_add_output(file, table, new_key, new_type, new_units, array, test_variable, test_id):
    id_, var = file.insert_variable(table, new_key, new_units, array, type_=new_type)
    assert id_ == test_id
    assert var == test_variable


@pytest.mark.parametrize(
    "file, table, new_key, new_type, new_units, array, variable1, variable2, id1, id2",
    [
        (
            pytest.lazy_fixture("copied_eplusout_all_intervals"),
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
            pytest.lazy_fixture("copied_simple_file"),
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
)
def test_add_two_identical_outputs(
    file, table, new_key, new_type, new_units, array, variable1, variable2, id1, id2
):
    id_, var = file.insert_variable(table, new_key, new_units, array, type_=new_type)
    assert id_ == id1
    assert var == variable1
    id_, var = file.insert_variable(table, new_key, new_units, array, type_=new_type)
    assert id_ == id2
    assert var == variable2


@pytest.mark.parametrize(
    "file, table, new_key, new_type, new_units, array",
    [
        (
            pytest.lazy_fixture("copied_eplusout_all_intervals"),
            "runperiod",
            "dummy",
            "type",
            "foo",
            [1.123],
        ),
        (
            pytest.lazy_fixture("copied_simple_file"),
            "monthly-simple",
            "new",
            None,
            "C",
            list(range(12)),
        ),
    ],
)
def test_add_output_test_tree(file, table, new_key, new_type, new_units, array):
    id_, var = file.insert_variable(table, new_key, new_units, array, type_=new_type)
    tree_id = file.find_id(var)
    assert [id_] == tree_id


def test_add_output_invalid_array(eplusout_all_intervals):
    out = eplusout_all_intervals.insert_variable("timestep", "new", "type", "C", [1])
    assert out is None


def test_add_output_invalid_table(eplusout_all_intervals):
    with pytest.raises(KeyError):
        _ = eplusout_all_intervals.insert_variable("foo", "new", "type", "C", [1])


def test_aggregate_variables(copied_eplusout_all_intervals):
    v = Variable(table="hourly", key=None, type="Zone People Occupant Count", units="")
    id_, var = esofile_reader.results_processing.aggregate_results.aggregate_variables(v, "sum")
    assert var == Variable(
        table="hourly", key="Custom Key - sum", type="Zone People Occupant Count", units="",
    )


def test_aggregate_variables_simple(copied_simple_file):
    v1 = SimpleVariable(table="monthly-simple", key="BLOCK1:ZONE1", units="")
    v2 = SimpleVariable(table="monthly-simple", key="BLOCK2:ZONE1", units="")
    v3 = SimpleVariable(table="monthly-simple", key="BLOCK3:ZONE1", units="")
    id_, var = copied_simple_file.aggregate_variables([v1, v2, v3], "sum")
    assert var == SimpleVariable(table="monthly-simple", key="Custom Key - sum", units="")


def test_aggregate_energy_rate(copied_eplusout_all_intervals):
    v1 = Variable("monthly", "CHILLER", "Chiller Electric Power", "W")
    v2 = Variable("monthly", "CHILLER", "Chiller Electric Energy", "J")

    id_, var = esofile_reader.results_processing.aggregate_results.aggregate_variables(
        [v1, v2], "sum"
    )
    df = copied_eplusout_all_intervals.get_results_df(var)

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


def test_aggregate_energy_rate_hourly(copied_eplusout_all_intervals):
    v1 = Variable("hourly", "CHILLER", "Chiller Electric Power", "W")
    v2 = Variable("hourly", "CHILLER", "Chiller Electric Energy", "J")
    test_sr = copied_eplusout_all_intervals.get_results_df([v1, v2], rate_to_energy=True).sum(
        axis=1
    )
    test_df = pd.DataFrame(test_sr)
    test_mi = pd.MultiIndex.from_tuples(
        [("Custom Key - sum", "Custom Type", "J")], names=["key", "type", "units"]
    )
    test_df.columns = test_mi
    id_, var = esofile_reader.results_processing.aggregate_results.aggregate_variables(
        [v1, v2], "sum"
    )
    df = copied_eplusout_all_intervals.get_results_df(id_)
    assert_frame_equal(test_df, df)


def test_aggregate_invalid_variables(copied_eplusout_all_intervals):
    vars = [
        Variable("hourly", "invalid", "variable1", "units"),
        Variable("hourly", "invalid", "type", "units"),
    ]
    with pytest.raises(CannotAggregateVariables):
        esofile_reader.results_processing.aggregate_results.aggregate_variables(vars, "sum")


def test_aggregate_energy_rate_invalid(copied_eplusout_all_intervals):
    copied_eplusout_all_intervals.tables["monthly"].drop(SPECIAL, axis=1, inplace=True, level=0)
    v1 = Variable("monthly", "CHILLER", "Chiller Electric Power", "W")
    v2 = Variable("monthly", "CHILLER", "Chiller Electric Energy", "J")
    with pytest.raises(CannotAggregateVariables):
        _ = esofile_reader.results_processing.aggregate_results.aggregate_variables(
            [v1, v2], "sum"
        )


def test_aggregate_variables_too_much_vars(copied_eplusout_all_intervals):
    v = Variable(table="hourly", key="BLOCK1:ZONE1", type=None, units=None)
    with pytest.raises(CannotAggregateVariables):
        _ = esofile_reader.results_processing.aggregate_results.aggregate_variables(v, "sum")


def test_aggregate_variables_invalid_too_many_tables(copied_eplusout_all_intervals):
    v = Variable(table=None, key=None, type="Zone People Occupant Count", units="")
    with pytest.raises(CannotAggregateVariables):
        _ = esofile_reader.results_processing.aggregate_results.aggregate_variables(v, "sum")


def test_as_df_invalid_table(eplusout_all_intervals):
    with pytest.raises(KeyError):
        _ = eplusout_all_intervals.get_numeric_table("foo")


def test_to_excel(eplusout1):
    p = Path("test.xlsx")
    try:
        eplusout1.to_excel(p)
        loaded_ef = ResultsFile.from_excel(p)
        assert loaded_ef.tables == eplusout1.tables
        assert p.exists()
    finally:
        p.unlink()
