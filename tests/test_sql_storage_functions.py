import os
import unittest
from datetime import datetime

import pandas as pd
from pandas.testing import assert_frame_equal

from esofile_reader import EsoFile
from esofile_reader import Variable
from esofile_reader.base_file import CannotAggregateVariables
from esofile_reader.constants import SPECIAL
from esofile_reader.storages.sql_storage import SQLStorage
from tests import ROOT, EF_ALL_INTERVALS


class TestSqlDBFileFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.storage = SQLStorage()
        id_ = cls.storage.store_file(EF_ALL_INTERVALS)
        cls.ef = cls.storage.files[id_]

    def test_table_names(self):
        self.assertListEqual(
            ["timestep", "hourly", "daily", "monthly", "runperiod", "annual"],
            self.ef.table_names,
        )

    def test_all_ids(self):
        self.assertEqual(114, len(self.ef.tables.get_all_variable_ids()))

    def test_created(self):
        self.assertTrue(isinstance(self.ef.file_created, datetime))

    def test_complete(self):
        self.assertTrue(self.ef.complete)

    def test_header_df(self):
        self.assertEqual(
            ["id", "table", "key", "type", "units"],
            self.ef.tables.get_all_variables_df().columns.to_list(),
        )
        self.assertEqual(len(self.ef.tables.get_all_variables_df().index), 114)

    def test_rename(self):
        original = self.ef.file_name
        self.ef.rename("foo")

        stmnt = f"SELECT file_name FROM 'result-files' WHERE id={self.ef.id_}"
        res = self.storage.engine.execute(stmnt).scalar()

        self.assertEqual("foo", res)
        self.assertEqual("foo", self.ef.file_name)
        self.ef.rename(original)

    def test__add_file_name_row(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        out = self.ef._add_file_name(df, "row")
        mi = pd.MultiIndex.from_product(
            [["eplusout_all_intervals"], index], names=["file", "timestamp"]
        )
        assert_frame_equal(out, pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=mi))

    def test__add_file_name_column(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        out = self.ef._add_file_name(df, "column")
        mi = pd.MultiIndex.from_product(
            [["eplusout_all_intervals"], ["a", "c"]], names=["file", None]
        )
        assert_frame_equal(out, pd.DataFrame([[1, 4], [2, 5], [3, 6]], index=index, columns=mi))

    def test__add_file_name_invalid(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        out = self.ef._add_file_name(df, "foo")
        mi = pd.MultiIndex.from_product(
            [["eplusout_all_intervals"], index], names=["file", "timestamp"]
        )
        assert_frame_equal(out, pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=mi))

    def test__merge_frame(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df1 = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        df2 = pd.DataFrame({"b": [1, 2, 3]}, index=index)
        mi = pd.MultiIndex.from_product(
            [["eplusout_all_intervals"], index], names=["file", "timestamp"]
        )
        df = self.ef._merge_frame([df1, df2], add_file_name="row")
        assert_frame_equal(
            df, pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6], "b": [1, 2, 3]}, index=mi)
        )

    def test__merge_frame_update_dt(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df1 = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        df2 = pd.DataFrame({"b": [1, 2, 3]}, index=index)
        mi = pd.MultiIndex.from_product(
            [["eplusout_all_intervals"], ["01-01", "02-01", "03-01"]],
            names=["file", "timestamp"],
        )
        df = self.ef._merge_frame([df1, df2], add_file_name="row", timestamp_format="%d-%m")
        assert_frame_equal(
            df, pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6], "b": [1, 2, 3]}, index=mi)
        )

    def test_find_ids(self):
        v = Variable(
            table="timestep", key="BLOCK1:ZONE1", type="Zone People Occupant Count", units="",
        )
        ids = self.ef.find_ids(v, part_match=False)
        self.assertListEqual([13], ids)

    def test_find_ids_part_match(self):
        v = Variable(
            table="timestep", key="BLOCK1", type="Zone People Occupant Count", units=""
        )
        ids = self.ef.find_ids(v, part_match=True)
        self.assertListEqual([13], ids)

    def test_find_ids_part_invalid(self):
        v = Variable(
            table="time", key="BLOCK1:ZONE1", type="Zone People Occupant Count", units="",
        )
        ids = self.ef.find_ids(v, part_match=False)
        self.assertListEqual([], ids)

    def test__find_pairs(self):
        v = Variable(
            table="timestep", key="BLOCK1:ZONE1", type="Zone People Occupant Count", units="",
        )
        out = self.ef._find_pairs(v, part_match=False)
        self.assertDictEqual({"timestep": [13]}, out)

    def test__find_pairs_part_match(self):
        v = Variable(
            table="timestep", key="BLOCK1", type="Zone People Occupant Count", units=""
        )
        out = self.ef._find_pairs(v, part_match=True)
        self.assertDictEqual({"timestep": [13]}, out)

    def test__find_pairs_invalid(self):
        v = Variable(
            table="timestep", key="BLOCK1", type="Zone People Occupant Count", units=""
        )
        out = self.ef._find_pairs(v, part_match=False)
        self.assertDictEqual({}, out)

    def test_create_new_header_variable(self):
        v1 = self.ef.create_header_variable("timestep", "dummy", "type", "foo")
        self.assertTupleEqual(
            v1, Variable(table="timestep", key="dummy", type="type", units="foo")
        )

    def test_rename_variable(self):
        v1 = Variable(
            table="timestep", key="BLOCK1:ZONE1", type="Zone People Occupant Count", units="",
        )
        self.ef.rename_variable(v1, new_key="NEW1", new_type="VARIABLE")

        v2 = Variable(table="timestep", key="NEW1", type="VARIABLE", units="")
        ids = self.ef.find_ids(v2)
        self.assertListEqual(ids, [13])

        # revert change
        self.ef.rename_variable(v2, new_key=v1.key, new_type=v1.type)
        ids = self.ef.find_ids(v1)
        self.assertListEqual(ids, [13])

    def test_rename_variable_invalid(self):
        v = Variable(table="timestep", key="foo", type="", units="")
        out = self.ef.rename_variable(v, new_key="NEW2", new_type="VARIABLE")
        self.assertIsNone(out)

    def test_rename_variable_invalid_names(self):
        v = Variable(
            table="timestep", key="BLOCK2:ZONE1", type="Zone People Occupant Count", units="",
        )
        out = self.ef.rename_variable(v, new_key="", new_type="")
        self.assertIsNone(out)

        ids = self.ef.find_ids(v)
        self.assertListEqual(ids, [19])

    def test_add_output(self):
        id_, var = self.ef.add_variable("runperiod", "new", "type", "C", [1])
        self.assertTupleEqual(var, Variable("runperiod", "new", "type", "C"))
        self.assertEqual(100, id_)
        self.ef.remove_variables(var)

    def test_add_two_outputs(self):
        id_, var1 = self.ef.add_variable("runperiod", "new", "type", "C", [1])
        self.assertTupleEqual(var1, Variable("runperiod", "new", "type", "C"))

        id_, var2 = self.ef.add_variable("runperiod", "new", "type", "C", [1])
        self.assertTupleEqual(var2, Variable("runperiod", "new (1)", "type", "C"))
        self.ef.remove_variables(var1)
        self.ef.remove_variables(var2)

    def test_add_output_test_tree(self):
        id_, var = self.ef.add_variable("runperiod", "new", "type", "C", [1])
        self.assertTupleEqual(var, Variable("runperiod", "new", "type", "C"))

        ids = self.ef.search_tree.find_ids(var)
        self.assertIsNot(ids, [])
        self.assertEqual(len(ids), 1)

        self.ef.remove_variables(var)
        ids = self.ef.search_tree.find_ids(var)
        self.assertEqual(ids, [])

    def test_add_output_invalid(self):
        out = self.ef.add_variable("timestep", "new", "type", "C", [1])
        self.assertIsNone(out)

    def test_add_output_invalid_interval(self):
        with self.assertRaises(KeyError):
            _ = self.ef.add_variable("foo", "new", "type", "C", [1])

    def test_aggregate_variables(self):
        v = Variable(table="hourly", key=None, type="Zone People Occupant Count", units="")
        id_, var = self.ef.aggregate_variables(v, "sum")
        self.assertEqual(
            var,
            Variable(
                table="hourly",
                key="Custom Key - sum",
                type="Zone People Occupant Count",
                units="",
            ),
        )
        self.ef.remove_variables(var)
        id_, var = self.ef.aggregate_variables(v, "sum", new_key="foo", new_type="bar")
        self.assertEqual(var, Variable(table="hourly", key="foo", type="bar", units=""))
        self.ef.remove_variables(var)

    def test_aggregate_energy_rate(self):
        v1 = Variable("monthly", "CHILLER", "Chiller Electric Power", "W")
        v2 = Variable("monthly", "CHILLER", "Chiller Electric Energy", "J")

        id_, var = self.ef.aggregate_variables([v1, v2], "sum")
        df = self.ef.get_results(var)

        test_mi = pd.MultiIndex.from_tuples(
            [("Custom Key - sum", "Custom Variable", "J")], names=["key", "type", "units"]
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
        self.ef.remove_variables(var)

    def test_aggregate_energy_rate_invalid(self):
        ef = EsoFile(os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso"))
        ef.tables["monthly"].drop(SPECIAL, axis=1, inplace=True, level=0)

        v1 = Variable("monthly", "CHILLER", "Chiller Electric Power", "W")
        v2 = Variable("monthly", "CHILLER", "Chiller Electric Energy", "J")

        with self.assertRaises(CannotAggregateVariables):
            _ = ef.aggregate_variables([v1, v2], "sum")

    def test_aggregate_variables_too_much_vars(self):
        v = Variable(table="hourly", key="BLOCK1:ZONE1", type=None, units=None)
        with self.assertRaises(CannotAggregateVariables):
            _ = self.ef.aggregate_variables(v, "sum")

    def test_aggregate_variables_invalid_too_many_intervals(self):
        v = Variable(table=None, key=None, type="Zone People Occupant Count", units="")
        with self.assertRaises(CannotAggregateVariables):
            _ = self.ef.aggregate_variables(v, "sum")

    def test_as_df(self):
        df = self.ef.get_numeric_table("hourly")
        self.assertTupleEqual(df.shape, (8760, 19))
        self.assertListEqual(df.columns.names, ["id", "table", "key", "type", "units"])
        self.assertEqual(df.index.name, "timestamp")

    def test_as_df_invalid_interval(self):
        with self.assertRaises(KeyError):
            _ = self.ef.get_numeric_table("foo")


if __name__ == "__main__":
    unittest.main()
