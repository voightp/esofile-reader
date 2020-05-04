import os
import unittest
from datetime import datetime

import pandas as pd
from pandas.testing import assert_frame_equal

from esofile_reader import EsoFile, Variable
from esofile_reader.base_file import CannotAggregateVariables, BaseFile
from esofile_reader.constants import N_DAYS_COLUMN
from tests import ROOT, EF_ALL_INTERVALS, EF_ALL_INTERVALS_PEAKS


class TestFileFunctions(unittest.TestCase):

    def test_base_file_populate_content(self):
        bf = BaseFile()
        bf.populate_content()

    def test_available_intervals(self):
        self.assertListEqual(
            EF_ALL_INTERVALS.available_intervals,
            ["timestep", "hourly", "daily", "monthly", "runperiod", "annual"],
        )

    def test_all_ids(self):
        self.assertEqual(len(EF_ALL_INTERVALS.data.get_all_variable_ids()), 114)

    def test_created(self):
        self.assertTrue(isinstance(EF_ALL_INTERVALS.file_created, datetime))

    def test_complete(self):
        self.assertTrue(EF_ALL_INTERVALS.complete)
        self.assertIsNone(EF_ALL_INTERVALS.peak_outputs)

    def test_peak_complete(self):
        self.assertTrue(EF_ALL_INTERVALS_PEAKS.complete)
        self.assertIsNotNone(EF_ALL_INTERVALS_PEAKS.peak_outputs)

    def test_header_df(self):
        names = ["id", "interval", "key", "type", "units"]
        self.assertEqual(EF_ALL_INTERVALS.data.get_all_variables_df().columns.to_list(), names)
        self.assertEqual(len(EF_ALL_INTERVALS.data.get_all_variables_df().index), 114)

        frames = []
        for interval in EF_ALL_INTERVALS.available_intervals:
            frames.append(EF_ALL_INTERVALS.get_header_df(interval))
        df = pd.concat(frames, axis=0)
        assert_frame_equal(df, EF_ALL_INTERVALS.data.get_all_variables_df())

    def test_rename(self):
        original = EF_ALL_INTERVALS.file_name
        EF_ALL_INTERVALS.rename("foo")
        self.assertEqual(EF_ALL_INTERVALS.file_name, "foo")
        EF_ALL_INTERVALS.rename(original)

    def test__add_file_name_row(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        out = EF_ALL_INTERVALS._add_file_name(df, "row")
        mi = pd.MultiIndex.from_product(
            [["eplusout_all_intervals"], index], names=["file", "timestamp"]
        )
        assert_frame_equal(out, pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=mi))

    def test__add_file_name_column(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        out = EF_ALL_INTERVALS._add_file_name(df, "column")
        mi = pd.MultiIndex.from_product(
            [["eplusout_all_intervals"], ["a", "c"]], names=["file", None]
        )
        assert_frame_equal(out, pd.DataFrame([[1, 4], [2, 5], [3, 6]], index=index, columns=mi))

    def test__add_file_name_invalid(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        out = EF_ALL_INTERVALS._add_file_name(df, "foo")
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
        df = EF_ALL_INTERVALS._merge_frame([df1, df2], add_file_name="row")
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
        df = EF_ALL_INTERVALS._merge_frame(
            [df1, df2], add_file_name="row", timestamp_format="%d-%m"
        )
        assert_frame_equal(
            df, pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6], "b": [1, 2, 3]}, index=mi)
        )

    def test_find_ids(self):
        v = Variable(
            interval="timestep",
            key="BLOCK1:ZONE1",
            type="Zone People Occupant Count",
            units="",
        )
        ids = EF_ALL_INTERVALS.find_ids(v, part_match=False)
        self.assertEqual(ids, [13])

    def test_find_ids_part_match(self):
        v = Variable(
            interval="timestep", key="BLOCK1", type="Zone People Occupant Count", units=""
        )
        ids = EF_ALL_INTERVALS.find_ids(v, part_match=True)
        self.assertEqual(ids, [13])

    def test_find_ids_part_invalid(self):
        v = Variable(
            interval="time",
            key="BLOCK1:ZONE1",
            type="Zone People Occupant Count",
            units="",
        )
        ids = EF_ALL_INTERVALS.find_ids(v, part_match=False)
        self.assertEqual(ids, [])

    def test__find_pairs(self):
        v = Variable(
            interval="timestep",
            key="BLOCK1:ZONE1",
            type="Zone People Occupant Count",
            units="",
        )
        out = EF_ALL_INTERVALS._find_pairs(v, part_match=False)
        self.assertDictEqual(out, {"timestep": [13]})

    def test__find_pairs_part_match(self):
        v = Variable(
            interval="timestep", key="BLOCK1", type="Zone People Occupant Count", units=""
        )
        out = EF_ALL_INTERVALS._find_pairs(v, part_match=True)
        self.assertDictEqual(out, {"timestep": [13]})

    def test__find_pairs_invalid(self):
        v = Variable(
            interval="timestep", key="BLOCK1", type="Zone People Occupant Count", units=""
        )
        out = EF_ALL_INTERVALS._find_pairs(v, part_match=False)
        self.assertDictEqual(out, {})

    def test_create_new_header_variable(self):
        v1 = EF_ALL_INTERVALS.create_header_variable("timestep", "dummy", "type", "foo")

        self.assertTupleEqual(
            v1, Variable(interval="timestep", key="dummy", type="type", units="foo")
        )

    def test_rename_variable(self):
        v1 = Variable(
            interval="timestep",
            key="BLOCK1:ZONE1",
            type="Zone People Occupant Count",
            units="",
        )
        EF_ALL_INTERVALS.rename_variable(v1, new_key="NEW3", new_type="VARIABLE")

        v2 = Variable(interval="timestep", key="NEW3", type="VARIABLE", units="")
        ids = EF_ALL_INTERVALS.find_ids(v2)
        self.assertListEqual(ids, [13])

        # revert change
        EF_ALL_INTERVALS.rename_variable(v2, new_key=v1.key, new_type=v1.type)
        ids = EF_ALL_INTERVALS.find_ids(v1)
        self.assertListEqual(ids, [13])

    def test_rename_variable_invalid(self):
        v = Variable(interval="timestep", key="foo", type="", units="")
        out = EF_ALL_INTERVALS.rename_variable(v, new_key="NEW4", new_type="VARIABLE")
        self.assertIsNone(out)

    def test_rename_variable_invalid_names(self):
        v = Variable(
            interval="timestep",
            key="BLOCK2:ZONE1",
            type="Zone People Occupant Count",
            units="",
        )
        out = EF_ALL_INTERVALS.rename_variable(v, new_key="", new_type="")
        self.assertIsNone(out)

        ids = EF_ALL_INTERVALS.find_ids(v)
        self.assertListEqual(ids, [19])

    def test_add_output(self):
        id_, var = EF_ALL_INTERVALS.add_output("runperiod", "new", "type", "C", [1])
        self.assertTupleEqual(var, Variable("runperiod", "new", "type", "C"))
        EF_ALL_INTERVALS.remove_outputs(var)

    def test_add_two_outputs(self):
        id_, var1 = EF_ALL_INTERVALS.add_output("runperiod", "new", "type", "C", [1])
        self.assertTupleEqual(var1, Variable("runperiod", "new", "type", "C"))

        id_, var2 = EF_ALL_INTERVALS.add_output("runperiod", "new", "type", "C", [1])
        self.assertTupleEqual(var2, Variable("runperiod", "new (1)", "type", "C"))
        EF_ALL_INTERVALS.remove_outputs(var1)
        EF_ALL_INTERVALS.remove_outputs(var2)

    def test_add_output_test_tree(self):
        id_, var = EF_ALL_INTERVALS.add_output("runperiod", "new", "type", "C", [1])
        self.assertTupleEqual(var, Variable("runperiod", "new", "type", "C"))

        ids = EF_ALL_INTERVALS.search_tree.get_ids(*var)
        self.assertIsNot(ids, [])
        self.assertEqual(len(ids), 1)

        EF_ALL_INTERVALS.remove_outputs(var)
        ids = EF_ALL_INTERVALS.search_tree.get_ids(*var)
        self.assertEqual(ids, [])

    def test_add_output_invalid(self):
        out = EF_ALL_INTERVALS.add_output("timestep", "new", "type", "C", [1])
        self.assertIsNone(out)

    def test_add_output_invalid_interval(self):
        with self.assertRaises(KeyError):
            _ = EF_ALL_INTERVALS.add_output("foo", "new", "type", "C", [1])

    def test_aggregate_variables(self):
        v = Variable(
            interval="hourly", key=None, type="Zone People Occupant Count", units=""
        )
        id_, var = EF_ALL_INTERVALS.aggregate_variables(v, "sum")
        self.assertEqual(
            var,
            Variable(
                interval="hourly",
                key="Custom Key - sum",
                type="Zone People Occupant Count",
                units="",
            ),
        )
        EF_ALL_INTERVALS.remove_outputs(var)
        id_, var = EF_ALL_INTERVALS.aggregate_variables(
            v, "sum", new_key="foo", new_type="bar"
        )
        self.assertEqual(var, Variable(interval="hourly", key="foo", type="bar", units=""))
        EF_ALL_INTERVALS.remove_outputs(var)

    def test_aggregate_energy_rate(self):
        v1 = Variable("monthly", "CHILLER", "Chiller Electric Power", "W")
        v2 = Variable("monthly", "CHILLER", "Chiller Electric Energy", "J")

        id_, var = EF_ALL_INTERVALS.aggregate_variables([v1, v2], "sum")
        df = EF_ALL_INTERVALS.get_results(var)

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
        EF_ALL_INTERVALS.remove_outputs(var)

    def test_aggregate_invalid_variables(self):
        vars = [
            Variable("hourly", "invalid", "variable1", "units"),
            Variable("hourly", "invalid", "type", "units"),
        ]
        with self.assertRaises(CannotAggregateVariables):
            EF_ALL_INTERVALS.aggregate_variables(vars, "sum")

    def test_aggregate_energy_rate_invalid(self):
        ef = EsoFile(os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso"))
        ef.data.tables["monthly"].drop(N_DAYS_COLUMN, axis=1, inplace=True, level=0)

        v1 = Variable("monthly", "CHILLER", "Chiller Electric Power", "W")
        v2 = Variable("monthly", "CHILLER", "Chiller Electric Energy", "J")

        with self.assertRaises(CannotAggregateVariables):
            _ = ef.aggregate_variables([v1, v2], "sum")

    def test_aggregate_variables_too_much_vars(self):
        v = Variable(interval="hourly", key="BLOCK1:ZONE1", type=None, units=None)
        with self.assertRaises(CannotAggregateVariables):
            _ = EF_ALL_INTERVALS.aggregate_variables(v, "sum")

    def test_aggregate_variables_invalid_too_many_intervals(self):
        v = Variable(interval=None, key=None, type="Zone People Occupant Count", units="")
        with self.assertRaises(CannotAggregateVariables):
            _ = EF_ALL_INTERVALS.aggregate_variables(v, "sum")

    def test_as_df(self):
        df = EF_ALL_INTERVALS.as_df("hourly")
        self.assertTupleEqual(df.shape, (8760, 19))
        self.assertListEqual(df.columns.names, ["id", "interval", "key", "type", "units"])
        self.assertEqual(df.index.name, "timestamp")

    def test_as_df_invalid_interval(self):
        with self.assertRaises(KeyError):
            _ = EF_ALL_INTERVALS.as_df("foo")

    def test__find_pairs_by_id(self):
        pairs = EF_ALL_INTERVALS._find_pairs([31, 32, 297, 298, ])
        self.assertDictEqual({"timestep": [31, 297], "hourly": [32, 298]}, pairs)

    def test__find_pairs_by_interval_id(self):
        pairs = EF_ALL_INTERVALS._find_pairs(
            [("timestep", 31), ("hourly", 32), ("timestep", 297), ("hourly", 298)]
        )
        self.assertDictEqual({"timestep": [31, 297], "hourly": [32, 298]}, pairs)


if __name__ == "__main__":
    unittest.main()
