import os
import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from esofile_reader import EsoFile, Variable, ResultsFile
from esofile_reader.base_file import CannotAggregateVariables
from esofile_reader.constants import SPECIAL, ID_LEVEL
from tests import ROOT, EF_ALL_INTERVALS, EF_ALL_INTERVALS_PEAKS, EF1


class TestFileFunctions(unittest.TestCase):
    def test_table_names(self):
        self.assertListEqual(
            EF_ALL_INTERVALS.table_names,
            ["timestep", "hourly", "daily", "monthly", "runperiod", "annual"],
        )

    def test_can_convert_rate_to_energy(self):
        pairs = [
            ("timestep", True),
            ("hourly", True),
            ("daily", True),
            ("monthly", True),
            ("runperiod", True),
            ("annual", True),
        ]
        for table, can_convert in pairs:
            self.assertEqual(can_convert, EF_ALL_INTERVALS.can_convert_rate_to_energy(table))

    def test_all_ids(self):
        self.assertEqual(len(EF_ALL_INTERVALS.tables.get_all_variable_ids()), 114)

    def test_created(self):
        self.assertTrue(isinstance(EF_ALL_INTERVALS.file_created, datetime))

    def test_complete(self):
        self.assertTrue(EF_ALL_INTERVALS.complete)
        self.assertIsNone(EF_ALL_INTERVALS.peak_outputs)

    def test_peak_complete(self):
        self.assertTrue(EF_ALL_INTERVALS_PEAKS.complete)
        self.assertIsNotNone(EF_ALL_INTERVALS_PEAKS.peak_outputs)

    def test_header_df(self):
        names = ["id", "table", "key", "type", "units"]
        self.assertEqual(
            EF_ALL_INTERVALS.tables.get_all_variables_df().columns.to_list(), names
        )
        self.assertEqual(len(EF_ALL_INTERVALS.tables.get_all_variables_df().index), 114)

        frames = []
        for table in EF_ALL_INTERVALS.table_names:
            frames.append(EF_ALL_INTERVALS.get_header_df(table))
        df = pd.concat(frames, axis=0)
        assert_frame_equal(df, EF_ALL_INTERVALS.tables.get_all_variables_df())

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
            table="timestep", key="BLOCK1:ZONE1", type="Zone People Occupant Count", units="",
        )
        ids = EF_ALL_INTERVALS.find_id(v, part_match=False)
        self.assertEqual(ids, [13])

    def test_find_ids_part_match(self):
        v = Variable(
            table="timestep", key="BLOCK1", type="Zone People Occupant Count", units=""
        )
        ids = EF_ALL_INTERVALS.find_id(v, part_match=True)
        self.assertEqual(ids, [13])

    def test_find_ids_part_invalid(self):
        v = Variable(
            table="time", key="BLOCK1:ZONE1", type="Zone People Occupant Count", units="",
        )
        ids = EF_ALL_INTERVALS.find_id(v, part_match=False)
        self.assertEqual(ids, [])

    def test__find_pairs(self):
        v = Variable(
            table="timestep", key="BLOCK1:ZONE1", type="Zone People Occupant Count", units="",
        )
        out = EF_ALL_INTERVALS._find_pairs(v, part_match=False)
        self.assertDictEqual(out, {"timestep": [13]})

    def test__find_pairs_part_match(self):
        v = Variable(
            table="timestep", key="BLOCK1", type="Zone People Occupant Count", units=""
        )
        out = EF_ALL_INTERVALS._find_pairs(v, part_match=True)
        self.assertDictEqual(out, {"timestep": [13]})

    def test__find_pairs_invalid(self):
        v = Variable(
            table="timestep", key="BLOCK1", type="Zone People Occupant Count", units=""
        )
        out = EF_ALL_INTERVALS._find_pairs(v, part_match=False)
        self.assertDictEqual(out, {})

    def test_create_new_header_variable(self):
        v1 = EF_ALL_INTERVALS.create_header_variable("timestep", "dummy", "foo", type_="type")
        self.assertTupleEqual(
            v1, Variable(table="timestep", key="dummy", type="type", units="foo")
        )

    def test_create_new_header_variable_wrong_type(self):
        with self.assertRaises(TypeError):
            _ = EF_ALL_INTERVALS.create_header_variable("timestep", "dummy", "foo")

    def test_rename_variable(self):
        v1 = Variable(
            table="timestep", key="BLOCK1:ZONE1", type="Zone People Occupant Count", units="",
        )
        EF_ALL_INTERVALS.rename_variable(v1, new_key="NEW3", new_type="VARIABLE")

        v2 = Variable(table="timestep", key="NEW3", type="VARIABLE", units="")
        ids = EF_ALL_INTERVALS.find_id(v2)
        self.assertListEqual(ids, [13])

        # revert change
        EF_ALL_INTERVALS.rename_variable(v2, new_key=v1.key, new_type=v1.type)
        ids = EF_ALL_INTERVALS.find_id(v1)
        self.assertListEqual(ids, [13])

    def test_rename_variable_invalid(self):
        v = Variable(table="timestep", key="foo", type="", units="")
        out = EF_ALL_INTERVALS.rename_variable(v, new_key="NEW4", new_type="VARIABLE")
        self.assertIsNone(out)

    def test_rename_variable_invalid_names(self):
        v = Variable(
            table="timestep", key="BLOCK2:ZONE1", type="Zone People Occupant Count", units="",
        )
        out = EF_ALL_INTERVALS.rename_variable(v)
        self.assertIsNone(out)

        ids = EF_ALL_INTERVALS.find_id(v)
        self.assertListEqual(ids, [19])

    def test_add_output(self):
        id_, var = EF_ALL_INTERVALS.insert_variable("runperiod", "new", "C", [1], type_="type")
        self.assertTupleEqual(var, Variable("runperiod", "new", "type", "C"))
        EF_ALL_INTERVALS.remove_variables(var)

    def test_add_two_outputs(self):
        id_, var1 = EF_ALL_INTERVALS.insert_variable("runperiod", "new", "C", [1], "type")
        self.assertTupleEqual(var1, Variable("runperiod", "new", "type", "C"))

        id_, var2 = EF_ALL_INTERVALS.insert_variable("runperiod", "new", "C", [1], "type")
        self.assertTupleEqual(var2, Variable("runperiod", "new (1)", "type", "C"))
        EF_ALL_INTERVALS.remove_variables(var1)
        EF_ALL_INTERVALS.remove_variables(var2)

    def test_add_output_test_tree(self):
        id_, var = EF_ALL_INTERVALS.insert_variable("runperiod", "new", "C", [1], type_="type")
        self.assertTupleEqual(var, Variable("runperiod", "new", "type", "C"))

        ids = EF_ALL_INTERVALS.search_tree.find_ids(var)
        self.assertIsNot(ids, [])
        self.assertEqual(len(ids), 1)

        EF_ALL_INTERVALS.remove_variables(var)
        ids = EF_ALL_INTERVALS.search_tree.find_ids(var)
        self.assertEqual(ids, [])

    def test_add_output_invalid(self):
        out = EF_ALL_INTERVALS.insert_variable("timestep", "new", "type", "C", [1])
        self.assertIsNone(out)

    def test_add_output_invalid_table(self):
        with self.assertRaises(KeyError):
            _ = EF_ALL_INTERVALS.insert_variable("foo", "new", "type", "C", [1])

    def test_aggregate_variables(self):
        v = Variable(table="hourly", key=None, type="Zone People Occupant Count", units="")
        id_, var = EF_ALL_INTERVALS.aggregate_variables(v, "sum")
        self.assertEqual(
            var,
            Variable(
                table="hourly",
                key="Custom Key - sum",
                type="Zone People Occupant Count",
                units="",
            ),
        )
        EF_ALL_INTERVALS.remove_variables(var)
        id_, var = EF_ALL_INTERVALS.aggregate_variables(v, "sum", new_key="foo", new_type="bar")
        self.assertEqual(var, Variable(table="hourly", key="foo", type="bar", units=""))
        EF_ALL_INTERVALS.remove_variables(var)

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
        EF_ALL_INTERVALS.remove_variables(var)

    def test_aggregate_energy_rate_hourly(self):
        v1 = Variable("hourly", "CHILLER", "Chiller Electric Power", "W")
        v2 = Variable("hourly", "CHILLER", "Chiller Electric Energy", "J")
        test_sr = EF_ALL_INTERVALS.get_results([v1, v2], rate_to_energy=True).sum(axis=1)
        test_df = pd.DataFrame(test_sr)
        test_mi = pd.MultiIndex.from_tuples(
            [("Custom Key - sum", "Custom Variable", "J")], names=["key", "type", "units"]
        )
        test_df.columns = test_mi

        id_, var = EF_ALL_INTERVALS.aggregate_variables([v1, v2], "sum")
        df = EF_ALL_INTERVALS.get_results(id_)
        assert_frame_equal(test_df, df)

        EF_ALL_INTERVALS.remove_variables(var)

    def test_aggregate_invalid_variables(self):
        vars = [
            Variable("hourly", "invalid", "variable1", "units"),
            Variable("hourly", "invalid", "type", "units"),
        ]
        with self.assertRaises(CannotAggregateVariables):
            EF_ALL_INTERVALS.aggregate_variables(vars, "sum")

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
            _ = EF_ALL_INTERVALS.aggregate_variables(v, "sum")

    def test_aggregate_variables_invalid_too_many_tables(self):
        v = Variable(table=None, key=None, type="Zone People Occupant Count", units="")
        with self.assertRaises(CannotAggregateVariables):
            _ = EF_ALL_INTERVALS.aggregate_variables(v, "sum")

    def test_as_df(self):
        df = EF_ALL_INTERVALS.get_numeric_table("hourly")
        self.assertTupleEqual(df.shape, (8760, 19))
        self.assertListEqual(df.columns.names, ["id", "table", "key", "type", "units"])
        self.assertEqual(df.index.name, "timestamp")

    def test_as_df_invalid_table(self):
        with self.assertRaises(KeyError):
            _ = EF_ALL_INTERVALS.get_numeric_table("foo")

    def test__find_pairs_by_id(self):
        pairs = EF_ALL_INTERVALS._find_pairs([31, 32, 297, 298, ])
        self.assertDictEqual({"timestep": [31, 297], "hourly": [32, 298]}, pairs)

    def test__find_pairs_unexpected_type(self):
        with self.assertRaises(TypeError):
            _ = EF_ALL_INTERVALS._find_pairs(
                [("timestep", 31), ("hourly", 32), ("timestep", 297), ("hourly", 298)]
            )

    def test_to_excel(self):
        p = Path("test.xlsx")
        try:
            EF1.to_excel(p)
            self.assertTrue(p.exists())
            test_ef = ResultsFile.from_excel(p)
            for table_name in EF1.table_names:
                df = EF1.tables[table_name].copy()
                test_df = test_ef.tables[table_name]
                df.columns = df.columns.droplevel(ID_LEVEL)
                test_df.columns = test_df.columns.droplevel(ID_LEVEL)

                print(df)
                print(test_df)

                assert_frame_equal(df, test_df, check_dtype=False)

        finally:
            p.unlink()
