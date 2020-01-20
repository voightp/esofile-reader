import unittest
import os
import pandas as pd
from pandas.testing import assert_frame_equal, assert_index_equal
from datetime import datetime
from esofile_reader import EsoFile
from esofile_reader.base_file import CannotAggregateVariables
from esofile_reader import Variable
from tests import ROOT


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        file_path = os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso")
        cls.ef = EsoFile(file_path, ignore_peaks=True, report_progress=False)
        cls.ef_peaks = EsoFile(file_path, ignore_peaks=False, report_progress=False)

    def test_available_intervals(self):
        self.assertListEqual(self.ef.available_intervals,
                             ['timestep', 'hourly', 'daily',
                              'monthly', 'runperiod', 'annual'])

    def test_all_ids(self):
        self.assertEqual(len(self.ef.all_ids), 114)

    def test_created(self):
        self.assertTrue(isinstance(self.ef.created, datetime))

    def test_complete(self):
        self.assertTrue(self.ef.complete)
        self.assertIsNone(self.ef.peak_outputs)

    def test_peak_complete(self):
        self.assertTrue(self.ef_peaks.complete)
        self.assertIsNotNone(self.ef_peaks.peak_outputs)

    def test_header_df(self):
        self.assertEqual(self.ef.header_df.columns.to_list(), ["id", "interval", "key",
                                                               "variable", "units"])
        self.assertEqual(len(self.ef.header_df.index), 114)

    def test_rename(self):
        original = self.ef.file_name
        self.ef.rename("foo")
        self.assertEqual(self.ef.file_name, "foo")
        self.ef.rename(original)

    def test__add_file_name_row(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        out = self.ef._add_file_name(df, "row")
        mi = pd.MultiIndex.from_product([["eplusout_all_intervals"], index],
                                        names=["file", "timestamp"])
        assert_frame_equal(out, pd.DataFrame({"a": [1, 2, 3],
                                              "c": [4, 5, 6]}, index=mi))

    def test__add_file_name_column(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        out = self.ef._add_file_name(df, "column")
        mi = pd.MultiIndex.from_product([["eplusout_all_intervals"], ["a", "c"]],
                                        names=["file", None])
        assert_frame_equal(out, pd.DataFrame([[1, 4], [2, 5], [3, 6]],
                                             index=index, columns=mi))

    def test__add_file_name_invalid(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        out = self.ef._add_file_name(df, "foo")
        mi = pd.MultiIndex.from_product([["eplusout_all_intervals"], index],
                                        names=["file", "timestamp"])
        assert_frame_equal(out, pd.DataFrame({"a": [1, 2, 3],
                                              "c": [4, 5, 6]}, index=mi))

    def test__merge_frame(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df1 = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        df2 = pd.DataFrame({"b": [1, 2, 3]}, index=index)

        mi = pd.MultiIndex.from_product([["eplusout_all_intervals"], index],
                                        names=["file", "timestamp"])
        df = self.ef._merge_frame([df1, df2], add_file_name="row")
        assert_frame_equal(df, pd.DataFrame({"a": [1, 2, 3],
                                             "c": [4, 5, 6],
                                             "b": [1, 2, 3]}, index=mi))

    def test__merge_frame_update_dt(self):
        index = pd.Index(pd.date_range("1/1/2002", freq="d", periods=3), name="timestamp")
        df1 = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]}, index=index)
        df2 = pd.DataFrame({"b": [1, 2, 3]}, index=index)

        mi = pd.MultiIndex.from_product([["eplusout_all_intervals"], ["01-01", "02-01", "03-01"]],
                                        names=["file", "timestamp"])
        df = self.ef._merge_frame([df1, df2], add_file_name="row", timestamp_format="%d-%m")
        assert_frame_equal(df, pd.DataFrame({"a": [1, 2, 3],
                                             "c": [4, 5, 6],
                                             "b": [1, 2, 3]}, index=mi))

    def test_find_ids(self):
        v = Variable(interval='timestep', key='BLOCK1:ZONE1', variable='Zone People Occupant Count', units='')
        ids = self.ef.find_ids(v, part_match=False)
        self.assertEqual(ids, [13])

    def test_find_ids_part_match(self):
        v = Variable(interval='timestep', key='BLOCK1', variable='Zone People Occupant Count', units='')
        ids = self.ef.find_ids(v, part_match=True)
        self.assertEqual(ids, [13])

    def test_find_ids_part_invalid(self):
        v = Variable(interval='time', key='BLOCK1:ZONE1', variable='Zone People Occupant Count', units='')
        ids = self.ef.find_ids(v, part_match=False)
        self.assertEqual(ids, [])

    def test__find_pairs(self):
        v = Variable(interval='timestep', key='BLOCK1:ZONE1', variable='Zone People Occupant Count', units='')
        out = self.ef._find_pairs(v, part_match=False)
        self.assertDictEqual(out, {"timestep": [13]})

    def test__find_pairs_part_match(self):
        v = Variable(interval='timestep', key='BLOCK1', variable='Zone People Occupant Count', units='')
        out = self.ef._find_pairs(v, part_match=True)
        self.assertDictEqual(out, {"timestep": [13]})

    def test__find_pairs_invalid(self):
        v = Variable(interval='timestep', key='BLOCK1', variable='Zone People Occupant Count', units='')
        out = self.ef._find_pairs(v, part_match=False)
        self.assertDictEqual(out, {})

    def test__new_header_variable(self):
        v1 = self.ef._new_header_variable("timestep", "dummy", "variable", "foo")

        self.assertTupleEqual(v1, Variable(interval='timestep', key='dummy', variable='variable', units='foo'))

    def test_rename_variable(self):
        v = Variable(interval='timestep', key='BLOCK1:ZONE1', variable='Zone People Occupant Count', units='')
        self.ef.rename_variable(v, key_name="NEW", var_name="VARIABLE")

    def test_add_output(self):
        id_, var = self.ef.add_output("runperiod", "new", "variable", "C", [1])
        self.assertTupleEqual(var, Variable("runperiod", "new", "variable", "C"))
        self.ef.remove_outputs(var)

    def test_add_output_test_tree(self):
        id_, var = self.ef.add_output("runperiod", "new", "variable", "C", [1])
        self.assertTupleEqual(var, Variable("runperiod", "new", "variable", "C"))

        ids = self.ef._search_tree.get_ids(*var)
        self.assertIsNot(ids, [])
        self.assertEqual(len(ids), 1)

        self.ef.remove_outputs(var)
        ids = self.ef._search_tree.get_ids(*var)
        self.assertEqual(ids, [])

    def test_add_output_duplicate(self):
        out = self.ef.add_output("timestep", "new", "variable", "C", [1])
        self.assertIsNone(out)

    def test_add_output_invalid(self):
        out = self.ef.add_output("timestep", "new", "variable", "C", [1])
        self.assertIsNone(out)

    def test_add_output_invalid_interval(self):
        with self.assertRaises(KeyError):
            _ = self.ef.add_output("foo", "new", "variable", "C", [1])

    def test_aggregate_variables(self):
        v = Variable(interval='hourly', key=None, variable='Zone People Occupant Count', units='')
        id_, var = self.ef.aggregate_variables(v, "sum")
        self.assertEqual(var, Variable(interval='hourly', key='Custom Key - sum',
                                       variable='Zone People Occupant Count', units=''))
        self.ef.remove_outputs(var)
        id_, var = self.ef.aggregate_variables(v, "sum", key_name="foo", var_name="bar")
        self.assertEqual(var, Variable(interval='hourly', key='foo', variable='bar', units=''))
        self.ef.remove_outputs(var)

    def test_aggregate_variables_too_much_vars(self):
        v = Variable(interval='hourly', key="BLOCK1:ZONE1", variable=None, units=None)
        with self.assertRaises(CannotAggregateVariables):
            _ = self.ef.aggregate_variables(v, "sum")

    def test_aggregate_variables_invalid_too_many_intervals(self):
        v = Variable(interval=None, key=None, variable='Zone People Occupant Count', units='')
        with self.assertRaises(CannotAggregateVariables):
            _ = self.ef.aggregate_variables(v, "sum")

    def test_as_df(self):
        df = self.ef.as_df("hourly")
        self.assertTupleEqual(df.shape, (8760, 19))
        self.assertListEqual(df.columns.names, ["id", "interval", "key",
                                                "variable", "units"])
        self.assertEqual(df.index.name, "timestamp")

    def test_as_df_invalid_interval(self):
        with self.assertRaises(KeyError):
            _ = self.ef.as_df("foo")


if __name__ == '__main__':
    unittest.main()
