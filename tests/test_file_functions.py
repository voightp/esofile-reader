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

    def test__create_header_mi(self):
        m = pd.MultiIndex.from_tuples([(13, "value")], names=["id", "data"])
        mi = self.ef._create_header_mi("timestep", m)
        test_mi = pd.MultiIndex(
            levels=[['13'], ['timestep'], ['BLOCK1:ZONE1'], ['Zone People Occupant Count'], [''], ["value"]],
            codes=[[0], [0], [0], [0], [0], [0]],
            names=["id", "interval", "key", "variable", "units", "data"]
        )
        assert_index_equal(mi, test_mi)

    def test__create_header_mi_list(self):
        mi = self.ef._create_header_mi("timestep", [13])
        test_mi = pd.MultiIndex(
            levels=[['13'], ['timestep'], ['BLOCK1:ZONE1'], ['Zone People Occupant Count'], ['']],
            codes=[[0], [0], [0], [0], [0]],
            names=["id", "interval", "key", "variable", "units"]
        )
        assert_index_equal(mi, test_mi)

    def test__add_remove_header_variable(self):
        v1 = self.ef._add_header_variable(-999, "timestep", "dummy", "variable", "foo")
        v2 = self.ef._add_header_variable(-1000, "timestep", "dummy", "variable", "foo")

        self.assertTupleEqual(v1, Variable(interval='timestep', key='dummy', variable='variable', units='foo'))
        self.assertTupleEqual(v2, Variable(interval='timestep', key='dummy (1)', variable='variable', units='foo'))

        self.ef._remove_header_variables("timestep", [-999, -1000])
        with self.assertRaises(KeyError):
            _ = self.ef.header["timestep"][-999]

    def test_add_header_variable(self):
        new_var0 = self.ef._add_header_variable(-1, "foo", "bar", "baz", "u")
        new_var1 = self.ef._add_header_variable(-2, "foo", "bar", "baz", "u")
        new_var2 = self.ef._add_header_variable(-3, "fo", "bar", "baz", "u")

        self.assertTupleEqual(new_var0, Variable("foo", "bar", "baz", "u"))
        self.assertTupleEqual(new_var1, Variable("foo", "bar (1)", "baz", "u"))
        self.assertTupleEqual(new_var2, Variable("fo", "bar", "baz", "u"))

        self.ef._remove_header_variables("foo", [-1, -2])
        self.ef._remove_header_variables("fo", [-3])

    def test_remove_header_variable_invalid(self):
        with self.assertRaises(KeyError):
            self.ef._remove_header_variables("foo", [-1, -2])

    def test_remove_output_variable_invalid(self):
        with self.assertRaises(KeyError):
            self.ef._remove_header_variables("foo", [-1, -2])

    def test_rename_variable(self):
        v = Variable(interval='timestep', key='BLOCK1:ZONE1', variable='Zone People Occupant Count', units='')
        self.ef.rename_variable(v, key_name="NEW", var_name="VARIABLE")

    def test_add_output_invalid(self):
        out = self.ef.add_output("timestep", "new", "variable", "C", [1])
        self.assertIsNone(out)

    def test_add_output_invalid_interval(self):
        out = self.ef.add_output("foo", "new", "variable", "C", [1])
        self.assertIsNone(out)

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
