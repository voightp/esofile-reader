import unittest
import os
import pandas as pd
from pandas.testing import assert_frame_equal
from pandas.testing import assert_frame_equal, assert_index_equal
from esofile_reader import EsoFile, get_results
from esofile_reader.eso_file import PeaksNotIncluded
from esofile_reader.base_file import InvalidOutputType
from esofile_reader import Variable
from tests import ROOT


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        file_path1 = os.path.join(ROOT, "eso_files/eplusout1.eso")
        file_path2 = os.path.join(ROOT, "eso_files/eplusout2.eso")
        cls.ef1 = EsoFile(file_path1, ignore_peaks=True, report_progress=False)
        cls.ef2 = EsoFile(file_path2, ignore_peaks=False, report_progress=False)

    def test_get_results(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(self.ef1, v)

        test_names = ["key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples([("BLOCK1:ZONEA", "Zone Mean Air Temperature",
                                                   "C")], names=test_names)
        dates = [pd.datetime(2002, 4, 1), pd.datetime(2002, 5, 1), pd.datetime(2002, 6, 1),
                 pd.datetime(2002, 7, 1), pd.datetime(2002, 8, 1), pd.datetime(2002, 9, 1)]
        test_index = pd.MultiIndex.from_product([["eplusout1"], dates],
                                                names=["file", "timestamp"])

        test_df = pd.DataFrame([[22.592079], [24.163740],
                                [25.406725], [26.177191],
                                [25.619201], [23.862254]], columns=test_columns, index=test_index)

        assert_frame_equal(df, test_df)

    def test_get_results_from_path(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(os.path.join(ROOT, "eso_files/eplusout1.eso"), v)

        test_names = ["key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples([("BLOCK1:ZONEA", "Zone Mean Air Temperature",
                                                   "C")], names=test_names)
        dates = [pd.datetime(2002, 4, 1), pd.datetime(2002, 5, 1), pd.datetime(2002, 6, 1),
                 pd.datetime(2002, 7, 1), pd.datetime(2002, 8, 1), pd.datetime(2002, 9, 1)]
        test_index = pd.MultiIndex.from_product([["eplusout1"], dates],
                                                names=["file", "timestamp"])

        test_df = pd.DataFrame([[22.592079], [24.163740],
                                [25.406725], [26.177191],
                                [25.619201], [23.862254]], columns=test_columns, index=test_index)

        assert_frame_equal(df, test_df)

    def test_get_results_start_date(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(self.ef1, v, start_date=pd.datetime(2002, 4, 15))

        test_names = ["key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples([("BLOCK1:ZONEA", "Zone Mean Air Temperature",
                                                   "C")], names=test_names)
        dates = [pd.datetime(2002, 5, 1), pd.datetime(2002, 6, 1),
                 pd.datetime(2002, 7, 1), pd.datetime(2002, 8, 1), pd.datetime(2002, 9, 1)]
        test_index = pd.MultiIndex.from_product([["eplusout1"], dates],
                                                names=["file", "timestamp"])

        test_df = pd.DataFrame([[24.163740],
                                [25.406725], [26.177191],
                                [25.619201], [23.862254]], columns=test_columns, index=test_index)

        assert_frame_equal(df, test_df)

    def test_get_results_end_date(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(self.ef1, v, end_date=pd.datetime(2002, 8, 10))

        test_names = ["key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples([("BLOCK1:ZONEA", "Zone Mean Air Temperature",
                                                   "C")], names=test_names)
        dates = [pd.datetime(2002, 4, 1), pd.datetime(2002, 5, 1), pd.datetime(2002, 6, 1),
                 pd.datetime(2002, 7, 1), pd.datetime(2002, 8, 1)]
        test_index = pd.MultiIndex.from_product([["eplusout1"], dates],
                                                names=["file", "timestamp"])

        test_df = pd.DataFrame([[22.592079], [24.163740],
                                [25.406725], [26.177191],
                                [25.619201]], columns=test_columns, index=test_index)

        assert_frame_equal(df, test_df)

    def test_get_results_output_type_global_max(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(self.ef1, v, output_type="global_max")

        test_names = ["key", "variable", "units", "data"]
        test_columns = pd.MultiIndex.from_tuples([("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "value"),
                                                  ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "timestamp")],
                                                 names=test_names)
        test_index = pd.MultiIndex.from_product([["eplusout1"], [0]],
                                                names=["file", None])

        test_df = pd.DataFrame([[26.177191, pd.datetime(2002, 7, 1)]], columns=test_columns, index=test_index)

        assert_frame_equal(df, test_df)

    def test_get_results_output_type_start_end_date(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(self.ef1, v, output_type="global_max",
                         start_date=pd.datetime(2002, 4, 10), end_date=pd.datetime(2002, 6, 10))

        test_names = ["key", "variable", "units", "data"]
        test_columns = pd.MultiIndex.from_tuples([("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "value"),
                                                  ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "timestamp")],
                                                 names=test_names)
        test_index = pd.MultiIndex.from_product([["eplusout1"], [0]],
                                                names=["file", None])

        test_df = pd.DataFrame([[25.406725, pd.datetime(2002, 6, 1)]], columns=test_columns, index=test_index)

        assert_frame_equal(df, test_df)

    def test_get_results_output_type_global_min(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(self.ef1, v, output_type="global_min")

        test_names = ["key", "variable", "units", "data"]
        test_columns = pd.MultiIndex.from_tuples([("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "value"),
                                                  ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "timestamp")],
                                                 names=test_names)
        test_index = pd.MultiIndex.from_product([["eplusout1"], [0]],
                                                names=["file", None])

        test_df = pd.DataFrame([[22.592079, pd.datetime(2002, 4, 1)]], columns=test_columns, index=test_index)

        assert_frame_equal(df, test_df)

    def test_get_results_output_type_local_max(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(self.ef2, v, output_type="local_max")

        test_names = ["key", "variable", "units", "data"]
        test_columns = pd.MultiIndex.from_tuples([("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "value"),
                                                  ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "timestamp")],
                                                 names=test_names)
        dates = [pd.datetime(2002, 4, 1), pd.datetime(2002, 5, 1), pd.datetime(2002, 6, 1),
                 pd.datetime(2002, 7, 1), pd.datetime(2002, 8, 1), pd.datetime(2002, 9, 1)]
        test_index = pd.MultiIndex.from_product([["eplusout2"], dates],
                                                names=["file", "timestamp"])

        test_df = pd.DataFrame([[30.837382, pd.datetime(2002, 4, 20, 15, 30)],
                                [34.835386, pd.datetime(2002, 5, 26, 16, 0)],
                                [41.187972, pd.datetime(2002, 6, 30, 15, 30)],
                                [38.414505, pd.datetime(2002, 7, 21, 16, 0)],
                                [38.694873, pd.datetime(2002, 8, 18, 15, 30)],
                                [35.089822, pd.datetime(2002, 9, 15, 15, 0)]],
                               columns=test_columns, index=test_index)

        assert_frame_equal(df, test_df)

    def test_get_results_output_type_local_min(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(self.ef2, v, output_type="local_min")

        test_names = ["key", "variable", "units", "data"]
        test_columns = pd.MultiIndex.from_tuples([("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "value"),
                                                  ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "timestamp")],
                                                 names=test_names)
        dates = [pd.datetime(2002, 4, 1), pd.datetime(2002, 5, 1), pd.datetime(2002, 6, 1),
                 pd.datetime(2002, 7, 1), pd.datetime(2002, 8, 1), pd.datetime(2002, 9, 1)]
        test_index = pd.MultiIndex.from_product([["eplusout2"], dates],
                                                names=["file", "timestamp"])

        test_df = pd.DataFrame([[13.681526, pd.datetime(2002, 4, 10, 5, 30)],
                                [17.206312, pd.datetime(2002, 5, 7, 5, 30)],
                                [19.685125, pd.datetime(2002, 6, 12, 5, 0)],
                                [22.279566, pd.datetime(2002, 7, 4, 6, 0)],
                                [20.301202, pd.datetime(2002, 8, 31, 6, 0)],
                                [16.806496, pd.datetime(2002, 9, 24, 6, 0)]],
                               columns=test_columns, index=test_index)

        assert_frame_equal(df, test_df)

    def test_get_results_output_type_local_na(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        with self.assertRaises(PeaksNotIncluded):
            _ = get_results(self.ef1, v, output_type="local_min")

    def test_get_results_output_type_invalid(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        with self.assertRaises(InvalidOutputType):
            _ = get_results(self.ef1, v, output_type="foo")

    def test_get_results_add_file_name(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(self.ef1, v, add_file_name="")

        test_names = ["key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples([("BLOCK1:ZONEA", "Zone Mean Air Temperature",
                                                   "C")], names=test_names)
        dates = [pd.datetime(2002, 4, 1), pd.datetime(2002, 5, 1), pd.datetime(2002, 6, 1),
                 pd.datetime(2002, 7, 1), pd.datetime(2002, 8, 1), pd.datetime(2002, 9, 1)]
        test_index = pd.Index(dates, name="timestamp")

        test_df = pd.DataFrame([[22.592079], [24.163740],
                                [25.406725], [26.177191],
                                [25.619201], [23.862254]], columns=test_columns, index=test_index)

        assert_frame_equal(df, test_df)

    def test_get_results_include_interval(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(self.ef1, v, add_file_name="")

        test_names = ["key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples([("BLOCK1:ZONEA", "Zone Mean Air Temperature",
                                                   "C")], names=test_names)
        dates = [pd.datetime(2002, 4, 1), pd.datetime(2002, 5, 1), pd.datetime(2002, 6, 1),
                 pd.datetime(2002, 7, 1), pd.datetime(2002, 8, 1), pd.datetime(2002, 9, 1)]
        test_index = pd.Index(dates, name="timestamp")

        test_df = pd.DataFrame([[22.592079], [24.163740],
                                [25.406725], [26.177191],
                                [25.619201], [23.862254]], columns=test_columns, index=test_index)

        assert_frame_equal(df, test_df)

    def test_get_results_include_day(self):
        v = Variable("daily", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(self.ef1, v, add_file_name="", start_date=pd.datetime(2002, 4, 1),
                         end_date=pd.datetime(2002, 4, 6), include_day=True)

        test_names = ["key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples([("BLOCK1:ZONEA", "Zone Mean Air Temperature",
                                                   "C")], names=test_names)
        dates = [pd.datetime(2002, 4, 1), pd.datetime(2002, 4, 2), pd.datetime(2002, 4, 3),
                 pd.datetime(2002, 4, 4), pd.datetime(2002, 4, 5), pd.datetime(2002, 4, 6)]
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
        test_index = pd.MultiIndex.from_arrays([dates, days], names=["timestamp", "day"])

        test_df = pd.DataFrame([[22.620627], [22.796563],
                                [22.992970], [22.884158],
                                [22.308314], [22.097302]], columns=test_columns, index=test_index)

        assert_frame_equal(df, test_df)

    def test_get_results_units_system_si(self):
        pass

    def test_get_results_units_system_ip(self):
        pass

    def test_get_results_units_system_invalid(self):
        pass

    def test_get_results_rate_to_energy(self):
        pass

    def test_get_results_rate(self):
        pass

    def test_get_results_energy(self):
        pass

    def test_get_results_timestamp_format(self):
        pass

    def test_get_results_report_progress(self):
        pass

    def test_get_results_ignore_peaks(self):
        pass

    def test_suppress_errors(self):
        pass

    def test_multiple_files(self):
        pass

    def test_multiple_files_invalid_variable(self):
        pass
