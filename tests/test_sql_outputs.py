import unittest
import os
import pandas as pd
from pandas.testing import assert_frame_equal, assert_index_equal

from tests import ROOT
from esofile_reader import EsoFile, Variable
from esofile_reader.outputs.sql_outputs import SQLOutputs


class TestDFOutputs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        file_path = os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso")
        ef = EsoFile(file_path, ignore_peaks=True, report_progress=False)
        SQLOutputs.set_up_db()

        cls.sql_file = SQLOutputs.store_file(ef)

    def test_get_available_intervals(self):
        intervals = self.sql_file.data.get_available_intervals()
        self.assertListEqual(
            intervals,
            ["timestep", "hourly", "daily", "monthly", "runperiod", "annual"]
        )

    def test_get_datetime_index(self):
        index = self.sql_file.data.get_datetime_index("monthly")
        assert_index_equal(index, pd.DatetimeIndex(['2002-01-01', '2002-02-01', '2002-03-01', '2002-04-01',
                                                    '2002-05-01', '2002-06-01', '2002-07-01', '2002-08-01',
                                                    '2002-09-01', '2002-10-01', '2002-11-01', '2002-12-01'],
                                                   dtype='datetime64[ns]', name='timestamp', freq=None))

    def test_get_all_variables_dct(self):
        variables = self.sql_file.data.get_all_variables_dct()
        self.assertListEqual(
            list(variables.keys()),
            ["timestep", "hourly", "daily", "monthly", "runperiod", "annual"]
        )

    def test_get_variables_dct(self):
        variables = self.sql_file.data.get_variables_dct("daily")
        self.assertListEqual(
            list(variables.keys()),
            [9, 15, 21, 27, 33, 299, 305, 311, 317, 323,
             329, 335, 341, 433, 477, 521, 565, 952, 982]
        )

    def test_get_variable_ids(self):
        ids = self.sql_file.data.get_variable_ids("daily")
        self.assertListEqual(
            ids,
            [9, 15, 21, 27, 33, 299, 305, 311, 317, 323,
             329, 335, 341, 433, 477, 521, 565, 952, 982]
        )

    def test_get_all_variable_ids(self):
        ids = self.sql_file.data.get_all_variable_ids()
        self.assertListEqual(
            ids,
            [7, 13, 19, 25, 31, 297, 303, 309, 315, 321, 327, 333, 339, 431, 475, 519, 563, 950, 956, 8, 14, 20, 26, 32,
             298, 304, 310, 316, 322, 328, 334, 340, 432, 476, 520, 564, 951, 981, 9, 15, 21, 27, 33, 299, 305, 311,
             317, 323, 329, 335, 341, 433, 477, 521, 565, 952, 982, 10, 16, 22, 28, 34, 300, 306, 312, 318, 324, 330,
             336, 342, 434, 478, 522, 566, 953, 983, 11, 17, 23, 29, 35, 301, 307, 313, 319, 325, 331, 337,
             343, 435, 479, 523, 567, 954, 984, 12, 18, 24, 30, 36, 302, 308, 314, 320, 326, 332, 338, 344, 436, 480,
             524, 568, 955, 985]
        )

    def test_get_variables_df(self):
        df = self.sql_file.data.get_variables_df("daily")
        self.assertListEqual(
            df.columns.tolist(),
            ["id", "interval", "key", "variable", "units"]
        )
        self.assertTupleEqual(
            df.shape,
            (19, 5)
        )

    def test_all_variables_df(self):
        df = self.sql_file.data.get_all_variables_df()
        self.assertListEqual(
            df.columns.tolist(),
            ["id", "interval", "key", "variable", "units"]
        )
        self.assertTupleEqual(
            df.shape,
            (114, 5)
        )

    # def test_rename_variable(self):
    #     self.ef.data.rename_variable("timestep", 7, "FOO", "BAR")
    #     col1 = self.ef.data.tables["timestep"].loc[:, (7, "timestep", "FOO", "BAR", "W/m2")]
    #
    #     self.ef.data.rename_variable("timestep", 7, "Environment",
    #                                  "Site Diffuse Solar Radiation Rate per Area")
    #     col2 = self.ef.data.tables["timestep"].loc[:, (7, "timestep", "Environment",
    #                                                    "Site Diffuse Solar Radiation Rate per Area", "W/m2")]
    #     self.assertListEqual(col1.tolist(), col2.tolist())
    #
    # def test_add_remove_variable(self):
    #     id_ = self.ef.data.add_variable(Variable("monthly", "FOO", "BAR", "C"), list(range(12)))
    #     self.ef.data.remove_variables("monthly", [id_])
    #     with self.assertRaises(KeyError):
    #         _ = self.ef.data.tables["monthly"][id_]
    #
    # def test_remove_variable_invalid(self):
    #     with self.assertRaises(KeyError):
    #         self.ef.data.remove_variables("monthly", [100000])
    #
    # def test_get_special_column_invalid(self):
    #     with self.assertRaises(KeyError):
    #         self.ef.data.get_special_column("FOO", "timestep")
    #
    # def test_get_number_of_days(self):
    #     col = self.ef.data.get_number_of_days("runperiod")
    #     self.assertEqual(col[0], 365)
    #     self.assertEqual(col.size, 1)
    #
    # def test_get_days_of_week(self):
    #     col = self.ef.data.get_days_of_week("daily")
    #     self.assertEqual(col[0], "Tuesday")
    #     self.assertEqual(col.size, 365)

    # def test_get_all_results(self):
    #     df = self.sql_file.data.get_all_results("daily")
    #     self.assertTupleEqual(df.shape, (365, 19))
    #
    # def test_get_results(self):
    #     df = self.ef.data.get_results("monthly", [324, 983])
    #     test_columns = pd.MultiIndex.from_tuples([(324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C"),
    #                                               (983, "monthly", "CHILLER", "Chiller Electric Energy", "J")],
    #                                              names=["id", "interval", "key", "variable", "units"])
    #     test_index = pd.Index([pd.datetime(2002, i, 1) for i in range(1, 13)], name="timestamp")
    #     test_df = pd.DataFrame([
    #         [18.948067, 2.582339e+08],
    #         [18.879265, 6.594828e+08],
    #         [20.987345, 1.805162e+09],
    #         [23.129456, 2.573239e+09],
    #         [24.993765, 3.762886e+09],
    #         [26.255885, 3.559705e+09],
    #         [27.007450, 5.093662e+09],
    #         [26.448572, 4.479418e+09],
    #         [24.684673, 3.334583e+09],
    #         [22.725196, 2.615657e+09],
    #         [20.549040, 1.485742e+09],
    #         [18.520034, 1.945721e+08]
    #     ], columns=test_columns, index=test_index)
    #
    #     # need to drop id as pandas does not treat Index([324, 983])
    #     # and IndexInt64([324, 983]) as identical
    #     df = df.droplevel("id", axis=1)
    #     test_df = test_df.droplevel("id", axis=1)
    #
    #     assert_frame_equal(df, test_df)
    #
    # def test_get_results_sliced(self):
    #     df = self.ef.data.get_results("monthly", [324, 983],
    #                                   start_date=pd.datetime(2002, 4, 1),
    #                                   end_date=pd.datetime(2002, 6, 1))
    #     test_columns = pd.MultiIndex.from_tuples([(324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C"),
    #                                               (983, "monthly", "CHILLER", "Chiller Electric Energy", "J")],
    #                                              names=["id", "interval", "key", "variable", "units"])
    #     test_index = pd.Index([pd.datetime(2002, i, 1) for i in range(4, 7)], name="timestamp")
    #     test_df = pd.DataFrame([
    #         [23.129456, 2.573239e+09],
    #         [24.993765, 3.762886e+09],
    #         [26.255885, 3.559705e+09],
    #     ], columns=test_columns, index=test_index)
    #
    #     # need to drop id as pandas does not treat Index([324, 983])
    #     # and IndexInt64([324, 983]) as identical
    #     df = df.droplevel("id", axis=1)
    #     test_df = test_df.droplevel("id", axis=1)
    #
    #     assert_frame_equal(df, test_df)
    #
    # def test_get_results_include_day(self):
    #     df = self.ef.data.get_results("monthly", [324, 983],
    #                                   start_date=pd.datetime(2002, 4, 1),
    #                                   end_date=pd.datetime(2002, 6, 1),
    #                                   include_day=True)
    #     test_columns = pd.MultiIndex.from_tuples([(324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C"),
    #                                               (983, "monthly", "CHILLER", "Chiller Electric Energy", "J")],
    #                                              names=["id", "interval", "key", "variable", "units"])
    #
    #     # days of week are picked up from actual date when not available on df
    #     test_index = pd.MultiIndex.from_arrays(
    #         [[pd.datetime(2002, i, 1) for i in range(4, 7)],
    #          ["Monday", "Wednesday", "Saturday"]], names=["timestamp", "day"])
    #     test_df = pd.DataFrame([
    #         [23.129456, 2.573239e+09],
    #         [24.993765, 3.762886e+09],
    #         [26.255885, 3.559705e+09],
    #     ], columns=test_columns, index=test_index)
    #
    #     # need to drop id as pandas does not treat Index([324, 983])
    #     # and IndexInt64([324, 983]) as identical
    #     df = df.droplevel("id", axis=1)
    #     test_df = test_df.droplevel("id", axis=1)
    #
    #     assert_frame_equal(df, test_df)
    #
    # def test_get_results_invalid_ids(self):
    #     with self.assertRaises(KeyError):
    #         _ = self.ef.data.get_results("daily", [7])
    #
    # def test_get_global_max_results(self):
    #     df = self.ef.data.get_global_max_results("monthly", [324, 983])
    #     test_columns = pd.MultiIndex.from_tuples(
    #         [(324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C", "value"),
    #          (324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C", "timestamp"),
    #          (983, "monthly", "CHILLER", "Chiller Electric Energy", "J", "value"),
    #          (983, "monthly", "CHILLER", "Chiller Electric Energy", "J", "timestamp")],
    #         names=["id", "interval", "key", "variable", "units", "data"])
    #     test_df = pd.DataFrame([
    #         [27.007450, pd.datetime(2002, 7, 1), 5.093662e+09, pd.datetime(2002, 7, 1)],
    #     ], columns=test_columns)
    #
    #     # need to drop id as pandas does not treat Index([324, 983])
    #     # and IndexInt64([324, 983]) as identical
    #     df = df.droplevel("id", axis=1)
    #     test_df = test_df.droplevel("id", axis=1)
    #     assert_frame_equal(df, test_df)
    #
    # def test_get_global_min_results(self):
    #     df = self.ef.data.get_global_min_results("monthly", [324, 983])
    #     test_columns = pd.MultiIndex.from_tuples(
    #         [(324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C", "value"),
    #          (324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C", "timestamp"),
    #          (983, "monthly", "CHILLER", "Chiller Electric Energy", "J", "value"),
    #          (983, "monthly", "CHILLER", "Chiller Electric Energy", "J", "timestamp")],
    #         names=["id", "interval", "key", "variable", "units", "data"])
    #     test_df = pd.DataFrame([
    #         [18.520034, pd.datetime(2002, 12, 1), 1.945721e+08, pd.datetime(2002, 12, 1)],
    #     ], columns=test_columns)
    #
    #     # need to drop id as pandas does not treat Index([324, 983])
    #     # and IndexInt64([324, 983]) as identical
    #     df = df.droplevel("id", axis=1)
    #     test_df = test_df.droplevel("id", axis=1)
    #     assert_frame_equal(df, test_df)
