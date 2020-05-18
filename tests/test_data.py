import unittest
from datetime import datetime

import numpy as np
import pandas as pd
from pandas.testing import assert_index_equal, assert_frame_equal
from parameterized import parameterized

from esofile_reader.data.df_functions import sr_dt_slicer, df_dt_slicer, sort_by_ids
from esofile_reader.mini_classes import Variable
from esofile_reader.storage.pqt_storage import ParquetStorage
from esofile_reader.storage.sql_storage import SQLStorage
from tests import EF_ALL_INTERVALS


class TestCommonData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dfs = ParquetStorage()
        id_ = cls.dfs.store_file(EF_ALL_INTERVALS)
        dff = cls.dfs.files[id_]

        cls.pqs = ParquetStorage()
        id_ = cls.pqs.store_file(EF_ALL_INTERVALS)
        pqf = cls.pqs.files[id_]

        cls.sqls = SQLStorage()
        id_ = cls.sqls.store_file(EF_ALL_INTERVALS)
        sqlf = cls.sqls.files[id_]

        cls.files = {
            "dff": dff,
            "pqf": pqf,
            "sqlf": sqlf
        }

        cls.data = {
            "dfd": dff.data,
            "pqd": pqf.data,
            "sqld": sqlf.data
        }

    @classmethod
    def tearDownClass(cls):
        cls.files["pqf"].clean_up()
        cls.files["pqf"] = None

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_is_simple(self, key):
        data = self.data[key]
        intervals = data.get_available_intervals()
        for interval in intervals:
            self.assertFalse(data.is_simple(interval))

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_levels(self, key):
        data = self.data[key]
        intervals = data.get_available_intervals()
        for interval in intervals:
            self.assertListEqual(
                ["id", "interval", "key", "type", "units"], data.get_levels(interval)
            )

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_available_intervals(self, key):
        data = self.data[key]
        intervals = data.get_available_intervals()
        self.assertListEqual(
            intervals, ["timestep", "hourly", "daily", "monthly", "runperiod", "annual"]
        )

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_datetime_index(self, key):
        data = self.data[key]
        index = data.get_datetime_index("monthly")
        assert_index_equal(
            index,
            pd.DatetimeIndex(
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
            ),
        )

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_variables_dct(self, key):
        data = self.data[key]
        variables = data.get_variables_dct("daily")
        self.assertListEqual(
            list(variables.keys()),
            [
                9,
                15,
                21,
                27,
                33,
                299,
                305,
                311,
                317,
                323,
                329,
                335,
                341,
                433,
                477,
                521,
                565,
                952,
                982,
            ],
        )

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_variable_ids(self, key):
        data = self.data[key]
        ids = data.get_variable_ids("daily")
        self.assertListEqual(
            ids,
            [
                9,
                15,
                21,
                27,
                33,
                299,
                305,
                311,
                317,
                323,
                329,
                335,
                341,
                433,
                477,
                521,
                565,
                952,
                982,
            ],
        )

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_all_variable_ids(self, key):
        data = self.data[key]
        ids = data.get_all_variable_ids()
        # fmt: off
        self.assertListEqual(
            ids,
            [
                7, 13, 19, 25, 31, 297, 303, 309, 315, 321, 327, 333, 339, 431, 475, 519, 563,
                950, 956, 8, 14, 20, 26, 32, 298, 304, 310, 316, 322, 328, 334, 340, 432, 476,
                520, 564, 951, 981, 9, 15, 21, 27, 33, 299, 305, 311, 317, 323, 329, 335, 341,
                433, 477, 521, 565, 952, 982, 10, 16, 22, 28, 34, 300, 306, 312, 318, 324, 330,
                336, 342, 434, 478, 522, 566, 953, 983, 11, 17, 23, 29, 35, 301, 307, 313, 319,
                325, 331, 337, 343, 435, 479, 523, 567, 954, 984, 12, 18, 24, 30, 36, 302, 308,
                314, 320, 326, 332, 338, 344, 436, 480, 524, 568, 955, 985,
            ],
        )
        # fmt: on

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_variables_df(self, key):
        data = self.data[key]
        df = data.get_variables_df("daily")
        self.assertListEqual(df.columns.tolist(), ["id", "interval", "key", "type", "units"])
        self.assertTupleEqual(df.shape, (19, 5))

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_all_variables_df(self, key):
        data = self.data[key]
        df = data.get_all_variables_df()
        self.assertListEqual(df.columns.tolist(), ["id", "interval", "key", "type", "units"])
        self.assertTupleEqual(df.shape, (114, 5))

    def test_rename_variable_sql(self):
        sqld = self.data["sqld"]
        sqld.update_variable_name("timestep", 7, "FOO", "BAR")
        with self.sqls.engine.connect() as conn:
            table = sqld._get_results_table("timestep")
            res = conn.execute(table.select().where(table.c.id == 7)).first()
            var = (res[0], res[1], res[2], res[3], res[4])
            self.assertTupleEqual(var, (7, "timestep", "FOO", "BAR", "W/m2"))

        sqld.update_variable_name(
            "timestep", 7, "Environment", "Site Diffuse Solar Radiation Rate per Area"
        )
        with self.sqls.engine.connect() as conn:
            table = sqld._get_results_table("timestep")
            res = conn.execute(table.select().where(table.c.id == 7)).first()
            var = (res[0], res[1], res[2], res[3], res[4])
            self.assertTupleEqual(
                var,
                (
                    7,
                    "timestep",
                    "Environment",
                    "Site Diffuse Solar Radiation Rate per Area",
                    "W/m2",
                ),
            )

    @parameterized.expand(["dfd", "pqd"])
    def test_rename_variable(self, key):
        data = self.data[key]
        data.update_variable_name("timestep", 7, "FOO", "BAR")
        col1 = data.tables["timestep"].loc[:, (7, "timestep", "FOO", "BAR", "W/m2")]

        data.update_variable_name(
            "timestep", 7, "Environment", "Site Diffuse Solar Radiation Rate per Area"
        )
        col2 = data.tables["timestep"].loc[
               :,
               (
                   7,
                   "timestep",
                   "Environment",
                   "Site Diffuse Solar Radiation Rate per Area",
                   "W/m2",
               ),
               ]
        self.assertListEqual(col1.iloc[:, 0].tolist(), col2.iloc[:, 0].tolist())

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_add_remove_variable(self, key):
        data = self.data[key]
        id_ = data.insert_variable(
            Variable("monthly", "FOO", "BAR", "C"), list(range(12))
        )
        data.delete_variables("monthly", [id_])
        if key != "sqld":
            with self.assertRaises(KeyError):
                _ = data.tables["monthly"][id_]

    @parameterized.expand(["dfd", "pqd"])
    def test_remove_variable_invalid(self, key):
        data = self.data[key]
        with self.assertRaises(KeyError):
            data.delete_variables("monthly", [100000])

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_update_variable(self, key):
        data = self.data[key]
        original_vals = data.get_results("monthly", 983).iloc[:, 0]
        data.update_variable_results("monthly", 983, list(range(12)))
        vals = data.get_results("monthly", 983).iloc[:, 0].to_list()
        self.assertListEqual(vals, list(range(12)))
        data.update_variable_results("monthly", 983, original_vals)

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_update_variable_invalid(self, key):
        data = self.data[key]
        original_vals = data.get_results("monthly", 983).iloc[:, 0]
        data.update_variable_results("monthly", 983, list(range(11)))
        vals = data.get_results("monthly", 983).iloc[:, 0].to_list()
        self.assertListEqual(vals, original_vals.to_list())
        data.update_variable_results("monthly", 983, original_vals)

    @parameterized.expand(["dfd", "pqd"])
    def test_get_special_column_invalid(self, key):
        data = self.data[key]
        with self.assertRaises(KeyError):
            data._get_special_column("FOO", "timestep")

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_number_of_days(self, key):
        data = self.data[key]
        col = data.get_number_of_days("monthly")
        self.assertEqual(col.to_list(), [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
        self.assertEqual(col.size, 12)

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_days_of_week(self, key):
        data = self.data[key]
        col = data.get_days_of_week("daily")
        self.assertEqual(col[0], "Tuesday")
        self.assertEqual(col.size, 365)

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_all_results(self, key):
        data = self.data[key]
        df = data.get_all_results("daily")
        self.assertTupleEqual(df.shape, (365, 19))

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_results(self, key):
        data = self.data[key]
        df = data.get_results("monthly", [324, 983])
        test_columns = pd.MultiIndex.from_tuples(
            [
                (324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C"),
                (983, "monthly", "CHILLER", "Chiller Electric Energy", "J"),
            ],
            names=["id", "interval", "key", "type", "units"],
        )
        test_index = pd.Index([datetime(2002, i, 1) for i in range(1, 13)], name="timestamp")
        test_df = pd.DataFrame(
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
            columns=test_columns,
            index=test_index,
        )

        # need to drop id as pandas does not treat Index([324, 983])
        # and IndexInt64([324, 983]) as identical
        df = df.droplevel("id", axis=1)
        test_df = test_df.droplevel("id", axis=1)

        assert_frame_equal(df, test_df)

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_results_sliced(self, key):
        data = self.data[key]
        df = data.get_results(
            "monthly",
            [324, 983],
            start_date=datetime(2002, 4, 1),
            end_date=datetime(2002, 6, 1),
        )
        test_columns = pd.MultiIndex.from_tuples(
            [
                (324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C"),
                (983, "monthly", "CHILLER", "Chiller Electric Energy", "J"),
            ],
            names=["id", "interval", "key", "type", "units"],
        )
        test_index = pd.Index([datetime(2002, i, 1) for i in range(4, 7)], name="timestamp")
        test_df = pd.DataFrame(
            [[23.129456, 2.573239e09], [24.993765, 3.762886e09], [26.255885, 3.559705e09], ],
            columns=test_columns,
            index=test_index,
        )

        # need to drop id as pandas does not treat Index([324, 983])
        # and IndexInt64([324, 983]) as identical
        df = df.droplevel("id", axis=1)
        test_df = test_df.droplevel("id", axis=1)

        assert_frame_equal(df, test_df)

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_results_include_day(self, key):
        data = self.data[key]
        df = data.get_results(
            "daily",
            [323, 982],
            start_date=datetime(2002, 4, 1),
            end_date=datetime(2002, 4, 3),
            include_day=True,
        )
        test_columns = pd.MultiIndex.from_tuples(
            [
                (323, "daily", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C"),
                (982, "daily", "CHILLER", "Chiller Electric Energy", "J"),
            ],
            names=["id", "interval", "key", "type", "units"],
        )

        # days of week are picked up from actual date when not available on df
        test_index = pd.MultiIndex.from_arrays(
            [[datetime(2002, 4, i) for i in range(1, 4)], ["Monday", "Tuesday", "Wednesday"]],
            names=["timestamp", "day"],
        )

        test_df = pd.DataFrame(
            [[21.828242, 9.549276e07], [23.032272, 1.075975e08], [23.716322, 1.293816e08], ],
            columns=test_columns,
            index=test_index,
        )

        assert_frame_equal(df, test_df, check_column_type=False)

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_results_include_day_from_date(self, key):
        data = self.data[key]
        df = data.get_results(
            "monthly",
            [324, 983],
            start_date=datetime(2002, 4, 1),
            end_date=datetime(2002, 6, 1),
            include_day=True,
        )
        test_columns = pd.MultiIndex.from_tuples(
            [
                (324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C"),
                (983, "monthly", "CHILLER", "Chiller Electric Energy", "J"),
            ],
            names=["id", "interval", "key", "type", "units"],
        )

        # days of week are picked up from actual date when not available on df
        test_index = pd.MultiIndex.from_arrays(
            [[datetime(2002, i, 1) for i in range(4, 7)], ["Monday", "Wednesday", "Saturday"]],
            names=["timestamp", "day"],
        )
        test_df = pd.DataFrame(
            [[23.129456, 2.573239e09], [24.993765, 3.762886e09], [26.255885, 3.559705e09], ],
            columns=test_columns,
            index=test_index,
        )

        assert_frame_equal(df, test_df, check_column_type=False)

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_results_invalid_ids(self, key):
        data = self.data[key]
        with self.assertRaises(KeyError):
            _ = data.get_results("daily", [7])

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_global_max_results(self, key):
        data = self.data[key]
        df = data.get_global_max_results("monthly", [324, 983])
        test_columns = pd.MultiIndex.from_tuples(
            [
                (324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C", "value"),
                (
                    324,
                    "monthly",
                    "BLOCK3:ZONE1",
                    "Zone Mean Air Temperature",
                    "C",
                    "timestamp",
                ),
                (983, "monthly", "CHILLER", "Chiller Electric Energy", "J", "value"),
                (983, "monthly", "CHILLER", "Chiller Electric Energy", "J", "timestamp"),
            ],
            names=["id", "interval", "key", "type", "units", "data"],
        )
        test_df = pd.DataFrame(
            [[27.007450, datetime(2002, 7, 1), 5.093662e09, datetime(2002, 7, 1)], ],
            columns=test_columns,
        )

        assert_frame_equal(df, test_df, check_column_type=False)

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_global_min_results(self, key):
        data = self.data[key]
        df = data.get_global_min_results("monthly", [324, 983])
        test_columns = pd.MultiIndex.from_tuples(
            [
                (324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C", "value"),
                (324, "monthly", "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C", "timestamp"),
                (983, "monthly", "CHILLER", "Chiller Electric Energy", "J", "value"),
                (983, "monthly", "CHILLER", "Chiller Electric Energy", "J", "timestamp"),
            ],
            names=["id", "interval", "key", "type", "units", "data"],
        )
        test_df = pd.DataFrame(
            [[18.520034, datetime(2002, 12, 1), 1.945721e08, datetime(2002, 12, 1)], ],
            columns=test_columns,
        )

        assert_frame_equal(df, test_df, check_column_type=False)

    def test_df_dt_slicer(self):
        index = pd.DatetimeIndex(pd.date_range("2002-01-01", freq="d", periods=5))
        df = pd.DataFrame({"a": list(range(5))}, index=index)

        pd.testing.assert_frame_equal(
            df_dt_slicer(df, start_date=datetime(2002, 1, 2), end_date=None), df.iloc[1:, :]
        )

        pd.testing.assert_frame_equal(
            df_dt_slicer(df, start_date=None, end_date=datetime(2002, 1, 2)), df.iloc[:2, :]
        )

        pd.testing.assert_frame_equal(
            df_dt_slicer(df, start_date=datetime(2002, 1, 2), end_date=datetime(2002, 1, 2)),
            df.iloc[[1], :],
        )

    def test_sr_dt_slicer(self):
        index = pd.DatetimeIndex(pd.date_range("2002-01-01", freq="d", periods=5))
        sr = pd.Series(list(range(5)), index=index)

        pd.testing.assert_series_equal(
            sr_dt_slicer(sr, start_date=datetime(2002, 1, 2), end_date=None), sr.iloc[1:],
        )

        pd.testing.assert_series_equal(
            sr_dt_slicer(sr, start_date=None, end_date=datetime(2002, 1, 2)), sr.iloc[:2]
        )

        pd.testing.assert_series_equal(
            sr_dt_slicer(sr, start_date=datetime(2002, 1, 2), end_date=datetime(2002, 1, 2)),
            sr.iloc[[1]],
        )

    def test_sort_by_ids(self):
        columns = pd.MultiIndex.from_tuples(
            [(1, "a", "b", "c"), (2, "d", "e", "f"), (3, "g", "h", "i")],
            names=["id", "interval", "key", "units"]
        )
        index = pd.date_range(start="01/01/2020", periods=8760, freq="h", name="datetime")
        df = pd.DataFrame(np.random.rand(8760, 3), index=index, columns=columns)
        expected_df = df.loc[:, [(3, "g", "h", "i"), (1, "a", "b", "c"), (2, "d", "e", "f")]]
        sorted_df = sort_by_ids(df, [3, 1, 2])
        assert_frame_equal(expected_df, sorted_df)

    def test_sort_by_ids_na_id(self):
        columns = pd.MultiIndex.from_tuples(
            [(1, "a", "b", "c"), (2, "d", "e", "f"), (3, "g", "h", "i")],
            names=["id", "interval", "key", "units"]
        )
        index = pd.date_range(start="01/01/2020", periods=8760, freq="h", name="datetime")
        df = pd.DataFrame(np.random.rand(8760, 3), index=index, columns=columns)
        expected_df = df.loc[:, [(3, "g", "h", "i"), (1, "a", "b", "c"), (2, "d", "e", "f")]]
        sorted_df = sort_by_ids(df, [4, 3, 1, 2])
        assert_frame_equal(expected_df, sorted_df)
