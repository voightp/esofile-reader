import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal, assert_index_equal, assert_series_equal
from parameterized import parameterized

from esofile_reader.constants import N_DAYS_COLUMN
from esofile_reader.mini_classes import SimpleVariable
from esofile_reader.results_file import ResultsFile
from esofile_reader.storages.df_storage import DFStorage
from esofile_reader.storages.pqt_storage import ParquetStorage
from esofile_reader.storages.sql_storage import SQLStorage
from tests import ROOT


class TestDataClassesSimple(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pth = Path(ROOT).joinpath("./eso_files/test_excel_results.xlsx")
        sheets = ["simple-template-monthly", "simple-template-range"]
        ef = ResultsFile.from_excel(pth, sheets)

        cls.dfs = DFStorage()
        id_ = cls.dfs.store_file(ef)
        dff = cls.dfs.files[id_]

        cls.pqs = ParquetStorage()
        id_ = cls.pqs.store_file(ef)
        pqf = cls.pqs.files[id_]

        cls.sqls = SQLStorage()
        id_ = cls.sqls.store_file(ef)
        sqlf = cls.sqls.files[id_]

        cls.files = {"dff": dff, "pqf": pqf, "sqlf": sqlf}

        cls.tables = {"dfd": dff.tables, "pqd": pqf.tables, "sqld": sqlf.tables}

    @classmethod
    def tearDownClass(cls):
        cls.files["pqf"].clean_up()
        cls.files["pqf"] = None

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_is_simple(self, key):
        tables = self.tables[key]
        table_names = tables.get_table_names()
        for table in table_names:
            self.assertTrue(tables.is_simple(table))

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_levels(self, key):
        tables = self.tables[key]
        table_names = tables.get_table_names()
        for table in table_names:
            self.assertListEqual(["id", "table", "key", "units"], tables.get_levels(table))

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_available_tables(self, key):
        tables = self.tables[key]
        table_names = tables.get_table_names()
        self.assertListEqual(["monthly-simple", "range"], table_names)

    @parameterized.expand(["dfd", "pqd"])
    def test_get_available_tables(self, key):
        tables = self.tables[key]
        table_names = tables.keys()
        self.assertListEqual(["monthly-simple", "range"], list(table_names))

    @parameterized.expand(["dfd", "pqd"])
    def test_set_table_invalid(self, key):
        tables = self.tables[key]
        variables = [
            (1, "test", "ZoneA", "Temperature", "C"),
            (2, "test", "ZoneB", "Temperature", "C"),
            (3, "test", "ZoneC", "Temperature", "C"),
            (4, "test", "ZoneC", "Heating Load", "W"),
        ]
        columns = pd.MultiIndex.from_tuples(
            variables, names=["id", "WRONG", "key", "type", "units"]
        )
        index = pd.RangeIndex(start=0, stop=3, step=1, name="range")
        df = pd.DataFrame(
            [
                [25.123, 27.456, 14.546, 1000],
                [25.123, 27.456, 14.546, 2000],
                [25.123, 27.456, 14.546, 3000],
            ],
            columns=columns,
            index=index,
        )
        with self.assertRaises(TypeError):
            tables["test"] = df

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_datetime_index(self, key):
        tables = self.tables[key]
        index = tables.get_datetime_index("monthly-simple")
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
        tables = self.tables[key]
        variables = tables.get_variables_dct("monthly-simple")
        self.assertListEqual(list(variables.keys()), [1, 2, 3, 4, 5, 6, 7])

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_variable_ids(self, key):
        tables = self.tables[key]
        ids = tables.get_variable_ids("range")
        self.assertListEqual(ids, [8, 9, 10, 11, 12, 13, 14])

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_all_variable_ids(self, key):
        tables = self.tables[key]
        ids = tables.get_all_variable_ids()
        self.assertListEqual(ids, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14])

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_variables_df(self, key):
        tables = self.tables[key]
        df = tables.get_variables_df("monthly-simple")
        self.assertListEqual(df.columns.tolist(), ["id", "table", "key", "units"])
        self.assertTupleEqual(df.shape, (7, 4))

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_all_variables_df(self, key):
        tables = self.tables[key]
        df = tables.get_all_variables_df()
        self.assertListEqual(df.columns.tolist(), ["id", "table", "key", "units"])
        self.assertTupleEqual(df.shape, (14, 4))

    def test_rename_variable_sql(self):
        sqld = self.tables["sqld"]
        sqld.update_variable_name("monthly-simple", 1, "FOO")
        with self.sqls.engine.connect() as conn:
            table = sqld._get_results_table("monthly-simple")
            res = conn.execute(table.select().where(table.c.id == 1)).first()
            var = (res[0], res[1], res[2], res[3])
            self.assertTupleEqual(var, (1, "monthly-simple", "FOO", "W/m2"))

        sqld.update_variable_name("monthly-simple", 1, "Environment")
        with self.sqls.engine.connect() as conn:
            table = sqld._get_results_table("monthly-simple")
            res = conn.execute(table.select().where(table.c.id == 1)).first()
            var = (res[0], res[1], res[2], res[3])
            self.assertTupleEqual(var, (1, "monthly-simple", "Environment", "W/m2"))

    @parameterized.expand(["dfd", "pqd"])
    def test_rename_variable(self, key):
        tables = self.tables[key]
        tables.update_variable_name("monthly-simple", 1, "FOO")
        col1 = tables["monthly-simple"].loc[:, [(1, "monthly-simple", "FOO", "W/m2")]]

        tables.update_variable_name("monthly-simple", 1, "Environment")
        col2 = tables["monthly-simple"].loc[:, [(1, "monthly-simple", "Environment", "W/m2")]]
        self.assertEqual(col1.iloc[:, 0].array, col2.iloc[:, 0].array)

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_add_remove_variable(self, key):
        tables = self.tables[key]
        id_ = tables.insert_column(SimpleVariable("monthly-simple", "FOO", "C"),
                                   list(range(12)))
        tables.delete_variables("monthly-simple", [id_])
        if key != "sqld":
            with self.assertRaises(KeyError):
                _ = tables["monthly-simple"][id_]

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_insert_special_column(self, key="sqld"):
        tables = self.tables[key]
        values = list("abcdefghijkl")
        tables.insert_special_column("monthly-simple", "TEST", values)
        sr = tables.get_special_column("monthly-simple", "TEST")
        index = pd.date_range(start="2002-01-01", freq="MS", periods=12, name="timestamp")
        test_sr = pd.Series(values, name=("special", "monthly-simple", "TEST", ""), index=index)
        assert_series_equal(sr, test_sr)

    @parameterized.expand(["dfd", "pqd"])
    def test_remove_variable_invalid(self, key):
        tables = self.tables[key]
        with self.assertRaises(KeyError):
            tables.delete_variables("monthly-simple", [100000])

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_update_variable(self, key):
        tables = self.tables[key]
        original_vals = tables.get_results("monthly-simple", 1).iloc[:, 0]
        tables.update_variable_values("monthly-simple", 1, list(range(12)))
        vals = tables.get_results("monthly-simple", 1).iloc[:, 0].to_list()
        self.assertListEqual(vals, list(range(12)))
        tables.update_variable_values("monthly-simple", 1, original_vals)

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_update_variable_invalid(self, key):
        tables = self.tables[key]
        original_vals = tables.get_results("monthly-simple", 1).iloc[:, 0]
        tables.update_variable_values("monthly-simple", 1, list(range(11)))
        vals = tables.get_results("monthly-simple", 1).iloc[:, 0].to_list()
        self.assertListEqual(vals, original_vals.to_list())
        tables.update_variable_values("monthly-simple", 1, original_vals)

    @parameterized.expand(["dfd", "pqd"])
    def test_get_special_column_invalid(self, key):
        tables = self.tables[key]
        with self.assertRaises(KeyError):
            tables.get_special_column("FOO", "timestep")

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_number_of_days(self, key):
        tables = self.tables[key]
        col = tables.get_special_column("monthly-simple", N_DAYS_COLUMN)
        self.assertEqual(col.to_list(), [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
        self.assertEqual(col.size, 12)

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_all_results(self, key):
        tables = self.tables[key]
        df = tables.get_numeric_table("monthly-simple")
        self.assertTupleEqual(df.shape, (12, 7))

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_results(self, key):
        tables = self.tables[key]
        df = tables.get_results("monthly-simple", [2, 6])
        test_columns = pd.MultiIndex.from_tuples(
            [(2, "monthly-simple", "BLOCK1:ZONE1", ""),
             (6, "monthly-simple", "BLOCK1:ZONE1", "C"), ],
            names=["id", "table", "key", "units"],
        )
        test_index = pd.Index([datetime(2002, i, 1) for i in range(1, 13)], name="timestamp")
        test_df = pd.DataFrame(
            [
                [4.44599391, 19.14850348],
                [4.280304696, 18.99527211],
                [4.059385744, 20.98875615],
                [4.394446155, 22.78142137],
                [4.44599391, 24.3208488],
                [3.99495105, 25.47972495],
                [4.44599391, 26.16745932],
                [4.252689827, 25.68404781],
                [4.194698603, 24.15289436],
                [4.44599391, 22.47691717],
                [4.194698603, 20.58877632],
                [4.252689827, 18.66182101],
            ],
            columns=test_columns,
            index=test_index,
        )

        assert_frame_equal(df, test_df, check_column_type=False)

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_results_sliced(self, key):
        tables = self.tables[key]
        df = tables.get_results(
            "monthly-simple", [2, 6], start_date=datetime(2002, 4, 1),
            end_date=datetime(2002, 6, 1),
        )
        test_columns = pd.MultiIndex.from_tuples(
            [(2, "monthly-simple", "BLOCK1:ZONE1", ""),
             (6, "monthly-simple", "BLOCK1:ZONE1", "C"), ],
            names=["id", "table", "key", "units"],
        )
        test_index = pd.Index([datetime(2002, i, 1) for i in range(4, 7)], name="timestamp")
        test_df = pd.DataFrame(
            [[4.394446155, 22.78142137], [4.44599391, 24.3208488], [3.99495105, 25.47972495], ],
            columns=test_columns,
            index=test_index,
        )

        assert_frame_equal(df, test_df, check_column_type=False)

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_results_invalid_ids(self, key):
        tables = self.tables[key]
        with self.assertRaises(KeyError):
            _ = tables.get_results("daily", [7])

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_global_max_results(self, key):
        tables = self.tables[key]
        df = tables.get_global_max_results("range", [11, 13])
        test_columns = pd.MultiIndex.from_tuples(
            [
                (11, "range", "BLOCK3:ZONE1", "", "value"),
                (11, "range", "BLOCK3:ZONE1", "", "timestamp",),
                (13, "range", "BLOCK1:ZONE1", "C", "value"),
                (13, "range", "BLOCK1:ZONE1", "C", "timestamp"),
            ],
            names=["id", "table", "key", "units", "data"],
        )

        test_df = pd.DataFrame([[4.44599391, 0, 26.16745932, 6]], columns=test_columns)

        assert_frame_equal(df, test_df, check_column_type=False)

    @parameterized.expand(["dfd", "pqd", "sqld"])
    def test_get_global_max_results(self, key):
        tables = self.tables[key]
        df = tables.get_global_min_results("range", [11, 13])
        test_columns = pd.MultiIndex.from_tuples(
            [
                (11, "range", "BLOCK3:ZONE1", "", "value"),
                (11, "range", "BLOCK3:ZONE1", "", "timestamp",),
                (13, "range", "BLOCK1:ZONE1", "C", "value"),
                (13, "range", "BLOCK1:ZONE1", "C", "timestamp"),
            ],
            names=["id", "table", "key", "units", "data"],
        )
        test_df = pd.DataFrame([[3.994951, 5, 18.661821, 11]], columns=test_columns)
        assert_frame_equal(df, test_df, check_column_type=False)
