import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal
from parameterized import parameterized

from esofile_reader.fetchers import get_results
from esofile_reader.mini_classes import SimpleVariable
from esofile_reader.results_file import ResultsFile
from esofile_reader.storages.df_storage import DFStorage
from esofile_reader.storages.pqt_storage import ParquetStorage
from esofile_reader.storages.sql_storage import SQLStorage
from tests import ROOT


class TestResultFetching(unittest.TestCase):
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

    @parameterized.expand(["dff", "pqf", "sqlf"])
    def test_get_results(self, key):
        file = self.files[key]
        v = SimpleVariable("monthly", "BLOCK1:ZONE1", "C")
        df = get_results(file, v)
        test_columns = pd.MultiIndex.from_tuples(
            [("BLOCK1:ZONE1", "C")], names=["key", "units"]
        )
        dates = pd.date_range(start="2002/01/01", freq="MS", periods=12)
        test_index = pd.MultiIndex.from_product(
            [["test_excel_results"], dates], names=["file", "timestamp"]
        )
        test_df = pd.DataFrame(
            {
                "dummy": [
                    19.14850348,
                    18.99527211,
                    20.98875615,
                    22.78142137,
                    24.3208488,
                    25.47972495,
                    26.16745932,
                    25.68404781,
                    24.15289436,
                    22.47691717,
                    20.58877632,
                    18.66182101,
                ]
            },
            index=test_index,
        )
        test_df.columns = test_columns

        assert_frame_equal(df, test_df)

    @parameterized.expand(["dff", "pqf", "sqlf"])
    def test_get_results_start_date(self, key):
        file = self.files[key]
        v = SimpleVariable("monthly", "BLOCK1:ZONE1", "C")
        df = get_results(file, v, start_date=datetime(2002, 4, 15))
        test_columns = pd.MultiIndex.from_tuples(
            [("BLOCK1:ZONE1", "C")], names=["key", "units"]
        )
        dates = pd.date_range(start="2002/05/01", freq="MS", periods=8)
        test_index = pd.MultiIndex.from_product(
            [["test_excel_results"], dates], names=["file", "timestamp"]
        )
        test_df = pd.DataFrame(
            {
                "dummy": [
                    24.3208488,
                    25.47972495,
                    26.16745932,
                    25.68404781,
                    24.15289436,
                    22.47691717,
                    20.58877632,
                    18.66182101,
                ]
            },
            index=test_index,
        )
        test_df.columns = test_columns

        assert_frame_equal(df, test_df)

    @parameterized.expand(["dff", "pqf", "sqlf"])
    def test_get_results_end_date(self, key):
        file = self.files[key]
        v = SimpleVariable("monthly", "BLOCK1:ZONE1", "C")
        df = get_results(file, v, end_date=datetime(2002, 8, 10))
        test_columns = pd.MultiIndex.from_tuples(
            [("BLOCK1:ZONE1", "C")], names=["key", "units"]
        )
        dates = pd.date_range(start="2002/01/01", freq="MS", periods=8)
        test_index = pd.MultiIndex.from_product(
            [["test_excel_results"], dates], names=["file", "timestamp"]
        )
        test_df = pd.DataFrame(
            {
                "dummy": [
                    19.14850348,
                    18.99527211,
                    20.98875615,
                    22.78142137,
                    24.3208488,
                    25.47972495,
                    26.16745932,
                    25.68404781,
                ]
            },
            index=test_index,
        )
        test_df.columns = test_columns

        assert_frame_equal(df, test_df)

    @parameterized.expand(["dff", "pqf", "sqlf"])
    def test_get_results_output_type_start_end_date(self, key):
        file = self.files[key]
        v = SimpleVariable("monthly", "BLOCK1:ZONE1", "C")
        df = get_results(
            file, v, start_date=datetime(2002, 4, 10), end_date=datetime(2002, 6, 10)
        )
        test_columns = pd.MultiIndex.from_tuples(
            [("BLOCK1:ZONE1", "C")], names=["key", "units"]
        )
        dates = [
            datetime(2002, 5, 1),
            datetime(2002, 6, 1),
        ]
        test_index = pd.MultiIndex.from_product(
            [["test_excel_results"], dates], names=["file", "timestamp"]
        )
        test_df = pd.DataFrame({"dummy": [24.3208488, 25.47972495, ]}, index=test_index, )
        test_df.columns = test_columns

        assert_frame_equal(df, test_df)

    @parameterized.expand(["dff", "pqf", "sqlf"])
    def test_get_results_include_table_name(self, key):
        file = self.files[key]
        v = SimpleVariable("monthly", "BLOCK1:ZONE1", "C")
        df = get_results(
            file,
            v,
            start_date=datetime(2002, 4, 10),
            end_date=datetime(2002, 6, 10),
            include_table_name=True,
        )
        test_columns = pd.MultiIndex.from_tuples(
            [("monthly", "BLOCK1:ZONE1", "C")], names=["table", "key", "units"]
        )
        dates = [
            datetime(2002, 5, 1),
            datetime(2002, 6, 1),
        ]
        test_index = pd.MultiIndex.from_product(
            [["test_excel_results"], dates], names=["file", "timestamp"]
        )
        test_df = pd.DataFrame({"dummy": [24.3208488, 25.47972495, ]}, index=test_index, )
        test_df.columns = test_columns

        assert_frame_equal(df, test_df)

    @parameterized.expand(["dff", "pqf", "sqlf"])
    def test_get_multiple_results_units_system_si(self, key):
        file = self.files[key]
        v = [
            SimpleVariable("monthly", "BLOCK3:ZONE1", ""),
            SimpleVariable("monthly", "Environment", "W/m2"),
            SimpleVariable("monthly", "BLOCK1:ZONE1", "kgWater/kgDryAir"),
        ]
        df = get_results(file, v, units_system="SI", rate_to_energy=False)
        test_columns = pd.MultiIndex.from_tuples(
            [
                ("BLOCK3:ZONE1", ""),
                ("Environment", "W/m2"),
                ("BLOCK1:ZONE1", "kgWater/kgDryAir"),
            ],
            names=["key", "units"],
        )
        dates = pd.date_range(start="2002/01/01", freq="MS", periods=12)
        test_index = pd.MultiIndex.from_product(
            [["test_excel_results"], dates], names=["file", "timestamp"]
        )
        test_df = pd.DataFrame(
            [
                [4.44599391, 19.04502688, 0.004855573],
                [4.280304696, 32.32626488, 0.004860482],
                [4.059385744, 62.03965054, 0.005461099],
                [4.394446155, 82.49756944, 0.005840664],
                [4.44599391, 111.5, 0.007228851],
                [3.99495105, 123.0475694, 0.007842664],
                [4.44599391, 120.1125672, 0.009539482],
                [4.252689827, 97.23555108, 0.009332843],
                [4.194698603, 72.05486111, 0.007949586],
                [4.44599391, 41.96303763, 0.007626202],
                [4.194698603, 28.640625, 0.006508911],
                [4.252689827, 17.43850806, 0.005512091],
            ],
            columns=test_columns,
            index=test_index,
        )
        assert_frame_equal(df, test_df)

    @parameterized.expand(["dff", "pqf", "sqlf"])
    def test_get_multiple_results_units_system_ip(self, key):
        file = self.files[key]
        v = [
            SimpleVariable("monthly", "BLOCK3:ZONE1", ""),
            SimpleVariable("monthly", "Environment", "W/m2"),
            SimpleVariable("monthly", "BLOCK1:ZONE1", "C"),
        ]
        df = get_results(file, v, units_system="IP", rate_to_energy=False)
        test_columns = pd.MultiIndex.from_tuples(
            [("BLOCK3:ZONE1", ""), ("Environment", "W/sqf"), ("BLOCK1:ZONE1", "F"), ],
            names=["key", "units"],
        )
        dates = pd.date_range(start="2002/01/01", freq="MS", periods=12)
        test_index = pd.MultiIndex.from_product(
            [["test_excel_results"], dates], names=["file", "timestamp"]
        )
        test_df = pd.DataFrame(
            [
                [4.44599391, 1.769984, 66.467306],
                [4.280304696, 3.004300, 66.191490],
                [4.059385744, 5.765767, 69.779761],
                [4.394446155, 7.667060, 73.006558],
                [4.44599391, 10.362454, 75.777528],
                [3.99495105, 11.435648, 77.863505],
                [4.44599391, 11.162878, 79.101427],
                [4.252689827, 9.036761, 78.231286],
                [4.194698603, 6.696548, 75.475210],
                [4.44599391, 3.899911, 72.458451],
                [4.194698603, 2.661768, 69.059797],
                [4.252689827, 1.620679, 65.591278],
            ],
            columns=test_columns,
            index=test_index,
        )
        assert_frame_equal(df, test_df)

    @parameterized.expand(["dff", "pqf", "sqlf"])
    def test_get_results_rate_to_energy(self, key):
        file = self.files[key]
        v = [SimpleVariable("monthly", "Environment", "W/m2")]
        df = get_results(file, v, rate_to_energy=True)
        test_columns = pd.MultiIndex.from_tuples(
            [("Environment", "J/m2"), ], names=["key", "units"],
        )
        dates = pd.date_range(start="2002/01/01", freq="MS", periods=12)
        test_index = pd.MultiIndex.from_product(
            [["test_excel_results"], dates], names=["file", "timestamp"]
        )
        test_df = pd.DataFrame(
            [
                [51010200.0],
                [78203700.0],
                [166167000.0],
                [213833700.0],
                [298641600.0],
                [318939300.0],
                [321709500.0],
                [260435700.0],
                [186766200.0],
                [112393800.0],
                [74236500.0],
                [46707300.0],
            ],
            columns=test_columns,
            index=test_index,
        )
        assert_frame_equal(df, test_df)
