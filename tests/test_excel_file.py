import unittest
from pathlib import Path

import numpy as np
import pandas as pd
from parameterized import parameterized

from esofile_reader.excel_file import ExcelFile, is_data_row
from tests import ROOT


# TODO add tests


class TestExcelFile(unittest.TestCase):
    def test_is_data_row_mixed(self):
        sr = pd.Series(["Saturday", pd.NaT, 0, 0, 1.23456])
        self.assertTrue(is_data_row(sr))

    def test_is_data_row_all_nat(self):
        sr = pd.Series([pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT])
        self.assertTrue(is_data_row(sr))

    def test_is_data_row_all_string(self):
        sr = pd.Series(["Saturday"] * 5)
        self.assertFalse(is_data_row(sr))

    def test_is_data_row_all_float(self):
        sr = pd.Series(np.random.rand(5))
        self.assertTrue(is_data_row(sr))

    def test_is_data_row_all_int(self):
        sr = pd.Series(np.random.randint(0, high=100, size=5))
        self.assertTrue(is_data_row(sr))

    def test_is_data_row_more_strings(self):
        sr = pd.Series(["a", "b", pd.NaT, pd.NaT, 0.1])
        self.assertFalse(is_data_row(sr))

    @parameterized.expand([
        (
                "simple-no-template-no-index",
                (12, 7),
                "range",
                pd.RangeIndex,
                ["id", "interval", "key", "units"]
        ),
        (
                "simple-no-template-dt-index",
                (12, 7),
                "timestamp",
                pd.DatetimeIndex,
                ["id", "interval", "key", "units"]
        ),
        (
                "simple-template-monthly",
                (12, 8),
                "timestamp",
                pd.DatetimeIndex,
                ["id", "interval", "key", "units"]
        ),
        (
                "simple-template-range",
                (12, 7),
                "range",
                pd.RangeIndex,
                ["id", "interval", "key", "units"]
        )
    ])
    def test_populate_simple_tables(
            self, sheet, shape, index_name, index_type, column_names
    ):
        path = Path(ROOT).joinpath("./eso_files/test_excel_results.xlsx")
        ef = ExcelFile(path, sheet_names=[sheet])
        df = ef.data.tables[sheet]
        self.assertEqual(shape, df.shape)
        self.assertEqual(index_name, df.index.name)
        self.assertTrue(isinstance(df.index, index_type))
        self.assertListEqual(column_names, df.columns.names)
        self.assertTrue(ef.data.is_simple(sheet))
