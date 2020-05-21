import unittest
from pathlib import Path

import pandas as pd

from esofile_reader.excel_file import ExcelFile, is_data_row


# from tests import ROOT


class TestExcelFile(unittest.TestCase):
    def test_populate_content(self):
        # path = Path(ROOT).joinpath("./eso_files/test_excel_results.xlsx")
        path = Path(
            r"C:\Users\vojtechp1\Desktop\Python\esofile-reader\tests\eso_files\test_excel_results.xlsx"
        )
        ef = ExcelFile(path)

    def test_is_data_row_mixed(self):
        sr = pd.Series(["Saturday", pd.NaT, 0, 0, 1.23456])
        self.assertTrue(is_data_row(sr))

    def test_is_data_row_all_nat(self):
        sr = pd.Series([pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT])
        self.assertTrue(is_data_row(sr))

    def test_is_data_row_all_string(self):
        sr = pd.Series(["Saturday"] * 5)
        self.assertFalse(is_data_row(sr))
