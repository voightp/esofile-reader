import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from esofile_reader.excel_file import ExcelFile, is_data_row


# from tests import ROOT


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

    def test_populate_content_no_template_no_index(self):
        # path = Path(ROOT).joinpath("./eso_files/test_excel_results.xlsx")
        path = Path(
            r"C:\Users\vojtechp1\Desktop\Python\esofile-reader\tests\eso_files\test_excel_results.xlsx"
        )
        ef = ExcelFile(path)
