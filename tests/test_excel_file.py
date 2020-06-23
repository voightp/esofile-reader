import unittest
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.testing import assert_index_equal
from parameterized import parameterized

from esofile_reader.exceptions import InsuficientHeaderInfo
from esofile_reader.processing.excel import is_data_row
from esofile_reader.results_file import ResultsFile
from tests import ROOT

RESULTS_PATH = Path(ROOT).joinpath("./eso_files/test_excel_results.xlsx")
EDGE_CASE_PATH = Path(ROOT).joinpath("./eso_files/test_excel_edge_cases.xlsx")


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

    @parameterized.expand(
        [
            (
                    "simple-no-template-no-index",
                    "simple-no-template-no-index",
                    (12, 7),
                    "range",
                    pd.RangeIndex,
                    ["id", "table", "key", "units"],
            ),
            (
                    "simple-no-template-dt-index",
                    "simple-no-template-dt-index",
                    (12, 7),
                    "timestamp",
                    pd.DatetimeIndex,
                    ["id", "table", "key", "units"],
            ),
            (
                    "simple-template-monthly",
                    "monthly",
                    (12, 8),
                    "timestamp",
                    pd.DatetimeIndex,
                    ["id", "table", "key", "units"],
            ),
            (
                    "simple-template-range",
                    "range",
                    (12, 7),
                    "range",
                    pd.RangeIndex,
                    ["id", "table", "key", "units"],
            ),
        ]
    )
    def test_populate_simple_tables(
            self, sheet, table, shape, index_name, index_type, column_names
    ):
        ef = ResultsFile.from_excel(RESULTS_PATH, sheet_names=[sheet])
        df = ef.tables[table]
        self.assertEqual(shape, df.shape)
        self.assertEqual(index_name, df.index.name)
        self.assertTrue(isinstance(df.index, index_type))
        self.assertListEqual(column_names, df.columns.names)
        self.assertTrue(ef.tables.is_simple(table))

    @parameterized.expand(
        [
            (
                    "no-template-full-dt-index",
                    "no-template-full-dt-index",
                    (12, 8),
                    "timestamp",
                    pd.DatetimeIndex,
                    ["id", "table", "key", "type", "units"],
            ),
            (
                    "full-template-hourly",
                    "hourly",
                    (8760, 8),
                    "timestamp",
                    pd.DatetimeIndex,
                    ["id", "table", "key", "type", "units"],
            ),
            (
                    "full-template-daily",
                    "daily",
                    (365, 8),
                    "timestamp",
                    pd.DatetimeIndex,
                    ["id", "table", "key", "type", "units"],
            ),
            (
                    "full-template-monthly",
                    "monthly",
                    (12, 8),
                    "timestamp",
                    pd.DatetimeIndex,
                    ["id", "table", "key", "type", "units"],
            ),
            (
                    "full-template-runperiod",
                    "runperiod",
                    (1, 20),
                    "timestamp",
                    pd.DatetimeIndex,
                    ["id", "table", "key", "type", "units"],
            ),
        ]
    )
    def test_populate_full_tables(
            self, sheet, table, shape, index_name, index_type, column_names
    ):
        ef = ResultsFile.from_excel(RESULTS_PATH, sheet_names=[sheet])
        df = ef.tables[table]
        self.assertEqual(shape, df.shape)
        self.assertEqual(index_name, df.index.name)
        self.assertTrue(isinstance(df.index, index_type))
        self.assertListEqual(column_names, df.columns.names)
        self.assertFalse(ef.tables.is_simple(table))

    def test_drop_blank_lines(self):
        ef = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["blank-lines"])
        df = ef.tables["blank-lines"]
        self.assertEqual((12, 7), df.shape)

    def test_force_index_generic_column(self):
        ef = ResultsFile.from_excel(
            EDGE_CASE_PATH, sheet_names=["force-index"], force_index=True
        )
        df = ef.tables["force-index"]
        self.assertEqual((12, 6), df.shape)
        self.assertEqual("index", df.index.name)
        assert_index_equal(pd.Index(list("abcdefghijkl"), name="index"), df.index)

    def test_index_duplicate_values(self):
        ef = ResultsFile.from_excel(
            EDGE_CASE_PATH, sheet_names=["duplicate-index"], force_index=True
        )
        df = ef.tables["duplicate-index"]
        self.assertEqual((6, 6), df.shape)
        self.assertEqual("index", df.index.name)
        assert_index_equal(pd.Index(list("aaadef"), name="index"), df.index)

    def test_column_duplicate_values(self):
        ef = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["duplicate-columns"])
        df = ef.tables["monthly"]
        self.assertEqual((12, 7), df.shape)

    def test_too_few_header_rows(self):
        with self.assertRaises(InsuficientHeaderInfo):
            _ = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["too-few-header-items"])

    def test_too_many_header_rows(self):
        with self.assertRaises(InsuficientHeaderInfo):
            _ = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["too-many-header-items"])

    def test_too_many_header_rows_template(self):
        ef = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["too-many-items-template"])
        df = ef.tables["monthly"]
        self.assertEqual((12, 7), df.shape)
        self.assertListEqual(["id", "table", "key", "type", "units"], df.columns.names)

    def test_too_switched_template_levels(self):
        ef = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["switched-template-levels"])
        df = ef.tables["monthly"]
        self.assertEqual((12, 7), df.shape)
        self.assertListEqual(["id", "table", "key", "type", "units"], df.columns.names)

    def test_template_missing_key_level(self):
        with self.assertRaises(InsuficientHeaderInfo):
            _ = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["missing-key"])

    def test_multiple_tables(self):
        ef = ResultsFile.from_excel(EDGE_CASE_PATH, sheet_names=["multiple-tables"])
        df = ef.tables["table1"]
        self.assertEqual((12, 3), df.shape)
        self.assertEqual("timestamp", df.index.name)

        df = ef.tables["table2"]
        self.assertEqual((12, 5), df.shape)
        self.assertEqual("timestamp", df.index.name)
