from datetime import datetime
from pathlib import Path
from unittest import TestCase

import pandas as pd

from esofile_reader import ResultsFile, Variable
from esofile_reader.exceptions import NoResults
from esofile_reader.search_tree import Tree
from esofile_reader.tables.df_tables import DFTables
from tests import ROOT


class TestTotalsFile(TestCase):
    @classmethod
    def setUpClass(cls):
        daily_variables = [
            (1, "daily", "BLOCK1:ZONE1", "Zone Temperature", "C"),
            (2, "daily", "BLOCK1:ZONE2", "Zone Temperature", "C"),
            (3, "daily", "BLOCK1:ZONE3", "Zone Temperature", "C"),
            (4, "daily", "BLOCK1:ZONE1", "Heating Load", "W"),
            (5, "daily", "BLOCK1:ZONE1_WALL_3_0_0_0_0_0_WIN", "Window Gain", "W"),
            (6, "daily", "BLOCK1:ZONE1_WALL_4_0_0_0_0_0_WIN", "Window Gain", "W"),
            (7, "daily", "BLOCK1:ZONE1_WALL_5_0_0_0_0_0_WIN", "Window Gain", "W"),
            (8, "daily", "BLOCK1:ZONE1_WALL_6_0_0_0_0_0_WIN", "Window Lost", "W"),
            (9, "daily", "BLOCK1:ZONE1_WALL_5_0_0", "Wall Gain", "W"),
            (10, "daily", "BLOCK1:ZONE2_WALL_4_8_9", "Wall Gain", "W"),
            (11, "daily", "Meter", "BLOCK1:ZONE1#LIGHTS", "J"),
            (12, "daily", "Meter", "BLOCK1:ZONE2#LIGHTS", "J"),
            (13, "daily", "Some Flow 1", "Mass Flow", "kg/s"),
            (14, "daily", "Some Curve", "Performance Curve Input Variable 1", "kg/s"),
        ]
        names = ["id", "table", "key", "type", "units"]

        daily_columns = pd.MultiIndex.from_tuples(daily_variables, names=names)
        daily_index = pd.DatetimeIndex(
            pd.date_range("2002-1-1", freq="d", periods=3), name="timestamp"
        )
        daily_results = pd.DataFrame(
            [
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            ],
            columns=daily_columns,
            index=daily_index,
        )

        monthly_variables = [(15, "monthly", "Some Flow 1", "Mass Flow", "kg/s")]
        monthly_columns = pd.MultiIndex.from_tuples(monthly_variables, names=names)
        monthly_index = pd.DatetimeIndex([datetime(2002, 1, 1)], name="timestamp")
        monthly_results = pd.DataFrame([[1]], columns=monthly_columns, index=monthly_index)

        range_variables = [
            (16, "range", "BLOCK1:ZONE1", "Zone Temperature", "DON'T GROUP"),
            (17, "range", "BLOCK1:ZONE2", "Zone Temperature", "DON'T GROUP"),
            (18, "range", "BLOCK1:ZONE3", "Zone Temperature", "C"),
            (19, "range", "BLOCK1:ZONE1", "Heating Load", "W"),
        ]

        range_columns = pd.MultiIndex.from_tuples(range_variables, names=names)
        range_index = pd.RangeIndex(start=0, step=1, stop=2, name="range")
        range_results = pd.DataFrame(
            [[1, 2, 3, 4], [1, 2, 3, 4],], columns=range_columns, index=range_index
        )

        tables = DFTables()
        tables["daily"] = daily_results
        tables["monthly"] = monthly_results
        tables["range"] = range_results

        tree = Tree.from_header_dict(tables.get_all_variables_dct())

        bf = ResultsFile("dummy/path", "base", datetime.utcnow(), tables, tree, "test")
        cls.tf = ResultsFile.from_totals(bf)

    def test_file_name(self):
        self.assertEqual(self.tf.file_name, "base - totals")

    def test_file_path(self):
        self.assertEqual(self.tf.file_path, "dummy/path")

    def test_file_type(self):
        self.assertEqual("totals", self.tf.file_type)

    def test_search_tree(self):
        ids = self.tf.find_id(
            [
                Variable("daily", "Zone", "Zone Temperature", "C"),
                Variable("daily", "Meter", "LIGHTS", "J"),
                Variable("range", "Heating", "Heating Load", "W"),
            ]
        )
        self.assertListEqual(ids, [1, 6, 10])

    def test_grouped_variables(self):
        test_columns = pd.MultiIndex.from_tuples(
            [
                (1, "daily", "Zone", "Zone Temperature", "C"),
                (2, "daily", "Heating", "Heating Load", "W"),
                (3, "daily", "Windows", "Window Gain", "W"),
                (4, "daily", "Windows", "Window Lost", "W"),
                (5, "daily", "Walls", "Wall Gain", "W"),
                (6, "daily", "Meter", "LIGHTS", "J"),
            ],
            names=["id", "table", "key", "type", "units"],
        )

        test_index = pd.DatetimeIndex(
            pd.date_range("2002-1-1", freq="d", periods=3), name="timestamp"
        )
        test_results = pd.DataFrame(
            [[2, 4, 6, 8, 9.5, 23], [2, 4, 6, 8, 9.5, 23], [2, 4, 6, 8, 9.5, 23],],
            columns=test_columns,
            index=test_index,
        )

        pd.testing.assert_frame_equal(self.tf.tables["daily"], test_results, check_dtype=False)

    def test_non_grouped_variables(self):
        test_columns = pd.MultiIndex.from_tuples(
            [
                (7, "range", "BLOCK1:ZONE1", "Zone Temperature", "DON'T GROUP"),
                (8, "range", "BLOCK1:ZONE2", "Zone Temperature", "DON'T GROUP"),
                (9, "range", "Zone", "Zone Temperature", "C"),
                (10, "range", "Heating", "Heating Load", "W"),
            ],
            names=["id", "table", "key", "type", "units"],
        )

        test_index = pd.RangeIndex(start=0, step=1, stop=2, name="range")
        test_results = pd.DataFrame(
            [[1, 2, 3, 4], [1, 2, 3, 4],], columns=test_columns, index=test_index
        )

        pd.testing.assert_frame_equal(self.tf.tables["range"], test_results)

    def test_empty_table(self):
        with self.assertRaises(KeyError):
            _ = self.tf.tables["monthly"]

    def test_generate_diff_file(self):
        df = ResultsFile.from_diff(self.tf, self.tf)
        self.assertTrue(df.complete)

    def test_only_simple_tables(self):
        rf = ResultsFile.from_excel(
            Path(ROOT, "eso_files/test_excel_results.xlsx"),
            sheet_names=["simple-template-monthly", "simple-template-daily"],
        )
        with self.assertRaises(NoResults):
            _ = ResultsFile.from_totals(rf)
