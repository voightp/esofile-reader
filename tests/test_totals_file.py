from unittest import TestCase
from esofile_reader import TotalsFile
from esofile_reader.storage.df_storage import DFStorage
from esofile_reader.base_file import BaseFile
from esofile_reader.utils.search_tree import Tree
from esofile_reader import Variable
from tests import ROOT
import pandas as pd
from esofile_reader.utils.utils import incremental_id_gen


class TestTotalsFile(TestCase):
    @classmethod
    def setUpClass(cls):
        bf = BaseFile()
        bf.file_name = "base"
        bf.file_path = "dummy/path"
        daily_variables = [
            (1, "daily", "BLOCK1:ZONE1", "Temperature", "C"),
            (2, "daily", "BLOCK1:ZONE2", "Temperature", "C"),
            (3, "daily", "BLOCK1:ZONE3", "Temperature", "C"),
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
        names = ["id", "interval", "key", "variable", "units"]

        daily_columns = pd.MultiIndex.from_tuples(daily_variables, names=names)
        daily_index = pd.DatetimeIndex(pd.date_range("2002-1-1", freq="d", periods=3),
                                       name="timestamp")
        daily_results = pd.DataFrame([
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
        ], columns=daily_columns, index=daily_index)

        monthly_variables = [(15, "monthly", "Some Flow 1", "Mass Flow", "kg/s")]
        monthly_columns = pd.MultiIndex.from_tuples(monthly_variables, names=names)
        monthly_index = pd.DatetimeIndex([pd.datetime(2002, 1, 1)], name="timestamp")
        monthly_results = pd.DataFrame([[1]], columns=monthly_columns, index=monthly_index)

        range_variables = [
            (16, "range", "BLOCK1:ZONE1", "Temperature", "DON'T GROUP"),
            (17, "range", "BLOCK1:ZONE2", "Temperature", "DON'T GROUP"),
            (18, "range", "BLOCK1:ZONE3", "Temperature", "C"),
            (19, "range", "BLOCK1:ZONE1", "Heating Load", "W")
        ]

        range_columns = pd.MultiIndex.from_tuples(range_variables, names=names)
        range_index = pd.RangeIndex(start=0, step=1, stop=2, name="range")
        range_results = pd.DataFrame([
            [1, 2, 3, 4],
            [1, 2, 3, 4],
        ], columns=range_columns, index=range_index)

        data = DFStorage()
        data.populate_table("daily", daily_results)
        data.populate_table("monthly", monthly_results)
        data.populate_table("range", range_results)

        tree = Tree()
        tree.populate_tree(data.get_all_variables_dct())

        bf.storage = data
        bf._search_tree = tree

        cls.tf = TotalsFile(bf)

    def test_file_name(self):
        self.assertEqual(self.tf.file_name, "base - totals")

    def test_file_path(self):
        self.assertEqual(self.tf.file_path, "dummy/path")

    def test_search_tree(self):
        print(self.tf._search_tree)
        ids = self.tf.find_ids([
            Variable("daily", "Temperature", "Temperature", "C"),
            Variable("daily", "Meter", "LIGHTS", "J"),
            Variable("range", "Heating", "Heating Load", "W")
        ])
        self.assertListEqual(ids, [1, 6, 10])

    def test_grouped_variables(self):
        pd.set_option("display.max_columns", 10)
        for t in self.tf.available_intervals:
            print(self.tf.storage.tables[t])

    def test_generate_diff_file(self):
        pass
