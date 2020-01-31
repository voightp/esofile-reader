from unittest import TestCase
from esofile_reader import TotalsFile
from esofile_reader.storage.df_storage import DFStorage
from esofile_reader.base_file import BaseFile
from esofile_reader.utils.search_tree import Tree
from tests import ROOT
import pandas as pd


class TestTotalsFile(TestCase):
    @classmethod
    def setUpClass(cls):
        bf = BaseFile()
        bf.file_name = "totals"
        variables = [
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
            (11, "daily", "Meter", "BLOCK1:ZONE1#LIGHTS", "W"),
            (12, "daily", "Meter", "BLOCK1:ZONE2#LIGHTS", "W"),
        ]
        columns = pd.MultiIndex.from_tuples(variables, names=["id", "interval", "key",
                                                              "variable", "units"])
        index = pd.DatetimeIndex(pd.date_range("2002-1-1", freq="d", periods=3),
                                 name="timestamp")
        results = pd.DataFrame([
            [25.123, 27.456, 14.546, 1000],
            [25.123, 27.456, 14.546, 2000],
            [25.123, 27.456, 14.546, 3000]
        ], columns=columns, index=index)

        data = DFStorage()
        data.populate_table("range", results)

        tree = Tree()
        tree.populate_tree(data.get_all_variables_dct())

        bf.storage = data
        bf._search_tree = tree

        cls.bf = bf

        # SQLStorage.set_up_db()
        # cls.db_bf = SQLStorage.store_file(bf)

    def test__get_group_key(self):
        self.fail()

    def test__get_keyword(self):
        self.fail()

    def test__calculate_totals(self):
        self.fail()

    def test__get_grouped_vars(self):
        self.fail()

    def test_process_totals(self):
        self.fail()

    def test_populate_content(self):
        self.fail()

    def test_generate_diff(self):
        self.fail()
