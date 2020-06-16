import unittest

import pandas as pd

from esofile_reader import TotalsFile
from esofile_reader.storage.df_storage import DFStorage
from tests import EF_ALL_INTERVALS


class TestDFDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.storage = DFStorage()

    def test_01_store_file(self):
        id1 = self.storage.store_file(EF_ALL_INTERVALS)
        id2 = self.storage.store_file(TotalsFile(EF_ALL_INTERVALS))

        self.assertEqual(id1, 0)
        self.assertEqual(id2, 1)

        for table in EF_ALL_INTERVALS.table_names:
            self.assertEqual(EF_ALL_INTERVALS.file_name, self.storage.files[0].file_name)
            self.assertEqual(EF_ALL_INTERVALS.file_path, self.storage.files[0].file_path)
            self.assertEqual(EF_ALL_INTERVALS.file_created, self.storage.files[0].file_created)
            self.assertEqual(EF_ALL_INTERVALS.search_tree, self.storage.files[0].search_tree)
            pd.testing.assert_frame_equal(
                EF_ALL_INTERVALS.data.tables[table],
                self.storage.files[0].data.tables[table],
            )

        self.assertEqual(self.storage.files[0].type_, "EsoFile")
        self.assertEqual(self.storage.files[1].type_, "TotalsFile")

    def test_02_delete_file(self):
        self.storage.delete_file(1)

        with self.assertRaises(KeyError):
            self.storage.delete_file(1)

    def test_03_get_all_file_names(self):
        names = self.storage.get_all_file_names()
        self.assertListEqual(names, ["eplusout_all_intervals"])
