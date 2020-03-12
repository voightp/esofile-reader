import unittest

import pandas as pd

from esofile_reader import TotalsFile
from esofile_reader.storage.df_storage import DFStorage
from tests import EF_ALL_INTERVALS


class TestDFDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.storage = DFStorage()
        cls.ef = EF_ALL_INTERVALS

    def test_01_store_file(self):
        id1 = self.storage.store_file(self.ef)
        id2 = self.storage.store_file(TotalsFile(self.ef))

        self.assertEqual(id1, 0)
        self.assertEqual(id2, 1)

        for interval in self.ef.available_intervals:
            self.assertEqual(self.ef.file_name, self.storage.files[0].file_name)
            self.assertEqual(self.ef.file_path, self.storage.files[0].file_path)
            self.assertEqual(self.ef.file_created, self.storage.files[0].file_created)
            self.assertEqual(self.ef.search_tree, self.storage.files[0].search_tree)
            pd.testing.assert_frame_equal(
                self.ef.data.tables[interval], self.storage.files[0].data.tables[interval]
            )

        self.assertFalse(self.storage.files[0].totals)
        self.assertTrue(self.storage.files[1].totals)

    def test_02_delete_file(self):
        self.storage.delete_file(1)

        with self.assertRaises(KeyError):
            self.storage.delete_file(1)

    def test_03_get_all_file_names(self):
        names = self.storage.get_all_file_names()
        self.assertListEqual(names, ["eplusout_all_intervals"])
