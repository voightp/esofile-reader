import os
import unittest

import pandas as pd
from esofile_reader import EsoFile
from esofile_reader.storage.df_storage import DFStorage
from tests import ROOT


class TestDFDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        file_path = os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso")
        cls.ef = EsoFile(file_path, ignore_peaks=True, report_progress=False)

    def test_01_store_file(self):
        id1 = DFStorage.store_file(self.ef)
        id2 = DFStorage.store_file(self.ef, totals=True)

        self.assertEqual(id1, 0)
        self.assertEqual(id2, 1)

        for interval in self.ef.available_intervals:
            self.assertEqual(self.ef.file_name, DFStorage.FILES[0].file_name)
            self.assertEqual(self.ef.file_path, DFStorage.FILES[0].file_path)
            self.assertEqual(self.ef.file_created, DFStorage.FILES[0].file_created)
            self.assertEqual(self.ef._search_tree, DFStorage.FILES[0]._search_tree)
            pd.testing.assert_frame_equal(
                self.ef.storage.tables[interval],
                DFStorage.FILES[0].storage.tables[interval]
            )

        self.assertFalse(DFStorage.FILES[0].totals)
        self.assertTrue(DFStorage.FILES[1].totals)

    def test_02_delete_file(self):
        DFStorage.delete_file(1)

        with self.assertRaises(KeyError):
            DFStorage.delete_file(1)

    def test_03_get_all_file_names(self):
        names = DFStorage.get_all_file_names()
        self.assertListEqual(names, ['eplusout_all_intervals'])
