import os
import unittest

from pathlib import Path
from esofile_reader import EsoFile
from esofile_reader.data.sql_data import SQLData
from esofile_reader.storage.sql_storage import SQLStorage
from esofile_reader.storage.pqt_storage import ParquetStorage
from esofile_reader import TotalsFile
from tests import ROOT


class TestParquetDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        file_path1 = os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso")
        file_path2 = os.path.join(ROOT, "eso_files/eplusout1.eso")
        cls.ef1 = EsoFile(file_path1, ignore_peaks=True)
        cls.ef2 = EsoFile(file_path2, ignore_peaks=True)
        cls.storage = None

    def setUp(self):
        self.storage = ParquetStorage()

    def tearDown(self):
        self.storage = None

    @classmethod
    def tearDownClass(cls):
        pass

    def test_01_set_up_db(self):
        self.assertTrue(Path(self.storage.temp_dir).exists())
        self.assertIsNone(self.storage.path)

    def test_02_store_file(self):
        self.storage.store_file(self.ef1)
        self.assertEqual("eplusout_all_intervals", self.storage.files[0].file_name)
        self.assertFalse(self.storage.files[0].totals)

    def test_03_store_totals_file(self):
        tf = TotalsFile(self.ef1)
        self.storage.store_file(tf)
        self.assertEqual("eplusout_all_intervals - totals", self.storage.files[0].file_name)
        self.assertTrue(self.storage.files[0].totals)

    def test_04_store_multiple_files(self):
        id1 = self.storage.store_file(self.ef1)
        id2 = self.storage.store_file(self.ef2)
        self.assertListEqual(
            ["eplusout_all_intervals", "eplusout1"],
            self.storage.get_all_file_names()
        )
        self.assertEqual(
            "eplusout_all_intervals",
            self.storage.files[id1].file_name
        )
        self.assertEqual(
            "eplusout1",
            self.storage.files[id2].file_name
        )

    def test_05_delete_file_(self):
        id1 = self.storage.store_file(self.ef1)
        pqf = self.storage.files[id1]

        path = pqf.path
        self.assertTrue(path.exists())

        self.storage.delete_file(id1)
        self.assertFalse(path.exists())

    #
    # def test_05_delete_file(self):
    #     storage = SQLStorage()
    #     id_ = storage.store_file(self.ef)
    #     storage.delete_file(1)
    #
    #     self.assertListEqual(
    #         list(storage.metadata.tables.keys()),
    #         ["result-files"]
    #     )
    #
    #     res = storage.engine.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
    #     self.assertEqual(res.fetchone()[0], "result-files")
    #
    # def test_06_load_all_files(self):
    #     storage = SQLStorage()
    #     self.maxDiff = None
    #     id1 = storage.store_file(self.ef)
    #     id2 = storage.store_file(EsoFile(os.path.join(ROOT, "eso_files/eplusout1.eso")))
    #
    #     db_file1 = storage.files[id1]
    #     db_file2 = storage.files[id2]
    #
    #     del storage.files[id1]
    #     del storage.files[id2]
    #
    #     storage.load_all_files()
    #     loaded_db_files = storage.files.values()
    #
    #     for f, lf in zip([db_file1, db_file2], loaded_db_files):
    #         self.assertEqual(f.file_name, lf.file_name)
    #         self.assertEqual(f.file_path, lf.file_path)
    #         self.assertEqual(f.id_, lf.id_)
    #         self.assertEqual(len(f.search_tree.str_tree()), len(lf.search_tree.str_tree()))
    #
    #         for interval in f.available_intervals:
    #             self.assertEqual(
    #                 len(f.get_header_dictionary(interval)),
    #                 len(lf.get_header_dictionary(interval))
    #             )
    #
    # def test_07_delete_file_invalid(self):
    #     storage = SQLStorage()
    #     with self.assertRaises(KeyError):
    #         storage.delete_file(1000)
    #
    # def test_08_get_all_file_names(self):
    #     storage = SQLStorage()
    #     storage.store_file(self.ef)
    #     storage.load_all_files()
    #     names = storage.get_all_file_names()
    #     self.assertEqual(names, ["eplusout_all_intervals"])
