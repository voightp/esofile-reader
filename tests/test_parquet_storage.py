import logging
import os
import unittest
from pathlib import Path

from pandas.testing import assert_frame_equal

from esofile_reader import EsoFile
from esofile_reader import TotalsFile
from esofile_reader.data.pqt_data import ParquetFrame
from esofile_reader.processor.monitor import DefaultMonitor
from esofile_reader.storage.pqt_storage import ParquetStorage
from esofile_reader.storage.storage_files import ParquetFile
from tests import ROOT

logging.basicConfig(level=logging.INFO)


class TestParquetDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        file_path1 = os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso")
        file_path2 = os.path.join(ROOT, "eso_files/eplusout1.eso")
        cls.ef1 = EsoFile(file_path1, ignore_peaks=True)
        cls.ef2 = EsoFile(file_path2, ignore_peaks=True)
        cls.ef3 = TotalsFile(cls.ef2)

    @classmethod
    def tearDownClass(cls):
        try:
            Path("pqs" + ParquetStorage.EXT).unlink()
        except FileNotFoundError:
            pass

    def setUp(self):
        self.storage = ParquetStorage()

    def tearDown(self):
        self.storage = None

    def test_01_set_up_db(self):
        self.assertTrue(Path(self.storage.workdir).exists())
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
            ["eplusout_all_intervals", "eplusout1"], self.storage.get_all_file_names()
        )
        self.assertEqual("eplusout_all_intervals", self.storage.files[id1].file_name)
        self.assertEqual("eplusout1", self.storage.files[id2].file_name)

    def test_05_delete_file_(self):
        id1 = self.storage.store_file(self.ef1)
        pqf = self.storage.files[id1]

        path = pqf.workdir
        self.assertTrue(path.exists())

        self.storage.delete_file(id1)
        self.assertFalse(path.exists())

    def test_06_save_parquet_file(self):
        ParquetFrame.CHUNK_SIZE = 10
        id_ = self.storage.store_file(self.ef1)
        self.storage.files[id_].save_as("", "pqf")
        self.assertTrue(Path("pqf" + ParquetFile.EXT).exists())

    def test_07_load_parquet_file(self):
        pqf = ParquetFile.load_file("pqf" + ParquetFile.EXT, "")
        Path("pqf" + ParquetFile.EXT).unlink()

        self.assertEqual(self.ef1.file_path, pqf.file_path)
        self.assertEqual(self.ef1.file_name, pqf.file_name)
        self.assertEqual(self.ef1.file_created, pqf.file_created)
        self.assertFalse(pqf.totals)
        self.assertEqual(self.ef1.file_path, pqf.file_path)

        for interval in self.ef1.available_intervals:
            assert_frame_equal(
                self.ef1.as_df(interval), pqf.as_df(interval), check_column_type=False
            )

    def test_08_save_as_storage(self):
        ParquetFrame.CHUNK_SIZE = 10
        self.storage.store_file(self.ef1)
        self.storage.store_file(self.ef2)
        self.storage.store_file(TotalsFile(self.ef2))
        self.storage.save_as("", "pqs")
        self.assertTrue(Path("pqs" + ParquetStorage.EXT).exists())

    def test_09_load_storage(self):
        pqs = ParquetStorage.load_storage("pqs" + ParquetStorage.EXT)

        self.assertEqual(self.ef1.file_path, pqs.files[0].file_path)
        self.assertEqual(self.ef1.file_name, pqs.files[0].file_name)
        self.assertEqual(self.ef1.file_created, pqs.files[0].file_created)
        self.assertFalse(pqs.files[0].totals)
        self.assertEqual(self.ef1.file_path, pqs.files[0].file_path)

        self.assertEqual(self.ef2.file_path, pqs.files[1].file_path)
        self.assertEqual(self.ef2.file_name, pqs.files[1].file_name)
        self.assertEqual(self.ef2.file_created, pqs.files[1].file_created)
        self.assertFalse(pqs.files[1].totals)
        self.assertEqual(self.ef2.file_path, pqs.files[1].file_path)

        self.assertEqual(self.ef3.file_path, pqs.files[2].file_path)
        self.assertEqual(self.ef3.file_name, pqs.files[2].file_name)
        self.assertEqual(self.ef3.file_created, pqs.files[2].file_created)
        self.assertTrue(pqs.files[2].totals)
        self.assertEqual(self.ef3.file_path, pqs.files[2].file_path)

        for interval in self.ef1.available_intervals:
            assert_frame_equal(
                self.ef1.as_df(interval), pqs.files[0].as_df(interval), check_column_type=False
            )

    def test_10_delete_file_save_storage(self):
        pqs = ParquetStorage.load_storage("pqs" + ParquetStorage.EXT)
        pqs.delete_file(0)
        pqs.delete_file(1)
        pqs.save()

        loaded_pqs = ParquetStorage.load_storage(pqs.path)
        for interval in self.ef3.available_intervals:
            assert_frame_equal(
                self.ef3.as_df(interval),
                loaded_pqs.files[2].as_df(interval),
                check_column_type=False,
            )
        self.assertEqual(len(loaded_pqs.files.keys()), 1)

    def test_11_invalid_extension(self):
        with self.assertRaises(IOError):
            ParquetStorage.load_storage("test.foo")

    def test_12_path_not_set(self):
        with self.assertRaises(FileNotFoundError):
            pqs = ParquetStorage()
            pqs.save()

    def test_12_storage_monitor(self):
        monitor = DefaultMonitor("foo")
        ef = EsoFile(
            os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso"), monitor=monitor
        )
        pqs = ParquetStorage()
        id_ = pqs.store_file(ef, monitor=monitor)
        self.assertEqual(0, id_)

        monitor = DefaultMonitor("bar")
        tf = TotalsFile(ef)
        id_ = pqs.store_file(tf, monitor=monitor)
        self.assertEqual(1, id_)

    def test_13_merge_storages(self):
        self.storage.store_file(self.ef1)
        self.storage.store_file(self.ef2)

        self.storage.save_as("", "pqs1")
        self.storage.save_as("", "pqs2")

        p1 = Path("pqs1" + ParquetStorage.EXT)
        p2 = Path("pqs2" + ParquetStorage.EXT)

        self.storage.merge_with([p1, p2])

        self.assertEqual(6, len(self.storage.files))

        for f in self.storage.files.values():
            print(f.id_)
            print(f)

        for interval in self.ef1.available_intervals:
            test_df = self.ef1.as_df(interval)
            assert_frame_equal(
                test_df, self.storage.files[0].as_df(interval), check_column_type=False
            )
            assert_frame_equal(
                test_df, self.storage.files[2].as_df(interval), check_column_type=False
            )
            assert_frame_equal(
                test_df, self.storage.files[4].as_df(interval), check_column_type=False
            )

        for interval in self.ef2.available_intervals:
            test_df = self.ef2.as_df(interval)
            assert_frame_equal(
                test_df, self.storage.files[1].as_df(interval), check_column_type=False
            )
            assert_frame_equal(
                test_df, self.storage.files[3].as_df(interval), check_column_type=False
            )
            assert_frame_equal(
                test_df, self.storage.files[5].as_df(interval), check_column_type=False
            )

        p1.unlink()
        p2.unlink()
