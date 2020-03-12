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
from tests import ROOT, EF1, EF_ALL_INTERVALS

logging.basicConfig(level=logging.INFO)


class TestParquetStorage(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tf = TotalsFile(EF1)

    @classmethod
    def tearDownClass(cls):
        try:
            Path("pqs" + ParquetStorage.EXT).unlink()
        except FileNotFoundError:
            pass

    def setUp(self):
        self.storage = ParquetStorage()

    def tearDown(self):
        del self.storage
        self.storage = None

    def test_01_set_up_db(self):
        self.assertTrue(Path(self.storage.workdir).exists())
        self.assertIsNone(self.storage.path)

    def test_02_store_file(self):
        self.storage.store_file(EF_ALL_INTERVALS)
        self.assertEqual("eplusout_all_intervals", self.storage.files[0].file_name)
        self.assertFalse(self.storage.files[0].totals)

    def test_03_store_totals_file(self):
        tf = TotalsFile(EF_ALL_INTERVALS)
        self.storage.store_file(tf)
        self.assertEqual("eplusout_all_intervals - totals", self.storage.files[0].file_name)
        self.assertTrue(self.storage.files[0].totals)

    def test_04_store_multiple_files(self):
        id1 = self.storage.store_file(EF_ALL_INTERVALS)
        id2 = self.storage.store_file(EF1)
        self.assertListEqual(
            ["eplusout_all_intervals", "eplusout1"], self.storage.get_all_file_names()
        )
        self.assertEqual("eplusout_all_intervals", self.storage.files[id1].file_name)
        self.assertEqual("eplusout1", self.storage.files[id2].file_name)

    def test_05_delete_file(self):
        id1 = self.storage.store_file(EF_ALL_INTERVALS)
        pqf = self.storage.files[id1]

        path = pqf.workdir
        self.assertTrue(path.exists())

        self.storage.delete_file(id1)
        self.assertFalse(path.exists())

    def test_06_save_parquet_file(self):
        ParquetFrame.CHUNK_SIZE = 10
        id_ = self.storage.store_file(EF_ALL_INTERVALS)
        self.storage.files[id_].save_as("", "pqf")
        self.assertTrue(Path("pqf" + ParquetFile.EXT).exists())

    def test_07_load_parquet_file(self):
        pqf = ParquetFile.load_file("pqf" + ParquetFile.EXT, "")
        Path("pqf" + ParquetFile.EXT).unlink()

        self.assertEqual(EF_ALL_INTERVALS.file_path, pqf.file_path)
        self.assertEqual(EF_ALL_INTERVALS.file_name, pqf.file_name)
        self.assertEqual(EF_ALL_INTERVALS.file_created, pqf.file_created)
        self.assertFalse(pqf.totals)
        self.assertEqual(EF_ALL_INTERVALS.file_path, pqf.file_path)

        for interval in EF_ALL_INTERVALS.available_intervals:
            assert_frame_equal(
                EF_ALL_INTERVALS.as_df(interval), pqf.as_df(interval), check_column_type=False
            )
        pqf.clean_up()

    def test_08_save_as_storage(self):
        ParquetFrame.CHUNK_SIZE = 10
        self.storage.store_file(EF_ALL_INTERVALS)
        self.storage.store_file(EF1)
        self.storage.store_file(TotalsFile(EF1))
        self.storage.save_as("", "pqs")
        self.assertTrue(Path("pqs" + ParquetStorage.EXT).exists())

    def test_09_load_storage(self):
        pqs = ParquetStorage.load_storage("pqs" + ParquetStorage.EXT)

        self.assertEqual(EF_ALL_INTERVALS.file_path, pqs.files[0].file_path)
        self.assertEqual(EF_ALL_INTERVALS.file_name, pqs.files[0].file_name)
        self.assertEqual(EF_ALL_INTERVALS.file_created, pqs.files[0].file_created)
        self.assertFalse(pqs.files[0].totals)
        self.assertEqual(EF_ALL_INTERVALS.file_path, pqs.files[0].file_path)

        self.assertEqual(EF1.file_path, pqs.files[1].file_path)
        self.assertEqual(EF1.file_name, pqs.files[1].file_name)
        self.assertEqual(EF1.file_created, pqs.files[1].file_created)
        self.assertFalse(pqs.files[1].totals)
        self.assertEqual(EF1.file_path, pqs.files[1].file_path)

        self.assertEqual(self.tf.file_path, pqs.files[2].file_path)
        self.assertEqual(self.tf.file_name, pqs.files[2].file_name)
        self.assertEqual(self.tf.file_created, pqs.files[2].file_created)
        self.assertTrue(pqs.files[2].totals)
        self.assertEqual(self.tf.file_path, pqs.files[2].file_path)

        for interval in EF_ALL_INTERVALS.available_intervals:
            assert_frame_equal(
                EF_ALL_INTERVALS.as_df(interval),
                pqs.files[0].as_df(interval),
                check_column_type=False,
            )

    def test_10_delete_file_save_storage(self):
        pqs = ParquetStorage.load_storage("pqs" + ParquetStorage.EXT)
        pqs.delete_file(0)
        pqs.delete_file(1)
        pqs.save()

        loaded_pqs = ParquetStorage.load_storage(pqs.path)
        for interval in self.tf.available_intervals:
            assert_frame_equal(
                self.tf.as_df(interval),
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

    def test_13_storage_monitor(self):
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

    def test_14_merge_storages(self):
        self.storage.store_file(EF_ALL_INTERVALS)
        self.storage.store_file(EF1)

        self.storage.save_as("", "pqs1")
        self.storage.save_as("", "pqs2")

        p1 = Path("pqs1" + ParquetStorage.EXT)
        p2 = Path("pqs2" + ParquetStorage.EXT)

        self.storage.merge_with([p1, p2])
        ef1_files = [
            f for f in self.storage.files.values() if f.file_name == EF_ALL_INTERVALS.file_name
        ]
        ef2_files = [f for f in self.storage.files.values() if f.file_name == EF1.file_name]

        self.assertEqual(6, len(self.storage.files))

        for interval in EF_ALL_INTERVALS.available_intervals:
            test_df = EF_ALL_INTERVALS.as_df(interval)
            for f in ef1_files:
                assert_frame_equal(test_df, f.as_df(interval), check_column_type=False)

        for interval in EF1.available_intervals:
            test_df = EF1.as_df(interval)
            for f in ef2_files:
                assert_frame_equal(test_df, f.as_df(interval), check_column_type=False)

        p1.unlink()
        p2.unlink()

    def test_15_parquet_file_context_manager(self):
        with ParquetFile(
            id_=0,
            file_path=EF_ALL_INTERVALS.file_path,
            file_name=EF_ALL_INTERVALS.file_name,
            data=EF_ALL_INTERVALS.data,
            file_created=EF_ALL_INTERVALS.file_created,
            search_tree=EF_ALL_INTERVALS.search_tree,
            totals=isinstance(EF_ALL_INTERVALS, TotalsFile),
            pardir="",
            name="foo",
            monitor=None,
        ) as pqf:
            workdir = pqf.workdir
            self.assertTrue(workdir.exists())
        self.assertFalse(workdir.exists())
