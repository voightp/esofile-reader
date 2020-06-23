import contextlib
import logging
import os
import unittest
from pathlib import Path

from pandas.testing import assert_frame_equal

from esofile_reader import EsoFile, ResultsFile
from esofile_reader.processing.monitor import DefaultMonitor
from esofile_reader.storages.pqt_storage import ParquetStorage, ParquetFile
from esofile_reader.tables.pqt_tables import ParquetFrame
from tests import ROOT, EF1, EF_ALL_INTERVALS

logging.basicConfig(level=logging.INFO)


class TestParquetStorage(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tf = ResultsFile.from_totals(EF1)

    @classmethod
    def tearDownClass(cls):
        files = [Path("pqs" + ParquetStorage.EXT), Path("file-0")]
        for f in files:
            with contextlib.suppress(FileNotFoundError):
                f.unlink()

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
        self.assertEqual("eso", self.storage.files[0].file_type)

    def test_03_store_totals_file(self):
        tf = ResultsFile.from_totals(EF_ALL_INTERVALS)
        self.storage.store_file(tf)
        self.assertEqual("eplusout_all_intervals - totals", self.storage.files[0].file_name)
        self.assertEqual("totals", self.storage.files[0].file_type)

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

        self.assertEqual(str(EF_ALL_INTERVALS.file_path), str(pqf.file_path))
        self.assertEqual(EF_ALL_INTERVALS.file_name, pqf.file_name)
        self.assertEqual(EF_ALL_INTERVALS.file_created, pqf.file_created)
        self.assertEqual("eso", pqf.file_type)
        self.assertEqual(str(EF_ALL_INTERVALS.file_path), str(pqf.file_path))

        for interval in EF_ALL_INTERVALS.table_names:
            assert_frame_equal(
                EF_ALL_INTERVALS.get_numeric_table(interval),
                pqf.get_numeric_table(interval),
                check_column_type=False,
            )
            pqf.tables[interval].get_df()
            assert_frame_equal(
                EF_ALL_INTERVALS.tables[interval],
                pqf.tables[interval].get_df(),
                check_column_type=False,
            )
        pqf.clean_up()

    def test_08_save_as_storage(self):
        ParquetFrame.CHUNK_SIZE = 10
        self.storage.store_file(EF_ALL_INTERVALS)
        self.storage.store_file(EF1)
        self.storage.store_file(ResultsFile.from_totals(EF1))
        self.storage.save_as("", "pqs")
        self.assertTrue(Path("pqs" + ParquetStorage.EXT).exists())

    def test_09_load_storage(self):
        pqs = ParquetStorage.load_storage("pqs" + ParquetStorage.EXT)

        self.assertEqual(str(EF_ALL_INTERVALS.file_path), str(pqs.files[0].file_path))
        self.assertEqual(EF_ALL_INTERVALS.file_name, pqs.files[0].file_name)
        self.assertEqual(EF_ALL_INTERVALS.file_created, pqs.files[0].file_created)
        self.assertEqual(EF_ALL_INTERVALS.file_type, pqs.files[0].file_type)
        self.assertEqual(str(EF_ALL_INTERVALS.file_path), pqs.files[0].file_path)

        self.assertEqual(str(EF1.file_path), str(pqs.files[1].file_path))
        self.assertEqual(EF1.file_name, pqs.files[1].file_name)
        self.assertEqual(EF1.file_created, pqs.files[1].file_created)
        self.assertEqual(EF1.file_type, pqs.files[1].file_type)
        self.assertEqual(str(EF1.file_path), pqs.files[1].file_path)

        self.assertEqual(str(self.tf.file_path), str(pqs.files[2].file_path))
        self.assertEqual(self.tf.file_name, pqs.files[2].file_name)
        self.assertEqual(self.tf.file_created, pqs.files[2].file_created)
        self.assertEqual("totals", pqs.files[2].file_type)
        self.assertEqual(str(self.tf.file_path), str(pqs.files[2].file_path))

        for interval in EF_ALL_INTERVALS.table_names:
            assert_frame_equal(
                EF_ALL_INTERVALS.get_numeric_table(interval),
                pqs.files[0].get_numeric_table(interval),
                check_column_type=False,
            )

    def test_10_delete_file_save_storage(self):
        pqs = ParquetStorage.load_storage("pqs" + ParquetStorage.EXT)
        pqs.delete_file(0)
        pqs.delete_file(1)
        pqs.save()

        loaded_pqs = ParquetStorage.load_storage(pqs.path)
        for interval in self.tf.table_names:
            assert_frame_equal(
                self.tf.get_numeric_table(interval),
                loaded_pqs.files[2].get_numeric_table(interval),
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
        tf = ResultsFile.from_totals(ef)
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

        for table in EF_ALL_INTERVALS.table_names:
            test_df = EF_ALL_INTERVALS.get_numeric_table(table)
            for f in ef1_files:
                assert_frame_equal(test_df, f.get_numeric_table(table), check_column_type=False)

        for table in EF1.table_names:
            test_df = EF1.get_numeric_table(table)
            for f in ef2_files:
                assert_frame_equal(test_df, f.get_numeric_table(table), check_column_type=False)

        p1.unlink()
        p2.unlink()

    def test_15_parquet_file_context_manager(self):
        with ParquetFile(
                id_=0,
                file_path=EF_ALL_INTERVALS.file_path,
                file_name=EF_ALL_INTERVALS.file_name,
                tables=EF_ALL_INTERVALS.tables,
                file_created=EF_ALL_INTERVALS.file_created,
                search_tree=EF_ALL_INTERVALS.search_tree,
                file_type=EF_ALL_INTERVALS.file_type,
                pardir="",
                name="foo",
                monitor=None,
        ) as pqf:
            workdir = pqf.workdir
            self.assertTrue(workdir.exists())
        self.assertFalse(workdir.exists())

    def test_16_load_invalid_parquet_file(self):
        with self.assertRaises(IOError):
            ParquetFile.load_file("foo.bar")

    def test_parquet_file_as_bytes(self):
        import io

        id_ = self.storage.store_file(EF_ALL_INTERVALS)
        out = self.storage.files[id_].save_as()
        self.assertIsInstance(out, io.BytesIO)
