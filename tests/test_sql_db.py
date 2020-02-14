import os
import unittest

from esofile_reader import EsoFile, TotalsFile
from esofile_reader.data.sql_data import SQLData
from esofile_reader.storage.sql_storage import SQLStorage
from tests import ROOT


class TestSqlDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        file_path = os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso")
        cls.ef = EsoFile(file_path, ignore_peaks=True)

    def test_01_store_file_not_set_up(self):
        storage = SQLStorage()
        storage.engine = None
        storage.metadata = None
        with self.assertRaises(AttributeError):
            storage.store_file(self.ef)

    def test_02_set_up_db(self):
        storage = SQLStorage()
        self.assertIsNotNone(storage.engine)
        self.assertIsNotNone(storage.metadata)
        self.assertListEqual(list(storage.metadata.tables.keys()), ["result-files"])

        self.assertListEqual(
            list(storage.metadata.tables.keys()),
            ["result-files"]
        )

        res = storage.engine.execute("""SELECT name FROM sqlite_master""")
        self.assertEqual(res.fetchone()[0], "result-files")

    def test_03_store_file(self):
        storage = SQLStorage()
        storage.store_file(self.ef)
        tables = [
            'result-files', '1-results-timestep', '1-index-timestep',
            '1-day-timestep', '1-results-hourly', '1-index-hourly', '1-day-hourly',
            '1-results-daily', '1-index-daily', '1-day-daily', '1-results-monthly',
            '1-index-monthly', '1-n_days-monthly', '1-results-runperiod',
            '1-index-runperiod', '1-n_days-runperiod', '1-results-annual',
            '1-index-annual', '1-n_days-annual'
        ]

        self.assertListEqual(list(storage.metadata.tables.keys()), tables)

        res = storage.engine.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
        self.assertListEqual([i[0] for i in res.fetchall()], tables)

    def test_04_store_file_totals(self):
        storage = SQLStorage()
        id_ = storage.store_file(TotalsFile(self.ef))
        res = storage.engine.execute(f"""SELECT totals FROM 'result-files' WHERE id={id_};""").scalar()
        self.assertTrue(res)

    def test_05_delete_file(self):
        storage = SQLStorage()
        id_ = storage.store_file(self.ef)
        storage.delete_file(1)

        self.assertListEqual(
            list(storage.metadata.tables.keys()),
            ["result-files"]
        )

        res = storage.engine.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
        self.assertEqual(res.fetchone()[0], "result-files")

    def test_06_load_all_files(self):
        storage = SQLStorage()
        self.maxDiff = None
        id1 = storage.store_file(self.ef)
        id2 = storage.store_file(EsoFile(os.path.join(ROOT, "eso_files/eplusout1.eso")))

        db_file1 = storage.files[id1]
        db_file2 = storage.files[id2]

        del storage.files[id1]
        del storage.files[id2]

        storage.load_all_files()
        loaded_db_files = storage.files.values()

        for f, lf in zip([db_file1, db_file2], loaded_db_files):
            self.assertEqual(f.file_name, lf.file_name)
            self.assertEqual(f.file_path, lf.file_path)
            self.assertEqual(f.id_, lf.id_)
            self.assertEqual(len(f.search_tree.str_tree()), len(lf.search_tree.str_tree()))

            for interval in f.available_intervals:
                self.assertEqual(
                    len(f.get_header_dictionary(interval)),
                    len(lf.get_header_dictionary(interval))
                )

    def test_07_delete_file_invalid(self):
        storage = SQLStorage()
        with self.assertRaises(KeyError):
            storage.delete_file(1000)

    def test_08_get_all_file_names(self):
        storage = SQLStorage()
        storage.store_file(self.ef)
        storage.load_all_files()
        names = storage.get_all_file_names()
        self.assertEqual(names, ["eplusout_all_intervals"])
