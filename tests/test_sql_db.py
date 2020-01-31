import os
import unittest

from esofile_reader import EsoFile
from esofile_reader.outputs.sql_data import SQLData
from tests import ROOT


class TestSqlDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        file_path = os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso")
        cls.ef = EsoFile(file_path, ignore_peaks=True, report_progress=False)

    def test_1_store_file_not_set_up(self):
        SQLData.ENGINE = None
        SQLData.METADATA = None
        with self.assertRaises(AttributeError):
            SQLData.store_file(self.ef)

    def test_2_set_up_db(self):
        SQLData.set_up_db()
        self.assertIsNotNone(SQLData.ENGINE)
        self.assertIsNotNone(SQLData.METADATA)
        self.assertListEqual(list(SQLData.METADATA.tables.keys()), ["result-files"])

        self.assertListEqual(
            list(SQLData.METADATA.tables.keys()),
            ["result-files"]
        )

        res = SQLData.ENGINE.execute("""SELECT name FROM sqlite_master""")
        self.assertEqual(res.fetchone()[0], "result-files")

    def test_store_file(self):
        SQLData.set_up_db()
        SQLData.store_file(self.ef)
        tables = [
            'result-files', '1-results-timestep', '1-index-timestep',
            '1-day-timestep', '1-results-hourly', '1-index-hourly', '1-day-hourly',
            '1-results-daily', '1-index-daily', '1-day-daily', '1-results-monthly',
            '1-index-monthly', '1-n_days-monthly', '1-results-runperiod',
            '1-index-runperiod', '1-n_days-runperiod', '1-results-annual',
            '1-index-annual', '1-n_days-annual'
        ]

        self.assertListEqual(list(SQLData.METADATA.tables.keys()), tables)

        res = SQLData.ENGINE.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
        self.assertListEqual([i[0] for i in res.fetchall()], tables)

    def test_delete_file(self):
        SQLData.set_up_db()
        SQLData.store_file(self.ef)
        SQLData.delete_file(1)

        self.assertListEqual(
            list(SQLData.METADATA.tables.keys()),
            ["result-files"]
        )

        res = SQLData.ENGINE.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
        self.assertEqual(res.fetchone()[0], "result-files")

    def test_load_file(self):
        SQLData.set_up_db()
        db_file = SQLData.store_file(self.ef)

        loaded_db_file = SQLData.load_file(db_file.id_)

        self.assertEqual(db_file.file_name, loaded_db_file.file_name)
        self.assertEqual(db_file.file_path, loaded_db_file.file_path)
        self.assertEqual(db_file.id_, loaded_db_file.id_)
        self.assertEqual(db_file._search_tree.str_tree(), loaded_db_file._search_tree.str_tree())

        for interval in db_file.available_intervals:
            self.assertEqual(
                len(db_file.get_header_dictionary(interval)),
                len(loaded_db_file.get_header_dictionary(interval))
            )

    def test_load_all_files(self):
        SQLData.set_up_db()
        self.maxDiff = None
        db_file1 = SQLData.store_file(self.ef)
        db_file2 = SQLData.store_file(EsoFile(os.path.join(ROOT, "eso_files/eplusout1.eso")))

        loaded_db_files = SQLData.load_all_files()

        for f, lf in zip([db_file1, db_file2], loaded_db_files):
            self.assertEqual(f.file_name, lf.file_name)
            self.assertEqual(f.file_path, lf.file_path)
            self.assertEqual(f.id_, lf.id_)
            self.assertEqual(len(f._search_tree.str_tree()), len(lf._search_tree.str_tree()))

            for interval in f.available_intervals:
                self.assertEqual(
                    len(f.get_header_dictionary(interval)),
                    len(lf.get_header_dictionary(interval))
                )

    def test_load_file_invalid(self):
        with self.assertRaises(KeyError):
            SQLData.load_file(1000)

    def test_delete_file_invalid(self):
        with self.assertRaises(KeyError):
            SQLData.delete_file(1000)
