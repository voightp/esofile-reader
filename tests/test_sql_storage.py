import unittest

from esofile_reader import ResultsFile
from esofile_reader.storages.sql_storage import SQLStorage
from tests import EF_ALL_INTERVALS, EF1


class TestSqlDB(unittest.TestCase):
    def test_01_store_file_not_set_up(self):
        storage = SQLStorage()
        storage.engine = None
        storage.metatables = None
        with self.assertRaises(AttributeError):
            storage.store_file(EF_ALL_INTERVALS)

    def test_02_set_up_db(self):
        storage = SQLStorage()
        self.assertIsNotNone(storage.engine)
        self.assertIsNotNone(storage.metadata)
        self.assertListEqual(list(storage.metadata.tables.keys()), ["result-files"])
        self.assertListEqual(list(storage.metadata.tables.keys()), ["result-files"])

        res = storage.engine.execute("""SELECT name FROM sqlite_master""")
        self.assertEqual(res.fetchone()[0], "result-files")

    def test_03_store_file(self):
        storage = SQLStorage()
        storage.store_file(EF_ALL_INTERVALS)
        tables = [
            "result-files",
            "1-results-timestep",
            "1-index-timestep",
            "1-day-timestep",
            "1-results-hourly",
            "1-index-hourly",
            "1-day-hourly",
            "1-results-daily",
            "1-index-daily",
            "1-day-daily",
            "1-results-monthly",
            "1-index-monthly",
            "1-n days-monthly",
            "1-results-runperiod",
            "1-index-runperiod",
            "1-n days-runperiod",
            "1-results-annual",
            "1-index-annual",
            "1-n days-annual",
        ]
        self.assertListEqual(list(storage.metadata.tables.keys()), tables)
        res = storage.engine.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
        self.assertListEqual([i[0] for i in res.fetchall()], tables)

    def test_04_store_file_totals(self):
        storage = SQLStorage()
        id_ = storage.store_file(ResultsFile.from_totals(EF_ALL_INTERVALS))
        res = storage.engine.execute(
            f"""SELECT file_type FROM 'result-files' WHERE id={id_};"""
        ).scalar()
        self.assertEqual("totals", res)

    def test_05_delete_file(self):
        storage = SQLStorage()
        _ = storage.store_file(EF_ALL_INTERVALS)
        storage.delete_file(1)
        res = storage.engine.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
        self.assertListEqual(list(storage.metadata.tables.keys()), ["result-files"])
        self.assertEqual(res.fetchone()[0], "result-files")

    def test_06_load_all_files(self):
        storage = SQLStorage()
        self.maxDiff = None
        id1 = storage.store_file(EF_ALL_INTERVALS)
        id2 = storage.store_file(EF1)

        db_file1 = storage.files[id1]
        db_file2 = storage.files[id2]

        del storage.files[id1]
        del storage.files[id2]

        storage.load_all_files()
        loaded_db_files = storage.files.values()

        for f, lf in zip([db_file1, db_file2], loaded_db_files):
            self.assertEqual(f.file_name, lf.file_name)
            self.assertEqual(str(f.file_path), str(lf.file_path))
            self.assertEqual(f.id_, lf.id_)
            self.assertEqual(len(f.search_tree.__repr__()), len(lf.search_tree.__repr__()))
            self.assertEqual("eso", f.file_type)

            for interval in f.table_names:
                self.assertEqual(
                    len(f.get_header_dictionary(interval)),
                    len(lf.get_header_dictionary(interval)),
                )

    def test_07_delete_file_invalid(self):
        storage = SQLStorage()
        with self.assertRaises(KeyError):
            storage.delete_file(1000)

    def test_08_get_all_file_names(self):
        storage = SQLStorage()
        storage.store_file(EF_ALL_INTERVALS)
        storage.load_all_files()
        names = storage.get_all_file_names()
        self.assertEqual(names, ["eplusout_all_intervals"])
