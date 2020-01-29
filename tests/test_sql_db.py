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

    def test_set_up_db(self):
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
