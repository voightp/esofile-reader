import unittest
import os
from esofile_reader.outputs.sql_outputs import SQLOutputs
from esofile_reader import EsoFile
from tests import ROOT


class TestSqlDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        file_path = os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso")
        cls.ef = EsoFile(file_path, ignore_peaks=True, report_progress=False)

    def test_set_up_db(self):
        SQLOutputs.set_up_db()
        self.assertIsNotNone(SQLOutputs.ENGINE)
        self.assertIsNotNone(SQLOutputs.METADATA)
        self.assertListEqual(list(SQLOutputs.METADATA.tables.keys()), ["result_files"])

    def test_store_file(self):
        SQLOutputs.set_up_db()
        SQLOutputs.store_file(self.ef)

        self.assertListEqual(
            list(SQLOutputs.METADATA.tables.keys()),
            ['result_files', 'indexes-1', 'outputs-timestep-1', 'outputs-hourly-1',
             'outputs-daily-1', 'outputs-monthly-1', 'outputs-runperiod-1', 'outputs-annual-1']
        )

    def test_delete_file(self):
        SQLOutputs.set_up_db()
        SQLOutputs.store_file(self.ef)
        SQLOutputs.delete_file(1)
        print(SQLOutputs.METADATA.tables.keys())

        self.assertListEqual(
            list(SQLOutputs.METADATA.tables.keys()),
            ['result_files']
        )
