import unittest
from esofile_reader import EsoFile, DiffFile, TotalsFile


class TestFileGeneration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ef = EsoFile("../tests/eso_files/eplusout_all_intervals.eso", ignore_peaks=True, report_progress=False)
        cls.ef_peaks = EsoFile("../tests/eso_files/eplusout_all_intervals.eso", ignore_peaks=False,
                               report_progress=False)

    def test_eso_file(self):
        self.assertTrue(self.ef.complete)

    def test_peak_eso_file(self):
        self.assertTrue(self.ef_peaks.complete)

    def test_eso_file_to_totals_file(self):
        tf = TotalsFile(self.ef)
        self.assertTrue(tf.complete)

    def test_diff_file(self):
        df = DiffFile(self.ef, self.ef)
        self.assertTrue(df.complete)

    def test_diff_file_to_totals_file(self):
        df = DiffFile(self.ef, self.ef)
        tf = TotalsFile(df)
        self.assertTrue(tf.complete)
