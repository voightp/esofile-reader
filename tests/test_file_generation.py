import os
import unittest

from esofile_reader import EsoFile, DiffFile, TotalsFile
from tests import ROOT, EF_ALL_INTERVALS


class TestFileGeneration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        file_path = os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso")
        cls.ef_peaks = EsoFile(file_path, ignore_peaks=False)

    def test_eso_file(self):
        self.assertTrue(EF_ALL_INTERVALS.complete)

    def test_peak_eso_file(self):
        self.assertTrue(self.ef_peaks.complete)

    def test_eso_file_to_totals_file(self):
        tf = TotalsFile(EF_ALL_INTERVALS)
        self.assertTrue(tf.complete)

    def test_diff_file(self):
        df = DiffFile(EF_ALL_INTERVALS, EF_ALL_INTERVALS)
        self.assertTrue(df.complete)

    def test_diff_file_to_totals_file(self):
        df = DiffFile(EF_ALL_INTERVALS, EF_ALL_INTERVALS)
        tf = TotalsFile(df)
        self.assertTrue(tf.complete)

    def test_generate_totals_eso_file(self):
        tf = EF_ALL_INTERVALS.generate_totals()
        self.assertTrue(tf.complete)

    def test_generate_diff_eso_file(self):
        df = EF_ALL_INTERVALS.generate_diff(self.ef_peaks)
        self.assertTrue(df.complete)

    def test_generate_diff_totals_file(self):
        tf = EF_ALL_INTERVALS.generate_totals()
        df = tf.generate_diff(EF_ALL_INTERVALS)
        self.assertTrue(df.complete)


if __name__ == "__main__":
    unittest.main()
