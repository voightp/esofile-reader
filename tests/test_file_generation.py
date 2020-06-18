import unittest

from esofile_reader import ResultsFile
from tests import EF_ALL_INTERVALS, EF_ALL_INTERVALS_PEAKS


class TestFileGeneration(unittest.TestCase):
    def test_eso_file(self):
        self.assertTrue(EF_ALL_INTERVALS.complete)

    def test_peak_eso_file(self):
        self.assertTrue(EF_ALL_INTERVALS_PEAKS.complete)

    def test_eso_file_to_totals_file(self):
        tf = ResultsFile.from_totals(EF_ALL_INTERVALS)
        self.assertTrue(tf.complete)

    def test_diff_file(self):
        df = ResultsFile.from_diff(EF_ALL_INTERVALS, EF_ALL_INTERVALS)
        self.assertTrue(df.complete)

    def test_diff_file_to_totals_file(self):
        df = ResultsFile.from_diff(EF_ALL_INTERVALS, EF_ALL_INTERVALS)
        tf = ResultsFile.from_totals(df)
        self.assertTrue(tf.complete)

    def test_generate_totals_eso_file(self):
        tf = ResultsFile.from_totals(EF_ALL_INTERVALS)
        self.assertTrue(tf.complete)

    def test_generate_diff_eso_file(self):
        df = ResultsFile.from_diff(EF_ALL_INTERVALS, EF_ALL_INTERVALS_PEAKS)
        self.assertTrue(df.complete)

    def test_generate_diff_totals_file(self):
        tf = ResultsFile.from_totals(EF_ALL_INTERVALS)
        df = ResultsFile.from_diff(tf, tf)
        self.assertTrue(df.complete)


if __name__ == "__main__":
    unittest.main()
