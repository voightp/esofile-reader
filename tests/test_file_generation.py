import unittest

from esofile_reader import DiffFile, TotalsFile
from esofile_reader.utils.exceptions import NoSharedVariables
from tests import EF_ALL_INTERVALS, EF_ALL_INTERVALS_PEAKS


class TestFileGeneration(unittest.TestCase):

    def test_eso_file(self):
        self.assertTrue(EF_ALL_INTERVALS.complete)

    def test_peak_eso_file(self):
        self.assertTrue(EF_ALL_INTERVALS_PEAKS.complete)

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
        df = EF_ALL_INTERVALS.generate_diff(EF_ALL_INTERVALS_PEAKS)
        self.assertTrue(df.complete)

    def test_generate_diff_totals_file(self):
        tf = EF_ALL_INTERVALS.generate_totals()
        df = tf.generate_diff(tf)
        self.assertTrue(df.complete)


if __name__ == "__main__":
    unittest.main()
