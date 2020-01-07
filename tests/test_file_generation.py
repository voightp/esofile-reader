import unittest
from esofile_reader import EsoFile, DiffFile, TotalsFile


class TestFileGeneration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ef = EsoFile("../tests/eso_files/eplusout.eso", ignore_peaks=True)
        cls.ef_peaks = EsoFile("../tests/eso_files/eplusout.eso", ignore_peaks=False)

    def test_eso_file(self):
        pass

    def test_totals_file(self):
        pass

    def test_diff_file(self):
        pass
