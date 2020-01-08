import unittest
from esofile_reader import EsoFile, DiffFile, TotalsFile, Variable


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # cls.ef = EsoFile("../tests/eso_files/eplusout.eso", ignore_peaks=True)
        # cls.ef_peaks = EsoFile("../tests/eso_files/eplusout.eso", ignore_peaks=False)
        cls.ef = EsoFile("../tests/eso_files/eplusout1.eso")

    def test_standard_results(self):
        v1 = Variable("hourly", None, None, None)
        print(self.ef.get_results(v1, include_day=True))


if __name__ == '__main__':
    unittest.main()
