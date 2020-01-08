import unittest
from esofile_reader import EsoFile, DiffFile, TotalsFile


class MyTestCase(unittest.TestCase):
    def test_something(self):
        print(EsoFile.get_results.__doc__)


if __name__ == '__main__':
    unittest.main()
