import unittest
from datetime import datetime
from esofile_reader import EsoFile
from esofile_reader import TotalsFile
from esofile_reader import Variable


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ef = EsoFile("../tests/eso_files/eplusout.eso", ignore_peaks=True)
        cls.ef_peaks = EsoFile("../tests/eso_files/eplusout.eso", ignore_peaks=False)

    def test_available_intervals(self):
        self.assertListEqual(self.ef.available_intervals,
                             ['hourly', 'daily', 'monthly', 'runperiod'])

    def test_all_ids(self):
        self.assertEqual(len(self.ef.all_ids), 2564)

    def test_modified(self):
        self.assertEqual(self.ef.modified, datetime(2019, 7, 25, 19, 8, 34, 963648))

    def test_created(self):
        self.assertEqual(self.ef.modified, datetime(2019, 7, 25, 19, 8, 34, 963648))

    def test_complete(self):
        self.assertTrue(self.ef.complete)
        self.assertIsNone(self.ef.peak_outputs)

        self.assertTrue(self.ef_peaks.complete)
        self.assertIsNotNone(self.ef_peaks.peak_outputs)

    def test_header_df(self):
        print(self.ef.header_df)

    def test_create_variable(self):
        new_var0 = self.ef._add_header_variable(-1, "foo", "bar", "baz", "u")
        new_var1 = self.ef._add_header_variable(-2, "foo", "bar", "baz", "u")
        new_var2 = self.ef._add_header_variable(-3, "fo", "bar", "baz", "u")

        self.assertTupleEqual(new_var0, Variable("foo", "bar", "baz", "u"))
        self.assertTupleEqual(new_var1, Variable("foo", "bar (1)", "baz", "u"))
        self.assertTupleEqual(new_var2, Variable("fo", "bar", "baz", "u"))

    def test_create_totals_file(self):
        tf = TotalsFile(self.ef)
        self.assertTrue(tf.complete)


if __name__ == '__main__':
    unittest.main()
