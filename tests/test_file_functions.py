import unittest
from datetime import datetime
from esofile_reader import EsoFile
from esofile_reader import TotalsFile
from esofile_reader import Variable


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ef = EsoFile("../tests/eso_files/eplusout_all_intervals.eso", ignore_peaks=True)
        cls.ef_peaks = EsoFile("../tests/eso_files/eplusout_all_intervals.eso", ignore_peaks=False)

    def test_available_intervals(self):
        self.assertListEqual(self.ef.available_intervals,
                             ['timestep', 'hourly', 'daily',
                              'monthly', 'runperiod', 'annual'])

    def test_all_ids(self):
        self.assertEqual(len(self.ef.all_ids), 114)

    def test_created(self):
        self.assertTrue(isinstance(self.ef.created, datetime))

    def test_complete(self):
        self.assertTrue(self.ef.complete)
        self.assertIsNone(self.ef.peak_outputs)

        self.assertTrue(self.ef_peaks.complete)
        self.assertIsNotNone(self.ef_peaks.peak_outputs)

    def test_header_df(self):
        self.assertEqual(self.ef.header_df.columns.to_list(), ["id", "interval", "key",
                                                               "variable", "units"])
        self.assertEqual(len(self.ef.header_df.index), 114)

    def test_add_header_variable(self):
        new_var0 = self.ef._add_header_variable(-1, "foo", "bar", "baz", "u")
        new_var1 = self.ef._add_header_variable(-2, "foo", "bar", "baz", "u")
        new_var2 = self.ef._add_header_variable(-3, "fo", "bar", "baz", "u")

        self.assertTupleEqual(new_var0, Variable("foo", "bar", "baz", "u"))
        self.assertTupleEqual(new_var1, Variable("foo", "bar (1)", "baz", "u"))
        self.assertTupleEqual(new_var2, Variable("fo", "bar", "baz", "u"))

        self.ef._remove_header_variables("foo", [-1, -2])
        self.ef._remove_header_variables("fo", [-3])

    def test_create_totals_file(self):
        tf = TotalsFile(self.ef)
        self.assertTrue(tf.complete)


if __name__ == '__main__':
    unittest.main()
