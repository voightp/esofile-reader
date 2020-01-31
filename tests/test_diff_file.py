import os
from pandas.testing import assert_series_equal
from unittest import TestCase
from esofile_reader import EsoFile, DiffFile
from esofile_reader.constants import *

from tests import ROOT


class TestDiffFile(TestCase):
    ef1 = EsoFile(os.path.join(ROOT, "eso_files/eplusout1.eso"), report_progress=False)
    ef2 = EsoFile(os.path.join(ROOT, "eso_files/eplusout2.eso"), report_progress=False)
    ef3 = EsoFile(os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso"), report_progress=False)

    def test_process_diff_identical_files(self):
        diff = DiffFile(self.ef1, self.ef1)
        for interval in diff.available_intervals:
            df = diff.as_df(interval)
            bool_df = df == 0

            # check if all calculated values are 0
            self.assertTrue(bool_df.all().all())

            # check if n days and day of week columns are copied
            if interval in [TS, H, D]:
                c1 = diff.storage.get_days_of_week(interval)
                c2 = self.ef1.storage.get_days_of_week(interval)
                assert_series_equal(c1, c2)

            if interval in [M, A, RP]:
                c1 = diff.storage.get_number_of_days(interval)
                c2 = self.ef1.storage.get_number_of_days(interval)
                assert_series_equal(c1, c2)

    def test_process_diff_similar_files(self):
        diff = DiffFile(self.ef1, self.ef2)
        shapes = [(4392, 59), (183, 59), (6, 59)]
        for interval, test_shape in zip(diff.available_intervals, shapes):
            print(diff.storage.tables[interval].shape)

    def test_process_diff_different_datetime(self):
        diff = DiffFile(self.ef1, self.ef3)
        shapes = [(4392, 3), (183, 3), (6, 3)]
        for interval, test_shape in zip(diff.available_intervals, shapes):
            self.assertTupleEqual(diff.storage.tables[interval].shape, test_shape)
