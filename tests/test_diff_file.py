import os
from unittest import TestCase

from pandas.testing import assert_series_equal

from esofile_reader import EsoFile, DiffFile
from esofile_reader.constants import *
from esofile_reader.utils.exceptions import NoSharedVariables
from tests import EF1, EF2, EF_ALL_INTERVALS, ROOT


class TestDiffFile(TestCase):
    ef1 = EsoFile(os.path.join(ROOT, "eso_files/eplusout1.eso"))
    ef2 = EsoFile(os.path.join(ROOT, "eso_files/eplusout2.eso"))
    ef3 = EsoFile(os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso"))

    def test_process_diff_identical_files(self):
        diff = DiffFile(EF1, EF1)
        for interval in diff.available_intervals:
            df = diff.as_df(interval)
            bool_df = df == 0

            # check if all calculated values are 0
            self.assertTrue(bool_df.all().all())

            # check if n days and day of week columns are copied
            if interval in [TS, H, D]:
                c1 = diff.data.get_days_of_week(interval)
                c2 = EF1.data.get_days_of_week(interval)
                assert_series_equal(c1, c2)

            if interval in [M, A, RP]:
                c1 = diff.data.get_number_of_days(interval)
                c2 = EF1.data.get_number_of_days(interval)
                assert_series_equal(c1, c2)

    def test_process_diff_similar_files(self):
        diff = DiffFile(EF1, EF2)
        shapes = [(4392, 59), (183, 59), (6, 59)]
        for interval, test_shape in zip(diff.available_intervals, shapes):
            self.assertTupleEqual(diff.data.tables[interval].shape, test_shape)

    def test_process_diff_different_datetime(self):
        diff = DiffFile(EF1, EF_ALL_INTERVALS)
        shapes = [(4392, 3), (183, 3), (6, 3)]
        for interval, test_shape in zip(diff.available_intervals, shapes):
            self.assertTupleEqual(diff.data.tables[interval].shape, test_shape)

    def test_no_shared_intervals(self):
        ef1 = EsoFile(os.path.join(ROOT, "eso_files/eplusout1.eso"))
        ef2 = EsoFile(os.path.join(ROOT, "eso_files/eplusout1.eso"))

        del ef1.data.tables["hourly"]
        del ef1.data.tables["daily"]

        del ef2.data.tables["monthly"]
        del ef2.data.tables["runperiod"]

        with self.assertRaises(NoSharedVariables):
            DiffFile(ef1, ef2)
