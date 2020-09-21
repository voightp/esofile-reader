import os
from unittest import TestCase

from pandas.testing import assert_series_equal

from esofile_reader import EsoFile, ResultsFile
from esofile_reader.constants import *
from esofile_reader.exceptions import NoResults
from tests import EF1, EF2, EF_ALL_INTERVALS, ROOT


class TestDiffFIle(TestCase):
    def test_process_diff_identical_files(self):
        diff = ResultsFile.from_diff(EF1, EF1)
        for interval in diff.table_names:
            df = diff.get_numeric_table(interval)
            bool_df = df == 0

            # check if all calculated values are 0
            self.assertTrue(bool_df.all().all())

            # check if n days and day of week columns are copied
            if interval in [TS, H, D]:
                c1 = diff.tables.get_special_column(interval, DAY_COLUMN)
                c2 = EF1.tables.get_special_column(interval, DAY_COLUMN)
                assert_series_equal(c1, c2)

            if interval in [M, A, RP]:
                c1 = diff.tables.get_special_column(interval, N_DAYS_COLUMN)
                c2 = EF1.tables.get_special_column(interval, N_DAYS_COLUMN)
                assert_series_equal(c1, c2)

    def test_process_diff_similar_files(self):
        diff = ResultsFile.from_diff(EF1, EF2)
        shapes = [(4392, 59), (183, 59), (6, 59)]
        for interval, test_shape in zip(diff.table_names, shapes):
            self.assertTupleEqual(diff.tables[interval].shape, test_shape)

    def test_process_diff_different_datetime(self):
        diff = ResultsFile.from_diff(EF1, EF_ALL_INTERVALS)
        shapes = [(4392, 3), (183, 3), (6, 3)]
        for interval, test_shape in zip(diff.table_names, shapes):
            self.assertTupleEqual(diff.tables[interval].shape, test_shape)

    def test_no_shared_intervals(self):
        ef1 = EsoFile(os.path.join(ROOT, "eso_files/eplusout1.eso"))
        ef2 = EsoFile(os.path.join(ROOT, "eso_files/eplusout1.eso"))

        del ef1.tables["hourly"]
        del ef1.tables["daily"]

        del ef2.tables["monthly"]
        del ef2.tables["runperiod"]

        with self.assertRaises(NoResults):
            _ = ResultsFile.from_diff(ef1, ef2)
