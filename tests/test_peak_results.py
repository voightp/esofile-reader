import unittest
import pandas as pd
from esofile_reader import EsoFile, Variable
from datetime import datetime
from openpyxl import writer


class TestPeakResults(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pd.set_option("display.max_columns", 10)
        cls.ef = EsoFile("../tests/eso_files/eplusout_all_intervals.eso",
                         ignore_peaks=False)

    def test_global_max_results(self):
        results = [
            [15.9798042, datetime(2002, 1, 1, 9, 30), 37.92728106443822, datetime(2002, 6, 30, 16, 0)],
            [15.9798042, datetime(2002, 1, 1, 10, 0), 37.92205857804634, datetime(2002, 6, 30, 16, 0)],
            [5.992426575, datetime(2002, 1, 1, 0, 0), 33.821448780485376, datetime(2002, 6, 30, 0, 0)],
            [4.445993910483896, datetime(2002, 1, 1, 0, 0), 26.167459322530668, datetime(2002, 7, 1, 0, 0)],
            [4.284995441301313, datetime(2002, 1, 1, 0, 0), 22.47356464391624, datetime(2002, 1, 1, 0, 0)],
            [4.284995441301313, datetime(2002, 1, 1, 0, 0), 22.47356464391624, datetime(2002, 1, 1, 0, 0)],
        ]
        for interval, res in zip(self.ef.available_intervals, results):
            variables = [
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
                Variable(interval, "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
            ]
            r = self.ef.get_results(variables, output_type="global_max")
            self.assertEqual(r.iloc[0, :].to_list(), res)

    def test_global_min_results(self):
        results = [
            [0.0, datetime(2002, 1, 1, 0, 30, 0), 12.00004526197334, datetime(2002, 1, 27, 14, 0, 0)],
            [0.0, datetime(2002, 1, 1, 1, 0, 0), 12.000045478634433, datetime(2002, 1, 27, 14, 0, 0)],
            [0.0, datetime(2002, 1, 5, 0, 0, 0), 12.03386104902603, datetime(2002, 1, 27, 0, 0, 0)],
            [3.994951050000018, datetime(2002, 6, 1, 0, 0, 0), 18.66182100603165, datetime(2002, 12, 1, 0, 0, 0)],
            [4.284995441301313, datetime(2002, 1, 1, 0, 0, 0), 22.47356464391624, datetime(2002, 1, 1, 0, 0, 0)],
        ]
        for interval, res in zip(self.ef.available_intervals, results):
            variables = [
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
                Variable(interval, "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
            ]
            r = self.ef.get_results(variables, output_type="global_min")
            self.assertEqual(r.iloc[0, :].to_list(), res)
