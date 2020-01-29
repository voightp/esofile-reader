import os
import unittest
from datetime import datetime

from esofile_reader import EsoFile, Variable
from tests import ROOT


class TestStandardResults(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        file_path = os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso")
        cls.ef = EsoFile(file_path, ignore_peaks=False, report_progress=False)

    def test_basic_standard_results(self):
        v = Variable(None, None, None, None)
        r = self.ef.get_results(v)
        self.assertEqual(r.shape, (17521, 114))
        self.assertEqual(r.columns.names, ['key', 'variable', 'units'])
        self.assertEqual(r.index.names, ["file", "timestamp"])

        shapes = [(17520, 9), (8760, 9), (365, 9), (12, 9), (1, 9), (1, 9)]

        for interval, shape in zip(self.ef.available_intervals, shapes):
            variables = [
                Variable(interval, "Environment", "Site Diffuse Solar Radiation Rate per Area", "W/m2"),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
                Variable(interval, "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
                Variable(interval, "BLOCK2:ZONE1", "Zone Mean Air Temperature", "C"),
                Variable(interval, "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C"),
                Variable(interval, "BLOCK4:ZONE1", "Zone Mean Air Temperature", "C"),
                Variable(interval, "BLOCK1:ZONE1", "Zone Air Relative Humidity", "%"),
                Variable(interval, "CHILLER", "Chiller Electric Power", "W"),
                Variable(interval, "CHILLER", "Chiller Electric Energy", "J"),
                Variable(interval, "non", "existing", "variable")
            ]
            r = self.ef.get_results(variables)
            self.assertEqual(r.shape, shape)

    def test_standard_results_dates(self):
        shapes = [(1480, 2), (740, 2), (31, 2), (1, 2), (0, 2), (0, 2)]

        start_date = datetime(2002, 5, 1, 12, 30)
        end_date = datetime(2002, 6, 1, 8, 0)

        for interval, shape in zip(self.ef.available_intervals, shapes):
            variables = [
                Variable(interval, "Environment", "Site Diffuse Solar Radiation Rate per Area", "W/m2"),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
                Variable(interval, "non", "existing", "variable")
            ]
            r1 = self.ef.get_results(variables, start_date=start_date, end_date=end_date)
            r2 = self.ef.get_results(variables, start_date=start_date, end_date=end_date,
                                     include_day=True, include_interval=True)

            self.assertEqual(r1.shape, shape)
            self.assertEqual(r2.shape, shape)

            if not r1.empty:
                self.assertEqual(r1.index.names, ["file", "timestamp"])
            if not r2.empty:
                self.assertEqual(r2.index.names, ["file", "timestamp", "day"])

    def test_standard_results_file_name(self):
        for interval in self.ef.available_intervals:
            variables = [
                Variable(interval, "Environment", "Site Diffuse Solar Radiation Rate per Area", "W/m2"),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            ]
            r = self.ef.get_results(variables, add_file_name="row")
            self.assertEqual(r.index.names, ["file", "timestamp"])
            self.assertEqual(r.columns.names, ["key", "variable", "units"])

            r = self.ef.get_results(variables, add_file_name="column")
            self.assertEqual(r.index.names, ["timestamp"])
            self.assertEqual(r.columns.names, ["file", "key", "variable", "units"])

            r = self.ef.get_results(variables, add_file_name=False)
            self.assertEqual(r.index.names, ["timestamp"])
            self.assertEqual(r.columns.names, ["key", "variable", "units"])

    def test_standard_results_include_id(self):
        for interval in self.ef.available_intervals:
            variables = [
                Variable(interval, "Environment", "Site Diffuse Solar Radiation Rate per Area", "W/m2"),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            ]

            r = self.ef.get_results(variables, add_file_name="row", include_id=True)
            self.assertEqual(r.index.names, ["file", "timestamp"])
            self.assertEqual(r.columns.names, ["id", "key", "variable", "units"])

            r = self.ef.get_results(variables, add_file_name="row", include_id=False)
            self.assertEqual(r.index.names, ["file", "timestamp"])
            self.assertEqual(r.columns.names, ["key", "variable", "units"])

    def test_standard_results_include_day(self):
        for interval in self.ef.available_intervals:
            variables = [
                Variable(interval, "Environment", "Site Diffuse Solar Radiation Rate per Area", "W/m2"),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            ]

            r = self.ef.get_results(variables, add_file_name="row", include_day=True)
            self.assertEqual(r.index.names, ["file", "timestamp", "day"])
            self.assertEqual(r.columns.names, ["key", "variable", "units"])

            r = self.ef.get_results(variables, add_file_name="row", include_day=False)
            self.assertEqual(r.index.names, ["file", "timestamp"])
            self.assertEqual(r.columns.names, ["key", "variable", "units"])

    def test_standard_results_include_interval(self):
        for interval in self.ef.available_intervals:
            variables = [
                Variable(interval, "Environment", "Site Diffuse Solar Radiation Rate per Area", "W/m2"),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            ]

            r = self.ef.get_results(variables, add_file_name="row", include_interval=True)
            self.assertEqual(r.index.names, ["file", "timestamp"])
            self.assertEqual(r.columns.names, ["interval", "key", "variable", "units"])

            r = self.ef.get_results(variables, add_file_name="row", include_interval=False)
            self.assertEqual(r.index.names, ["file", "timestamp"])
            self.assertEqual(r.columns.names, ["key", "variable", "units"])

    def test_standard_results_full_index(self):
        for interval in self.ef.available_intervals:
            variables = [
                Variable(interval, "Environment", "Site Diffuse Solar Radiation Rate per Area", "W/m2"),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            ]
            r = self.ef.get_results(variables, add_file_name="row", include_day=True,
                                    include_interval=True, include_id=True)
            self.assertEqual(r.index.names, ["file", "timestamp", "day"])
            self.assertEqual(r.columns.names, ["id", "interval", "key", "variable", "units"])

            r = self.ef.get_results(variables, add_file_name="column", include_day=True,
                                    include_interval=True, include_id=True)
            self.assertEqual(r.index.names, ["timestamp", "day"])
            self.assertEqual(r.columns.names, ["file", "id", "interval", "key", "variable", "units"])

            r = self.ef.get_results(variables, add_file_name=False, include_day=True,
                                    include_interval=True, include_id=True)
            self.assertEqual(r.index.names, ["timestamp", "day"])
            self.assertEqual(r.columns.names, ["id", "interval", "key", "variable", "units"])

    def test_variable_partial_match(self):
        for interval in self.ef.available_intervals:
            variables = [
                Variable(interval, "envir", "Site", None),
                Variable(interval, "BLOCK", "Zone People Occupant Count", ""),
            ]
            r = self.ef.get_results(variables)
            self.assertIsNone(r)

            r = self.ef.get_results(variables, part_match=True)
            self.assertEqual(r.shape[1], 5)

    def test_timestamp_format(self):
        first = [
            "2002-01-01-00-30",
            "2002-01-01-01-00",
            "2002-01-01-00-00",
            "2002-01-01-00-00",
            "2002-01-01-00-00",
            "2002-01-01-00-00",
        ]
        for interval, f in zip(self.ef.available_intervals, first):
            variables = [
                Variable(interval, "Environment", "Site Diffuse Solar Radiation Rate per Area", "W/m2"),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            ]

            r = self.ef.get_results(variables, timestamp_format="%Y-%m-%d-%H-%M",
                                    include_interval=True)
            self.assertEqual(r.index.get_level_values("timestamp")[0], f)

            r = self.ef.get_results(variables, timestamp_format="%Y-%m-%d-%H-%M",
                                    include_interval=True, include_day=True)
            self.assertEqual(r.index.get_level_values("timestamp")[0], f)

            r = self.ef.get_results(variables, timestamp_format="%Y-%m-%d-%H-%M",
                                    include_interval=True, add_file_name=None)
            self.assertEqual(r.index.get_level_values("timestamp")[0], f)


if __name__ == '__main__':
    unittest.main()
