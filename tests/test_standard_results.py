import unittest
from datetime import datetime

from esofile_reader import Variable
from tests import EF_ALL_INTERVALS


class TestStandardResults(unittest.TestCase):
    def test_basic_standard_results(self):
        v = Variable(None, None, None, None)
        r = EF_ALL_INTERVALS.get_results(v)
        self.assertEqual(r.shape, (17521, 114))
        self.assertEqual(r.columns.names, ["key", "type", "units"])
        self.assertEqual(r.index.names, ["file", "timestamp"])

        shapes = [(17520, 9), (8760, 9), (365, 9), (12, 9), (1, 9), (1, 9)]

        for interval, shape in zip(EF_ALL_INTERVALS.table_names, shapes):
            variables = [
                Variable(
                    interval,
                    "Environment",
                    "Site Diffuse Solar Radiation Rate per Area",
                    "W/m2",
                ),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
                Variable(interval, "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
                Variable(interval, "BLOCK2:ZONE1", "Zone Mean Air Temperature", "C"),
                Variable(interval, "BLOCK3:ZONE1", "Zone Mean Air Temperature", "C"),
                Variable(interval, "BLOCK4:ZONE1", "Zone Mean Air Temperature", "C"),
                Variable(interval, "BLOCK1:ZONE1", "Zone Air Relative Humidity", "%"),
                Variable(interval, "CHILLER", "Chiller Electric Power", "W"),
                Variable(interval, "CHILLER", "Chiller Electric Energy", "J"),
                Variable(interval, "non", "existing", "type"),
            ]
            r = EF_ALL_INTERVALS.get_results(variables)
            self.assertEqual(r.shape, shape)

    def test_standard_results_dates(self):
        shapes = [(1480, 2), (740, 2), (31, 2), (1, 2), (0, 2), (0, 2)]

        start_date = datetime(2002, 5, 1, 12, 30)
        end_date = datetime(2002, 6, 1, 8, 0)

        for interval, shape in zip(EF_ALL_INTERVALS.table_names, shapes):
            variables = [
                Variable(
                    interval,
                    "Environment",
                    "Site Diffuse Solar Radiation Rate per Area",
                    "W/m2",
                ),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
                Variable(interval, "non", "existing", "type"),
            ]
            r1 = EF_ALL_INTERVALS.get_results(
                variables, start_date=start_date, end_date=end_date
            )
            r2 = EF_ALL_INTERVALS.get_results(
                variables,
                start_date=start_date,
                end_date=end_date,
                include_day=True,
                include_table_name=True,
            )

            self.assertEqual(r1.shape, shape)
            self.assertEqual(r2.shape, shape)

            if not r1.empty:
                self.assertEqual(r1.index.names, ["file", "timestamp"])
            if not r2.empty:
                self.assertEqual(r2.index.names, ["file", "timestamp", "day"])

    def test_standard_results_file_name(self):
        for interval in EF_ALL_INTERVALS.table_names:
            variables = [
                Variable(
                    interval,
                    "Environment",
                    "Site Diffuse Solar Radiation Rate per Area",
                    "W/m2",
                ),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            ]
            r = EF_ALL_INTERVALS.get_results(variables, add_file_name="row")
            self.assertEqual(r.index.names, ["file", "timestamp"])
            self.assertEqual(r.columns.names, ["key", "type", "units"])

            r = EF_ALL_INTERVALS.get_results(variables, add_file_name="column")
            self.assertEqual(r.index.names, ["timestamp"])
            self.assertEqual(r.columns.names, ["file", "key", "type", "units"])

            r = EF_ALL_INTERVALS.get_results(variables, add_file_name=False)
            self.assertEqual(r.index.names, ["timestamp"])
            self.assertEqual(r.columns.names, ["key", "type", "units"])

    def test_standard_results_include_id(self):
        for interval in EF_ALL_INTERVALS.table_names:
            variables = [
                Variable(
                    interval,
                    "Environment",
                    "Site Diffuse Solar Radiation Rate per Area",
                    "W/m2",
                ),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            ]

            r = EF_ALL_INTERVALS.get_results(variables, add_file_name="row", include_id=True)
            self.assertEqual(r.index.names, ["file", "timestamp"])
            self.assertEqual(r.columns.names, ["id", "key", "type", "units"])

            r = EF_ALL_INTERVALS.get_results(variables, add_file_name="row", include_id=False)
            self.assertEqual(r.index.names, ["file", "timestamp"])
            self.assertEqual(r.columns.names, ["key", "type", "units"])

    def test_standard_results_include_day(self):
        for interval in EF_ALL_INTERVALS.table_names:
            variables = [
                Variable(
                    interval,
                    "Environment",
                    "Site Diffuse Solar Radiation Rate per Area",
                    "W/m2",
                ),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            ]

            r = EF_ALL_INTERVALS.get_results(variables, add_file_name="row", include_day=True)
            self.assertEqual(r.index.names, ["file", "timestamp", "day"])
            self.assertEqual(r.columns.names, ["key", "type", "units"])

            r = EF_ALL_INTERVALS.get_results(variables, add_file_name="row", include_day=False)
            self.assertEqual(r.index.names, ["file", "timestamp"])
            self.assertEqual(r.columns.names, ["key", "type", "units"])

    def test_standard_results_include_table_name(self):
        for interval in EF_ALL_INTERVALS.table_names:
            variables = [
                Variable(
                    interval,
                    "Environment",
                    "Site Diffuse Solar Radiation Rate per Area",
                    "W/m2",
                ),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            ]

            df = EF_ALL_INTERVALS.get_results(
                variables, add_file_name="row", include_table_name=True
            )
            self.assertEqual(df.index.names, ["file", "timestamp"])
            self.assertEqual(df.columns.names, ["table", "key", "type", "units"])

            df = EF_ALL_INTERVALS.get_results(
                variables, add_file_name="row", include_table_name=False
            )
            self.assertEqual(df.index.names, ["file", "timestamp"])
            self.assertEqual(df.columns.names, ["key", "type", "units"])

    def test_standard_results_full_index(self):
        for interval in EF_ALL_INTERVALS.table_names:
            variables = [
                Variable(
                    interval,
                    "Environment",
                    "Site Diffuse Solar Radiation Rate per Area",
                    "W/m2",
                ),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            ]
            df = EF_ALL_INTERVALS.get_results(
                variables,
                add_file_name="row",
                include_day=True,
                include_table_name=True,
                include_id=True,
            )
            self.assertEqual(df.index.names, ["file", "timestamp", "day"])
            self.assertEqual(df.columns.names, ["id", "table", "key", "type", "units"])

            df = EF_ALL_INTERVALS.get_results(
                variables,
                add_file_name="column",
                include_day=True,
                include_table_name=True,
                include_id=True,
            )
            self.assertEqual(df.index.names, ["timestamp", "day"])
            self.assertEqual(
                df.columns.names, ["file", "id", "table", "key", "type", "units"]
            )

            df = EF_ALL_INTERVALS.get_results(
                variables,
                add_file_name=False,
                include_day=True,
                include_table_name=True,
                include_id=True,
            )
            self.assertEqual(df.index.names, ["timestamp", "day"])
            self.assertEqual(df.columns.names, ["id", "table", "key", "type", "units"])

    def test_variable_partial_match(self):
        for interval in EF_ALL_INTERVALS.table_names:
            variables = [
                Variable(interval, "envir", "Site", None),
                Variable(interval, "BLOCK", "Zone People Occupant Count", ""),
            ]
            r = EF_ALL_INTERVALS.get_results(variables)
            self.assertIsNone(r)

            r = EF_ALL_INTERVALS.get_results(variables, part_match=True)
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
        for interval, f in zip(EF_ALL_INTERVALS.table_names, first):
            variables = [
                Variable(
                    interval,
                    "Environment",
                    "Site Diffuse Solar Radiation Rate per Area",
                    "W/m2",
                ),
                Variable(interval, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            ]

            r = EF_ALL_INTERVALS.get_results(
                variables, timestamp_format="%Y-%m-%d-%H-%M", include_table_name=True
            )
            self.assertEqual(r.index.get_level_values("timestamp")[0], f)

            r = EF_ALL_INTERVALS.get_results(
                variables,
                timestamp_format="%Y-%m-%d-%H-%M",
                include_table_name=True,
                include_day=True,
            )
            self.assertEqual(r.index.get_level_values("timestamp")[0], f)

            r = EF_ALL_INTERVALS.get_results(
                variables,
                timestamp_format="%Y-%m-%d-%H-%M",
                include_table_name=True,
                add_file_name=None,
            )
            self.assertEqual(r.index.get_level_values("timestamp")[0], f)


if __name__ == "__main__":
    unittest.main()
