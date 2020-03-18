import os
import unittest
from datetime import datetime

import pandas as pd
from pandas.testing import assert_frame_equal

from esofile_reader import EsoFile, get_results
from esofile_reader import Variable
from esofile_reader.base_file import InvalidOutputType, InvalidUnitsSystem
from esofile_reader.eso_file import PeaksNotIncluded
from tests import ROOT, EF1


class TestResultFetching(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        file_path = os.path.join(ROOT, "eso_files/eplusout2.eso")
        cls.ef2 = EsoFile(file_path, ignore_peaks=False)

    def test_get_results(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(EF1, v)

        test_names = ["key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples(
            [("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")], names=test_names
        )
        dates = [
            datetime(2002, 4, 1),
            datetime(2002, 5, 1),
            datetime(2002, 6, 1),
            datetime(2002, 7, 1),
            datetime(2002, 8, 1),
            datetime(2002, 9, 1),
        ]
        test_index = pd.MultiIndex.from_product(
            [["eplusout1"], dates], names=["file", "timestamp"]
        )

        test_df = pd.DataFrame(
            [[22.592079], [24.163740], [25.406725], [26.177191], [25.619201], [23.862254]],
            columns=test_columns,
            index=test_index,
        )

        assert_frame_equal(df, test_df)

    def test_get_results_from_path(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(os.path.join(ROOT, "eso_files/eplusout1.eso"), v)

        test_names = ["key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples(
            [("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")], names=test_names
        )
        dates = [
            datetime(2002, 4, 1),
            datetime(2002, 5, 1),
            datetime(2002, 6, 1),
            datetime(2002, 7, 1),
            datetime(2002, 8, 1),
            datetime(2002, 9, 1),
        ]
        test_index = pd.MultiIndex.from_product(
            [["eplusout1"], dates], names=["file", "timestamp"]
        )

        test_df = pd.DataFrame(
            [[22.592079], [24.163740], [25.406725], [26.177191], [25.619201], [23.862254]],
            columns=test_columns,
            index=test_index,
        )

        assert_frame_equal(df, test_df)

    def test_get_results_start_date(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(EF1, v, start_date=datetime(2002, 4, 15))

        test_names = ["key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples(
            [("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")], names=test_names
        )
        dates = [
            datetime(2002, 5, 1),
            datetime(2002, 6, 1),
            datetime(2002, 7, 1),
            datetime(2002, 8, 1),
            datetime(2002, 9, 1),
        ]
        test_index = pd.MultiIndex.from_product(
            [["eplusout1"], dates], names=["file", "timestamp"]
        )

        test_df = pd.DataFrame(
            [[24.163740], [25.406725], [26.177191], [25.619201], [23.862254]],
            columns=test_columns,
            index=test_index,
        )

        assert_frame_equal(df, test_df)

    def test_get_results_end_date(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(EF1, v, end_date=datetime(2002, 8, 10))

        test_names = ["key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples(
            [("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")], names=test_names
        )
        dates = [
            datetime(2002, 4, 1),
            datetime(2002, 5, 1),
            datetime(2002, 6, 1),
            datetime(2002, 7, 1),
            datetime(2002, 8, 1),
        ]
        test_index = pd.MultiIndex.from_product(
            [["eplusout1"], dates], names=["file", "timestamp"]
        )

        test_df = pd.DataFrame(
            [[22.592079], [24.163740], [25.406725], [26.177191], [25.619201]],
            columns=test_columns,
            index=test_index,
        )

        assert_frame_equal(df, test_df)

    def test_get_results_output_type_global_max(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(EF1, v, output_type="global_max")

        test_names = ["key", "variable", "units", "data"]
        test_columns = pd.MultiIndex.from_tuples(
            [
                ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "value"),
                ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "timestamp"),
            ],
            names=test_names,
        )
        test_index = pd.MultiIndex.from_product([["eplusout1"], [0]], names=["file", None])

        test_df = pd.DataFrame(
            [[26.177191, datetime(2002, 7, 1)]], columns=test_columns, index=test_index
        )

        assert_frame_equal(df, test_df)

    def test_get_results_output_type_start_end_date(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(
            EF1,
            v,
            output_type="global_max",
            start_date=datetime(2002, 4, 10),
            end_date=datetime(2002, 6, 10),
        )

        test_names = ["key", "variable", "units", "data"]
        test_columns = pd.MultiIndex.from_tuples(
            [
                ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "value"),
                ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "timestamp"),
            ],
            names=test_names,
        )
        test_index = pd.MultiIndex.from_product([["eplusout1"], [0]], names=["file", None])

        test_df = pd.DataFrame(
            [[25.406725, datetime(2002, 6, 1)]], columns=test_columns, index=test_index
        )

        assert_frame_equal(df, test_df)

    def test_get_results_output_type_global_min(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(EF1, v, output_type="global_min")

        test_names = ["key", "variable", "units", "data"]
        test_columns = pd.MultiIndex.from_tuples(
            [
                ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "value"),
                ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "timestamp"),
            ],
            names=test_names,
        )
        test_index = pd.MultiIndex.from_product([["eplusout1"], [0]], names=["file", None])

        test_df = pd.DataFrame(
            [[22.592079, datetime(2002, 4, 1)]], columns=test_columns, index=test_index
        )

        assert_frame_equal(df, test_df)

    def test_get_results_output_type_local_max(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(self.ef2, v, output_type="local_max")

        test_names = ["key", "variable", "units", "data"]
        test_columns = pd.MultiIndex.from_tuples(
            [
                ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "value"),
                ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "timestamp"),
            ],
            names=test_names,
        )
        dates = [
            datetime(2002, 4, 1),
            datetime(2002, 5, 1),
            datetime(2002, 6, 1),
            datetime(2002, 7, 1),
            datetime(2002, 8, 1),
            datetime(2002, 9, 1),
        ]
        test_index = pd.MultiIndex.from_product(
            [["eplusout2"], dates], names=["file", "timestamp"]
        )

        test_df = pd.DataFrame(
            [
                [30.837382, datetime(2002, 4, 20, 15, 30)],
                [34.835386, datetime(2002, 5, 26, 16, 0)],
                [41.187972, datetime(2002, 6, 30, 15, 30)],
                [38.414505, datetime(2002, 7, 21, 16, 0)],
                [38.694873, datetime(2002, 8, 18, 15, 30)],
                [35.089822, datetime(2002, 9, 15, 15, 0)],
            ],
            columns=test_columns,
            index=test_index,
        )

        assert_frame_equal(df, test_df)

    def test_get_results_output_type_local_min(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(self.ef2, v, output_type="local_min")

        test_names = ["key", "variable", "units", "data"]
        test_columns = pd.MultiIndex.from_tuples(
            [
                ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "value"),
                ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C", "timestamp"),
            ],
            names=test_names,
        )
        dates = [
            datetime(2002, 4, 1),
            datetime(2002, 5, 1),
            datetime(2002, 6, 1),
            datetime(2002, 7, 1),
            datetime(2002, 8, 1),
            datetime(2002, 9, 1),
        ]
        test_index = pd.MultiIndex.from_product(
            [["eplusout2"], dates], names=["file", "timestamp"]
        )

        test_df = pd.DataFrame(
            [
                [13.681526, datetime(2002, 4, 10, 5, 30)],
                [17.206312, datetime(2002, 5, 7, 5, 30)],
                [19.685125, datetime(2002, 6, 12, 5, 0)],
                [22.279566, datetime(2002, 7, 4, 6, 0)],
                [20.301202, datetime(2002, 8, 31, 6, 0)],
                [16.806496, datetime(2002, 9, 24, 6, 0)],
            ],
            columns=test_columns,
            index=test_index,
        )

        assert_frame_equal(df, test_df)

    def test_get_results_output_type_local_na(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        with self.assertRaises(PeaksNotIncluded):
            _ = get_results(EF1, v, output_type="local_min")

    def test_get_results_output_type_invalid(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        with self.assertRaises(InvalidOutputType):
            _ = get_results(EF1, v, output_type="foo")

    def test_get_results_add_file_name(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(EF1, v, add_file_name="")

        test_names = ["key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples(
            [("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")], names=test_names
        )
        dates = [
            datetime(2002, 4, 1),
            datetime(2002, 5, 1),
            datetime(2002, 6, 1),
            datetime(2002, 7, 1),
            datetime(2002, 8, 1),
            datetime(2002, 9, 1),
        ]
        test_index = pd.Index(dates, name="timestamp")

        test_df = pd.DataFrame(
            [[22.592079], [24.163740], [25.406725], [26.177191], [25.619201], [23.862254]],
            columns=test_columns,
            index=test_index,
        )

        assert_frame_equal(df, test_df)

    def test_get_results_include_interval(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(EF1, v, add_file_name="")

        test_names = ["key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples(
            [("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")], names=test_names
        )
        dates = [
            datetime(2002, 4, 1),
            datetime(2002, 5, 1),
            datetime(2002, 6, 1),
            datetime(2002, 7, 1),
            datetime(2002, 8, 1),
            datetime(2002, 9, 1),
        ]
        test_index = pd.Index(dates, name="timestamp")

        test_df = pd.DataFrame(
            [[22.592079], [24.163740], [25.406725], [26.177191], [25.619201], [23.862254]],
            columns=test_columns,
            index=test_index,
        )

        assert_frame_equal(df, test_df)

    def test_get_results_include_day(self):
        v = Variable("daily", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(
            EF1,
            v,
            add_file_name="",
            start_date=datetime(2002, 4, 1),
            end_date=datetime(2002, 4, 6),
            include_day=True,
        )

        test_names = ["key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples(
            [("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")], names=test_names
        )
        dates = [
            datetime(2002, 4, 1),
            datetime(2002, 4, 2),
            datetime(2002, 4, 3),
            datetime(2002, 4, 4),
            datetime(2002, 4, 5),
            datetime(2002, 4, 6),
        ]
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
        test_index = pd.MultiIndex.from_arrays([dates, days], names=["timestamp", "day"])

        test_df = pd.DataFrame(
            [[22.620627], [22.796563], [22.992970], [22.884158], [22.308314], [22.097302]],
            columns=test_columns,
            index=test_index,
        )

        assert_frame_equal(df, test_df)

    def test_get_results_units_system_si(self):
        v = [
            Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C"),
            Variable("runperiod", "Meter", "Electricity:Facility", "J"),
            Variable("runperiod", "Meter", "InteriorLights:Electricity", "J"),
            Variable("runperiod", "BLOCK1:ZONEB", "Zone Air Relative Humidity", "%"),
        ]
        df = get_results(EF1, v, units_system="SI")

        test_names = ["key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples(
            [
                ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "C"),
                ("BLOCK1:ZONEB", "Zone Air Relative Humidity", "%"),
                ("Meter", "Electricity:Facility", "J"),
                ("Meter", "InteriorLights:Electricity", "J"),
            ],
            names=test_names,
        )

        dates = [
            datetime(2002, 4, 1),
            datetime(2002, 5, 1),
            datetime(2002, 6, 1),
            datetime(2002, 7, 1),
            datetime(2002, 8, 1),
            datetime(2002, 9, 1),
        ]
        test_index = pd.MultiIndex.from_product(
            [["eplusout1"], dates], names=["file", "timestamp"]
        )

        test_df = pd.DataFrame(
            [
                [22.592079, 42.1419698525608, 26409744634.6392, 9873040320],
                [24.163740, None, None, None],
                [25.406725, None, None, None],
                [26.177191, None, None, None],
                [25.619201, None, None, None],
                [23.862254, None, None, None],
            ],
            columns=test_columns,
            index=test_index,
        )

        assert_frame_equal(df, test_df)

    def test_get_results_units_system_ip(self):
        v = [
            Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C"),
            Variable("runperiod", "Meter", "Electricity:Facility", "J"),
            Variable("runperiod", "Meter", "InteriorLights:Electricity", "J"),
            Variable("runperiod", "BLOCK1:ZONEB", "Zone Air Relative Humidity", "%"),
        ]
        df = get_results(EF1, v, units_system="IP")

        test_names = ["key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples(
            [
                ("BLOCK1:ZONEA", "Zone Mean Air Temperature", "F"),
                ("BLOCK1:ZONEB", "Zone Air Relative Humidity", "%"),
                ("Meter", "Electricity:Facility", "J"),
                ("Meter", "InteriorLights:Electricity", "J"),
            ],
            names=test_names,
        )

        dates = [
            datetime(2002, 4, 1),
            datetime(2002, 5, 1),
            datetime(2002, 6, 1),
            datetime(2002, 7, 1),
            datetime(2002, 8, 1),
            datetime(2002, 9, 1),
        ]
        test_index = pd.MultiIndex.from_product(
            [["eplusout1"], dates], names=["file", "timestamp"]
        )

        test_df = pd.DataFrame(
            [
                [72.6657414, 42.1419698525608, 26409744634.6392, 9873040320],
                [75.49473256, None, None, None],
                [77.73210562, None, None, None],
                [79.11894354, None, None, None],
                [78.11456209, None, None, None],
                [74.95205651, None, None, None],
            ],
            columns=test_columns,
            index=test_index,
        )

        assert_frame_equal(df, test_df)

    def test_get_results_units_system_invalid(self):
        v = [
            Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C"),
            Variable("runperiod", "Meter", "Electricity:Facility", "J"),
            Variable("runperiod", "Meter", "InteriorLights:Electricity", "J"),
            Variable("runperiod", "BLOCK1:ZONEB", "Zone Air Relative Humidity", "%"),
        ]
        with self.assertRaises(InvalidUnitsSystem):
            _ = get_results(EF1, v, units_system="FOO")

    def test_get_results_rate_to_energy(self):
        rate_to_energy = {
            "timestep": True,
            "hourly": True,
            "daily": True,
            "monthly": True,
            "annual": True,
            "runperiod": True,
        }

        v = [
            Variable(None, "Environment", "Site Diffuse Solar Radiation Rate per Area", "W/m2"),
            Variable(None, "BLOCK1:ZONEB", "Zone People Sensible Heating Rate", "W"),
        ]
        df = get_results(EF1, v, rate_to_energy_dct=rate_to_energy)

        self.assertListEqual(df.columns.get_level_values("units").tolist(), ["J/m2", "J"] * 4)
        self.assertAlmostEqual(df.iloc[7, 0], 217800, 5)
        self.assertAlmostEqual(df.iloc[7, 1], 335583.646986, 5)
        self.assertAlmostEqual(df.iloc[0, 2], 6253200, 5)
        self.assertAlmostEqual(df.iloc[0, 3], 10918518.194915276, 5)
        self.assertAlmostEqual(df.iloc[0, 4], 213833700, 5)
        self.assertAlmostEqual(df.iloc[0, 5], 241058966.115303, 5)
        self.assertAlmostEqual(df.iloc[0, 6], 1600326000, 5)
        self.assertAlmostEqual(df.iloc[0, 7], 1415172751.05001, 5)

    def test_get_results_rate(self):
        rate_to_energy = {
            "timestep": False,
            "hourly": False,
            "daily": False,
            "monthly": False,
            "annual": False,
            "runperiod": False,
        }

        v = [
            Variable(None, "Environment", "Site Diffuse Solar Radiation Rate per Area", "W/m2"),
            Variable(None, "BLOCK1:ZONEB", "Zone People Sensible Heating Rate", "W"),
        ]
        df = get_results(EF1, v, rate_to_energy_dct=rate_to_energy, rate_units="kW")

        self.assertListEqual(df.columns.get_level_values("units").tolist(), ["kW/m2", "kW"] * 4)
        self.assertAlmostEqual(df.iloc[7, 0], 0.0605, 5)
        self.assertAlmostEqual(df.iloc[7, 1], 0.0932176797185792, 5)
        self.assertAlmostEqual(df.iloc[0, 2], 0.072375, 5)
        self.assertAlmostEqual(df.iloc[0, 3], 0.126371738367075, 5)
        self.assertAlmostEqual(df.iloc[0, 4], 0.0824975694444444, 5)
        self.assertAlmostEqual(df.iloc[0, 5], 0.0930011443346075, 5)
        self.assertAlmostEqual(df.iloc[0, 6], 0.10121470856102, 5)
        self.assertAlmostEqual(df.iloc[0, 7], 0.0895044494440654, 5)

    def test_get_results_energy(self):
        rate_to_energy = {
            "timestep": True,
            "hourly": True,
            "daily": True,
            "monthly": True,
            "annual": True,
            "runperiod": True,
        }

        v = [
            Variable(None, "Environment", "Site Diffuse Solar Radiation Rate per Area", "W/m2"),
            Variable(None, "BLOCK1:ZONEB", "Zone People Sensible Heating Rate", "W"),
        ]
        df = get_results(EF1, v, rate_to_energy_dct=rate_to_energy, energy_units="MJ")

        self.assertListEqual(df.columns.get_level_values("units").tolist(), ["MJ/m2", "MJ"] * 4)
        self.assertAlmostEqual(df.iloc[7, 0], 0.21780, 5)
        self.assertAlmostEqual(df.iloc[7, 1], 0.33558, 5)
        self.assertAlmostEqual(df.iloc[0, 2], 6.2532, 5)
        self.assertAlmostEqual(df.iloc[0, 3], 10.918518, 5)
        self.assertAlmostEqual(df.iloc[0, 4], 213.8337, 5)
        self.assertAlmostEqual(df.iloc[0, 5], 241.0589661, 5)
        self.assertAlmostEqual(df.iloc[0, 6], 1600.3259999, 5)
        self.assertAlmostEqual(df.iloc[0, 7], 1415.172751, 5)

    def test_get_results_timestamp_format(self):
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(EF1, v, add_file_name="", timestamp_format="%m-%d")

        dates = ["04-01", "05-01", "06-01", "07-01", "08-01", "09-01"]
        self.assertListEqual(df.index.tolist(), dates)

    def test_get_results_report_progress(self):
        EsoFile(os.path.join(ROOT, "eso_files/eplusout1.eso"))

    def test_get_results_ignore_peaks(self):
        ef = EsoFile(os.path.join(ROOT, "eso_files/eplusout1.eso"), ignore_peaks=False)
        self.assertIsNotNone(ef.peak_outputs)

    def test_multiple_files_invalid_variable(self):
        files = [EF1, self.ef2]
        v = Variable(None, "foo", "bar", "baz")
        self.assertIsNone(get_results(files, v))

    def test_multiple_files_invalid_variables(self):
        files = [EF1, self.ef2]
        v = Variable(None, "foo", "bar", "baz")
        self.assertIsNone(get_results(files, [v, v]))

    def test_get_results_multiple_files(self):
        files = [EF1, self.ef2]
        v = Variable("monthly", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C")
        df = get_results(files, v, add_file_name="column")

        test_names = ["file", "key", "variable", "units"]
        test_columns = pd.MultiIndex.from_tuples(
            [
                ("eplusout1", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C"),
                ("eplusout2", "BLOCK1:ZONEA", "Zone Mean Air Temperature", "C"),
            ],
            names=test_names,
        )

        dates = [
            datetime(2002, 4, 1),
            datetime(2002, 5, 1),
            datetime(2002, 6, 1),
            datetime(2002, 7, 1),
            datetime(2002, 8, 1),
            datetime(2002, 9, 1),
        ]
        test_index = pd.DatetimeIndex(dates, name="timestamp")

        test_df = pd.DataFrame(
            [
                [22.592079, 23.448357],
                [24.163740, 24.107510],
                [25.406725, 24.260228],
                [26.177191, 24.458445],
                [25.619201, 24.378681],
                [23.862254, 24.010489],
            ],
            columns=test_columns,
            index=test_index,
        )

        assert_frame_equal(df, test_df)
