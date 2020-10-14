import os
import unittest
from datetime import datetime

import pytest

from esofile_reader import EsoFile, Variable
from esofile_reader.eso_file import PeaksNotIncluded
from tests import ROOT, EF_ALL_INTERVALS_PEAKS


class TestPeakResults(unittest.TestCase):
    def test_global_max_results(self):
        results = [
            [
                15.9798042,
                datetime(2002, 1, 1, 9, 30),
                37.92728106443822,
                datetime(2002, 6, 30, 16, 0),
            ],
            [
                15.9798042,
                datetime(2002, 1, 1, 10, 0),
                37.92205857804634,
                datetime(2002, 6, 30, 16, 0),
            ],
            [
                5.992426575,
                datetime(2002, 1, 1, 0, 0),
                33.821448780485376,
                datetime(2002, 6, 30, 0, 0),
            ],
            [
                4.445993910483896,
                datetime(2002, 1, 1, 0, 0),
                26.167459322530668,
                datetime(2002, 7, 1, 0, 0),
            ],
            [
                4.284995441301313,
                datetime(2002, 1, 1, 0, 0),
                22.47356464391624,
                datetime(2002, 1, 1, 0, 0),
            ],
            [
                4.284995441301313,
                datetime(2002, 1, 1, 0, 0),
                22.47356464391624,
                datetime(2002, 1, 1, 0, 0),
            ],
        ]
        for table, res in zip(EF_ALL_INTERVALS_PEAKS.table_names, results):
            variables = [
                Variable(table, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
                Variable(table, "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
            ]
            r = EF_ALL_INTERVALS_PEAKS.get_results_df(variables, output_type="global_max")
            self.assertEqual(r.iloc[0, :].to_list(), res)

    def test_global_min_results(self):
        results = [
            [
                0.0,
                datetime(2002, 1, 1, 0, 30, 0),
                12.00004526197334,
                datetime(2002, 1, 27, 14, 0, 0),
            ],
            [
                0.0,
                datetime(2002, 1, 1, 1, 0, 0),
                12.000045478634433,
                datetime(2002, 1, 27, 14, 0, 0),
            ],
            [
                0.0,
                datetime(2002, 1, 5, 0, 0, 0),
                12.03386104902603,
                datetime(2002, 1, 27, 0, 0, 0),
            ],
            [
                3.994951050000018,
                datetime(2002, 6, 1, 0, 0, 0),
                18.66182100603165,
                datetime(2002, 12, 1, 0, 0, 0),
            ],
            [
                4.284995441301313,
                datetime(2002, 1, 1, 0, 0, 0),
                22.47356464391624,
                datetime(2002, 1, 1, 0, 0, 0),
            ],
        ]
        for table, res in zip(EF_ALL_INTERVALS_PEAKS.table_names, results):
            variables = [
                Variable(table, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
                Variable(table, "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
            ]
            r = EF_ALL_INTERVALS_PEAKS.get_results_df(variables, output_type="global_min")
            self.assertEqual(r.iloc[0, :].to_list(), res)
            self.assertEqual(len(r.index), 1)

    def test_sliced_global_max_results(self):
        results = [
            [
                15.9798042,
                datetime(2002, 5, 1, 8, 30, 0),
                37.92728106443822,
                datetime(2002, 6, 30, 16, 0, 0),
            ],
            [
                15.9798042,
                datetime(2002, 5, 1, 9, 0, 0),
                37.92205857804634,
                datetime(2002, 6, 30, 16, 0, 0),
            ],
        ]
        start_date = datetime(2002, 5, 1)
        end_date = datetime(2002, 8, 1)
        for table, res in zip(["timestep", "hourly"], results):
            variables = [
                Variable(table, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
                Variable(table, "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
            ]
            r = EF_ALL_INTERVALS_PEAKS.get_results_df(
                variables, output_type="global_max", start_date=start_date, end_date=end_date
            )
            self.assertEqual(r.iloc[0, :].to_list(), res)
            self.assertEqual(len(r.index), 1)

    def test_sliced_global_min_results(self):
        results = [
            [
                0.0,
                datetime(2002, 5, 1, 0, 0, 0),
                19.803843276264704,
                datetime(2002, 5, 7, 4, 0, 0),
            ],
            [
                0.0,
                datetime(2002, 5, 1, 0, 0, 0),
                19.879691524393415,
                datetime(2002, 5, 7, 4, 0, 0),
            ],
        ]
        start_date = datetime(2002, 5, 1)
        end_date = datetime(2002, 8, 1)
        for table, res in zip(["timestep", "hourly"], results):
            variables = [
                Variable(table, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
                Variable(table, "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
            ]
            r = EF_ALL_INTERVALS_PEAKS.get_results_df(
                variables, output_type="global_min", start_date=start_date, end_date=end_date
            )
            self.assertEqual(r.iloc[0, :].to_list(), res)
            self.assertEqual(len(r.index), 1)

    def test_global_peak_results_full_index(self):
        for table in EF_ALL_INTERVALS_PEAKS.table_names:
            variables = [
                Variable(
                    table, "Environment", "Site Diffuse Solar Radiation Rate per Area", "W/m2",
                ),
                Variable(table, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            ]
            df = EF_ALL_INTERVALS_PEAKS.get_results_df(
                variables,
                add_file_name="row",
                include_day=True,
                include_table_name=True,
                include_id=True,
                output_type="global_max",
            )
            self.assertEqual(df.index.names, ["file", None])
            self.assertEqual(df.columns.names, ["id", "table", "key", "type", "units", "data"])

            df = EF_ALL_INTERVALS_PEAKS.get_results_df(
                variables,
                add_file_name="column",
                include_day=True,
                include_table_name=True,
                include_id=True,
                output_type="global_max",
            )
            self.assertEqual(df.index.names, [None])
            self.assertEqual(
                df.columns.names, ["file", "id", "table", "key", "type", "units", "data"]
            )

            df = EF_ALL_INTERVALS_PEAKS.get_results_df(
                variables,
                add_file_name=False,
                include_day=True,
                include_table_name=True,
                include_id=True,
                output_type="global_max",
            )
            self.assertEqual(df.index.names, [None])
            self.assertEqual(df.columns.names, ["id", "table", "key", "type", "units", "data"])

    def test_timestamp_format(self):
        first = [
            ["2002-01-01-00-30", "2002-01-01-00-30"],
            ["2002-01-01-01-00", "2002-01-01-01-00"],
            ["2002-12-24-00-00", "2002-01-05-00-00"],
            ["2002-12-01-00-00", "2002-06-01-00-00"],
            ["2002-01-01-00-00", "2002-01-01-00-00"],
            ["2002-01-01-00-00", "2002-01-01-00-00"],
        ]
        for table, f in zip(EF_ALL_INTERVALS_PEAKS.table_names, first):
            variables = [
                Variable(
                    table, "Environment", "Site Diffuse Solar Radiation Rate per Area", "W/m2",
                ),
                Variable(table, "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            ]

            df = EF_ALL_INTERVALS_PEAKS.get_results_df(
                variables,
                timestamp_format="%Y-%m-%d-%H-%M",
                include_table_name=True,
                output_type="global_min",
            )
            self.assertListEqual(
                df.loc[
                    ("eplusout_all_intervals", 0),
                    df.columns.get_level_values("data") == "timestamp",
                ].to_list(),
                f,
            )

    def test_local_daily_max_results(self):
        results = [
            15.9798042,
            datetime(2002, 1, 9, 9, 30, 0),
            23.999269436716204,
            datetime(2002, 1, 9, 17, 0, 0),
        ]
        variables = [
            Variable("daily", "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            Variable("daily", "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
        ]
        r = EF_ALL_INTERVALS_PEAKS.get_results_df(variables, output_type="local_max")
        self.assertEqual(
            r.loc[("eplusout_all_intervals", datetime(2002, 1, 9)), :].to_list(), results
        )

    def test_local_daily_min_results(self):
        results = [
            0.0,
            datetime(2002, 1, 9, 0, 30, 0),
            16.108152209877154,
            datetime(2002, 1, 9, 5, 0, 0),
        ]
        variables = [
            Variable("daily", "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            Variable("daily", "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
        ]
        r = EF_ALL_INTERVALS_PEAKS.get_results_df(variables, output_type="local_min")
        self.assertEqual(
            r.loc[("eplusout_all_intervals", datetime(2002, 1, 9)), :].to_list(), results
        )

    def test_local_monthly_max_results(self):
        results = [
            15.9798042,
            datetime(2002, 2, 1, 9, 30, 0),
            23.99999028797709,
            datetime(2002, 2, 7, 19, 0, 0),
        ]
        variables = [
            Variable("monthly", "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            Variable("monthly", "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
        ]
        r = EF_ALL_INTERVALS_PEAKS.get_results_df(variables, output_type="local_max")
        self.assertEqual(
            r.loc[("eplusout_all_intervals", datetime(2002, 2, 1)), :].to_list(), results
        )

    def test_local_monthly_min_results(self):
        results = [
            0.0,
            datetime(2002, 2, 1, 0, 30, 0),
            12.000145716190598,
            datetime(2002, 2, 25, 3, 0, 0),
        ]
        variables = [
            Variable("monthly", "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            Variable("monthly", "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
        ]
        r = EF_ALL_INTERVALS_PEAKS.get_results_df(variables, output_type="local_min")
        self.assertEqual(
            r.loc[("eplusout_all_intervals", datetime(2002, 2, 1)), :].to_list(), results
        )

    def test_local_annual_max_results(self):
        results = [
            15.9798042,
            datetime(2002, 1, 1, 9, 30, 0),
            37.92728106443822,
            datetime(2002, 6, 30, 16, 0, 0),
        ]
        variables = [
            Variable("annual", "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            Variable("annual", "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
        ]
        r = EF_ALL_INTERVALS_PEAKS.get_results_df(variables, output_type="local_max")
        self.assertEqual(
            r.loc[("eplusout_all_intervals", datetime(2002, 1, 1)), :].to_list(), results
        )

    def test_local_annual_min_results(self):
        results = [
            0.0,
            datetime(2002, 1, 1, 0, 30, 0),
            12.00004526197334,
            datetime(2002, 1, 27, 14, 0, 0),
        ]
        variables = [
            Variable("annual", "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            Variable("annual", "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
        ]
        r = EF_ALL_INTERVALS_PEAKS.get_results_df(variables, output_type="local_min")
        self.assertEqual(
            r.loc[("eplusout_all_intervals", datetime(2002, 1, 1)), :].to_list(), results
        )

    def test_local_runperiod_max_results(self):
        results = [
            15.9798042,
            datetime(2002, 1, 1, 9, 30, 0),
            37.92728106443822,
            datetime(2002, 6, 30, 16, 0, 0),
        ]
        variables = [
            Variable("runperiod", "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            Variable("runperiod", "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
        ]
        r = EF_ALL_INTERVALS_PEAKS.get_results_df(variables, output_type="local_max")
        self.assertEqual(
            r.loc[("eplusout_all_intervals", datetime(2002, 1, 1)), :].to_list(), results
        )

    def test_local_runperiod_min_results(self):
        results = [
            0.0,
            datetime(2002, 1, 1, 0, 30, 0),
            12.00004526197334,
            datetime(2002, 1, 27, 14, 0, 0),
        ]
        variables = [
            Variable("runperiod", "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
            Variable("runperiod", "BLOCK1:ZONE1", "Zone Mean Air Temperature", "C"),
        ]
        r = EF_ALL_INTERVALS_PEAKS.get_results_df(variables, output_type="local_min")
        self.assertEqual(
            r.loc[("eplusout_all_intervals", datetime(2002, 1, 1)), :].to_list(), results
        )

    def test_get_results_missing_peaks(self):
        ef = EsoFile(
            os.path.join(ROOT, "eso_files/eplusout_all_intervals.eso"), ignore_peaks=True
        )
        with pytest.raises(PeaksNotIncluded):
            ef.get_results(
                [Variable("runperiod", "BLOCK1:ZONE1", "Zone People Occupant Count", "")],
                output_type="local_min",
            )

    def test_local_peaks_incorrect_interval(self):
        variables = [
            Variable("hourly", "BLOCK1:ZONE1", "Zone People Occupant Count", ""),
        ]
        with pytest.raises(PeaksNotIncluded):
            _ = EF_ALL_INTERVALS_PEAKS.get_results_df(variables, output_type="local_min")
