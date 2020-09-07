import datetime
import logging
import os
import unittest
from collections import defaultdict
from functools import partial

import numpy as np

from esofile_reader import EsoFile, logger, ResultsFile
from esofile_reader.base_file import IncompleteFile
from esofile_reader.constants import *
from esofile_reader.exceptions import InvalidLineSyntax, BlankLineError
from esofile_reader.mini_classes import Variable, IntervalTuple
from esofile_reader.processing.esofile import (
    process_statement_line,
    process_header_line,
    read_header,
    process_sub_monthly_interval_lines,
    process_monthly_plus_interval_lines,
    read_body,
    read_file,
    generate_outputs,
    generate_peak_outputs,
    remove_duplicates
)
from esofile_reader.processing.esofile_intervals import process_raw_date_data
from esofile_reader.processing.monitor import EsoFileMonitor
from esofile_reader.search_tree import Tree
from tests import ROOT


# fmt: off
class TestEsoFileProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.header_pth = os.path.join(ROOT, "eso_files/header.txt")
        cls.body_pth = os.path.join(ROOT, "eso_files/body.txt")
        cls.incomplete = os.path.join(ROOT, "eso_files/eplusout_incomplete.eso")

    def test_esofile_statement(self):
        line = "Program Version,EnergyPlus, " "Version 9.1.0-08d2e308bb, YMD=2019.07.23 15:19"
        version, timestamp = process_statement_line(line)
        self.assertEqual(version, 910)
        self.assertEqual(timestamp, datetime.datetime(2019, 7, 23, 15, 19, 00))

    def test_header_line1(self):
        line = (
            "8,7,Environment,Site Outdoor Air Drybulb Temperature [C] "
            "!Daily [Value,Min,Hour,Minute,Max,Hour,Minute]"
        )
        line_id, key, var, units, interval = process_header_line(line)

        self.assertEqual(line_id, 8)
        self.assertEqual(key, "Environment")
        self.assertEqual(var, "Site Outdoor Air Drybulb Temperature")
        self.assertEqual(units, "C")
        self.assertEqual(interval, "daily")

    def test_header_line2(self):
        line = "302,1,InteriorEquipment:Electricity [J] !Hourly"
        line_id, key, var, units, interval = process_header_line(line)

        self.assertEqual(line_id, 302)
        self.assertEqual(key, "Meter")
        self.assertEqual(var, "InteriorEquipment:Electricity")
        self.assertEqual(units, "J")
        self.assertEqual(interval, "hourly")

    def test_header_line3(self):
        line = "302,1,InteriorEquipment,Electricity,[J], !Hourly"
        with self.assertRaises(AttributeError):
            process_header_line(line)

    def test_read_header1(self):
        f = [
            "7,1,Environment,Site Outdoor Air Drybulb Temperature [C] !Hourly",
            "3676,11,Some meter [ach] !RunPeriod [Value,Min,Month,Day,Hour,Minute,Max,Month,Day,Hour,Minute]",
            "End of Data Dictionary",
        ]
        g = (l for l in f)

        header_dct = read_header(g, EsoFileMonitor("foo"))

        test_header = defaultdict(partial(defaultdict))
        test_header["hourly"][7] = Variable(
            "hourly", "Environment", "Site Outdoor Air Drybulb Temperature", "C"
        )
        test_header["runperiod"][3676] = Variable("runperiod", "Meter", "Some meter", "ach")

        self.assertDictEqual(header_dct, test_header)

    def test_read_header2(self):
        f = [
            "7,1,Environment,Site Outdoor Air Drybulb Temperature [C] !Hourly",
            "",
            "End of TableType Dictionary",
        ]
        g = (l for l in f)
        with self.assertRaises(BlankLineError):
            read_header(g, EsoFileMonitor("foo"))

    def test_read_header3(self):
        with open(self.header_pth, "r") as f:
            header = read_header(f, EsoFileMonitor("foo"))
            self.assertEqual(header.keys(), header.keys())

            for interval, variables in header.items():
                self.assertEqual(variables.keys(), header[interval].keys())

            v1 = Variable(
                "timestep", "Environment", "Site Direct Solar Radiation Rate per Area", "W/m2"
            )
            v2 = Variable("hourly", "BLOCK1:ZONE1", "Zone Mean Radiant Temperature", "C")
            v3 = Variable("daily", "Environment", "Site Wind Speed", "m/s")
            v4 = Variable("monthly", "Environment", "Site Solar Azimuth Angle", "deg")
            v5 = Variable("runperiod", "Environment", "Site Wind Direction", "deg")
            v6 = Variable("runperiod", "Meter", "DistrictCooling:Facility", "J")

            self.assertEqual(header["timestep"][37], v1)
            self.assertEqual(header["hourly"][163], v2)
            self.assertEqual(header["daily"][24], v3)
            self.assertEqual(header["monthly"][45], v4)
            self.assertEqual(header["runperiod"][31], v5)
            self.assertEqual(header["runperiod"][562], v6)

    def test_process_interval_line(self):
        l0 = [" 1", " 2", " 3", " 0", "10.00", "0.00", "60.00", "Saturday"]
        l1 = [" 1", " 2", " 3", " 0", "10.00", "0.00", "30.00", "Saturday"]
        l2 = [" 20", " 1", " 2", " 0", "Saturday"]
        l3 = [" 58", " 1"]
        l4 = ["365"]
        l5 = ["1"]

        l0 = process_sub_monthly_interval_lines(2, l0)
        l1 = process_sub_monthly_interval_lines(2, l1)
        l2 = process_sub_monthly_interval_lines(3, l2)
        l3 = process_monthly_plus_interval_lines(4, l3)
        l4 = process_monthly_plus_interval_lines(5, l4)
        l5 = process_monthly_plus_interval_lines(6, l5)

        self.assertEqual(l0[0], H)
        self.assertEqual(l1[0], TS)
        self.assertEqual(l2[0], D)
        self.assertEqual(l3[0], M)
        self.assertEqual(l4[0], RP)
        self.assertEqual(l5[0], A)

        self.assertEqual(l0[1], IntervalTuple(2, 3, 10, 60))
        self.assertEqual(l1[1], IntervalTuple(2, 3, 10, 30))
        self.assertEqual(l2[1], IntervalTuple(1, 2, 0, 0))
        self.assertEqual(l3[1], IntervalTuple(1, 1, 0, 0))
        self.assertEqual(l4[1], IntervalTuple(1, 1, 0, 0))
        self.assertEqual(l5[1], IntervalTuple(1, 1, 0, 0))

        self.assertEqual(l0[2], "Saturday")
        self.assertEqual(l1[2], "Saturday")
        self.assertEqual(l2[2], "Saturday")
        self.assertEqual(l3[2], 58)
        self.assertEqual(l4[2], 365)
        self.assertEqual(l5[2], None)

    def test_read_body(self):
        with open(self.header_pth, "r") as f:
            header = read_header(f, EsoFileMonitor("foo"))

        with open(self.body_pth, "r") as f:
            (
                env_names,
                raw_outputs,
                raw_peak_outputs,
                dates,
                cumulative_days,
                day_of_week,
            ) = read_body(f, 6, header, False, EsoFileMonitor("dummy"))
            # fmt: off
            self.assertEqual(
                raw_outputs[0]["timestep"][7],
                [
                    15.65, 14.3, 14.15, 14.0, 12.8, 11.6, 10.899999999999999, 10.2, 11.05, 11.9,
                    13.0, 14.1, 15.05, 16.0, 17.35, 18.7, 20.4, 22.1, 23.75, 25.4, 26.4, 27.4,
                    28.0, 28.6, 29.1, 29.6, 30.200000000000004, 30.8, 31.05, 31.3, 30.75, 30.2,
                    29.6, 29.0, 28.7, 28.4, 27.45, 26.5, 25.7, 24.9, 24.049999999999998, 23.2,
                    22.25, 21.3, 20.25, 19.2, 18.1, 17.0, 16.05, 15.1, 14.3, 13.5, 12.95, 12.4,
                    12.100000000000002, 11.8, 11.600000000000002, 11.4, 11.15, 10.9, 11.45,
                    12.0, 11.9, 11.8, 12.5, 13.2, 13.75, 14.3, 14.75, 15.2, 15.35, 15.5, 16.55,
                    17.6, 17.200000000000004, 16.8, 16.5, 16.2, 15.5, 14.8, 15.6, 16.4,
                    16.299999999999998, 16.2, 15.7, 15.2, 14.45, 13.7, 13.05, 12.4, 11.9, 11.4,
                    10.850000000000002, 10.3, 10.2, 10.1, ],
            )
            self.assertEqual(
                raw_outputs[0]["hourly"][8],
                [
                    14.975000000000002, 14.075, 12.2, 10.549999999999999, 11.475000000000002,
                    13.55, 15.525, 18.025, 21.25, 24.575, 26.9, 28.3, 29.35, 30.5, 31.175,
                    30.475, 29.3, 28.549999999999998, 26.975, 25.299999999999998, 23.625,
                    21.775, 19.725, 17.55, 15.575, 13.9, 12.675, 11.950000000000001, 11.5,
                    11.025, 11.725, 11.850000000000002, 12.85, 14.025, 14.975, 15.425,
                    17.075000000000004, 17.0, 16.35, 15.15, 16.0, 16.25, 15.45, 14.075,
                    12.725000000000002, 11.65, 10.575000000000001, 10.149999999999999, ],
            )

            self.assertEqual(
                raw_outputs[0]["hourly"][163],
                [
                    np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN,
                    np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN,
                    np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN,
                    np.NaN, np.NaN, np.NaN, 31.939895115385128, 31.703936337515786,
                    32.280660803461618, 32.62177706428757, 32.88418951192571,
                    33.009496155093547, 33.03911553829569, 32.92907267649866,
                    32.65682359572439, 32.31898695867979, 32.197143544621329,
                    31.872368037056775, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, ],
            )
            # fmt: onn

            self.assertListEqual(
                raw_outputs[0]["daily"][14], [12.883333333333335, 10.19895789513146]
            )
            self.assertListEqual(
                raw_outputs[0]["monthly"][15], [12.883333333333335, 10.19895789513146]
            )
            self.assertListEqual(raw_outputs[0]["runperiod"][16], [11.541145614232397])

            self.assertListEqual(
                raw_peak_outputs[0]["daily"][9],
                [[10.2, 4, 60, 31.3, 15, 60], [10.1, 24, 60, 17.6, 13, 60]],
            )
            self.assertListEqual(
                raw_peak_outputs[0]["daily"][622],
                [[0.0, 1, 15, 0.0, 1, 15], [0.0, 1, 15, 1.1844716168217186, 10, 60]],
            )
            self.assertListEqual(
                raw_peak_outputs[0]["monthly"][10],
                [[10.2, 30, 4, 60, 31.3, 30, 15, 60], [10.1, 1, 24, 60, 17.6, 1, 13, 60]],
            )
            self.assertListEqual(
                raw_peak_outputs[0]["runperiod"][11],
                [[10.1, 7, 1, 24, 60, 31.3, 6, 30, 15, 60]],
            )

            self.assertEqual(
                dates[0]["timestep"][0], IntervalTuple(month=6, day=30, hour=1, end_minute=30)
            )
            self.assertEqual(
                dates[0]["timestep"][-1], IntervalTuple(month=7, day=1, hour=24, end_minute=60)
            )

            self.assertEqual(
                dates[0]["hourly"][0], IntervalTuple(month=6, day=30, hour=1, end_minute=60)
            )
            self.assertEqual(
                dates[0]["hourly"][-1], IntervalTuple(month=7, day=1, hour=24, end_minute=60)
            )

            self.assertEqual(
                dates[0]["daily"][0], IntervalTuple(month=6, day=30, hour=0, end_minute=0)
            )
            self.assertEqual(
                dates[0]["daily"][-1], IntervalTuple(month=7, day=1, hour=0, end_minute=0)
            )

            self.assertEqual(
                dates[0]["monthly"][0], IntervalTuple(month=6, day=1, hour=0, end_minute=0)
            )
            self.assertEqual(
                dates[0]["monthly"][0], IntervalTuple(month=6, day=1, hour=0, end_minute=0)
            )

            self.assertEqual(
                dates[0]["runperiod"][-1], IntervalTuple(month=1, day=1, hour=0, end_minute=0)
            )

            self.assertListEqual(day_of_week[0]["timestep"], ["Sunday"] * 48 + ["Monday"] * 48)
            self.assertListEqual(day_of_week[0]["hourly"], ["Sunday"] * 24 + ["Monday"] * 24)
            self.assertListEqual(day_of_week[0]["daily"], ["Sunday", "Monday"])

    def test_generate_peak_outputs(self):
        monitor = EsoFileMonitor("foo")
        with open(self.header_pth, "r") as f:
            header = read_header(f, monitor)

        with open(self.body_pth, "r") as f:
            content = read_body(f, 6, header, False, monitor)
            env_names, _, raw_peak_outputs, dates, cumulative_days, day_of_week = content

        dates, n_days = process_raw_date_data(dates[0], cumulative_days[0], 2002)
        outputs = generate_peak_outputs(
            raw_peak_outputs[0], header, dates, monitor, 1
        )

        min_outputs = outputs["local_min"]
        max_outputs = outputs["local_max"]

        self.assertEqual(list(min_outputs.tables.keys()), ["daily", "monthly", "runperiod"])
        self.assertEqual(list(max_outputs.tables.keys()), ["daily", "monthly", "runperiod"])

        self.assertEqual(min_outputs.tables["daily"].shape, (2, 42))
        self.assertEqual(max_outputs.tables["daily"].shape, (2, 42))

        self.assertEqual(min_outputs.tables["monthly"].shape, (2, 42))
        self.assertEqual(max_outputs.tables["monthly"].shape, (2, 42))

        self.assertEqual(min_outputs.tables["runperiod"].shape, (1, 42))
        self.assertEqual(max_outputs.tables["runperiod"].shape, (1, 42))

    def test_generate_outputs(self):
        monitor = EsoFileMonitor("foo")
        with open(self.header_pth, "r") as f:
            header = read_header(f, monitor)

        with open(self.body_pth, "r") as f:
            (
                env_names,
                raw_outputs,
                raw_peak_outputs,
                dates,
                cumulative_days,
                day_of_week,
            ) = read_body(f, 6, header, False, EsoFileMonitor("dummy"))

        dates, n_days = process_raw_date_data(dates[0], cumulative_days[0], 2002)

        other_data = {N_DAYS_COLUMN: n_days, DAY_COLUMN: day_of_week[0]}
        outputs = generate_outputs(raw_outputs[0], header, dates, other_data, monitor, 1)

        for interval, df in outputs.tables.items():
            key_level = df.columns.get_level_values("key")
            if N_DAYS_COLUMN in key_level:
                self.assertEqual(df.iloc[:, 0].dtype, np.dtype("int64"))
            if DAY_COLUMN in key_level:
                self.assertEqual(df.iloc[:, 0].dtype, np.dtype("object"))

            cond = key_level.isin([N_DAYS_COLUMN, DAY_COLUMN])
            self.assertEqual(set(df.loc[:, ~cond].dtypes), {np.dtype("float64")})

            if interval in [TS, H, D]:
                self.assertTrue(("special", interval, "day", "", "") == df.columns[0])
            else:
                self.assertTrue(("special", interval, "n days", "", "") == df.columns[0])

    def test_create_tree(self):
        with open(self.header_pth, "r") as f:
            header = read_header(f, EsoFileMonitor("foo"))
            tree = Tree()
            dup_ids = tree.populate_tree(header)

            self.assertEqual(dup_ids, {})

            dup1 = Variable(
                "runperiod",
                "BLOCK1:ZONE1",
                "Zone Mechanical Ventilation Air Changes per Hour",
                "ach",
            )
            dup2 = Variable(
                "daily", "Environment", "Site Outdoor Air Drybulb Temperature", "C"
            )

            header["runperiod"][625] = dup1
            header["daily"][626] = dup2
            header["daily"][627] = dup2

            tree = Tree()
            dup_ids = tree.populate_tree(header)
            self.assertDictEqual(dup_ids, {626: dup2, 627: dup2, 625: dup1})

    def test_remove_duplicates(self):
        v1 = Variable("hourly", "a", "b", "c")
        v2 = Variable("hourly", "d", "e", "f")
        v3 = Variable("hourly", "g", "h", "i")
        ids = {1: v1, 2: v2}
        header_dct = {"hourly": {1: v1, 2: v2, 3: v3}}
        outputs_dct = {"hourly": {1: v1, 3: v3}}

        remove_duplicates(ids, header_dct, outputs_dct)

        self.assertEqual(header_dct["hourly"], {3: v3})
        self.assertEqual(outputs_dct["hourly"], {3: v3})

    def test_header_invalid_line(self):
        f = (line for line in ["this is wrong!"])
        with self.assertRaises(AttributeError):
            read_header(f, EsoFileMonitor("foo"))

    def test_body_invalid_line(self):
        f = (line for line in ["this is wrong!"])
        with self.assertRaises(InvalidLineSyntax):
            read_body(f, 6, {"a": []}, False, EsoFileMonitor("foo"))

    def test_body_blank_line(self):
        f = (line for line in [""])
        with self.assertRaises(BlankLineError):
            read_body(f, 6, {"a": []}, False, EsoFileMonitor("foo"))

    def test_file_blank_line(self):
        with self.assertRaises(IncompleteFile):
            read_file(self.incomplete, EsoFileMonitor("some/path"))

    def test_non_numeric_line(self):
        with self.assertRaises(InvalidLineSyntax):
            read_file(
                os.path.join(ROOT, "eso_files/eplusout_invalid_line.eso"),
                EsoFileMonitor("some/path")
            )

    def test_logging_level_info(self):
        try:
            logger.setLevel(logging.INFO)
            EsoFile(os.path.join(ROOT, "eso_files/eplusout1.eso"))

        finally:
            logger.setLevel(logging.ERROR)

# fmt: on
