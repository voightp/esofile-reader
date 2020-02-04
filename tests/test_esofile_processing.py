import datetime
import os
import unittest

from esofile_reader.processing.esofile_processor import *
from esofile_reader.processing.esofile_processor import (_process_statement, _process_header_line,
                                                         _last_standard_item_id, _process_raw_line,
                                                         _process_interval_line, _process_result_line)
from esofile_reader.processing.monitor import DefaultMonitor
from esofile_reader.utils.mini_classes import Variable
from esofile_reader.base_file import IncompleteFile
from tests import ROOT


class TestEsoFileProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.header_pth = os.path.join(ROOT, "eso_files/header.txt")
        cls.body_pth = os.path.join(ROOT, "eso_files/body.txt")

    def test_esofile_statement(self):
        line = "Program Version,EnergyPlus, " \
               "Version 9.1.0-08d2e308bb, YMD=2019.07.23 15:19"

        version, timestamp = _process_statement(line)

        self.assertEqual(version, 910)
        self.assertEqual(timestamp, datetime.datetime(2019, 7, 23, 15, 19, 00))

    def test_header_line1(self):
        line = "8,7,Environment,Site Outdoor Air Drybulb Temperature [C] " \
               "!Daily [Value,Min,Hour,Minute,Max,Hour,Minute]"
        line_id, key, var, units, interval = _process_header_line(line)

        self.assertEqual(line_id, 8)
        self.assertEqual(key, "Environment")
        self.assertEqual(var, "Site Outdoor Air Drybulb Temperature")
        self.assertEqual(units, "C")
        self.assertEqual(interval, "daily")

    def test_header_line2(self):
        line = "302,1,InteriorEquipment:Electricity [J] !Hourly"
        line_id, key, var, units, interval = _process_header_line(line)

        self.assertEqual(line_id, 302)
        self.assertEqual(key, "Meter")
        self.assertEqual(var, "InteriorEquipment:Electricity")
        self.assertEqual(units, "J")
        self.assertEqual(interval, "hourly")

    def test_header_line3(self):
        line = "302,1,InteriorEquipment,Electricity,[J], !Hourly"
        with self.assertRaises(InvalidLineSyntax):
            _process_header_line(line)

    def test_read_header1(self):
        f = ["7,1,Environment,Site Outdoor Air Drybulb Temperature [C] !Hourly",
             "3676,11,Some meter [ach] !RunPeriod [Value,Min,Month,Day,Hour,Minute,Max,Month,Day,Hour,Minute]",
             "End of Data Dictionary"]
        g = (l for l in f)

        header_dct, outputs = read_header(g)

        test_header = defaultdict(partial(defaultdict))
        test_header["hourly"][7] = Variable("hourly",
                                            "Environment",
                                            "Site Outdoor Air Drybulb Temperature",
                                            "C")
        test_header["runperiod"][3676] = Variable("runperiod",
                                                  "Meter",
                                                  "Some meter",
                                                  "ach")

        test_outputs = defaultdict(partial(defaultdict))
        test_outputs["hourly"][7] = []
        test_outputs["runperiod"][3676] = []

        self.assertDictEqual(header_dct, test_header)
        self.assertDictEqual(outputs, test_outputs)

    def test_read_header2(self):
        f = ["7,1,Environment,Site Outdoor Air Drybulb Temperature [C] !Hourly",
             "",
             "End of Data Dictionary"]
        g = (l for l in f)
        with self.assertRaises(BlankLineError):
            read_header(g)

    def test_read_header3(self):
        with open(self.header_pth, "r") as f:
            header, init_outputs = read_header(f)
            self.assertEqual(header.keys(), init_outputs.keys())

            for interval, variables in header.items():
                self.assertEqual(variables.keys(), init_outputs[interval].keys())

            v1 = Variable("timestep", "Environment", "Site Direct Solar Radiation Rate per Area", "W/m2")
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

    def test_last_standard_item_id(self):
        self.assertEqual(_last_standard_item_id(890), 6)
        self.assertEqual(_last_standard_item_id(900), 6)
        self.assertEqual(_last_standard_item_id(750), 5)

    def test_process_raw_line(self):
        l0 = _process_raw_line("945,217.68491613470054")
        l1 = _process_raw_line("3604,0.2382358160619045,0.0,10, 4, 1,30,11.73497,12, 2, 6, 1")
        self.assertEqual(l0, (945, ["217.68491613470054"]))
        self.assertEqual(l1, (3604, ["0.2382358160619045", "0.0", "10", " 4", " 1",
                                     "30", "11.73497", "12", " 2", " 6", " 1"]))

    def test_process_interval_line(self):
        l0 = [" 1", " 2", " 3", " 0", "10.00", "0.00", "60.00", "Saturday"]
        l1 = [" 1", " 2", " 3", " 0", "10.00", "0.00", "30.00", "Saturday"]
        l2 = [" 20", " 1", " 2", " 0", "Saturday"]
        l3 = [" 58", " 1"]
        l4 = ["365"]
        l5 = ["1"]

        l0 = _process_interval_line(2, l0)
        l1 = _process_interval_line(2, l1)
        l2 = _process_interval_line(3, l2)
        l3 = _process_interval_line(4, l3)
        l4 = _process_interval_line(5, l4)
        l5 = _process_interval_line(6, l5)

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

    def test_process_result_line(self):
        l0 = ["102.13019653252035", "0.0", "10", "60", "160.3332731467023", "7", "60"]

        l0a = _process_result_line(l0, True)
        self.assertEqual(l0a[0], "102.13019653252035")
        self.assertIsNone(l0a[1])

        l0b = _process_result_line(l0, False)
        self.assertEqual(l0b[0], "102.13019653252035")
        self.assertEqual(l0b[1], [0.0, 10, 60, 160.3332731467023, 7, 60])

    def test_read_body(self):
        with open(self.header_pth, "r") as f:
            _, init_outputs = read_header(f)

        with open(self.body_pth, "r") as f:
            (env_names, raw_outputs, raw_peak_outputs, dates,
             cumulative_days, day_of_week) = read_body(f, 6, init_outputs, False, DefaultMonitor("dummy"))

            self.assertEqual(raw_outputs[0]["timestep"][7],
                             ['15.65\n', '14.3\n', '14.15\n', '14.0\n', '12.8\n', '11.6\n',
                              '10.899999999999999\n', '10.2\n', '11.05\n', '11.9\n',
                              '13.0\n', '14.1\n', '15.05\n', '16.0\n', '17.35\n', '18.7\n',
                              '20.4\n', '22.1\n', '23.75\n', '25.4\n', '26.4\n', '27.4\n',
                              '28.0\n', '28.6\n', '29.1\n', '29.6\n',
                              '30.200000000000004\n', '30.8\n', '31.05\n', '31.3\n',
                              '30.75\n', '30.2\n', '29.6\n', '29.0\n', '28.7\n', '28.4\n',
                              '27.45\n', '26.5\n', '25.7\n', '24.9\n',
                              '24.049999999999998\n', '23.2\n', '22.25\n', '21.3\n',
                              '20.25\n', '19.2\n', '18.1\n', '17.0\n', '16.05\n',
                              '15.1\n', '14.3\n', '13.5\n', '12.95\n', '12.4\n',
                              '12.100000000000002\n', '11.8\n', '11.600000000000002\n',
                              '11.4\n', '11.15\n', '10.9\n',
                              '11.45\n', '12.0\n', '11.9\n', '11.8\n', '12.5\n', '13.2\n',
                              '13.75\n', '14.3\n', '14.75\n', '15.2\n', '15.35\n', '15.5\n',
                              '16.55\n', '17.6\n', '17.200000000000004\n', '16.8\n',
                              '16.5\n', '16.2\n', '15.5\n', '14.8\n', '15.6\n',
                              '16.4\n', '16.299999999999998\n', '16.2\n', '15.7\n',
                              '15.2\n', '14.45\n', '13.7\n', '13.05\n', '12.4\n', '11.9\n',
                              '11.4\n', '10.850000000000002\n', '10.3\n', '10.2\n',
                              '10.1\n'])
            self.assertEqual(raw_outputs[0]["hourly"][8],
                             ['14.975000000000002\n', '14.075\n', '12.2\n', '10.549999999999999\n',
                              '11.475000000000002\n', '13.55\n', '15.525\n', '18.025\n', '21.25\n', '24.575\n',
                              '26.9\n', '28.3\n', '29.35\n', '30.5\n', '31.175\n', '30.475\n', '29.3\n',
                              '28.549999999999998\n', '26.975\n', '25.299999999999998\n', '23.625\n', '21.775\n',
                              '19.725\n', '17.55\n', '15.575\n', '13.9\n', '12.675\n', '11.950000000000001\n', '11.5\n',
                              '11.025\n', '11.725\n', '11.850000000000002\n', '12.85\n', '14.025\n', '14.975\n',
                              '15.425\n', '17.075000000000004\n', '17.0\n', '16.35\n', '15.15\n', '16.0\n', '16.25\n',
                              '15.45\n', '14.075\n', '12.725000000000002\n', '11.65\n', '10.575000000000001\n',
                              '10.149999999999999\n'])

            self.assertEqual(raw_outputs[0]["hourly"][163],
                             [np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN,
                              np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN,
                              np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, '31.939895115385128\n',
                              '31.703936337515786\n', '32.280660803461618\n', '32.62177706428757\n',
                              '32.88418951192571\n', '33.009496155093547\n', '33.03911553829569\n',
                              '32.92907267649866\n', '32.65682359572439\n', '32.31898695867979\n',
                              '32.197143544621329\n', '31.872368037056775\n', np.NaN, np.NaN, np.NaN, np.NaN, np.NaN,
                              np.NaN])

            self.assertListEqual(raw_outputs[0]["daily"][14], ['12.883333333333335', '10.19895789513146'])
            self.assertListEqual(raw_outputs[0]["monthly"][15], ['12.883333333333335', '10.19895789513146'])
            self.assertListEqual(raw_outputs[0]["runperiod"][16], ['11.541145614232397'])

            self.assertListEqual(raw_peak_outputs[0]["daily"][9], [[10.2, 4, 60, 31.3, 15, 60],
                                                                   [10.1, 24, 60, 17.6, 13, 60]])
            self.assertListEqual(raw_peak_outputs[0]["daily"][622], [[0.0, 1, 15, 0.0, 1, 15],
                                                                     [0.0, 1, 15, 1.1844716168217186, 10, 60]])
            self.assertListEqual(raw_peak_outputs[0]["monthly"][10], [[10.2, 30, 4, 60, 31.3, 30, 15, 60],
                                                                      [10.1, 1, 24, 60, 17.6, 1, 13, 60]])
            self.assertListEqual(raw_peak_outputs[0]["runperiod"][11], [[10.1, 7, 1, 24, 60, 31.3, 6, 30, 15, 60]])

            self.assertEqual(dates[0]["timestep"][0], IntervalTuple(month=6, day=30, hour=1, end_minute=30))
            self.assertEqual(dates[0]["timestep"][-1], IntervalTuple(month=7, day=1, hour=24, end_minute=60))

            self.assertEqual(dates[0]["hourly"][0], IntervalTuple(month=6, day=30, hour=1, end_minute=60))
            self.assertEqual(dates[0]["hourly"][-1], IntervalTuple(month=7, day=1, hour=24, end_minute=60))

            self.assertEqual(dates[0]["daily"][0], IntervalTuple(month=6, day=30, hour=0, end_minute=0))
            self.assertEqual(dates[0]["daily"][-1], IntervalTuple(month=7, day=1, hour=0, end_minute=0))

            self.assertEqual(dates[0]["monthly"][0], IntervalTuple(month=6, day=1, hour=0, end_minute=0))
            self.assertEqual(dates[0]["monthly"][0], IntervalTuple(month=6, day=1, hour=0, end_minute=0))

            self.assertEqual(dates[0]["runperiod"][-1], IntervalTuple(month=1, day=1, hour=0, end_minute=0))

            self.assertListEqual(day_of_week[0]["timestep"], ["Sunday"] * 48 + ["Monday"] * 48)
            self.assertListEqual(day_of_week[0]["hourly"], ["Sunday"] * 24 + ["Monday"] * 24)
            self.assertListEqual(day_of_week[0]["daily"], ["Sunday", "Monday"])

    def test_generate_peak_outputs(self):
        with open(self.header_pth, "r") as f:
            header, init_outputs = read_header(f)

        with open(self.body_pth, "r") as f:
            (env_names, _, raw_peak_outputs, dates,
             cumulative_days, day_of_week) = read_body(f, 6, init_outputs, False, DefaultMonitor("dummy"))

        dates, n_days = interval_processor(dates[0], cumulative_days[0], 2002)

        outputs = generate_peak_outputs(raw_peak_outputs[0], header, dates)

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
        with open(self.header_pth, "r") as f:
            header, init_outputs = read_header(f)

        with open(self.body_pth, "r") as f:
            (env_names, raw_outputs, raw_peak_outputs, dates,
             cumulative_days, day_of_week) = read_body(f, 6, init_outputs, False, DefaultMonitor("dummy"))

        dates, n_days = interval_processor(dates[0], cumulative_days[0], 2002)

        other_data = {N_DAYS_COLUMN: n_days, DAY_COLUMN: day_of_week[0]}
        outputs = generate_outputs(raw_outputs[0], header, dates, other_data)

        for interval, df in outputs.tables.items():
            if N_DAYS_COLUMN in df.columns:
                self.assertEqual(df[N_DAYS_COLUMN].dtype, np.dtype("int64"))
            if DAY_COLUMN in df.columns:
                self.assertEqual(df[DAY_COLUMN].dtype, np.dtype("object"))

            cond = df.columns.get_level_values("id").isin([N_DAYS_COLUMN, DAY_COLUMN])
            self.assertEqual(set(df.loc[:, ~cond].dtypes), {np.dtype("float64")})

    def test_create_tree(self):
        with open(self.header_pth, "r") as f:
            header, init_outputs = read_header(f)
            tree, dup_ids = create_tree(header)

            self.assertEqual(dup_ids, [])

            dup1 = Variable('runperiod', 'BLOCK1:ZONE1', 'Zone Mechanical Ventilation Air Changes per Hour', 'ach')
            dup2 = Variable('daily', 'Environment', 'Site Outdoor Air Drybulb Temperature', 'C')

            header["runperiod"][625] = dup1
            header["daily"][626] = dup2
            header["daily"][627] = dup2

            tree, dup_ids = create_tree(header)
            self.assertListEqual(dup_ids, [626, 627, 625])

    def test_remove_duplicates(self):
        ids = [1, 2]
        header_dct = {"foo": {1: "a", 2: "b", 3: "c"}}
        outputs_dct = {"foo": {1: "a", 3: "c"}}

        remove_duplicates(ids, header_dct, outputs_dct)

        self.assertEqual(header_dct["foo"], {3: "c"})
        self.assertEqual(outputs_dct["foo"], {3: "c"})

    def test_monitor_invalid_path(self):
        m = DefaultMonitor("abc")
        with self.assertRaises(FileNotFoundError):
            m.preprocess()

    def test_header_invalid_line(self):
        f = (line for line in ["this is wrong!"])
        with self.assertRaises(AttributeError):
            read_header(f)

    def test_body_invalid_line(self):
        f = (line for line in ["this is wrong!"])
        with self.assertRaises(ValueError):
            read_body(f, 6, {"a": []}, False, DefaultMonitor("foo"))

    def test_body_blank_line(self):
        f = (line for line in [""])
        with self.assertRaises(BlankLineError):
            read_body(f, 6, {"a": []}, False, DefaultMonitor("foo"))

    def test_file_blank_line(self):
        with self.assertRaises(IncompleteFile):
            read_file(self.header_pth)


if __name__ == "__main__":
    unittest.main()
