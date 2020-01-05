import unittest
import datetime

from esofile_reader.processing.esofile_processor import *
from esofile_reader.processing.esofile_processor import (_process_statement, _process_header_line,
                                                         _last_standard_item_id, _process_raw_line,
                                                         _process_interval_line, _process_result_line)

from esofile_reader.utils.mini_classes import Variable
from esofile_reader.constants import *


class TestEsoFileProcessing(unittest.TestCase):

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
        pass

    def test_generate_peak_outputs(self):
        pass

    def test_generate_outputs(self):
        pass

    def test_create_tree(self):
        pass

    def test_remove_duplicates(self):
        pass

    def test_process_file(self):
        pass

    def test_read_file(self):
        pass


if __name__ == "__main__":
    unittest.main()

    # import pandas as pd
    # pd.set_option('display.max_rows', 20)
    # pd.set_option('display.max_columns', 500)
    # pd.set_option('display.width', 1000)
    #
    # types = ["standard", "local_max", "global_max", "timestep_max", "local_min", "global_min", "timestep_min"]
    # rate_units = ['W', 'kW', 'MW', 'Btu/h', 'kBtu/h', 'MBtu/h']
    # energy_units = ['J', 'kJ', 'MJ', 'GJ', 'Btu', 'kBtu', 'MBtu', 'kWh', 'MWh']
    # interval = "daily"
    # req = [
    #     Variable(interval=interval, key=None, variable="Boiler Gas Rate", units=None),
    #     Variable(interval=interval, key=None, variable="Gas:Facility", units=None),
    #     Variable(interval=interval, key=None, variable="Electricity:Facility", units=None),
    #     Variable(interval=interval, key=None, variable="Cooling Tower Fan Electric Power", units=None),
    #     Variable(interval=interval, key=None, variable="Zone Air Relative Humidity", units=None),
    #     Variable(interval=interval, key=None, variable="Zone Ventilation Sensible Heat Loss Energy", units=None),
    #     Variable(interval=interval, key=None, variable="Zone Mean Air Temperature", units=None),
    # ]
    #
    # eso_file = EsoFile("eso_files/eplusout.eso", ignore_peaks=False)
    # b_eso_file = BuildingEsoFile(eso_file)
