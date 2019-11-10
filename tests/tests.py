import unittest
import datetime
from collections import defaultdict
from eso_reader.eso_processor import *
from eso_reader.eso_processor import (_process_statement, _process_header_line,
                                      _last_standard_item_id, _process_raw_line,
                                      _process_interval_line)

from eso_reader.eso_file import EsoFile, get_results
from eso_reader.mini_classes import Variable
from eso_reader.building_eso_file import BuildingEsoFile


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

    def test_create_variable(self):
        variables = [Variable("foo", "bar", "baz", "u")]
        new_var1 = create_variable(variables, "foo", "bar", "baz", "u")
        new_var2 = create_variable(variables, "fo", "bar", "baz", "u")

        self.assertTupleEqual(new_var1, Variable("foo", "bar (1)", "baz", "u"))
        self.assertTupleEqual(new_var2, Variable("fo", "bar", "baz", "u"))

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
        l1 = _process_raw_line("1 , a  ,b  , c")
        l2 = _process_raw_line("945,217.68491613470054")
        l3 = _process_raw_line("3604,0.2382358160619045,0.0,10, 4, 1,30,11.73497,12, 2, 6, 1")
        self.assertEqual(l1, (1, ["a", "b", "c"]))
        self.assertEqual(l2, (945, ["217.68491613470054"]))
        self.assertEqual(l3, (3604, ["0.2382358160619045", "0.0", "10", "4", "1",
                                     "30", "11.73497", "12", "2", "6", "1"]))


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
