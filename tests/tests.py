from eso_reader.eso_file import EsoFile, get_results
from eso_reader.mini_classes import Variable
from eso_reader.building_eso_file import BuildingEsoFile

import pandas as pd
pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

types = ["standard", "local_max", "global_max", "timestep_max", "local_min", "global_min", "timestep_min"]
rate_units = ['W', 'kW', 'MW', 'Btu/h', 'kBtu/h', 'MBtu/h']
energy_units = ['J', 'kJ', 'MJ', 'GJ', 'Btu', 'kBtu', 'MBtu', 'kWh', 'MWh']
interval = "daily"
req = [
    Variable(interval=interval, key=None, variable="Boiler Gas Rate", units=None),
    Variable(interval=interval, key=None, variable="Gas:Facility", units=None),
    Variable(interval=interval, key=None, variable="Electricity:Facility", units=None),
    Variable(interval=interval, key=None, variable="Cooling Tower Fan Electric Power", units=None),
    Variable(interval=interval, key=None, variable="Zone Air Relative Humidity", units=None),
    Variable(interval=interval, key=None, variable="Zone Ventilation Sensible Heat Loss Energy", units=None),
    Variable(interval=interval, key=None, variable="Zone Mean Air Temperature", units=None),
]

eso_file = EsoFile("eso_files/eplusout.eso", ignore_peaks=False)
b_eso_file = BuildingEsoFile(eso_file)
