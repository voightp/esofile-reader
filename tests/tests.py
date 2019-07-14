from eso_reader.eso_file import EsoFile, get_results
from eso_reader.mini_classes import Variable

import pandas as pd

types = ["standard", "local_max", "global_max", "timestep_max", "local_min", "global_min", "timestep_min"]
energy_units = ['W', 'kW', 'MW', 'Btu/h', 'kBtu/h', 'MBtu/h']
rate_units = ['J', 'kJ', 'MJ', 'GJ', 'Btu', 'kBtu', 'MBtu', 'kWh', 'MWh']
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

with pd.ExcelWriter("C:/users/vojte/desktop/test.xlsx")as wr:
    for t in types:
        r = get_results(eso_file, req, type=t)
        r.to_excel(wr, sheet_name=t)
