from eso_reader.eso_file import EsoFile, get_results
from eso_reader.mini_classes import Variable

import pandas as pd

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
print(eso_file.header_tree)

with pd.ExcelWriter("C:/users/vojtechp1/desktop/types.xlsx")as wr:
    for t in types:
        r = get_results(eso_file, req, type=t, units_system="IP")
        r.to_excel(wr, sheet_name=t)

with pd.ExcelWriter("C:/users/vojtechp1/desktop/e_units.xlsx")as wr:
    for e in energy_units:
        r = get_results(eso_file, req, type="standard", energy_units=e, units_system="IP")
        r.to_excel(wr, sheet_name=e.replace("/", "_"))

with pd.ExcelWriter("C:/users/vojtechp1/desktop/r_units.xlsx")as wr:
    for ra in rate_units:
        r = get_results(eso_file, req, type="standard", rate_units=ra, units_system="IP")
        r.to_excel(wr, sheet_name=ra.replace("/", "_"))
