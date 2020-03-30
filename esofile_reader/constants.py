TS = "timestep"
H = "hourly"
D = "daily"
M = "monthly"
A = "annual"
RP = "runperiod"
RANGE = "range"

N_DAYS_COLUMN = "n days"
DAY_COLUMN = "day"
TIMESTAMP_COLUMN = "timestamp"
VALUE_COLUMN = "value"
RANGE_COLUMN = "range"

RATE_TO_ENERGY_DCT = {TS: False, H: False, D: True, M: True, A: True, RP: True}

AVERAGED_UNITS = [
    "W",
    "W/m2",
    "C",
    "deltaC",
    "",
    "W/m2-K",
    "ppm",
    "ach",
    "hr",
    "%",
    "kgWater/kgDryAir"
]

SUMMED_UNITS = ["J", "J/m2"]

IGNORED_UNITS = ["kg/s", "m3/s"]
