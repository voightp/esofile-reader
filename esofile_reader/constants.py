TS = "timestep"
H = "hourly"
D = "daily"
M = "monthly"
A = "annual"
RP = "runperiod"

N_DAYS_COLUMN = "n days"
DAY_COLUMN = "day"
TIMESTAMP_COLUMN = "timestamp"
VALUE_COLUMN = "value"

RATE_TO_ENERGY_DCT = {TS: False,
                      H: False,
                      D: True,
                      M: True,
                      A: True,
                      RP: True}

ALL_INTERVALS = ["timestep", "hourly", "daily", "monthly", "annual", "runperiod"]

# AVERAGED_VARIABLES = ["C", "F", "W", "ach", "ppm", "%", "", " ", "kgWater/kgDryAir"]

AVERAGED_UNITS = [
    "W",
    "W/m2",
    "C",
    "",
    "W/m2-K",
    "ppm",
    "ach",
    "hr",
]

SUMMED_UNITS = [
    "J",
    "J/m2"
]

IGNORED_UNITS = [
    "kg/s"
]
