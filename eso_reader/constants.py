import datetime as dt

TS = "timestep"
H = "hourly"
D = "daily"
M = "monthly"
A = "annual"
RP = "runperiod"

MIN_DATE = dt.datetime(1990, 1, 1)
MAX_DATE = dt.datetime(2100, 1, 1)

RATE_TO_ENERGY_DCT = {TS: False,
                      H: False,
                      D: True,
                      M: True,
                      A: True,
                      RP: True}

YEAR = 2002

ALL_INTERVALS = ["timestep", "hourly", "daily", "monthly", "annual", "runperiod"]

AVERAGED_VARIABLES = ["C", "F", "W", "ach", "ppm", "%", "", " ", "kgWater/kgDryAir"]
