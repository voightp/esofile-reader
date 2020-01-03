
TS = "timestep"
H = "hourly"
D = "daily"
M = "monthly"
A = "annual"
RP = "runperiod"

N_DAYS_COLUMN = "n days"
DAY_COLUMN = "day"

RATE_TO_ENERGY_DCT = {TS: False,
                      H: False,
                      D: True,
                      M: True,
                      A: True,
                      RP: True}

YEAR = 2002

ALL_INTERVALS = ["timestep", "hourly", "daily", "monthly", "annual", "runperiod"]

AVERAGED_VARIABLES = ["C", "F", "W", "ach", "ppm", "%", "", " ", "kgWater/kgDryAir"]
