TS = "timestep"
H = "hourly"
D = "daily"
M = "monthly"
A = "annual"
RP = "runperiod"
RANGE = "range"
INDEX = "index"

N_DAYS_COLUMN = "n days"
DAY_COLUMN = "day"
TIMESTAMP_COLUMN = "timestamp"
DATA_LEVEL = "data"
VALUE_LEVEL = "value"
ID_LEVEL = "id"
GROUP_ID_LEVEL = "group id"
TABLE_LEVEL = "table"
KEY_LEVEL = "key"
TYPE_LEVEL = "type"
UNITS_LEVEL = "units"
STR_VALUES = "str_values"
COLUMN_LEVELS = ("id", "table", "key", "type", "units")
SIMPLE_COLUMN_LEVELS = ("id", "table", "key", "units")
PEAK_COLUMN_LEVELS = ("id", "table", "key", "type", "units", "data")
SPECIAL = "special"
REFERENCE_YEAR = 2020

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
    "kgWater/kgDryAir",
]

SUMMED_UNITS = ["J", "J/m2"]
IGNORED_UNITS = ["kg/s", "m3/s"]
IGNORED_TYPES = {
    "Performance Curve Input Variable",
    "Performance Curve Output Value",
}
