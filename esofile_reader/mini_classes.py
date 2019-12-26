from collections import namedtuple

# Request is an object which must be used when getting results
Variable = namedtuple("Variable", "interval key variable units")

# A mini class to store interval data
IntervalTuple = namedtuple("IntervalTuple", "month day hour end_minute")
