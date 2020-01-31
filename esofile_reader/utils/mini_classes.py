from collections import namedtuple
from typing import Union

try:
    from typing import ForwardRef
except ImportError:
    from typing import _ForwardRef as ForwardRef

# Request is an object which must be used when getting results
Variable = namedtuple("Variable", "interval key variable units")

# A mini class to store interval data
IntervalTuple = namedtuple("IntervalTuple", "month day hour end_minute")

# type hint to wrap all result types
ResultsFile = Union[ForwardRef("EsoFile"),
                    ForwardRef("DiffFile"),
                    ForwardRef("TotalsFile"),
                    ForwardRef("DatabaseFile")]

# type hint to wrap all storage types
Storage = Union[ForwardRef("SQLStorage"),
                ForwardRef("DFStorage")]
