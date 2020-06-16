from collections import namedtuple
from typing import Union

try:
    from typing import ForwardRef
except ImportError:
    from typing import _ForwardRef as ForwardRef

# Request is an object which must be used when getting results
Variable = namedtuple("Variable", "interval key type units")
SimpleVariable = namedtuple("SimpleVariable", "interval key units")

# A mini class to store interval data
IntervalTuple = namedtuple("IntervalTuple", "month day hour end_minute")

# type_ hint to wrap all result types
ResultsFile = Union[
    ForwardRef("EsoFile"),  # noqa: F821
    ForwardRef("DiffFile"),  # noqa: F821
    ForwardRef("TotalsFile"),  # noqa: F821
    ForwardRef("DatabaseFile"),  # noqa: F821
]

# type_ hint to wrap all storage types
Data = Union[ForwardRef("SQLData"), ForwardRef("DFData")]  # noqa: F821
