from collections import namedtuple
from typing import Union

try:
    from typing import ForwardRef
except ImportError:
    from typing import _ForwardRef as ForwardRef

# Request is an object which must be used when getting results
Variable = namedtuple("Variable", "table key type units")
SimpleVariable = namedtuple("SimpleVariable", "table key units")

# A mini class to store table data
IntervalTuple = namedtuple("IntervalTuple", "month day hour end_minute")

# type_ hint to wrap all result types
ResultsFileType = Union[
    ForwardRef("EsoFile"),  # noqa: F821
    ForwardRef("ResultsFile"),  # noqa: F821
    ForwardRef("ParquetFile"),  # noqa: F821
    ForwardRef("DatabaseFile"),  # noqa: F821
]

# type_ hint to wrap all storage types
TableType = Union[
    ForwardRef("SQLTables"), ForwardRef("DFTables"), ForwardRef("ParquetTables")  # noqa: F821
]
