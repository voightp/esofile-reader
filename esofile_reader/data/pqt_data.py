from typing import Sequence, List

# from esofile_reader import Variable
from esofile_reader.data.df_data import DFData
from esofile_reader.data.parquet_frame import ParquetFrame
from contextlib import suppress
import tempfile
import os
from pyarrow.parquet import write_table
from pyarrow import Table
from pathlib import Path


class ParquetData(DFData):
    def __init__(self, tables, pardir):
        super().__init__()
        self.tables = {k: ParquetFrame(v, k, pardir) for k, v in tables.items()}

    def relative_table_paths(self, path: Path) -> List[str]:
        return [str(tbl.relative_to(path)) for tbl in self.tables.values()]
