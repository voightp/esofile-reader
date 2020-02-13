from typing import Sequence

from esofile_reader import Variable
from esofile_reader.data.df_data import DFData
from contextlib import suppress
import tempfile
import os
from pyarrow.parquet import write_table
from pyarrow import Table
from pathlib import Path


class ParquetData(DFData):
    def __init__(self, tables, dir):
        super().__init__()
        self.tables = tables
        self.table_paths = {k: Path(dir, f"results-{k}.parquet") for k in tables}
        self.header_paths = {k: Path(dir, f"header-{k}.parquet") for k in tables}
        self.update_all()

    def update_parquet(self, interval):
        with suppress(OSError):
            os.remove(self.header_paths[interval])

        header = self.get_variables_df(interval)
        tbl = Table.from_pandas(header)
        write_table(tbl, self.header_paths[interval])

        with suppress(OSError):
            os.remove(self.table_paths[interval])

        df = self.tables[interval]
        df.columns = df.columns.droplevel(["interval", "key", "variable", "units"])
        df.columns = df.columns.astype(str)

        tbl = Table.from_pandas(df)
        write_table(tbl, self.table_paths[interval])

    def update_all(self):
        for interval in self.get_available_intervals():
            self.update_parquet(interval)

    def update_variable_name(self, interval: str, id_, key_name, var_name) -> None:
        super().update_variable_name(interval, id_, key_name, var_name)
        self.update_parquet(interval)

    def insert_variable(self, variable: Variable, array: Sequence) -> None:
        id_ = super().insert_variable(variable, array)
        if id_:
            self.update_parquet(variable.interval)
            return id_

    def update_variable(self, interval: str, id_: int, array: Sequence[float]):
        id_ = super().update_variable(interval, id_, array)
        if id_:
            self.update_parquet(interval)
            return id_

    def delete_variables(self, interval: str, ids: Sequence[int]) -> None:
        super().delete_variables(interval, ids)
        self.update_parquet(interval)
