from typing import Sequence, List

# from esofile_reader import Variable
from esofile_reader.data.df_data import DFData
from contextlib import suppress
import tempfile
import os
from pyarrow.parquet import write_table
from pyarrow import Table
from pathlib import Path


class ParquetDFData(DFData):
    def __init__(self, tables, pardir):
        super().__init__()
        self.tables = tables
        self.results_tables = {k: Path(pardir, f"results-{k}.parquet") for k in tables}
        self.header_tables = {k: Path(pardir, f"header-{k}.parquet") for k in tables}
        self.update_all()

    def relative_results_paths(self, path: Path) -> List[str]:
        return [str(p.relative_to(path)) for p in self.results_tables.values()]

    def relative_header_paths(self, path: Path) -> List[str]:
        return [str(p.relative_to(path)) for p in self.header_tables.values()]

    def update_header_parquet(self, interval):
        with suppress(OSError):
            os.remove(self.header_tables[interval])

        header = self.get_variables_df(interval)
        tbl = Table.from_pandas(header)
        write_table(tbl, self.header_tables[interval])

    def update_results_parquet(self, interval):
        with suppress(OSError):
            os.remove(self.results_tables[interval])

        df = self.tables[interval]

        # store columns to reapply index as consequent operations mutate the original df
        columns = df.columns.copy()
        df.columns = df.columns.droplevel(["interval", "key", "variable", "units"])
        df.columns = df.columns.astype(str)

        tbl = Table.from_pandas(df)
        write_table(tbl, self.results_tables[interval])
        # restore the original columns index
        df.columns = columns

    def update_all(self):
        for interval in self.get_available_intervals():
            self.update_header_parquet(interval)
            self.update_results_parquet(interval)

    def update_variable_name(self, interval: str, id_, key_name, var_name) -> None:
        super().update_variable_name(interval, id_, key_name, var_name)
        self.update_header_parquet(interval)

    def insert_variable(self, variable, array: Sequence) -> None:
        id_ = super().insert_variable(variable, array)
        if id_:
            self.update_header_parquet(variable.interval)
            self.update_results_parquet(variable.interval)
            return id_

    def update_variable_results(self, interval: str, id_: int, array: Sequence[float]):
        id_ = super().update_variable_results(interval, id_, array)
        if id_:
            self.update_header_parquet(interval)
            self.update_results_parquet(interval)
            return id_

    def delete_variables(self, interval: str, ids: Sequence[int]) -> None:
        super().delete_variables(interval, ids)
        self.update_header_parquet(interval)
        self.update_results_parquet(interval)

class ParquetData(ParquetDFData):
    
