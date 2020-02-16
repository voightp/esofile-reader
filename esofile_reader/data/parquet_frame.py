import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import pandas as pd
from datetime import datetime
import numpy as np
import contextlib
import os
import re
import ast


class ParquetIndexer:
    """
    Very simplified indexer to provide partial  compatibility
    with DataFrame.loc[]. Indexer attempts to slice columns
    index to pass columns argument when reading parquet file.

    """

    def __init__(self, frame, columns, shape):
        self.frame = frame
        self.columns = columns
        self.shape = shape

    def __getitem__(self, item):
        def _is_boolean():
            return all(map(lambda x: isinstance(x, bool), col))

        def _is_id():
            return all(map(lambda x: isinstance(x, (str, int)), col))

        if isinstance(item, tuple):
            row, col = item
            col = [col] if isinstance(col, (int, str)) else col
            if _is_boolean() and self.shape[1] == len(item):
                mi = self.columns[col]
            elif _is_id():
                mi = self.columns[self.columns.get_level_values("id").isin(col)]
            else:
                raise IndexError("Cannot slice ParquetFrame. Column slice only"
                                 "accepts list of int ids or boolean array.")
            if mi.empty:
                raise KeyError(
                    f"Cannot find ids: {', '.join([str(i) for i in col])}"
                )
            str_col = []
            for ix in mi:
                s = rf"('{ix[0]}', '{ix[1]}', '{ix[2]}', '{ix[3]}', '{ix[4]}')"
                str_col.append(s)
        else:
            row = item
            str_col = None

        df = self.frame.as_df(columns=str_col)
        return df.loc[row]


class ParquetFrame:
    def __init__(self, df, name, pardir):
        self.table_path = Path(pardir, f"results-{name}.parquet")
        self._indexer = ParquetIndexer(self, df.columns, df.shape)
        self._temp = None
        self.store_parquet(df)

    def __del__(self):
        with contextlib.suppress(FileNotFoundError):
            os.remove(self.table_path)

    def __getitem__(self, item):
        return self.loc[:, item]

    def __setitem__(self, key, value):
        pass

    @property
    def metadata(self):
        # metadata.metadata returns metadata b' string
        return pq.ParquetFile(self.table_path).metadata.metadata

    @property
    def schema(self):
        return pq.ParquetFile(self.table_path).schema

    @property
    def index(self):
        # extract index name from table metadata
        m = self.metadata[b"pandas"].decode("UTF-8")
        p = re.compile("\"index_columns\": \[\"(\S*)\"\]")
        name = p.search(m).groups()[0]

        # read index column and create pandas index
        table = pq.read_table(self.table_path, columns=[name])
        data = table.column(name).to_pandas()
        return pd.Index(data, name=name)

    @property
    def columns(self):
        return self._indexer.columns

    @property
    def loc(self):
        return self._indexer

    @index.setter
    def index(self, val):
        df = self.as_df()
        df.index = val
        self.update_parquet(df)

    @columns.setter
    def columns(self, val):
        df = self.as_df()
        df.columns = val
        self.update_parquet(df)

    def as_df(self, columns=None):
        table = pq.read_pandas(self.table_path, columns=columns)
        df = table.to_pandas()
        del table  # not necessary, but a good practice
        return df

    def store_parquet(self, df):
        # stringify ids as parquet index cannot be numeric
        header_df = df.columns.to_frame(index=False)
        header_df["id"] = header_df["id"].astype(str)
        df.columns = pd.MultiIndex.from_frame(header_df)

        results_table = pa.Table.from_pandas(df)
        pq.write_table(results_table, self.table_path)

    def update_parquet(self, df):
        with contextlib.suppress(FileNotFoundError):
            os.remove(self.table_path)
        self._indexer.columns = df.columns
        self._indexer.shape = df.shape
        self.store_parquet(df)
