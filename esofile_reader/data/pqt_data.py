import contextlib
import os
import re
import numpy as np
from pathlib import Path
from typing import List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from esofile_reader.data.df_data import DFData


class ParquetIndexer:
    """
    Very simplified indexer to provide partial  compatibility
    with DataFrame.loc[]. Indexer attempts to slice columns
    index to pass columns argument when reading parquet file.

    Ids are stored as int to provide compatibility with the
    standard DfData.

    """

    def __init__(self, frame, columns, shape):
        self.frame = frame
        self.columns = columns
        self.shape = shape

    def __setitem__(self, key, value):
        df = self.frame.as_df()
        try:
            df.loc[key] = value
        except KeyError:
            df[key] = value

        self.frame.update_parquet(df)

    def __getitem__(self, item):
        def _is_boolean():
            return all(map(lambda x: isinstance(x, (bool, np.bool_)), col))

        def _is_id():
            return all(map(lambda x: isinstance(x, (str, int)), col))

        def _is_tuple():
            return all(map(lambda x: isinstance(x, tuple), col))

        if isinstance(item, tuple):
            row, col = item
            col = [col] if isinstance(col, (int, str, tuple)) else col
            if (_is_boolean() and self.shape[1] == len(col)):
                mi = self.columns[col]
            elif _is_id():
                mi = self.columns[self.columns.get_level_values("id").isin(col)]
            elif _is_tuple():
                arr = [ix in col for ix in self.columns]
                mi = self.columns[arr]
            else:
                raise IndexError("Cannot slice ParquetFrame. Column slice only "
                                 "accepts list of int ids, boolean arrays or"
                                 "multiindex tuples.")
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
        df = df.loc[row]

        if len(df.columns) == 1:
            # reduce dimension
            df = df.iloc[:, 0]

        return df


class ParquetFrame:
    def __init__(self, df, name, pardir):
        self.table_path = Path(pardir, f"results-{name}.parquet")
        self._indexer = ParquetIndexer(self, df.columns, df.shape)
        self._temp = None
        self.store_parquet(df)

    def __del__(self):
        print("REMOVING PARQUET FRAME " + str(self.table_path))
        with contextlib.suppress(FileNotFoundError):
            os.remove(self.table_path)

    def __getitem__(self, item):
        return self._indexer[:, item]

    def __setitem__(self, key, value):
        self._indexer[key] = value

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

        def to_int(val):
            try:
                return int(val)
            except ValueError:
                return val

        table = pq.read_pandas(self.table_path, columns=columns)
        df = table.to_pandas()
        del table  # not necessary, but a good practice

        # destringify numeric ids
        header_df = df.columns.to_frame(index=False)
        header_df["id"] = header_df["id"].apply(to_int)
        df.columns = pd.MultiIndex.from_frame(header_df)

        return df

    def drop(self, *args, **kwargs):
        df = self.as_df()
        df.drop(*args, **kwargs)
        self.update_parquet(df)

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


class ParquetData(DFData):
    def __init__(self, tables, pardir):
        super().__init__()
        self.tables = {k: ParquetFrame(v, k, pardir) for k, v in tables.items()}

    def relative_table_paths(self, rel_to: Path) -> List[str]:
        return [str(tbl.relative_to(rel_to)) for tbl in self.tables.values()]
