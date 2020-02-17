import contextlib
import os
import re
import math
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

    def __init__(self, frame):
        self.frame = frame

    def __setitem__(self, key, value):
        df = self.frame.as_df()
        try:
            df.loc[key] = value
        except KeyError:
            df[key] = value

        self.frame._columns = df.columns
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
            if _is_boolean() and self.frame.columns.size == len(col):
                mi = self.frame.columns[col]
            elif _is_id():
                mi = self.frame.columns[self.frame.columns.get_level_values("id").isin(col)]
            elif _is_tuple():
                arr = [ix in col for ix in self.frame.columns]
                mi = self.frame.columns[arr]
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
    CHUNK_SIZE = 100

    def __init__(self, df, name, pardir):
        self.root_path = Path(pardir, f"results-{name}")
        self.root_path.mkdir()
        self.chunks = None
        self._indexer = ParquetIndexer(self)
        self._index = df.index
        self._columns = df.columns

        self.store_df(df)

    def __del__(self):
        print("REMOVING PARQUET FRAME " + str(self.table_path))
        with contextlib.suppress(FileNotFoundError):
            os.remove(self.table_path)

    def __getitem__(self, item):
        return self._indexer[:, item]

    def __setitem__(self, key, value):
        self._indexer[key] = value

    @property
    def index(self):
        return self._index

    @property
    def columns(self):
        return self._columns

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

    def get_df(self, name: str, columns: List[str] = None):

        def to_int(val):
            try:
                return int(val)
            except ValueError:
                return val

        table = pq.read_pandas(
            Path(self.root_path, name),
            columns=columns,
            memory_map=True
        )
        df = table.to_pandas()
        del table  # not necessary, but a good practice

        # destringify numeric ids
        header_df = df.columns.to_frame(index=False)
        header_df["id"] = header_df["id"].apply(to_int)
        df.columns = pd.MultiIndex.from_frame(header_df)

        return df

    def store_df(self, df):
        n = math.ceil(df.size[1] / self.CHUNK_SIZE)
        start = 0
        frames = []
        for i in range(n):
            dfi = df.iloc[:, 0:start + self.CHUNK_SIZE]

            # stringify ids as parquet index cannot be numeric
            header_df = dfi.columns.to_frame(index=False)
            header_df["id"] = header_df["id"].astype(str)
            dfi.columns = pd.MultiIndex.from_frame(header_df)
            name = f"chunk-{i}.parquet"

            frames.append(
                pd.DataFrame({"id": header_df["id"], "chunk": [name] * dfi.shape[1]})
            )
            self.store_parquet(name, dfi)
            start += self.CHUNK_SIZE

        self.chunks = pd.concat(frames)

    def find_parquet(self, ids: List[str]):
        pairs = {}
        out = self.chunks.loc[self.chunks["id"].isin(ids)]
        groups = out.groupby(["chunk"])
        for key, df in groups:
            pairs[key] = df.loc[:, "id"].tolist()
        return pairs

    def store_parquet(self, name: str, df: pd.DataFrame):
        table = pa.Table.from_pandas(df)
        pq.write_table(table, Path(self.root_path, name))

    def update_parquet(self, name: str, df: pd.DataFrame):
        with contextlib.suppress(FileNotFoundError):
            os.remove(Path(self.root_path, name))
        self.store_parquet(name, df)

    def drop(self, columns: List[int], **kwargs):
        ids = [str(i) for i in columns]
        pairs = self.find_parquet(ids)

        for chunk, ids in pairs.items():
            df = self.get_df(chunk)
            df = self.as_df()
            df.drop(columns=ids, inplace=True, level="id")

        # TODO str vs int, columns setter
        self._columns = df.columns
        self.update_parquet(df)


class ParquetData(DFData):
    def __init__(self, tables, pardir):
        super().__init__()
        self.tables = {k: ParquetFrame(v, k, pardir) for k, v in tables.items()}

    def relative_table_paths(self, rel_to: Path) -> List[str]:
        return [str(tbl.relative_to(rel_to)) for tbl in self.tables.values()]
