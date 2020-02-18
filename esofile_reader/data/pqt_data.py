import contextlib
import os
import re
import math
import numpy as np
from pathlib import Path
from typing import List
from uuid import uuid1
from collections import defaultdict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from esofile_reader.data.df_data import DFData
import shutil


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

    def __setitem__(self, key, value):
        df = self.frame.as_df()
        try:
            df.loc[key] = value
        except KeyError:
            df[key] = value

        self.frame._columns = df.columns
        self.frame.update_parquet(df)


class ParquetFrame:
    CHUNK_SIZE = 100

    def __init__(self, df, name, pardir=""):
        self.root_path = Path(pardir, f"results-{name}").absolute()
        self.root_path.mkdir()
        self._chunks_table = None
        self._indexer = ParquetIndexer(self)
        self._index = df.index
        self._columns = df.columns
        self.store_df(df)

    def __del__(self):
        print("REMOVING PARQUET FRAME " + str(self.root_path))
        shutil.rmtree(self.root_path, ignore_errors=True)

    def __getitem__(self, item):
        return self._indexer[:, item]

    def __setitem__(self, key, value):
        self._indexer[key] = value

    @property
    def chunk_paths(self):
        paths = []
        for chunk in self.chunks:
            paths.append(Path(self.root_path, chunk))
        return paths

    @property
    def chunks(self):
        return self._chunks_table["chunk"].drop_duplicates().tolist()

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
        self._index = val
        for chunk in self._chunks_table["chunk"]:
            df = self.get_df(chunk)
            df.index = val
            self.update_parquet(chunk, df)

    @columns.setter
    def columns(self, val):
        if not isinstance(val, (pd.Index, pd.MultiIndex)):
            raise IndexError("Invalid index, columns needs to be "
                             "an instance of pd.Index or pd.Multiindex.")

        if len(val) != len(self._columns):
            raise IndexError(f"Invalid columns index! Input length '{len(val)}'"
                             f"!= '{len(self._columns)}'")
        mi = []
        items = {}
        for orig, new in zip(self._columns, val):
            if orig != new:
                items[new[0]] = new
            mi.append(new)

        # update reference column indexer
        self._columns = pd.MultiIndex.from_tuples(mi, names=self._columns.names)

        # update parquet data
        pairs = self.get_chunk_id_pairs([str(v) for v in items.keys()])
        for chunk_name, _ in pairs.items():
            mi = []
            df = self.get_df(chunk_name)
            for item in df.columns:
                if item[0] in items.keys():
                    mi.append(items[item[0]])
                else:
                    mi.append(item)
            df.columns = pd.MultiIndex.from_tuples(mi, names=self._columns.names)
            self.update_parquet(chunk_name, df)

    def store_parquet(self, chunk: str, df: pd.DataFrame):
        table = pa.Table.from_pandas(df)
        pq.write_table(table, Path(self.root_path, chunk))

    def update_parquet(self, chunk: str, df: pd.DataFrame):
        with contextlib.suppress(FileNotFoundError):
            os.remove(Path(self.root_path, chunk))
        self.store_parquet(chunk, df)

    def get_df(self, chunk: str, ids: List[str] = None):

        def to_int(val):
            try:
                return int(val)
            except ValueError:
                return val

        table = pq.read_pandas(
            Path(self.root_path, chunk),
            columns=ids,
            memory_map=True
        )
        df = table.to_pandas()
        del table  # not necessary, but a good practice

        # destringify numeric ids
        header_df = df.columns.to_frame(index=False)
        header_df["id"] = header_df["id"].apply(to_int)
        df.columns = pd.MultiIndex.from_frame(header_df)

        return df

    def get_full_df(self):
        frames = []
        for chunk in self.chunks:
            frames.append(self.get_df(chunk))
        return pd.concat(frames, axis=1, sort=False)

    @staticmethod
    def create_chunk(ids: List[str]):
        chunk_name = f"{str(uuid1())}.parquet"
        chunk_df = pd.DataFrame({"id": ids, "chunk": [chunk_name] * len(ids)})
        return chunk_name, chunk_df

    def store_df(self, df):
        n = math.ceil(df.shape[1] / self.CHUNK_SIZE)
        start = 0
        frames = []
        for i in range(n):
            dfi = df.iloc[:, start:start + self.CHUNK_SIZE]

            # stringify ids as parquet index cannot be numeric
            header_df = dfi.columns.to_frame(index=False)
            header_df["id"] = header_df["id"].astype(str)
            dfi.columns = pd.MultiIndex.from_frame(header_df)

            # create chunk reference df
            chunk_name, chunk_df = self.create_chunk(header_df["id"].tolist())
            frames.append(chunk_df)

            self.store_parquet(chunk_name, dfi)
            start += self.CHUNK_SIZE

        self._chunks_table = pd.concat(frames)

    def insert(self, item, array):
        smallest = self._chunks_table.groupby("chunk").count().iloc[0]
        chunk_name = smallest.name
        count = smallest.iloc[0]

        if count == self.CHUNK_SIZE:
            # create a new chunk
            df = pd.DataFrame({"dummy": array}, index=self.index)
            df.columns = pd.MultiIndex.from_tuples([item], names=self._columns.names)
            chunk_name, chunk_df = self.create_chunk([str(item[0])])
            self.add_column_item(item)
        else:
            df = self.get_df(chunk_name)

            # find position of the last chunk item
            last_item_id = df.columns.tolist()[-1][0]
            all_ids = self._columns.get_level_values("id").tolist()
            pos = all_ids.index(last_item_id) + 1

            df[item] = array
            chunk_df = pd.DataFrame({"id": str(item[0]), "chunk": [chunk_name]})

            self.add_column_item(item, pos=pos)

        self._chunks_table.append(chunk_df)
        self.store_parquet(chunk_name, df)

    def get_all_chunk_id_pairs(self):
        pairs = {}
        groups = self._chunks_table.groupby(["chunk"])
        for chunk_name, df in groups:
            # ids passed as 'None' will get whole tables
            pairs[chunk_name] = None
        return pairs

    def get_chunk_id_pairs(self, ids: List[str]):
        pairs = {}
        out = self._chunks_table.loc[self._chunks_table["id"].isin(ids)]
        groups = out.groupby(["chunk"])
        for chunk_name, df in groups:
            pairs[chunk_name] = df.loc[:, "id"].tolist()
        return pairs

    def drop(self, columns: List[int], **kwargs):
        ids = [str(i) for i in columns]
        pairs = self.get_chunk_id_pairs(ids)

        for chunk, ids in pairs.items():
            df = self.get_df(chunk)
            df.drop(columns=ids, inplace=True, level="id")
            if df.empty:
                os.remove(Path(self.root_path, chunk))
            self._chunks_table.drop(self._chunks_table.loc[df["id"].isin(ids)].index, axis=1)
            self.delete_column_items(ids)
            self.update_parquet(df)

    def delete_column_items(self, ids):
        mi = []
        for item in self._columns:
            id_ = item[0]
            if id_ in ids:
                mi.append(item)
        self._columns = pd.MultiIndex.from_tuples(mi, names=self._columns.names)

    def add_column_item(self, variable: tuple, pos: int = None):
        mi = []
        if not pos or pos == len(self._columns):
            mi = self._columns.tolist()
            mi.append(variable)
        elif pos < 0 or pos > len(self._columns):
            raise IndexError(f"Invalid column position '{pos}'! "
                             f"Position must be between 0 and {len(self._columns)}.")
        else:
            for i, item in enumerate(self._columns):
                if i == pos:
                    mi.extend([variable, item])
                else:
                    mi.append(item)
        self._columns = pd.MultiIndex.from_tuples(mi, names=self._columns.names)


class ParquetData(DFData):
    def __init__(self, tables, pardir):
        super().__init__()
        self.tables = {k: ParquetFrame(v, k, pardir) for k, v in tables.items()}

    def relative_table_paths(self, rel_to: Path) -> List[str]:
        return [str(tbl.relative_to(rel_to)) for tbl in self.tables.values()]
