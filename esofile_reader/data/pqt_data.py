import contextlib
import os
import shutil
from pathlib import Path
from typing import List, Dict, Tuple, Sequence
from uuid import uuid1

import math
import numpy as np
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

    def __init__(self, frame: "ParquetFrame"):
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
            all_ids = self.frame.columns.get_level_values("id").to_series()
            if _is_id():
                ids = all_ids.where(all_ids.isin(col)).dropna().values
            elif _is_boolean() and self.frame.columns.size == len(col):
                ids = all_ids.where(col).dropna().values
            elif _is_tuple():
                ids = [ix[0] for ix in col if ix in self.frame.columns]
            else:
                raise IndexError("Cannot slice ParquetFrame. Column slice only "
                                 "accepts list of int ids, boolean arrays or"
                                 "multiindex tuples.")
            if not ids:
                raise KeyError(
                    f"Cannot find ids: {', '.join([str(i) for i in col])}"
                )
        else:
            row = item
            ids = None

        df = self.frame.get_df(ids=ids)
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
    def chunk_paths(self) -> List[Path]:
        paths = []
        for chunk in self._chunks_table["chunk"].drop_duplicates().tolist():
            paths.append(Path(self.root_path, chunk))
        return paths

    @property
    def index(self) -> pd.Index:
        return self._index

    @property
    def columns(self) -> pd.MultiIndex:
        return self._columns

    @property
    def loc(self) -> ParquetIndexer:
        return self._indexer

    @index.setter
    def index(self, val: pd.Index) -> None:
        self._index = val
        for chunk_name in self._chunks_table["chunk"]:
            df = self.get_df_from_parquet(chunk_name)
            df.index = val
            self.update_parquet(chunk_name, df)

    @columns.setter
    def columns(self, val: pd.MultiIndex):
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
        pairs = self.get_chunk_id_pairs(list(items.keys()))
        for chunk_name, _ in pairs.items():
            mi = []
            df = self.get_df_from_parquet(chunk_name)
            for item in df.columns:
                if item[0] in items.keys():
                    mi.append(items[item[0]])
                else:
                    mi.append(item)
            df.columns = pd.MultiIndex.from_tuples(mi, names=self._columns.names)
            self.update_parquet(chunk_name, df)

    def store_parquet(self, chunk: str, df: pd.DataFrame) -> None:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, Path(self.root_path, chunk))

    def update_parquet(self, chunk: str, df: pd.DataFrame) -> None:
        with contextlib.suppress(FileNotFoundError):
            os.remove(Path(self.root_path, chunk))
        self.store_parquet(chunk, df)

    def get_df_from_parquet(self, chunk_name: str, ids: List[int] = None) -> pd.DataFrame:

        def to_int(val):
            try:
                return int(val)
            except ValueError:
                return val

        if ids:
            mi = self.columns[self.columns.get_level_values("id").isin(ids)]
            columns = []
            for ix in mi:
                s = rf"('{ix[0]}', '{ix[1]}', '{ix[2]}', '{ix[3]}', '{ix[4]}')"
                columns.append(s)
        else:
            columns = None

        table = pq.read_pandas(
            Path(self.root_path, chunk_name),
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

    def get_df(self, ids: List[int] = None) -> pd.DataFrame:
        if ids:
            pairs = self.get_chunk_id_pairs(ids)
        else:
            pairs = self.get_all_chunk_id_pairs()

        frames = []
        for chunk_name, ids in pairs.items():
            frames.append(self.get_df_from_parquet(chunk_name, ids=ids))

        return pd.concat(frames, axis=1, sort=False)

    @staticmethod
    def create_chunk(ids: List[int]) -> Tuple[str, pd.DataFrame]:
        chunk_name = f"{str(uuid1())}.parquet"
        chunk_df = pd.DataFrame({"id": ids, "chunk": [chunk_name] * len(ids)})
        return chunk_name, chunk_df

    def store_df(self, df: pd.DataFrame) -> None:
        n = math.ceil(df.shape[1] / self.CHUNK_SIZE)
        start = 0
        frames = []
        for i in range(n):
            dfi = df.iloc[:, start:start + self.CHUNK_SIZE]

            # create chunk reference df
            chunk_name, chunk_df = self.create_chunk(
                dfi.columns.get_level_values("id").tolist()
            )
            frames.append(chunk_df)

            # stringify ids as parquet index cannot be numeric
            header_df = dfi.columns.to_frame(index=False)
            header_df["id"] = header_df["id"].astype(str)
            dfi.columns = pd.MultiIndex.from_frame(header_df)

            self.store_parquet(chunk_name, dfi)
            start += self.CHUNK_SIZE

        self._chunks_table = pd.concat(frames)

    def add_column_item(self, variable: tuple, pos: int = None) -> None:
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

    def insert(self, item: Tuple[int, str, str, str, str], array: Sequence) -> None:
        smallest = self._chunks_table.groupby("chunk").count().iloc[0]
        chunk_name = smallest.name
        count = smallest.iloc[0]

        if count == self.CHUNK_SIZE:
            # create a new chunk
            df = pd.DataFrame({"dummy": array}, index=self.index)
            df.columns = pd.MultiIndex.from_tuples([item], names=self._columns.names)
            chunk_name, chunk_df = self.create_chunk([item[0]])
            self.add_column_item(item)
        else:
            df = self.get_df_from_parquet(chunk_name)

            # find position of the last chunk item
            last_item_id = df.columns.tolist()[-1][0]
            all_ids = self._columns.get_level_values("id").tolist()
            pos = all_ids.index(last_item_id) + 1

            df[item] = array
            chunk_df = pd.DataFrame({"id": str(item[0]), "chunk": [chunk_name]})

            self.add_column_item(item, pos=pos)

        self._chunks_table.append(chunk_df)
        self.store_parquet(chunk_name, df)

    def get_all_chunk_id_pairs(self) -> Dict[str, None]:
        pairs = {}
        groups = self._chunks_table.groupby(["chunk"])
        for chunk_name, df in groups:
            # ids passed as 'None' will get whole tables
            pairs[chunk_name] = None
        return pairs

    def get_chunk_id_pairs(self, ids: List[int]) -> Dict[str, List[int]]:
        pairs = {}
        out = self._chunks_table.loc[self._chunks_table["id"].isin(ids)]
        groups = out.groupby(["chunk"])
        for chunk_name, df in groups:
            pairs[chunk_name] = df.loc[:, "id"].tolist()
        return pairs

    def delete_column_items(self, ids: List[int]) -> None:
        mi = []
        for item in self._columns:
            id_ = item[0]
            if id_ in ids:
                mi.append(item)
        self._columns = pd.MultiIndex.from_tuples(mi, names=self._columns.names)

    def drop(self, columns: List[int], **kwargs) -> None:
        self.delete_column_items(columns)
        for chunk_name, ids in self.get_chunk_id_pairs(columns).items():
            df = self.get_df_from_parquet(chunk_name)
            df.drop(columns=ids, inplace=True, level="id")
            self._chunks_table.drop(self._chunks_table.loc[df["id"].isin(ids)].index, axis=1)
            if df.empty:
                os.remove(Path(self.root_path, chunk_name))
            else:
                self.update_parquet(chunk_name, df)


class ParquetData(DFData):
    def __init__(self, tables, pardir):
        super().__init__()
        self.tables = {k: ParquetFrame(v, k, pardir) for k, v in tables.items()}

    def relative_table_paths(self, rel_to: Path) -> List[str]:
        return [str(tbl.relative_to(rel_to)) for tbl in self.tables.values()]
