import collections
import contextlib
import math
import shutil
from pathlib import Path
from typing import List, Dict, Tuple, Sequence, Union, Optional
from uuid import uuid1

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from esofile_reader.constants import *
from esofile_reader.data.df_data import DFData
from esofile_reader.processor.monitor import DefaultMonitor


def to_int(val):
    try:
        return int(val)
    except ValueError:
        return val


class _ParquetIndexer:
    """
    Very simplified indexer to provide partial  compatibility
    with DataFrame.loc[]. Indexer attempts to slice columns
    index to pass columns argument when reading parquet file.

    Ids are stored as int to provide compatibility with the
    standard DfData.

    """

    def __init__(self, frame: "ParquetFrame"):
        self.frame = frame

    def get_column_items(
            self,
            col: Union[Sequence[Union[str, int, bool, tuple]], Union[str, int, bool, tuple]],
    ) -> Tuple[List[tuple], Optional[List[Union[str, int, tuple]]]]:
        """ Get column multiindex items as a list of tuples. """

        def is_boolean():
            return all(map(lambda x: isinstance(x, (bool, np.bool_)), col))

        def is_primitive():
            return all(map(lambda x: isinstance(x, (str, int)), col))

        def is_tuple():
            n = self.frame.columns.nlevels
            # all child items must be primitive types and
            if all(map(lambda x: isinstance(x, tuple), col)) and all(n == len(t) for t in col):
                return [isinstance(ch, (int, str)) for t in col for ch in t]

        # transform for compatibility with further checks
        col = [col] if isinstance(col, (int, str, tuple)) else col
        if is_primitive() or is_tuple():
            if is_primitive():
                vals = set(self.frame.columns.get_level_values(0))
            else:
                vals = set(self.frame.columns)
            items = [item for item in col if item in vals]
        elif is_boolean() and self.frame.columns.size == len(col):
            items = self.frame.columns[col].tolist()
        else:
            raise IndexError(
                "Cannot slice ParquetFrame. Column slice only "
                "accepts list of {int, str}, boolean arrays or"
                "multiindex tuples of primitive types."
            )
        missing = None if len(items) == len(col) else [c for c in col if c not in items]
        # return requested items as a list of tuples
        return items, missing

    def __getitem__(self, item):
        if isinstance(item, tuple):
            row, col = item
            items, missing = self.get_column_items(col)
            if missing:
                raise KeyError(f"Cannot find {missing} in column index!")
        else:
            row = item
            items = None  # this will pick up all columns

        # when there are not any items specified, full unordered df is returned
        df = self.frame.get_df(items=items)
        items = items if items else self.frame.columns.tolist()
        print(items)
        return df.loc[row, items]

    def __setitem__(self, key, value):
        if not isinstance(value, (int, float, str, pd.Series, collections.Sequence)):
            raise TypeError(
                f"Invalid value type: {value.__class__.__name__}, "
                f"only standard python types and arrays are allowed!"
            )
        if isinstance(key, tuple):
            row, col = key
            items, new = self.get_column_items(col)
            if new:
                for item in new:
                    self.frame.insert_column(item, value)
        else:
            row = key
            items = self.frame.columns.tolist()
        # only update columns if existing data changes
        if items:
            self.frame.update_columns(items, value, rows=row)


class ParquetFrame:
    CHUNK_SIZE = 100
    INDEX_PARQUET = "index.parquet"
    COLUMNS_PARQUET = "columns.parquet"
    CHUNKS_PARQUET = "chunks.parquet"

    def __init__(self, name, pardir="", df: pd.DataFrame = None):
        self.workdir = Path(pardir, f"table-{name}").absolute()
        self.workdir.mkdir(exist_ok=True)
        self._chunks_table = None
        self._indexer = _ParquetIndexer(self)
        self._index = None
        self._columns = None
        if df is not None:
            self.store_df(df)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clean_up()

    def __getitem__(self, item):
        return self._indexer[:, item]

    def __setitem__(self, key, value):
        self._indexer[:, key] = value

    @classmethod
    def from_df(cls, df, name, pardir="", monitor: DefaultMonitor = None):
        pqf = ParquetFrame(name, pardir)
        pqf.store_df(df, monitor=monitor)
        return pqf

    @classmethod
    def from_fs(cls, name, pardir=""):
        pqf = ParquetFrame(name, pardir)
        pqf.load_info_parquets()
        return pqf

    def clean_up(self):
        shutil.rmtree(self.workdir, ignore_errors=True)

    @property
    def name(self):
        return self.workdir.name

    @property
    def chunk_names(self) -> List[str]:
        names = []
        for chunk in self._chunks_table["chunk"].drop_duplicates().tolist():
            names.append(chunk)
        return names

    @property
    def chunk_paths(self) -> List[Path]:
        return [Path(self.workdir, chunk) for chunk in self.chunk_names]

    @property
    def index(self) -> pd.Index:
        return self._index

    @property
    def columns(self) -> pd.MultiIndex:
        return self._columns

    @property
    def loc(self) -> _ParquetIndexer:
        return self._indexer

    @index.setter
    def index(self, val: pd.Index) -> None:
        self._index = val
        for chunk_name in self.chunk_names:
            df = self.get_df_from_parquet(chunk_name)
            df.index = val
            self.update_parquet(chunk_name, df)

    @columns.setter
    def columns(self, val: pd.MultiIndex):
        if not isinstance(val, (pd.Index, pd.MultiIndex)):
            raise IndexError(
                "Invalid index, columns needs to be "
                "an instance of pd.Index or pd.Multiindex."
            )

        if len(val) != len(self._columns):
            raise IndexError(
                f"Invalid columns index! Input length '{len(val)}'" f"!= '{len(self._columns)}'"
            )
        mi = []
        items = {}
        for old, new in zip(self._columns, val):
            if old != new:
                items[old] = new
            mi.append(new)

        # update reference column indexer
        self._columns = pd.MultiIndex.from_tuples(mi, names=self._columns.names)

        # update parquet data
        pairs = self.get_chunk_item_pairs(list(items.keys()))
        for chunk_name, _ in pairs.items():
            mi = []
            df = self.get_df_from_parquet(chunk_name)
            for item in df.columns:
                if item in items.keys():
                    mi.append(items[item])
                else:
                    mi.append(item)
            df.columns = pd.MultiIndex.from_tuples(mi, names=self._columns.names)
            self.update_parquet(chunk_name, df)

    def save_info_parquets(self):
        """ Save columns, index and chunk data as parquets. """
        paths = [
            Path(self.workdir, self.INDEX_PARQUET),
            Path(self.workdir, self.COLUMNS_PARQUET),
            Path(self.workdir, self.CHUNKS_PARQUET),
        ]
        for path in paths:
            with contextlib.suppress(FileNotFoundError):
                path.unlink()

        index = self._index.to_frame(index=False)
        pq.write_table(pa.Table.from_pandas(index), paths[0])

        columns = self._columns.to_frame(index=False)
        columns[ID_LEVEL] = columns[ID_LEVEL].astype(str)
        pq.write_table(pa.Table.from_pandas(columns), paths[1])

        chunks = self._chunks_table.copy()
        chunks[ID_LEVEL] = chunks[ID_LEVEL].astype(str)
        pq.write_table(pa.Table.from_pandas(chunks), paths[2])

    def load_info_parquets(self):
        """ Load index, columns and chunk parquets from fs. """
        paths = [
            Path(self.workdir, self.INDEX_PARQUET),
            Path(self.workdir, self.COLUMNS_PARQUET),
            Path(self.workdir, self.CHUNKS_PARQUET),
        ]

        for path in paths:
            if not path.exists():
                raise FileNotFoundError(
                    f"Cannot find info table: {path}. " f"File {self.workdir} cannot be loaded!"
                )

        index = pq.read_pandas(paths[0]).to_pandas().iloc[:, 0]
        if index.name == TIMESTAMP_COLUMN:
            index = pd.DatetimeIndex(index, name=TIMESTAMP_COLUMN)
        else:
            index = pd.Index(index, name=index.name)
        self._index = pd.Index(index)

        columns = pq.read_pandas(paths[1]).to_pandas()
        columns[ID_LEVEL] = columns[ID_LEVEL].apply(to_int)
        self._columns = pd.MultiIndex.from_frame(columns)

        chunks = pq.read_pandas(paths[2]).to_pandas()
        chunks[ID_LEVEL] = columns[ID_LEVEL].apply(to_int)
        self._chunks_table = chunks

    def update_parquet(self, chunk: str, df: pd.DataFrame) -> None:
        """ Update previously stored parquet.  """
        with contextlib.suppress(FileNotFoundError):
            Path(self.workdir, chunk).unlink()
        header_df = df.columns.to_frame(index=False)
        header_df[ID_LEVEL] = header_df[ID_LEVEL].astype(str)
        df.columns = pd.MultiIndex.from_frame(header_df)

        table = pa.Table.from_pandas(df)
        pq.write_table(table, Path(self.workdir, chunk))

    def get_df_from_parquet(self, chunk_name: str, items: List[tuple] = None) -> pd.DataFrame:
        """ Get DataFrame from given chunk. ."""
        if items:
            columns = []
            for item in items:
                s = rf"""('{"', '".join([str(i) for i in item])}')"""
                columns.append(s)
        else:
            columns = None

        table = pq.read_pandas(Path(self.workdir, chunk_name), columns=columns, memory_map=True)
        df = table.to_pandas()
        del table  # not necessary, but a good practice

        # destringify numeric ids
        header_df = df.columns.to_frame(index=False)
        header_df[ID_LEVEL] = header_df[ID_LEVEL].apply(to_int)
        df.columns = pd.MultiIndex.from_frame(header_df)

        return df

    def get_df(self, items: List[tuple] = None) -> pd.DataFrame:
        """ Get a single DataFrame from multiple parquet files. """
        if items:
            pairs = self.get_chunk_item_pairs(items)
        else:
            pairs = self.get_all_chunk_item_pairs()

        frames = []
        for chunk_name, items in pairs.items():
            frames.append(self.get_df_from_parquet(chunk_name, items=items))

        try:
            df = pd.concat(frames, axis=1, sort=False)
        except ValueError:
            # DataFrame is empty, create an empty dummy
            df = pd.DataFrame(index=self.index, columns=self.columns)

        return df

    @staticmethod
    def create_chunk(items: List[tuple], names=List[str]) -> Tuple[str, pd.DataFrame]:
        """ Create unique chunk name and a piece of reference table. """
        chunk_name = f"{str(uuid1())}.parquet"
        mi = pd.MultiIndex.from_tuples(items, names=names)
        chunk_df = pd.DataFrame({"chunk": [chunk_name] * len(items)}, index=mi)
        return chunk_name, chunk_df

    def store_df(self, df: pd.DataFrame, monitor: DefaultMonitor = None) -> None:
        """ Save DataFrame as a set of parquet files. """
        n = math.ceil(df.shape[1] / self.CHUNK_SIZE)
        start = 0
        frames = []
        for i in range(n):
            dfi = df.iloc[:, start: start + self.CHUNK_SIZE]

            # create chunk reference df
            chunk_name, chunk_df = self.create_chunk(dfi.columns.values, dfi.columns.names)
            frames.append(chunk_df)

            self.update_parquet(chunk_name, dfi)
            start += self.CHUNK_SIZE

            if monitor:
                monitor.update_progress()

        self._chunks_table = pd.concat(frames)
        self._columns = df.columns
        self._index = df.index

    def update_columns(
            self,
            items: List[tuple],
            array: Sequence,
            rows: Union[slice, Sequence] = slice(None, None, None),
    ) -> None:
        """ Update column MultiIndex in stored parquet files. """
        for chunk_name, orig_items in self.get_chunk_item_pairs(items).items():
            df = self.get_df_from_parquet(chunk_name)
            df.loc[rows, orig_items] = array
            self.update_parquet(chunk_name, df)

    def add_mi_column_item(self, variable: tuple, pos: int = None) -> None:
        """ Insert or append new item into column MultiIndex. """
        mi = []
        if not pos or pos == len(self._columns):
            mi = self._columns.tolist()
            mi.append(variable)
        elif pos < 0 or pos > len(self._columns):
            raise IndexError(
                f"Invalid column position '{pos}'! "
                f"Position must be between 0 and {len(self._columns)}."
            )
        else:
            for i, item in enumerate(self._columns):
                if i == pos:
                    mi.extend([variable, item])
                else:
                    mi.append(item)
        self._columns = pd.MultiIndex.from_tuples(mi, names=self._columns.names)

    def insert_column(self, item: tuple, array: Sequence) -> None:
        """ Insert new column into frame. """
        try:
            counted = self._chunks_table.groupby("chunk").count()
            count = counted[ID_LEVEL].min()
            chunk_name = counted[ID_LEVEL].idxmin()
        except ValueError:
            # index error is raised when adding columns into empty frame
            # setting count to chunk size will invoke a new parquet
            count = self.CHUNK_SIZE
            chunk_name = ""

        if count == self.CHUNK_SIZE:
            # create a new chunk
            df = pd.DataFrame({"dummy": array}, index=self.index)
            df.columns = pd.MultiIndex.from_tuples([item], names=self._columns.names)
            chunk_name, chunk_df = self.create_chunk([item])
            pos = None  # this will place the index on very end
        else:
            df = self.get_df_from_parquet(chunk_name)

            # find position of the last chunk item
            last_item_id = df.columns.tolist()[-1][0]
            all_ids = self._columns.get_level_values(ID_LEVEL).tolist()
            pos = all_ids.index(last_item_id) + 1

            # create new item and chunk df to store ref
            df[item] = array
            chunk_df = pd.DataFrame({ID_LEVEL: item[0], "chunk": [chunk_name]})

        self.add_mi_column_item(item, pos=pos)
        self.update_parquet(chunk_name, df)
        self._chunks_table = self._chunks_table.append(chunk_df, ignore_index=True)

    def get_all_chunk_item_pairs(self) -> Dict[str, None]:
        """ Get a hash of all chunk name: item  pairs. """
        pairs = {}
        groups = self._chunks_table.groupby(["chunk"])
        for chunk_name, chunk_df in groups:
            # item passed as 'None' will get a whole table
            pairs[chunk_name] = None
        return pairs

    def get_chunk_item_pairs(self, items: List[tuple]) -> Dict[str, List[tuple]]:
        """ Get a hash of chunk name: ids pairs for given ids. """
        pairs = {}
        df = self._chunks_table.loc[items, :].reset_index()
        groups = df.groupby(["chunk"], sort=False)
        for chunk_name, chunk_df in groups:
            chunk_df = chunk_df.drop(columns="chunk", axis=1)
            pairs[chunk_name] = list(chunk_df.itertuples(index=False))
        return pairs

    def drop(self, columns: List[Union[int, tuple]], inplace=True, level=None) -> None:
        """ Drop columns with given ids from frame. """
        # update columns index
        if level:
            arr = self._columns.get_level_values(level).isin(columns)
        else:
            arr = self._columns.isin(columns)
        drop_items = self._columns[arr].tolist()
        items = self._columns[~arr].tolist()

        # recreate multiindex from remaining items
        self._columns = pd.MultiIndex.from_tuples(items, names=self._columns.names)

        # update parquet files
        for chunk_name, chunk_items in self.get_chunk_item_pairs(items=drop_items).items():
            df = self.get_df_from_parquet(chunk_name)
            df.drop(columns=chunk_items, inplace=True, axis=1)
            if df.empty:
                Path(self.workdir, chunk_name).unlink()
            else:
                self.update_parquet(chunk_name, df)

        # update chunks reference
        self._chunks_table.drop(drop_items, axis=0, inplace=True)


class ParquetData(DFData):
    def __init__(self):
        super().__init__()
        self.tables = {}

    @classmethod
    def from_dfdata(cls, dfdata, pardir, monitor: DefaultMonitor = None):
        """ Create parquet data from DataFrame like class. """
        pqd = ParquetData()
        for k, v in dfdata.tables.items():
            pqd.tables[k] = ParquetFrame.from_df(v, k, pardir, monitor=monitor)
        return pqd

    @classmethod
    def from_fs(cls, path, pardir, monitor: DefaultMonitor = None):
        """ Create parquet data from filesystem directory. """
        pqd = ParquetData()
        for p in [p for p in Path(path).iterdir() if p.is_dir()]:
            interval = str(p.name).split("-")[1]
            pqf = ParquetFrame.from_fs(interval, pardir)
            pqd.tables[interval] = pqf
        return pqd
