import collections.abc
import contextlib
import math
import shutil
from collections.abc import Iterable
from pathlib import Path
from typing import List, Dict, Tuple, Sequence, Union, Optional
from uuid import uuid1

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from esofile_reader.constants import *
from esofile_reader.processing.progress_logger import GenericProgressLogger
from esofile_reader.tables.df_tables import DFTables


@contextlib.contextmanager
def parquet_frame_factory(
    df: pd.DataFrame,
    name: str,
    pardir: Union[str, Path] = "",
    progress_logger: GenericProgressLogger = None,
):
    pqf = ParquetFrame.from_df(df, name, pardir, progress_logger)
    try:
        yield pqf
    finally:
        pqf.clean_up()


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
        self, col: Union[Sequence[Union[str, int, bool, tuple]], Union[str, int, bool, tuple]],
    ) -> Tuple[List[tuple], Optional[List[Union[str, int, tuple]]]]:
        """ Get column multiindex items as a list of tuples. """

        def is_boolean():
            return isinstance(col, Iterable) and all(
                map(lambda x: isinstance(x, (bool, np.bool_)), col)
            )

        def is_primitive():
            return all(map(lambda x: isinstance(x, (str, int)), col))

        def is_tuple():
            n = self.frame.columns.nlevels
            # all child items must be primitive types and
            if all(map(lambda x: isinstance(x, tuple), col)) and all(n == len(t) for t in col):
                return [isinstance(ch, (int, str)) for t in col for ch in t]

        missing = None
        if isinstance(col, slice) or (is_boolean() and self.frame.columns.size == len(col)):
            items = self.frame.columns[col].tolist()
        else:
            # transform for compatibility with further checks
            col = [col] if isinstance(col, (int, str, tuple)) else col
            if is_primitive():
                vals = set(self.frame.columns.get_level_values(0))
            elif is_tuple():
                vals = set(self.frame.columns)
            else:
                raise IndexError(
                    "Cannot slice ParquetFrame. Column slice only "
                    "accepts list of {int, str}, boolean arrays, slice or"
                    "multiindex tuples of primitive types."
                )
            items = [item for item in col if item in vals]
            if len(items) != len(col):
                missing = [c for c in col if c not in items]

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

        df = self.frame.get_df(items=items)

        return df.loc[row, :]

    def __setitem__(self, key, value):
        if not isinstance(value, (int, float, str, pd.Series, collections.abc.Sequence)):
            raise TypeError(
                f"Invalid value type: {value.__class__.__name__}, "
                f"only standard python types and arrays are allowed!"
            )
        if isinstance(key, tuple):
            row, col = key
            items, new = self.get_column_items(col)
            if new:
                for item in new:
                    self.frame._insert_column(item, value)
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

    def __init__(self, workdir: Path):
        self.workdir = workdir
        self._chunks_table = None
        self._indexer = _ParquetIndexer(self)
        self._index = None
        self._columns = None

    def __getitem__(self, item):
        return self._indexer[:, item]

    def __setitem__(self, key, value):
        self._indexer[:, key] = value

    @classmethod
    def from_df(
        cls,
        df: pd.DataFrame,
        name: str,
        pardir: Union[str, Path] = "",
        progress_logger: GenericProgressLogger = None,
    ) -> "ParquetFrame":
        workdir = Path(pardir, f"table-{name}").absolute()
        workdir.mkdir()
        pqf = ParquetFrame(workdir)
        pqf.store_df(df, progress_logger=progress_logger)
        return pqf

    @classmethod
    def from_fs(cls, workdir: Path) -> "ParquetFrame":
        pqf = ParquetFrame(workdir)
        pqf.load_index_parquets()
        return pqf

    @classmethod
    def get_n_chunks(cls, df: pd.DataFrame) -> int:
        return math.ceil(df.shape[1] / cls.CHUNK_SIZE)

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
            self.save_df_to_parquet(chunk_name, df)

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
        items_dct = {}
        for old, new in zip(self._columns, val):
            if old != new:
                items_dct[old] = new
            mi.append(new)

        # update reference column indexer
        self._columns = val

        # update parquet data
        pairs = self.get_chunk_item_pairs(list(items_dct.keys()))
        for chunk_name, items in pairs.items():
            items_dcti = tuple({k: items_dct[k] for k in items}.items())
            # full dataframe needs to be updated
            df = self.get_df_from_parquet(chunk_name)
            df.columns = self.replace_mi_items(df.columns, items_dcti)
            self.save_df_to_parquet(chunk_name, df)

        # update chunk reference
        self._chunks_table.index = self.replace_mi_items(
            self._chunks_table.index, tuple(items_dct.items())
        )

    @staticmethod
    def stringify_mi_level(mi: pd.MultiIndex, level: str):
        """ Convert miltiindex level to str type. """
        mi_df = mi.to_frame(index=False)
        mi_df[level] = mi_df[level].astype(str)
        return pd.MultiIndex.from_frame(mi_df, names=mi.names)

    @staticmethod
    def int_mi_level(mi: pd.MultiIndex, level: str):
        """ Convert miltiindex level to int type. """

        def to_int(val):
            try:
                return int(val)
            except ValueError:
                return val

        mi_df = mi.to_frame(index=False)
        mi_df[level] = mi_df[level].apply(to_int)
        return pd.MultiIndex.from_frame(mi_df, names=mi.names)

    @staticmethod
    def create_chunk(items: List[tuple], names: List[str]) -> Tuple[str, pd.DataFrame]:
        """ Create unique chunk name and a piece of reference table. """
        chunk_name = f"{str(uuid1())}.parquet"
        mi = pd.MultiIndex.from_tuples(items, names=names)
        chunk_df = pd.DataFrame({"chunk": [chunk_name] * len(items)}, index=mi)
        return chunk_name, chunk_df

    @staticmethod
    def insert_mi_column_item(
        mi: pd.MultiIndex, new_item: tuple, pos: int = None
    ) -> pd.MultiIndex:
        """ Insert or append new item into column MultiIndex. """
        if pos is None or pos == len(mi):
            mi = mi.append(pd.MultiIndex.from_tuples([new_item]))
        elif pos < 0 or pos > len(mi):
            raise IndexError(
                f"Invalid column position '{pos}'! "
                f"Position must be between 0 and {len(mi)}."
            )
        else:
            df = mi.to_frame(index=False)
            frames = [df.iloc[0:pos], pd.DataFrame([new_item], columns=mi.names), df.iloc[pos:]]
            mi = pd.MultiIndex.from_frame(
                pd.concat(frames, sort=False, ignore_index=True), names=mi.names
            )
        return mi

    @staticmethod
    def replace_mi_items(
        mi: pd.MultiIndex, old_new_tuple: Tuple[tuple, tuple]
    ) -> pd.MultiIndex:
        """ Insert or append new item into column MultiIndex. """
        items = mi.tolist()
        for old, new in old_new_tuple:
            pos = items.index(old)
            items[pos] = new
        return pd.MultiIndex.from_tuples(items, names=mi.names)

    def save_index_parquets(self):
        """ Save columns, index and chunk data as parquets. """
        paths = [
            Path(self.workdir, self.INDEX_PARQUET),
            Path(self.workdir, self.COLUMNS_PARQUET),
            Path(self.workdir, self.CHUNKS_PARQUET),
        ]
        for path in paths:
            with contextlib.suppress(FileNotFoundError):
                path.unlink()

        index_df = self._index.to_frame(index=False)
        pq.write_table(pa.Table.from_pandas(index_df), paths[0])

        columns = self.stringify_mi_level(self._columns, ID_LEVEL)
        columns_df = columns.to_frame(index=False)
        pq.write_table(pa.Table.from_pandas(columns_df), paths[1])

        chunks = self._chunks_table.copy()
        chunks.index = self.stringify_mi_level(chunks.index, ID_LEVEL)
        pq.write_table(pa.Table.from_pandas(chunks), paths[2])

    def load_index_parquets(self):
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
            self._index = pd.DatetimeIndex(index, name=TIMESTAMP_COLUMN)
        else:
            self._index = pd.Index(index, name=index.name)

        columns = pq.read_pandas(paths[1]).to_pandas()
        self._columns = self.int_mi_level(pd.MultiIndex.from_frame(columns), ID_LEVEL)

        chunks_table = pq.read_pandas(paths[2]).to_pandas()
        chunks_table.index = self.int_mi_level(chunks_table.index, ID_LEVEL)
        self._chunks_table = chunks_table

    def save_df_to_parquet(self, name: str, df: pd.DataFrame) -> None:
        """ Update previously stored parquet.  """
        path = Path(self.workdir, name)
        with contextlib.suppress(FileNotFoundError):
            path.unlink()
        df.columns = self.stringify_mi_level(df.columns, ID_LEVEL)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, path)

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
        df.columns = self.int_mi_level(df.columns, ID_LEVEL)

        return df

    def get_df(self, items: List[tuple] = None) -> pd.DataFrame:
        """ Get a single DataFrame from multiple parquet files. """
        if items:
            pairs = self.get_chunk_item_pairs(items)
        else:
            pairs = self.get_all_chunk_item_pairs()

        frames = []
        for chunk_name, chunk_items in pairs.items():
            frames.append(self.get_df_from_parquet(chunk_name, items=chunk_items))

        try:
            df = pd.concat(frames, axis=1, sort=False)
        except ValueError:
            # DataFrame is empty, create an empty dummy
            df = pd.DataFrame(index=self.index, columns=self.columns)

        # it's needed to reorder frame to match original items
        items = items if items else self.columns.tolist()

        return df.loc[:, items]

    def store_df(self, df: pd.DataFrame, progress_logger: GenericProgressLogger = None) -> None:
        """ Save DataFrame as a set of parquet files. """
        # avoid potential frame mutation
        df = df.copy()
        n = self.get_n_chunks(df)
        start = 0
        frames = []
        for i in range(n):
            dfi = df.iloc[:, start : start + self.CHUNK_SIZE]

            # create chunk reference df
            chunk_name, chunk_df = self.create_chunk(dfi.columns.values, dfi.columns.names)
            frames.append(chunk_df)

            self.save_df_to_parquet(chunk_name, dfi)
            start += self.CHUNK_SIZE

            if progress_logger:
                progress_logger.increment_progress()

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
            self.save_df_to_parquet(chunk_name, df)

    def _insert_column(
        self, item: Union[tuple, str, int], array: Sequence, pos: int = None
    ) -> None:
        """ Insert new column into frame with lowest number of columns. """
        if isinstance(item, (str, int)):
            item = (item, *[""] * (len(self.columns.names) - 1))

        counted = self._chunks_table["chunk"].value_counts()
        if counted.empty:
            min_count = self.CHUNK_SIZE
            chunk_name = ""
        else:
            min_count = counted.min()
            chunk_name = counted.idxmin()

        mi = pd.MultiIndex.from_tuples([item], names=self.columns.names)
        if min_count == self.CHUNK_SIZE:
            # frame is either empty or all chunks are full, create new parquet
            df = pd.DataFrame({"dummy": array}, index=self.index)
            df.columns = mi
            chunk_name, chunk_df = self.create_chunk([item], self.columns.names)
        else:
            df = self.get_df_from_parquet(chunk_name)
            df[item] = array
            chunk_df = pd.DataFrame({"chunk": [chunk_name]}, index=mi)

        # update column indexer and look up table
        self._columns = self.insert_mi_column_item(self.columns, item, pos=pos)
        self._chunks_table = self._chunks_table.append(chunk_df)

        # save updated dataframe to parquet
        self.save_df_to_parquet(chunk_name, df)

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

    def insert(self, pos: int, item: tuple, array: Sequence):
        """ Insert column at given position. """
        self._insert_column(item, array, pos=pos)

    def drop(self, columns: List[Union[int, tuple]], level: str = None, **kwargs) -> None:
        """ Drop columns with given ids from frame. """
        if level:
            if level not in self.columns.names:
                raise IndexError(
                    f"Cannot drop items: [{columns}]. Invalid level '{level}' specified."
                    f"\nAvailable levels are: [{self.columns.names}]."
                )
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
                self.save_df_to_parquet(chunk_name, df)

        # update chunks reference
        self._chunks_table.drop(drop_items, axis=0, inplace=True)


class ParquetTables(DFTables):
    def __init__(self):
        super().__init__()

    @classmethod
    def from_dftables(
        cls, dftables: DFTables, pardir: Path, progress_logger: GenericProgressLogger = None
    ) -> "ParquetTables":
        """ Create parquet data from DataFrame like class. """
        pqt = ParquetTables()
        for k, v in dftables.tables.items():
            pqt.tables[k] = ParquetFrame.from_df(v, k, pardir, progress_logger=progress_logger)
        return pqt

    @classmethod
    def from_fs(cls, pardir: Path, progress_logger: GenericProgressLogger = None):
        """ Create parquet data from filesystem directory. """
        pqt = ParquetTables()
        for p in [p for p in Path(pardir).iterdir() if p.is_dir()]:
            table = str(p.name).split("-", maxsplit=1)[1]
            pqf = ParquetFrame.from_fs(p)
            pqt.tables[table] = pqf
        return pqt
