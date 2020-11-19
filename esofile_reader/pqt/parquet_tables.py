import contextlib
import math
import shutil
from pathlib import Path
from typing import List, Dict, Tuple, Sequence, Union, Any, Optional
from uuid import uuid1
from zipfile import ZipFile

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from esofile_reader.constants import *
from esofile_reader.df.df_tables import DFTables
from esofile_reader.exceptions import CorruptedData
from esofile_reader.id_generator import get_unique_name
from esofile_reader.mini_classes import PathLike
from esofile_reader.processing.progress_logger import BaseLogger


@contextlib.contextmanager
def parquet_frame_factory(
    df: pd.DataFrame, name: str, pardir: PathLike = "", progress_logger: BaseLogger = None,
):
    pqf = ParquetFrame.from_df(df, name, pardir, progress_logger)
    try:
        yield pqf
    finally:
        pqf.clean_up()


def get_unique_workdir(workdir: Path) -> Path:
    old_name = workdir.name
    pardir = workdir.parent
    all_names = [p.name for p in pardir.iterdir()]
    new_name = get_unique_name(old_name, all_names)
    return Path(pardir, new_name)


PQT_ID = "pqt_id"


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

    def _get_column_items(self, items):
        items = [items] if isinstance(items, (tuple, str, int)) else items
        return self.frame._lookup_table.loc[items, :].index

    def _split_missing(self, items) -> Tuple[pd.Index, pd.Index]:
        items = [items] if isinstance(items, (tuple, str, int)) else items
        try:
            existing = self.frame._lookup_table.loc[items, :].index
            missing = pd.Index([])
        except (KeyError, ValueError):
            index = pd.Index(items)
            missing = index.difference(self.frame.columns)
            existing = index.difference(missing)
        return existing, missing

    def __getitem__(self, item):
        if isinstance(item, tuple):
            rows, col = item
            column_items = self._get_column_items(col)
            df = self.frame._get_df(items=column_items)
        else:
            rows = item
            df = self.frame._get_whole_df()  # this will pick up all columns
        return df.loc[rows, :]

    def __setitem__(self, key, value):
        if not isinstance(value, (int, float, str, pd.Series, list, np.ndarray)):
            raise TypeError(
                f"Invalid value type: {value.__class__.__name__}, "
                f"only standard python types and arrays are allowed!"
            )
        if isinstance(key, tuple):
            rows, column_items = key
            existing, missing = self._split_missing(column_items)
            if not missing.empty:
                for item in missing:
                    self.frame._insert_column(item, value)
        else:
            rows = key
            existing = self.frame.columns
        # only update columns if existing data changes
        if not existing.empty:
            self.frame.update_columns(existing, value, rows)


class ParquetFrame:
    CHUNK_SIZE = 100
    INDEX_PARQUET = "index.parquet"
    CHUNKS_PARQUET = "chunks.parquet"

    def __init__(self, workdir: Path):
        self.workdir = workdir.absolute()
        self._indexer = _ParquetIndexer(self)
        self._index = None
        self._lookup_table = None

    def __getitem__(self, item):
        return self._indexer[:, item]

    def __setitem__(self, key, value):
        self._indexer[:, key] = value

    def __copy__(self):
        new_workdir = get_unique_workdir(self.workdir)
        return self._copy(new_workdir)

    def _copy(self, new_workdir: Path):
        shutil.copytree(self.workdir, new_workdir)
        parquet_frame = ParquetFrame(new_workdir)
        parquet_frame.workdir = new_workdir
        parquet_frame._lookup_table = self._lookup_table.copy()
        parquet_frame._index = self._index.copy()
        return parquet_frame

    def copy_to(self, new_pardir: Path):
        new_workdir = Path(new_pardir, self.name)
        return self._copy(new_workdir)

    @classmethod
    def from_df(
        cls, df: pd.DataFrame, name: str, pardir: PathLike = "", logger: BaseLogger = None,
    ) -> "ParquetFrame":
        workdir = Path(pardir, f"table-{name}").absolute()
        workdir.mkdir()
        pqf = ParquetFrame(workdir)
        pqf._store_df(df, logger=logger)
        return pqf

    @classmethod
    def _read_from_fs(cls, pqf: "ParquetFrame") -> "ParquetFrame":
        missing = pqf.find_missing_indexing_parquets()
        if missing:
            raise CorruptedData(
                f"Cannot find info tables: {missing}. File {pqf.workdir} cannot be loaded!"
            )
        pqf.read_indexing_parquets()
        missing = pqf.find_missing_chunk_parquets()
        if missing:
            raise CorruptedData(
                f"Cannot find info tables: {missing}. File {pqf.workdir} cannot be loaded!"
            )
        pqf.clear_indexing_parquets()
        return pqf

    @classmethod
    def from_fs(cls, workdir: Path) -> "ParquetFrame":
        pqf = ParquetFrame(workdir)
        try:
            cls._read_from_fs(pqf)
        except Exception as e:
            pqf.clean_up()
            raise e
        return pqf

    @classmethod
    def get_n_chunks(cls, df: pd.DataFrame) -> int:
        return math.ceil(df.shape[1] / cls.CHUNK_SIZE)

    @property
    def name(self):
        return self.workdir.name

    @property
    def chunk_names(self) -> List[str]:
        names = []
        for chunk in self._lookup_table["chunk"].drop_duplicates().tolist():
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
        return self._lookup_table.index

    @property
    def loc(self) -> _ParquetIndexer:
        return self._indexer

    @property
    def index_parquet_path(self) -> Path:
        return Path(self.workdir, self.INDEX_PARQUET)

    @property
    def lookup_parquet_path(self) -> Path:
        return Path(self.workdir, self.CHUNKS_PARQUET)

    @property
    def indexing_paths(self) -> List[Path]:
        return [self.index_parquet_path, self.lookup_parquet_path]

    @index.setter
    def index(self, val: pd.Index) -> None:
        if not issubclass(type(val), pd.Index):
            raise TypeError("Index must be subclass if pd.Index.")
        if len(val) != len(self.index):
            raise ValueError(
                f"Expected index length is {len(self.index)}, new index length is {len(val)}."
            )
        self._index = val

    @columns.setter
    def columns(self, val: pd.MultiIndex) -> None:
        self._lookup_table.index = val

    @property
    def empty(self):
        return self._lookup_table.empty

    @staticmethod
    def stringify_mi_level(mi: pd.MultiIndex, level: str) -> pd.MultiIndex:
        """ Convert multiindex level to str type. """
        mi_df = mi.to_frame(index=False)
        mi_df[level] = mi_df[level].astype(str)
        return pd.MultiIndex.from_frame(mi_df, names=mi.names)

    @staticmethod
    def int_mi_level(mi: pd.MultiIndex, level: str) -> pd.MultiIndex:
        """ Convert multiindex level to int type. """

        def to_int(val):
            try:
                return int(val)
            except ValueError:
                return val

        mi_df = mi.to_frame(index=False)
        mi_df[level] = mi_df[level].apply(to_int)
        return pd.MultiIndex.from_frame(mi_df, names=mi.names)

    @staticmethod
    def create_chunk(index: pd.MultiIndex, pqt_ids: List[int]) -> Tuple[str, pd.DataFrame]:
        """ Create unique chunk name and a piece of reference table. """
        chunk_name = f"{str(uuid1())}.parquet"
        chunk_df = pd.DataFrame({PQT_ID: pqt_ids}, index=index)
        chunk_df["chunk"] = chunk_name
        return chunk_name, chunk_df

    def insert_to_lookup_table(self, pos: int, pqt_id: int, pqt_name: str, mi: pd.MultiIndex):
        length = len(self._lookup_table.index)
        if pos == length or pos is None:
            self._lookup_table = self.append_to_lookup_table(pqt_id, pqt_name, mi)
        elif 0 <= pos < length:
            frames = [
                self._lookup_table.iloc[0:pos],
                pd.DataFrame({PQT_ID: pqt_id, "chunk": pqt_name}, index=mi),
                self._lookup_table.iloc[pos:],
            ]
            self._lookup_table = pd.concat(frames)
        else:
            raise IndexError(
                f"Invalid column position '{pos}'! " f"Position must be between 0 and {length}."
            )

    def append_to_lookup_table(self, pqt_id: int, pqt_name: str, mi: pd.MultiIndex):
        df = pd.DataFrame({PQT_ID: pqt_id, "chunk": pqt_name}, index=mi)
        return self._lookup_table.append(df)

    @staticmethod
    def _write_table(df: pd.DataFrame, path: Path, preserve_index: bool = True) -> None:
        """ Write given parquet table into given path. """
        with open(path, "bw") as f:
            pq.write_table(pa.Table.from_pandas(df, preserve_index=preserve_index), f)

    def clean_up(self):
        shutil.rmtree(self.workdir, ignore_errors=True)

    def save_df_to_parquet(self, name: str, df: pd.DataFrame) -> None:
        """ Update previously stored parquet.  """
        path = Path(self.workdir, name)
        with contextlib.suppress(FileNotFoundError):
            path.unlink()
        df.reset_index(drop=True, inplace=True)
        self._write_table(df, path, preserve_index=False)

    def read_df_from_parquet(self, pqt_name: str, columns: List[int] = None) -> pd.DataFrame:
        """ Get DataFrame from given parquet. ."""
        columns = list(map(str, columns)) if columns else None
        table = pq.read_pandas(Path(self.workdir, pqt_name), columns=columns, memory_map=True)
        return table.to_pandas()

    def _assign_multiindex(self, pqt_ids: np.ndarray) -> pd.MultiIndex:
        """ Reconstruct columns multiindex. """
        df = self._lookup_table.loc[self._lookup_table[PQT_ID].isin(pqt_ids), :]
        sort_index = dict(zip(pqt_ids, range(len(pqt_ids))))
        df["sort"] = df[PQT_ID].map(sort_index)
        df = df.sort_values(by="sort", ascending=True)
        df.drop("sort", axis=1, inplace=True)
        return df.index

    def as_df(self) -> pd.DataFrame:
        """ Get a single DataFrame from multiple parquet files. """
        return self._get_whole_df()

    def _join_frames(self, frames: List[pd.DataFrame]):
        try:
            df = pd.concat(frames, axis=1, sort=False)
            df.index = self.index.copy()
            df.columns = self._assign_multiindex(df.columns.to_numpy())
        except ValueError:
            import traceback

            # DataFrame is empty, create an empty dummy
            print("WRONG!!!" + traceback.format_exc())
            df = pd.DataFrame(index=self.index)
        return df

    def _get_whole_df(self):
        frames = []
        for pqt_name in self.chunk_names:
            frames.append(self.read_df_from_parquet(pqt_name))
        df = self._join_frames(frames)
        return df.loc[:, self.columns]

    def _get_df(self, items: List[Tuple[Any, ...]] = None) -> pd.DataFrame:
        """ Get a single DataFrame from multiple parquet files. """
        pairs = self.get_chunk_id_pairs(items)
        frames = []
        for pqt_name, pqt_ids in pairs.items():
            frames.append(self.read_df_from_parquet(pqt_name, columns=pqt_ids))
        df = self._join_frames(frames)
        return df.loc[:, items]

    def _store_df(self, df: pd.DataFrame, logger: BaseLogger = None) -> None:
        """ Save DataFrame as a set of parquet files. """
        df = df.copy()  # avoid potential frame mutation
        n = self.get_n_chunks(df)
        start = 0
        frames = []
        for i in range(n):
            end = start + self.CHUNK_SIZE
            dfi = df.iloc[:, start:end]

            # use id as the only identifier
            pqt_columns = pd.RangeIndex(start=start, stop=start + len(dfi.columns))

            # create chunk reference series
            chunk_name, chunk_df = self.create_chunk(dfi.columns, pqt_columns.tolist())
            frames.append(chunk_df)

            # index and columns data are stored separately
            # index is not saved in parquet files
            dfi.columns = pqt_columns

            self.save_df_to_parquet(chunk_name, dfi)
            start += self.CHUNK_SIZE

            if logger:
                logger.increment_progress()

        self._lookup_table = pd.concat(frames)
        self._index = df.index.copy()

    def _get_unique_pqt_id(self):
        pqt_ids = self._lookup_table.loc[:, PQT_ID]
        max_ = pqt_ids.max()
        return 0 if np.isnan(max_) else max_ + 1

    def _add_new_parquet(self, counted):
        return counted.empty or counted.min == self.CHUNK_SIZE

    def update_columns(self, existing, value, rows):
        pairs = self.get_chunk_id_pairs(existing)
        for pqt_name, pqt_ids in pairs.items():
            df = self.read_df_from_parquet(pqt_name)
            df.index = self._index
            for pqt_id in pqt_ids:
                df.loc[rows, pqt_id] = value
            self.save_df_to_parquet(pqt_name, df)

    def _insert_column(
        self, item: Union[Tuple[Any, ...], str, int], array: Sequence, pos: Optional[int] = None
    ) -> None:
        """ Insert new column into frame with lowest number of columns. """
        if isinstance(item, (str, int)):
            item = (item, *[""] * (len(self.columns.names) - 1))
        mi = pd.MultiIndex.from_tuples([item], names=self.columns.names)
        pqt_id = self._get_unique_pqt_id()
        counted = self._lookup_table["chunk"].value_counts()
        if self._add_new_parquet(counted):
            pqt_name = f"{str(uuid1())}.parquet"
            df = pd.DataFrame({pqt_id: array})
        else:
            pqt_name = counted.idxmin()
            df = self.read_df_from_parquet(pqt_name)
            df.index = self._index
            df[pqt_id] = array
        self.insert_to_lookup_table(pos, pqt_id, pqt_name, mi)
        self.save_df_to_parquet(pqt_name, df)

    def get_chunk_id_pairs(self, items: List[Tuple[Any, ...]]) -> Dict[str, List[int]]:
        """ Get a hash of chunk name: ids pairs for given ids. """
        pairs = {}
        df = self._lookup_table.loc[items, :]
        groups = df.groupby(["chunk"], sort=False)
        for chunk_name, chunk_df in groups:
            pairs[chunk_name] = chunk_df[PQT_ID].tolist()
        return pairs

    def insert(self, pos: int, item: Tuple[Any, ...], array: Sequence):
        """ Insert column at given position. """
        self._insert_column(item, array, pos=pos)

    def drop(
        self,
        columns: Union[List[Union[int, tuple]], Union[int, tuple]],
        level: str = None,
        **kwargs,
    ) -> None:
        """ Drop columns with given ids from frame. """
        columns = columns if isinstance(columns, list) else [columns]
        if level:
            arr = self._lookup_table.index.get_level_values(level).isin(columns)
            drop_index = self._lookup_table.loc[arr, PQT_ID].index
        else:
            drop_index = self._lookup_table.loc[columns, PQT_ID].index

        # update parquet files
        for pqt_name, ids in self.get_chunk_id_pairs(items=drop_index).items():
            df = self.read_df_from_parquet(pqt_name)
            df.drop(columns=ids, inplace=True, axis=1)
            if df.empty:
                Path(self.workdir, pqt_name).unlink()
            else:
                self.save_df_to_parquet(pqt_name, df)
        # update chunks reference
        self._lookup_table.drop(drop_index, axis=0, inplace=True)

    def find_missing_chunk_parquets(self) -> List[Path]:
        """ Check if parquets referenced in chunks table exist. """
        return [p for p in self.chunk_paths if not p.exists()]

    def clear_indexing_parquets(self):
        """ Delete previously stored columns, index and chunk parquets."""
        for path in self.indexing_paths:
            with contextlib.suppress(FileNotFoundError):
                path.unlink()

    def find_missing_indexing_parquets(self):
        """ Check if there are all requited index parquets. """
        return [p for p in self.indexing_paths if not p.exists()]

    def read_indexing_parquets(self):
        """ Load index, columns and chunk parquets from filesystem. """
        index = pq.read_pandas(self.index_parquet_path).to_pandas().iloc[:, 0]
        if index.name == TIMESTAMP_COLUMN:
            self._index = pd.DatetimeIndex(index, name=TIMESTAMP_COLUMN)
        else:
            self._index = pd.Index(index, name=index.name)
        chunks_table = pq.read_pandas(self.lookup_parquet_path).to_pandas()
        chunks_table.index = self.int_mi_level(chunks_table.index, ID_LEVEL)
        self._lookup_table = chunks_table

    def save_indexing_parquets(self):
        """ Save index, columns and chunks parquets to filesystem. """
        index_df = self._index.to_frame(index=False)
        chunks = self._lookup_table.copy()
        chunks.index = self.stringify_mi_level(chunks.index, ID_LEVEL)
        for df, path in zip([index_df, chunks], self.indexing_paths):
            self._write_table(df, path, preserve_index=True)

    @contextlib.contextmanager
    def temporary_indexing_parquets(self):
        """ Temporarily store index, columns and chunks parquets to filesystem. """
        self.save_indexing_parquets()
        try:
            yield None
        finally:
            self.clear_indexing_parquets()

    def save_frame_to_zip(self, zf: ZipFile, relative_to: Path) -> None:
        """ Write parquets tp given zip file. """
        with self.temporary_indexing_parquets():
            all_paths = self.chunk_paths + self.indexing_paths
            for path in all_paths:
                zf.write(path, arcname=path.relative_to(relative_to))


class ParquetTables(DFTables):
    def __init__(self):
        super().__init__()

    @classmethod
    def from_dftables(
        cls, dftables: DFTables, pardir: Path, logger: BaseLogger = None
    ) -> "ParquetTables":
        """ Create parquet data from DataFrame like class. """
        pqt = ParquetTables()
        for k, v in dftables.tables.items():
            pqt.tables[k] = ParquetFrame.from_df(v, k, pardir, logger=logger)
        return pqt

    @classmethod
    def from_fs(cls, pardir: Path):
        """ Create parquet data from filesystem directory. """
        pqt = ParquetTables()
        dirs = [p for p in Path(pardir).iterdir() if p.is_dir()]
        for p in dirs:
            table = str(p.name).split("-", maxsplit=1)[1]
            pqf = ParquetFrame.from_fs(p)
            pqt.tables[table] = pqf
        return pqt

    def copy_to(self, new_pardir: Path) -> "ParquetTables":
        new_tables = ParquetTables()
        for table, pqf in self.tables.items():
            new_tables[table] = pqf.copy_to(new_pardir)
        return new_tables
