import contextlib
import shutil
from pathlib import Path
from typing import List, Dict, Tuple, Sequence, Union, Any, Optional
from uuid import uuid1
from zipfile import ZipFile

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from esofile_reader.df.df_tables import DFTables
from esofile_reader.df.level_names import TIMESTAMP_COLUMN, ID_LEVEL
from esofile_reader.exceptions import CorruptedData
from esofile_reader.id_generator import get_unique_name
from esofile_reader.processing.progress_logger import BaseLogger
from esofile_reader.typehints import PathLike

PARQUET_ID = "pqt_id"
PARQUET_NAME = "pqt_name"


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

    def _split_missing(self, items: Any) -> Tuple[pd.Index, pd.Index]:
        """ Identify missing index items. """
        items = [items] if isinstance(items, (tuple, str, int)) else items
        try:
            existing = self.frame._reference_df.loc[items, :].index
            missing = pd.Index([])
        except (KeyError, ValueError):
            index = pd.Index(items)
            missing = index.difference(self.frame.columns)
            existing = index.difference(missing)
        return existing, missing

    def __getitem__(self, item):
        if isinstance(item, tuple):
            rows, col = item
            df = self.frame._get_df(items=col)
        else:
            rows = item
            df = self.frame.as_df()
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
            self.frame._update_columns(existing, value, rows)


class ParquetFrame:
    MAX_SIZE = 1024
    MAX_N_COLUMNS = 100
    INDEX_PARQUET = "index.parquet"
    PQT_REF_PARQUET = "reference.parquet"

    def __init__(self, workdir: Path):
        self.workdir = workdir.absolute()
        self._indexer = _ParquetIndexer(self)
        self._index = pd.Index([])
        self._reference_df = pd.DataFrame(
            {PARQUET_ID: pd.Series([], dtype=int), PARQUET_NAME: pd.Series([], dtype=str),}
        )

    @property
    def name(self):
        return self.workdir.name

    @property
    def parquet_names(self) -> List[str]:
        names = []
        for chunk in self._reference_df[PARQUET_NAME].drop_duplicates().tolist():
            names.append(chunk)
        return names

    @property
    def parquet_paths(self) -> List[Path]:
        return [Path(self.workdir, chunk) for chunk in self.parquet_names]

    @property
    def index(self) -> pd.Index:
        return self._index

    @property
    def columns(self) -> pd.Index:
        return self._reference_df.index

    @property
    def loc(self) -> _ParquetIndexer:
        return self._indexer

    @property
    def index_parquet_path(self) -> Path:
        return Path(self.workdir, self.INDEX_PARQUET)

    @property
    def reference_parquet_path(self) -> Path:
        return Path(self.workdir, self.PQT_REF_PARQUET)

    @property
    def reference_paths(self) -> List[Path]:
        return [self.index_parquet_path, self.reference_parquet_path]

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
        self._reference_df.index = val

    @property
    def empty(self):
        return self._reference_df.empty

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
        parquet_frame._reference_df = self._reference_df.copy()
        parquet_frame._index = self._index.copy()
        return parquet_frame

    def copy_to(self, new_pardir: Path):
        return self._copy(Path(new_pardir, self.name))

    @classmethod
    def _get_columns_per_parquet(cls, df: pd.DataFrame) -> List[int]:
        """ Calculate number of columns per parquet for given DataFrame.  """
        sizes = df.memory_usage(index=False)
        max_size_in_bytes = cls.MAX_SIZE << 10
        n_columns = []
        column_counter = 0
        running_size = 0
        for size in sizes:
            running_size += size
            column_counter += 1
            if column_counter == cls.MAX_N_COLUMNS or running_size >= max_size_in_bytes:
                n_columns.append(column_counter)
                column_counter = 0
                running_size = 0
        if column_counter != 0:
            n_columns.append(column_counter)
        return n_columns

    @classmethod
    def predict_n_parquets(cls, df: pd.DataFrame) -> int:
        """ Predict number of parquets required to store DataFrame. """
        return len(cls._get_columns_per_parquet(df))

    @staticmethod
    def _create_unique_parquet_name():
        """ Create a unique filesystem name using uuid. """
        return f"{str(uuid1())}.parquet"

    def _append_reference(self, pqt_ids: List[int], pqt_name: str, mi: pd.MultiIndex):
        """ Append new items into reference DataFrame. """
        df = pd.DataFrame(
            {PARQUET_ID: pqt_ids, PARQUET_NAME: [pqt_name] * len(pqt_ids)}, index=mi
        )
        self._reference_df = self._reference_df.append(df)

    def _insert_reference(self, pos: int, pqt_ids: List[int], pqt_name: str, mi: pd.MultiIndex):
        """ Insert new items to reference DataFrame."""
        length = len(self._reference_df.index)
        if pos == length or pos is None:
            self._append_reference(pqt_ids, pqt_name, mi)
        elif 0 <= pos < length:
            frames = [
                self._reference_df.iloc[0:pos],
                pd.DataFrame(
                    {PARQUET_ID: pqt_ids, PARQUET_NAME: [pqt_name] * len(pqt_ids)}, index=mi
                ),
                self._reference_df.iloc[pos:],
            ]
            self._reference_df = pd.concat(frames)
        else:
            raise IndexError(
                f"Invalid column position '{pos}'! " f"Position must be between 0 and {length}."
            )

    @staticmethod
    def _write_table(df: pd.DataFrame, path: Path, preserve_index: bool = True) -> None:
        """ Write given parquet table into given path. """
        with open(path, "bw") as f:
            pq.write_table(pa.Table.from_pandas(df, preserve_index=preserve_index), f)

    def _save_df_to_parquet(self, name: str, df: pd.DataFrame) -> None:
        """ Replace previously stored parquet. """
        path = Path(self.workdir, name)
        with contextlib.suppress(FileNotFoundError):
            path.unlink()
        df.reset_index(drop=True, inplace=True)
        self._write_table(df, path, preserve_index=False)

    def _store_df(self, df: pd.DataFrame, logger: BaseLogger = None) -> None:
        """ Save DataFrame into multiple parquet files. """
        df = df.copy()  # avoid potential frame mutation
        self._index = df.index.copy()
        self._reference_df.index = pd.MultiIndex.from_tuples([], names=df.columns.names)
        n_columns = self._get_columns_per_parquet(df)
        start = 0
        for n in n_columns:
            end = start + n
            dfi = df.iloc[:, start:end]

            # use parquet id as the only identifier
            pqt_columns = pd.RangeIndex(start=start, stop=start + len(dfi.columns))
            pqt_name = self._create_unique_parquet_name()
            self._append_reference(pqt_columns.tolist(), pqt_name, dfi.columns)

            # index and columns data are stored separately
            # index is not saved in parquet files
            dfi.columns = pqt_columns

            self._save_df_to_parquet(pqt_name, dfi)
            start = end

            if logger:
                logger.increment_progress()
                logger.log_section(f"writing parquet {logger.progress}/{logger.max_progress}")

    @classmethod
    def from_df(
        cls, df: pd.DataFrame, name: str, pardir: PathLike = "", logger: BaseLogger = None,
    ) -> "ParquetFrame":
        """ Store pandas.DataFrame as a parquet frame. """
        workdir = Path(pardir, f"table-{name}").absolute()
        workdir.mkdir()
        pqf = ParquetFrame(workdir)
        pqf._store_df(df, logger=logger)
        return pqf

    def find_missing_ref_parquets(self) -> List[Path]:
        """ Check if parquets referenced in chunks table exist. """
        return [p for p in self.parquet_paths if not p.exists()]

    def clear_reference_parquets(self):
        """ Delete previously stored reference parquets."""
        for path in self.reference_paths:
            with contextlib.suppress(FileNotFoundError):
                path.unlink()

    def find_missing_reference_parquets(self):
        """ Check if there are all requited index parquets. """
        return [p for p in self.reference_paths if not p.exists()]

    def read_reference_parquets(self):
        """ Load reference parquets from filesystem. """
        index = pq.read_pandas(self.index_parquet_path).to_pandas().iloc[:, 0]
        if index.name == TIMESTAMP_COLUMN:
            self._index = pd.DatetimeIndex(index, name=TIMESTAMP_COLUMN)
        else:
            self._index = pd.Index(index, name=index.name)
        ref_df = pq.read_pandas(self.reference_parquet_path).to_pandas()
        ref_df.index = self.cast_mi_level_items_to_int(ref_df.index, ID_LEVEL)
        self._reference_df = ref_df

    @classmethod
    def _read_from_fs(cls, pqf: "ParquetFrame") -> "ParquetFrame":
        missing = pqf.find_missing_reference_parquets()
        if missing:
            raise CorruptedData(
                f"Cannot find info tables: {missing}. File {pqf.workdir} cannot be loaded!"
            )
        pqf.read_reference_parquets()
        missing = pqf.find_missing_ref_parquets()
        if missing:
            raise CorruptedData(
                f"Cannot find info tables: {missing}. File {pqf.workdir} cannot be loaded!"
            )
        pqf.clear_reference_parquets()
        return pqf

    @classmethod
    def from_fs(cls, workdir: Path) -> "ParquetFrame":
        """ Read already existing parquet frame from filesystem. """
        pqf = ParquetFrame(workdir)
        try:
            cls._read_from_fs(pqf)
        except Exception as e:
            pqf.clean_up()
            raise e
        return pqf

    @staticmethod
    def cast_mi_level_to_str(mi: pd.MultiIndex, level: str) -> pd.MultiIndex:
        """ Convert MultiIndex level to str type. """
        mi_df = mi.to_frame(index=False)
        mi_df[level] = mi_df[level].astype(str)
        return pd.MultiIndex.from_frame(mi_df, names=mi.names)

    @staticmethod
    def cast_mi_level_items_to_int(mi: pd.MultiIndex, level: str) -> pd.MultiIndex:
        """ Convert MultiIndex level to int type. """

        def to_int(val):
            try:
                return int(val)
            except ValueError:
                return val

        mi_df = mi.to_frame(index=False)
        mi_df[level] = mi_df[level].apply(to_int)
        return pd.MultiIndex.from_frame(mi_df, names=mi.names)

    def clean_up(self):
        shutil.rmtree(self.workdir, ignore_errors=True)

    def _read_df_from_parquet(self, pqt_name: str, columns: List[int] = None) -> pd.DataFrame:
        """ Read DataFrame from given parquet. """
        columns = list(map(str, columns)) if columns else None
        table = pq.read_pandas(Path(self.workdir, pqt_name), columns=columns, memory_map=True)
        df = table.to_pandas()
        df.columns = df.columns.astype(np.int32)
        return df

    def _get_columns(self, pqt_ids: List[int]) -> pd.MultiIndex:
        """ Reconstruct columns multiindex. """

        def sorter(sr: pd.Series) -> pd.Series:
            return sr.apply(lambda x: pqt_ids.index(x))

        df = self._reference_df.loc[self._reference_df[PARQUET_ID].isin(pqt_ids), :]
        df = df.sort_values(by=PARQUET_ID, key=sorter)
        return df.index

    def _assign_indexes(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Assign original index and columns indexes. """
        df.index = self.index.copy()
        df.columns = self._get_columns(df.columns.tolist())
        return df

    def _get_pqt_ref_pairs(self, items: pd.Index) -> Dict[str, List[int]]:
        """ Get a hash of parquet name: ids pairs for given items. """
        pairs = {}
        df = self._reference_df.loc[items, :]
        groups = df.groupby([PARQUET_NAME], sort=False)
        for chunk_name, chunk_df in groups:
            pairs[chunk_name] = chunk_df[PARQUET_ID].tolist()
        return pairs

    def _build_df(self, frames: List[pd.DataFrame]) -> pd.DataFrame:
        """ Join frames extracted from parquets and assign indexes.. """
        try:
            df = pd.concat(frames, axis=1, sort=False)
        except ValueError:
            df = pd.DataFrame([], index=self.index)
        df = self._assign_indexes(df)
        return df

    def _get_df(self, items: Any) -> pd.DataFrame:
        """ Get a single DataFrame from multiple parquets. """
        items = [items] if isinstance(items, (tuple, str, int)) else items
        items = self._reference_df.loc[items, :].index
        pairs = self._get_pqt_ref_pairs(items)
        frames = []
        for pqt_name, pqt_ids in pairs.items():
            frames.append(self._read_df_from_parquet(pqt_name, columns=pqt_ids))
        df = self._build_df(frames)
        return df.loc[:, items]

    def as_df(self) -> pd.DataFrame:
        """ Return parquet frame as a single DataFrame. """
        frames = []
        for pqt_name in self.parquet_names:
            frames.append(self._read_df_from_parquet(pqt_name))
        df = self._build_df(frames)
        return df.loc[:, self.columns]

    def _get_unique_pqt_id(self):
        """ Create unique parquet id. """
        pqt_ids = self._reference_df.loc[:, PARQUET_ID]
        max_ = pqt_ids.max()
        return 0 if np.isnan(max_) else max_ + 1

    def _find_smallest_parquet(self) -> str:
        """ Find parquet with smallest size. """
        sizes = pd.Series({p.name: p.stat().st_size for p in self.parquet_paths})
        return sizes.idxmin()

    def _is_new_parquet_required(self):
        """ Check if all parquets are filled to max size. """
        if self._reference_df.empty:
            required = True
        else:
            sizes = [p.stat().st_size for p in self.parquet_paths]
            required = all(map(lambda x: x > self.MAX_SIZE << 10, sizes))
        return required

    def _insert_column(
        self, item: Union[Tuple[Any, ...], str, int], array: Sequence, pos: Optional[int] = None
    ) -> None:
        """ Insert new column into parquet frame. """
        if isinstance(item, (str, int)):
            item = (item, *[""] * (len(self.columns.names) - 1))
        mi = pd.MultiIndex.from_tuples([item], names=self.columns.names)
        pqt_id = self._get_unique_pqt_id()
        if self._is_new_parquet_required():
            pqt_name = self._create_unique_parquet_name()
            df = pd.DataFrame({pqt_id: array})
        else:
            pqt_name = self._find_smallest_parquet()
            df = self._read_df_from_parquet(pqt_name)
            df.index = self._index
            df[pqt_id] = array
        self._insert_reference(pos, [pqt_id], pqt_name, mi)
        self._save_df_to_parquet(pqt_name, df)

    def _update_columns(self, existing: pd.Index, array: Sequence, rows):
        """ Overwrite given columns with new values. """
        pairs = self._get_pqt_ref_pairs(existing)
        for pqt_name, pqt_ids in pairs.items():
            df = self._read_df_from_parquet(pqt_name)
            df.index = self._index
            for pqt_id in pqt_ids:
                df.loc[rows, pqt_id] = array
            self._save_df_to_parquet(pqt_name, df)

    def insert(self, pos: int, item: Tuple[Any, ...], array: Sequence):
        """ Insert column at given position. """
        self._insert_column(item, array, pos=pos)

    def drop(self, columns: Any, level: str = None, **kwargs) -> None:
        """ Drop given columns from frame. """
        columns = columns if isinstance(columns, list) else [columns]
        if level:
            arr = self._reference_df.index.get_level_values(level).isin(columns)
            drop_index = self._reference_df.loc[arr, PARQUET_ID].index
        else:
            drop_index = self._reference_df.loc[columns, PARQUET_ID].index
        # update parquet files
        for pqt_name, ids in self._get_pqt_ref_pairs(items=drop_index).items():
            df = self._read_df_from_parquet(pqt_name)
            df.drop(columns=ids, inplace=True, axis=1)
            if df.empty:
                Path(self.workdir, pqt_name).unlink()
            else:
                self._save_df_to_parquet(pqt_name, df)
        # update reference
        self._reference_df.drop(drop_index, axis=0, inplace=True)

    def save_reference_parquets(self):
        """ Save reference parquets to filesystem. """
        index_df = self._index.to_frame(index=False)
        ref_df = self._reference_df.copy()
        ref_df.index = self.cast_mi_level_to_str(ref_df.index, ID_LEVEL)
        for df, path in zip([index_df, ref_df], self.reference_paths):
            self._write_table(df, path, preserve_index=True)

    @contextlib.contextmanager
    def temporary_reference_parquets(self):
        """ Temporarily store reference parquets to filesystem. """
        self.save_reference_parquets()
        try:
            yield None
        finally:
            self.clear_reference_parquets()

    def save_frame_to_zip(self, zf: ZipFile, relative_to: Path) -> None:
        """ Write parquets to given zip file. """
        with self.temporary_reference_parquets():
            all_paths = self.parquet_paths + self.reference_paths
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
