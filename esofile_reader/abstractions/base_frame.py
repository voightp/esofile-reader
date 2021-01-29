import shutil
from abc import abstractmethod
from pathlib import Path
from typing import Any, Tuple, Sequence, Union
from zipfile import ZipFile

import pandas as pd

from esofile_reader.id_generator import get_unique_name
from esofile_reader.processing.progress_logger import BaseLogger
from esofile_reader.typehints import PathLike


def get_unique_workdir(workdir: Path) -> Path:
    old_name = workdir.name
    pardir = workdir.parent
    all_names = [p.name for p in pardir.iterdir()]
    new_name = get_unique_name(old_name, all_names)
    return Path(pardir, new_name)


class BaseParquetFrame:
    INDEX_PARQUET = "index.parquet"
    PQT_REF_PARQUET = "reference.parquet"

    def __init__(self, workdir: Path):
        self.workdir = workdir.absolute()

    def __copy__(self):
        new_workdir = get_unique_workdir(self.workdir)
        return self._copy(new_workdir)

    @property
    def name(self):
        return self.workdir.name

    @property
    @abstractmethod
    def index(self) -> pd.Index:
        pass

    @property
    @abstractmethod
    def columns(self) -> pd.Index:
        pass

    @property
    @abstractmethod
    def loc(self) -> Any:
        pass

    @index.setter
    @abstractmethod
    def index(self, val: pd.Index) -> None:
        pass

    @columns.setter
    @abstractmethod
    def columns(self, val: pd.MultiIndex) -> None:
        pass

    @property
    @abstractmethod
    def empty(self):
        pass

    @abstractmethod
    def __getitem__(self, item):
        pass

    @abstractmethod
    def __setitem__(self, key, value):
        pass

    @abstractmethod
    def _copy(self, new_workdir: Path):
        pass

    @abstractmethod
    def _store_df(self, df: pd.DataFrame, logger: BaseLogger = None):
        pass

    @classmethod
    def from_frame(
        cls,
        df: Union[pd.DataFrame, "BaseParquetFrame"],
        name: str,
        pardir: PathLike = "",
        logger: BaseLogger = None,
    ) -> "BaseParquetFrame":
        """ Store pandas.DataFrame as a parquet frame. """
        workdir = Path(pardir, f"table-{name}").absolute()
        workdir.mkdir()
        frame = cls(workdir)
        if isinstance(df, BaseParquetFrame):
            df = df.as_df()
        frame._store_df(df, logger=logger)
        return frame

    @classmethod
    @abstractmethod
    def _read_from_fs(cls, pqf: "BaseParquetFrame") -> "BaseParquetFrame":
        pass

    @classmethod
    def from_fs(cls, workdir: Path) -> "BaseParquetFrame":
        """ Read already existing parquet frame from filesystem. """
        pqf = cls(workdir)
        try:
            cls._read_from_fs(pqf)
        except Exception as e:
            pqf.clean_up()
            raise e
        return pqf

    @abstractmethod
    def as_df(self) -> pd.DataFrame:
        """ Return parquet frame as a single DataFrame. """
        pass

    @abstractmethod
    def insert(self, pos: int, item: Tuple[Any, ...], array: Sequence):
        """ Insert column at given position. """
        pass

    @abstractmethod
    def drop(self, columns: Any, level: str = None, **kwargs) -> None:
        """ Drop given columns from frame. """
        pass

    @abstractmethod
    def save_frame_to_zip(
        self, zf: ZipFile, relative_to: Path, logger: BaseLogger = None
    ) -> None:
        """ Write parquets to given zip file. """
        pass

    def copy_to(self, new_pardir: Path):
        return self._copy(Path(new_pardir, self.name))

    def clean_up(self):
        shutil.rmtree(self.workdir, ignore_errors=True)
