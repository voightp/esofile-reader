import shutil
import tempfile
from datetime import datetime

from esofile_reader.base_file import BaseFile
from esofile_reader.data.pqt_data import ParquetData
from esofile_reader.data.sql_data import SQLData
from esofile_reader.data.df_data import DFData
from esofile_reader.totals_file import TotalsFile
from esofile_reader.utils.mini_classes import ResultsFile
from esofile_reader.utils.search_tree import Tree
from pathlib import Path
from typing import Union
import json


class SQLFile(BaseFile):
    """
    A class to represent database results set.

    Attributes need to be populated from one of file
    wrappers ('EsoFile', 'DiffFile', 'TotalsFile').


    Attributes
    ----------
    id_ : int
        Unique id identifier.
    file_path: str
        A file path of the reference file.
    file_name: str
        File name of the reference file.
    sql_data: SQLData
        A class to hold result tables.
    file_created: datetime
        A creation datetime of the reference file.
    search_tree: Tree
        Search tree instance.
    totals: bool
        A flag to check if the reference file was 'totals'.

    Notes
    -----
    Reference file must be complete!

    """

    def __init__(
            self,
            id_: int,
            file_path: str,
            file_name: str,
            sql_data: SQLData,
            file_created: datetime,
            search_tree: Tree,
            totals: bool
    ):
        super().__init__()
        self.id_ = id_
        self.file_path = file_path
        self.file_name = file_name
        self.data = sql_data
        self.file_created = file_created
        self.search_tree = search_tree
        self.totals = totals

    def rename(self, name: str) -> None:
        self.file_name = name
        self.data.update_file_name(name)


class DFFile(BaseFile):
    """
    A class to represent database results set.

    Attributes need to be populated from one of file
    wrappers ('EsoFile', 'DiffFile', 'TotalsFile').


    Attributes
    ----------
    id_ : int
        Unique id identifier.
    file : ResultsFile
        One of ('EsoFile', 'DiffFile', 'TotalsFile') results files..

    Notes
    -----
    Reference file must be complete!

    """

    def __init__(self, id_: int, file: ResultsFile):
        super().__init__()
        self.id_ = id_
        self.file_path = file.file_path
        self.file_name = file.file_name
        self.data = file.data
        self.file_created = file.file_created
        self.search_tree = file.search_tree
        self.totals = isinstance(file, TotalsFile)


class ParquetFile(BaseFile):
    def __init__(
            self,
            id_: int,
            file_path: str,
            file_name: str,
            data: DFData,
            file_created: datetime,
            search_tree,
            totals,
            pardir="",
            name=None
    ):
        super().__init__()
        self.id_ = id_
        self.file_path = file_path
        self.file_name = file_name
        self.file_created = file_created
        self.search_tree = search_tree
        self.totals = totals
        self.path = Path(pardir, name) if name else Path(pardir, f"file-{id_}")
        self.path.mkdir(exist_ok=True)
        self.data = ParquetData(data.tables, self.path)

    def __del__(self):
        print("REMOVING PARQUET FILE " + str(self.path))
        shutil.rmtree(self.path, ignore_errors=True)

    @classmethod
    def load_file(cls, path: Union[str, Path]):
        pass

    def as_dict(self):
        name = self.path.name
        return {
            "id_": self.id_,
            "name": name,
            "file_path": str(self.file_path),
            "file_name": self.file_name,
            "file_created": self.file_created.timestamp(),
            "totals": self.totals,
            "chunks": self.data.get_all_chunks()
        }

    def save_meta(self):
        tempson = str(Path(self.path, f"info.json"))
        with open(tempson, "w") as f:
            json.dump(self.as_dict(), f, indent=4)
