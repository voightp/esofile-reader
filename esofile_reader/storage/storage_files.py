import contextlib
import json
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict
from typing import Union
from zipfile import ZipFile

import pandas as pd

from esofile_reader.base_file import BaseFile
from esofile_reader.data.df_data import DFData
from esofile_reader.data.pqt_data import ParquetData, ParquetFrame
from esofile_reader.data.sql_data import SQLData
from esofile_reader.totals_file import TotalsFile
from esofile_reader.utils.mini_classes import ResultsFile
from esofile_reader.utils.search_tree import Tree


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
    EXT = ".chf"

    def __init__(
            self,
            id_: int,
            file_path: str,
            file_name: str,
            data: Union[DFData, str, Path],
            file_created: datetime,
            totals,
            pardir="",
            search_tree: Tree = None,
            name: str = None
    ):
        super().__init__()
        self.id_ = id_
        self.file_path = file_path
        self.file_name = file_name
        self.file_created = file_created
        self.totals = totals
        self.path = Path(pardir, name) if name else Path(pardir, f"file-{id_}")
        self.path.mkdir(exist_ok=True)
        self.data = ParquetData.from_dfdata(data, self.path) \
            if isinstance(data, DFData) \
            else ParquetData.from_fs(data, self.path)

        if search_tree:
            self.search_tree = search_tree
        else:
            tree = Tree()
            tree.populate_tree(self.data.get_all_variables_dct())
            self.search_tree = tree

    def __del__(self):
        print("REMOVING PARQUET FILE " + str(self.path))
        shutil.rmtree(self.path, ignore_errors=True)

    @classmethod
    def load_file(cls, source_path: Union[str, Path], pardir: Union[str, Path]):
        source_path = source_path if isinstance(source_path, Path) else Path(source_path)

        # extract content in temp folder
        with ZipFile(source_path, "r") as zf:
            tempdir = Path(tempfile.mkdtemp())
            zf.extractall(tempdir)

        with open(Path(tempdir, "info.json"), "r") as f:
            info = json.load(f)

        # clean up temp files
        shutil.rmtree(tempdir, ignore_errors=True)

        pqf = ParquetFile(
            id_=info["id"],
            file_path=info["file_path"],
            file_name=info["file_name"],
            file_created=datetime.fromtimestamp(info["file_created"]),
            data=df_data,
            totals=info["totals"],
            name=info["name"],
            pardir=pardir,
            search_tree=tree
        )

        return pqf

    def save_meta(self):
        path = Path(self.path, f"info.json")
        with contextlib.suppress(FileNotFoundError):
            path.unlink()

        with open(str(path), "w") as f:
            json.dump({
                "id": self.id_,
                "name": self.path.name,
                "file_path": str(self.file_path),
                "file_name": self.file_name,
                "file_created": self.file_created.timestamp(),
                "totals": self.totals,
                "chunks": self.data.get_all_chunks()
            }, f, indent=4)

    def save_as(self, dir_, name):
        """ Save parquet storage into given location. . """
        # store json summary file
        self.save_meta()

        # store all the tempdir content
        zf = shutil.make_archive(str(Path(dir_, f"{name}")), "zip", self.path)

        # change zip to custom extension
        p = Path(zf)
        path = p.with_suffix(self.EXT)
        with contextlib.suppress(FileNotFoundError):
            os.remove(path)
        p.rename(path)
        self.path = path
