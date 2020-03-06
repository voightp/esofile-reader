import contextlib
import io
import json
import logging
import shutil
import tempfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Union
from zipfile import ZipFile

from esofile_reader.base_file import BaseFile
from esofile_reader.data.df_data import DFData
from esofile_reader.data.pqt_data import ParquetData
from esofile_reader.data.sql_data import SQLData
from esofile_reader.processor.monitor import DefaultMonitor
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
        Processed SQL data instance.
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
            totals: bool,
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
    """
    A class to represent database results set.

    Attributes need to be populated from one of file
    wrappers ('EsoFile', 'DiffFile', 'TotalsFile').

    Tables are stored in filesystem as pyarrow parquets.

    Attributes
    ----------
    id_ : int
        Unique id identifier.
    file_path: str
        A file path of the reference file.
    file_name: str
        File name of the reference file.
    data: {DFData, path like}
        Original tables.
    file_created: datetime
        A creation datetime of the reference file.
    search_tree: Tree
        Search tree instance.
    totals: bool
        A flag to check if the reference file was 'totals'.

    Notes
    -----
    Reference file must be complete!

    Workdir needs to be cleaned up. This can be done
    either by calling 'clean_up()' or working with file
    with context manager:

    with ParquetFile.from_results_file(*args, **kwargs) as pqs:
        ...

    """

    EXT = ".cff"

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
            name: str = None,
            monitor: DefaultMonitor = None,
    ):
        super().__init__()
        self.id_ = id_
        self.file_path = file_path
        self.file_name = file_name
        self.file_created = file_created
        self.totals = totals
        self.workdir = Path(pardir, name) if name else Path(pardir, f"file-{id_}")
        self.workdir.mkdir(exist_ok=True)
        self.data = (
            ParquetData.from_dfdata(data, self.workdir, monitor=monitor)
            if isinstance(data, DFData)
            else ParquetData.from_fs(data, self.workdir, monitor=monitor)
        )

        if search_tree:
            self.search_tree = search_tree
        else:
            tree = Tree()
            tree.populate_tree(self.data.get_all_variables_dct())
            self.search_tree = tree

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clean_up()

    @property
    def name(self):
        return self.workdir.name

    @classmethod
    def from_results_file(
            cls,
            id_: int,
            results_file: ResultsFile,
            pardir: str = "",
            name: str = None,
            monitor: DefaultMonitor = None
    ):
        pqs = ParquetFile(
            id_=id_,
            file_path=results_file.file_path,
            file_name=results_file.file_name,
            data=results_file.data,
            file_created=results_file.file_created,
            search_tree=results_file.search_tree,
            totals=isinstance(results_file, TotalsFile),
            pardir=pardir,
            name=name,
            monitor=monitor
        )

        return pqs

    @classmethod
    def load_file(
            cls, source: Union[str, Path, io.BytesIO], dest_dir: Union[str, Path] = ""
    ) -> "ParquetFile":
        """ Load parquet storage into given location. """
        source = source if isinstance(source, (Path, io.BytesIO)) else Path(source)

        if isinstance(source, io.BytesIO) or source.suffix == cls.EXT:
            # extract content in temp folder
            with ZipFile(source, "r") as zf:
                # extract info to find out dir name
                tempdir = tempfile.mkdtemp()
                tempson = zf.extract("info.json", path=tempdir)
                with open(Path(tempson), "r") as f:
                    info = json.load(f)
                shutil.rmtree(tempdir, ignore_errors=True)

                # extract all the content
                file_dir = Path(dest_dir, info["name"])
                file_dir.mkdir()
                zf.extractall(file_dir)

        elif source.is_dir():
            with open(Path(source, "info.json"), "r") as f:
                info = json.load(f)
                file_dir = source
        else:
            raise IOError(f"Invalid file type loaded. Only '{cls.EXT}' files are allowed")

        pqf = ParquetFile(
            id_=info["id"],
            file_path=info["file_path"],
            file_name=info["file_name"],
            file_created=datetime.fromtimestamp(info["file_created"]),
            data=file_dir,
            totals=info["totals"],
            name=info["name"],
            pardir=file_dir.parent,
        )

        return pqf

    def clean_up(self):
        shutil.rmtree(self.workdir, ignore_errors=True)

    def save_meta(self) -> Path:
        """ Save index parquets and json info. """
        # store column, index and chunk table parquets
        for tbl in self.data.tables.values():
            tbl.save_info_parquets()

        # store attributes as json
        info = Path(self.workdir, f"info.json")
        with contextlib.suppress(FileNotFoundError):
            info.unlink()

        with open(str(info), "w") as f:
            json.dump(
                {
                    "id": self.id_,
                    "name": self.name,
                    "file_path": str(self.file_path),
                    "file_name": self.file_name,
                    "file_created": self.file_created.timestamp(),
                    "totals": self.totals,
                },
                f,
                indent=4,
            )

        return info

    def save_as(
            self, dir_: Union[str, Path] = None, name: str = None
    ) -> Union[Path, io.BytesIO]:
        """ Save parquet storage into given location. """
        info = self.save_meta()

        # use memory buffer if name or dir is not specified
        if name is None or dir_ is None:
            logging.info("Name or dir not specified. Saving zip into IO Buffer.")
            device = io.BytesIO()
        else:
            device = Path(dir_, f"{name}{self.EXT}")

        # store all the tempdir content
        with ZipFile(device, "w") as zf:
            zf.write(info, arcname=info.name)
            for dir_ in [d for d in self.workdir.iterdir() if d.is_dir()]:
                for file in [f for f in dir_.iterdir() if f.suffix == ".parquet"]:
                    zf.write(file, arcname=file.relative_to(self.workdir))

        return device
