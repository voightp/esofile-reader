import contextlib
import io
import json
import logging
import math
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Union
from zipfile import ZipFile

from esofile_reader.base_file import BaseFile
from esofile_reader.id_generator import incremental_id_gen
from esofile_reader.mini_classes import ResultsFileType
from esofile_reader.processing.progress_logger import GenericProgressLogger
from esofile_reader.search_tree import Tree
from esofile_reader.storages.df_storage import DFStorage
from esofile_reader.tables.df_tables import DFTables
from esofile_reader.tables.pqt_tables import ParquetFrame, ParquetTables


class ParquetFile(BaseFile):
    """
    A class to represent database results set.

    Attributes need to be populated from processed 'ResultsFileType'.

    Tables are stored in filesystem as pyarrow parquets.

    Attributes
    ----------
    id_ : int
        Unique id identifier.
    file_path: str
        A file path of the reference file.
    file_name: str
        File name of the reference file.
    tables: {DFTables, path like}
        Original tables.
    file_created: datetime
        A creation datetime of the reference file.
    search_tree: Tree
        Search tree instance.
    file_type: str
        The original results file type.

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
        tables: Union[DFTables, str, Path],
        file_created: datetime,
        file_type: str,
        pardir: str = "",
        search_tree: Tree = None,
        name: str = None,
        progress_logger: GenericProgressLogger = None,
    ):
        self.id_ = id_
        self.workdir = Path(pardir, name) if name else Path(pardir, f"file-{id_}")
        self.workdir.mkdir(exist_ok=True)
        tables = (
            ParquetTables.from_dftables(tables, self.workdir, progress_logger=progress_logger)
            if isinstance(tables, DFTables)
            else ParquetTables.from_fs(tables, self.workdir, progress_logger=progress_logger)
        )

        if search_tree is None:
            search_tree = Tree.from_header_dict(tables.get_all_variables_dct())

        super().__init__(file_path, file_name, file_created, tables, search_tree, file_type)

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
        results_file: ResultsFileType,
        pardir: str = "",
        name: str = None,
        progress_logger: GenericProgressLogger = None,
    ):
        pqs = ParquetFile(
            id_=id_,
            file_path=results_file.file_path,
            file_name=results_file.file_name,
            tables=results_file.tables,
            file_created=results_file.file_created,
            search_tree=results_file.search_tree,
            file_type=results_file.file_type,
            pardir=pardir,
            name=name,
            progress_logger=progress_logger,
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
            raise IOError(f"Invalid file type_ loaded. Only '{cls.EXT}' files are allowed")

        pqf = ParquetFile(
            id_=info["id"],
            file_path=info["file_path"],
            file_name=info["file_name"],
            file_created=datetime.fromtimestamp(info["file_created"]),
            tables=file_dir,
            file_type=info["file_type"],
            name=info["name"],
            pardir=file_dir.parent,
        )

        return pqf

    def clean_up(self):
        shutil.rmtree(self.workdir, ignore_errors=True)

    def save_meta(self) -> Path:
        """ Save index parquets and json info. """
        # store column, index and chunk table parquets
        for tbl in self.tables.values():
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
                    "file_type": self.file_type,
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


class ParquetStorage(DFStorage):
    EXT = ".cfs"

    def __init__(self, path: Union[str, Path] = None):
        super().__init__()
        self.files = {}
        self.path = Path(path) if path else path
        self.workdir = Path(tempfile.mkdtemp(prefix="chartify-"))

    def __del__(self):
        shutil.rmtree(self.workdir, ignore_errors=True)

    @classmethod
    def load_storage(cls, path: Union[str, Path]):
        """ Load ParquetStorage from filesystem. """
        path = path if isinstance(path, Path) else Path(path)
        if path.suffix != cls.EXT:
            raise IOError(f"Invalid file type loaded. Only '{cls.EXT}' files are allowed")

        pqs = ParquetStorage(path)
        with ZipFile(path, "r") as zf:
            zf.extractall(pqs.workdir)

        for dir_ in [d for d in pqs.workdir.iterdir() if d.is_dir()]:
            pqf = ParquetFile.load_file(dir_)
            pqs.files[pqf.id_] = pqf

        return pqs

    def store_file(
        self, results_file: ResultsFileType, progress_logger: GenericProgressLogger = None
    ) -> int:
        """ Store results file as 'ParquetFile'. """
        if not progress_logger:
            progress_logger = GenericProgressLogger(results_file.file_path)
        with progress_logger.log_task("Store file!"):
            # number of steps is equal to number of parquet files
            n_steps = 0
            for tbl in results_file.tables.values():
                n = int(math.ceil(tbl.shape[1] / ParquetFrame.CHUNK_SIZE))
                n_steps += n

            progress_logger.log_section("writing parquets!")
            progress_logger.set_new_maximum_progress(n_steps)

            id_gen = incremental_id_gen(checklist=list(self.files.keys()))
            id_ = next(id_gen)
            file = ParquetFile.from_results_file(
                id_=id_,
                results_file=results_file,
                pardir=self.workdir,
                name="",
                progress_logger=progress_logger,
            )
            self.files[id_] = file
        return id_

    def delete_file(self, id_: int) -> None:
        """ Delete file with given id. """
        shutil.rmtree(self.files[id_].workdir, ignore_errors=True)
        del self.files[id_]

    def get_all_parquets(self):
        """ Get all paths"""

    def save_as(self, dir_: Path, name: str):
        """ Save parquet storage into given location. """
        self.path = Path(dir_, f"{name}{self.EXT}")

        with ZipFile(self.path, "w") as zf:
            for f in self.files.values():
                info = f.save_meta()
                zf.write(info, arcname=info.relative_to(self.workdir))

            for file_dir in [d for d in self.workdir.iterdir() if d.is_dir()]:
                for pqt_dir in [d for d in file_dir.iterdir() if d.is_dir()]:
                    for file in [f for f in pqt_dir.iterdir() if f.suffix == ".parquet"]:
                        zf.write(file, arcname=file.relative_to(self.workdir))

    def save(self):
        """ Save parquet storage. """
        if not self.path:
            raise FileNotFoundError("Path not defined! Call 'save_as' first.")
        dir_ = self.path.parent
        name = self.path.with_suffix("").name
        self.save_as(dir_, name)

    def update_merged_file_attributes(self, file: ParquetFile, new_id: int):
        """ Update merged storage attributes to avoid id conflict. """
        new_name = f"file-{new_id}"
        new_workdir = Path(self.workdir, new_name)

        # clone all the parquet data
        shutil.copytree(file.workdir, new_workdir)

        # update parquet frame root
        for table in file.tables.values():
            table_name = table.name
            table.workdir = Path(new_workdir, table_name)

        # assign updated attributes
        file.workdir = new_workdir
        file.id_ = new_id

    def merge_with(self, storage_path: Union[str, Path]) -> None:
        """ Merge this storage with arbitrary number of other ones. """
        id_gen = incremental_id_gen(start=0, checklist=list(self.files.keys()))
        pqs = ParquetStorage.load_storage(storage_path)
        for id_, file in pqs.files.items():
            # create new identifiers in case that id already exists
            new_id = next(id_gen) if id_ in self.files.keys() else id_
            self.update_merged_file_attributes(file, new_id)
            self.files[new_id] = file
        del pqs
