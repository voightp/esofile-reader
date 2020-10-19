import contextlib
import io
import json
import shutil
import tempfile
from copy import copy
from datetime import datetime
from pathlib import Path
from typing import Union, Tuple, Dict, Any
from zipfile import ZipFile

from esofile_reader.base_file import BaseFile
from esofile_reader.id_generator import incremental_id_gen, get_str_identifier
from esofile_reader.mini_classes import ResultsFileType, PathLike
from esofile_reader.processing.progress_logger import GenericProgressLogger
from esofile_reader.search_tree import Tree
from esofile_reader.storages.df_storage import DFStorage
from esofile_reader.tables.pqt_tables import ParquetFrame, ParquetTables, get_unique_workdir


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
    INFO_JSON = "info.json"

    def __init__(
        self,
        id_: int,
        file_path: str,
        file_name: str,
        tables: ParquetTables,
        file_created: datetime,
        file_type: str,
        workdir: Path,
        search_tree: Tree,
    ):
        self.id_ = id_
        self.workdir = workdir
        self.tables = tables
        super().__init__(file_path, file_name, file_created, tables, search_tree, file_type)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clean_up()

    def __copy__(self):
        new_workdir = get_unique_workdir(self.workdir)
        self._copy(new_workdir)

    def _copy(self, new_workdir: Path, new_id: int = None) -> "ParquetFile":
        new_tables = self.tables.copy_to(new_workdir)
        new_file = ParquetFile(
            id_=new_id if new_id else self.id_,
            file_path=self.file_path,
            file_name=self.file_name,
            tables=new_tables,
            file_created=self.file_created,
            file_type=self.file_type,
            workdir=new_workdir,
            search_tree=copy(self.search_tree),
        )
        return new_file

    def copy_to(self, new_pardir: Path, new_id: int = None):
        """ Copy all data to another directory. """
        new_name = f"file-{new_id}" if new_id else self.name
        new_workdir = Path(new_pardir, new_name)
        new_workdir.mkdir()
        return self._copy(new_workdir, new_id=new_id)

    @property
    def name(self) -> str:
        return self.workdir.name

    @classmethod
    def from_results_file(
        cls,
        id_: int,
        results_file: ResultsFileType,
        pardir: str = "",
        progress_logger: GenericProgressLogger = None,
    ) -> "ParquetFile":
        workdir = Path(pardir, f"file-{id_}")
        workdir.mkdir()
        tables = ParquetTables.from_dftables(results_file.tables, workdir, progress_logger)
        pqf = ParquetFile(
            id_=id_,
            file_path=results_file.file_path,
            file_name=results_file.file_name,
            tables=tables,
            file_created=results_file.file_created,
            search_tree=results_file.search_tree,
            file_type=results_file.file_type,
            workdir=workdir,
        )
        return pqf

    @classmethod
    def unzip_source_file(
        cls, source: Union[Path, io.BytesIO], dest_dir: PathLike
    ) -> Tuple[Path, Dict[str, Any]]:
        # extract content in temp folder
        with ZipFile(source, "r") as zf:
            # extract info to find out dir name
            tempdir = tempfile.mkdtemp()
            tempson = zf.extract(cls.INFO_JSON, path=tempdir)
            with open(Path(tempson), "r") as f:
                info = json.load(f)
            shutil.rmtree(tempdir, ignore_errors=True)

            # extract all the content
            file_dir = Path(dest_dir, f"{info['name']}")
            file_dir.mkdir()
            zf.extractall(file_dir)
        return file_dir, info

    @classmethod
    def from_file_system(cls, source: PathLike, dest_dir: PathLike = "") -> "ParquetFile":
        """ Load parquet file into given location. """
        source = Path(source)
        if source.suffix == cls.EXT:
            workdir, info = cls.unzip_source_file(source, dest_dir)
        elif source.is_dir():
            with open(Path(source, cls.INFO_JSON), "r") as f:
                info = json.load(f)
                workdir = source
        else:
            raise IOError(f"Invalid file type_ loaded. Only '{cls.EXT}' files are allowed")

        tables = ParquetTables.from_fs(workdir)
        pqf = ParquetFile(
            id_=info["id"],
            file_path=Path(info["file_path"]),
            file_name=info["file_name"],
            file_created=datetime.fromtimestamp(info["file_created"]),
            tables=tables,
            file_type=info["file_type"],
            workdir=workdir,
            search_tree=tables.get_all_variables_dct(),
        )
        return pqf

    @classmethod
    def from_buffer(cls, source: io.BytesIO, dest_dir: PathLike) -> "ParquetFile":
        """ Load parquet file from buffer into given location. """
        workdir, info = cls.unzip_source_file(source, dest_dir)
        tables = ParquetTables.from_fs(workdir)
        pqf = ParquetFile(
            id_=info["id"],
            file_path=Path(info["file_path"]),
            file_name=info["file_name"],
            file_created=datetime.fromtimestamp(info["file_created"]),
            tables=tables,
            file_type=info["file_type"],
            workdir=workdir,
            search_tree=tables.get_all_variables_dct(),
        )
        return pqf

    def clean_up(self):
        shutil.rmtree(self.workdir, ignore_errors=True)

    def save_meta(self) -> Path:
        """ Save index parquets and json info. """
        # store column, index and chunk table parquets
        for tbl in self.tables.values():
            tbl.save_index_parquets()

        # store attributes as json
        file_info = Path(self.workdir, self.INFO_JSON)
        with contextlib.suppress(FileNotFoundError):
            file_info.unlink()

        with open(str(file_info), "w") as f:
            json.dump(
                {
                    "id": self.id_,
                    "file_path": str(self.file_path),
                    "file_name": self.file_name,
                    "file_created": self.file_created.timestamp(),
                    "file_type": self.file_type,
                    "name": self.name,
                },
                f,
                indent=4,
            )

        return file_info

    def write_zip(self, device: Union[Path, io.BytesIO]):
        """ Write content into given device. """
        file_info = self.save_meta()
        with ZipFile(device, "w") as zf:
            zf.write(file_info, arcname=file_info.name)
            for dir_ in [d for d in self.workdir.iterdir() if d.is_dir()]:
                for file in [f for f in dir_.iterdir() if f.suffix == ".parquet"]:
                    zf.write(file, arcname=file.relative_to(self.workdir))

    def save_as(self, dir_: PathLike, name: str) -> Path:
        """ Save parquet storage into given location. """
        device = Path(dir_, f"{name}{self.EXT}")
        self.write_zip(device)
        return device

    def save_into_buffer(self):
        """ Save parquet storage into buffer. """
        device = io.BytesIO()
        self.write_zip(device)
        return device


class ParquetStorage(DFStorage):
    EXT = ".cfs"

    def __init__(self):
        super().__init__()
        self.files = {}
        self.path = None
        self.workdir = Path(tempfile.mkdtemp(prefix="storage-"))

    def __del__(self):
        shutil.rmtree(self.workdir, ignore_errors=True)

    @classmethod
    def load_storage(cls, path: PathLike):
        """ Load ParquetStorage from filesystem. """
        path = path if isinstance(path, Path) else Path(path)
        if path.suffix != cls.EXT:
            raise IOError(f"Invalid file type loaded. Only '{cls.EXT}' files are allowed")

        pqs = ParquetStorage()
        pqs.path = path
        with ZipFile(path, "r") as zf:
            zf.extractall(pqs.workdir)

        for dir_ in [d for d in pqs.workdir.iterdir() if d.is_dir()]:
            pqf = ParquetFile.from_file_system(dir_)
            pqs.files[pqf.id_] = pqf

        return pqs

    def store_file(
        self, results_file: ResultsFileType, progress_logger: GenericProgressLogger = None
    ) -> int:
        """ Store results file as 'ParquetFile'. """
        if not progress_logger:
            progress_logger = GenericProgressLogger(results_file.file_path)
        with progress_logger.log_task("Store file!"):
            n = sum([ParquetFrame.get_n_chunks(df) for df in results_file.tables.values()])
            progress_logger.log_section("writing parquets!")
            progress_logger.set_new_maximum_progress(n)

            id_gen = incremental_id_gen(checklist=list(self.files.keys()))
            id_ = next(id_gen)
            file = ParquetFile.from_results_file(
                id_=id_,
                results_file=results_file,
                pardir=self.workdir,
                progress_logger=progress_logger,
            )
            self.files[id_] = file
        return id_

    def delete_file(self, id_: int) -> None:
        """ Delete file with given id. """
        shutil.rmtree(self.files[id_].workdir, ignore_errors=True)
        del self.files[id_]

    def save_as(self, dir_: Path, name: str) -> Path:
        """ Save parquet storage into given location. """
        path = Path(dir_, f"{name}{self.EXT}")
        self.path = path
        with ZipFile(self.path, "w") as zf:
            for f in self.files.values():
                info = f.save_meta()
                zf.write(info, arcname=info.relative_to(self.workdir))
            for file_dir in [d for d in self.workdir.iterdir() if d.is_dir()]:
                for pqt_dir in [d for d in file_dir.iterdir() if d.is_dir()]:
                    for file in [f for f in pqt_dir.iterdir() if f.suffix == ".parquet"]:
                        zf.write(file, arcname=file.relative_to(self.workdir))
        return path

    def save(self) -> None:
        """ Save parquet storage. """
        if not self.path:
            raise FileNotFoundError("Path not defined! Call 'save_as' first.")
        dir_ = self.path.parent
        name = self.path.with_suffix("").name
        self.save_as(dir_, name)

    def merge_with(self, storage_path: PathLike) -> None:
        """ Merge this storage with arbitrary number of other ones. """
        id_gen = incremental_id_gen(start=0, checklist=list(self.files.keys()))
        pqs = ParquetStorage.load_storage(storage_path)
        for id_, file in pqs.files.items():
            # create new identifiers in case that id already exists
            new_id = next(id_gen) if id_ in self.files.keys() else id_
            new_name = get_str_identifier(file.file_name, self.get_all_file_names())
            file.rename(new_name)
            new_file = file.copy_to(self.workdir, new_id=new_id)
            self.files[new_id] = new_file
        del pqs
