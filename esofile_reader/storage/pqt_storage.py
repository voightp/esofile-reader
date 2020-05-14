import contextlib
import io
import json
import logging
import math
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Union, List
from zipfile import ZipFile

from esofile_reader.base_file import BaseFile
from esofile_reader.data.df_data import DFData
from esofile_reader.data.pqt_data import ParquetFrame, ParquetData
from esofile_reader.id_generators import incremental_id_gen
from esofile_reader.mini_classes import ResultsFile
from esofile_reader.processor.monitor import DefaultMonitor
from esofile_reader.search_tree import Tree
from esofile_reader.storage.df_storage import DFStorage


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
    type_: str
        The original results file class.

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
            type_: str,
            pardir: str = "",
            search_tree: Tree = None,
            name: str = None,
            monitor: DefaultMonitor = None,
    ):
        super().__init__()
        self.id_ = id_
        self.file_path = file_path
        self.file_name = file_name
        self.file_created = file_created
        self.type_ = type_
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
            monitor: DefaultMonitor = None,
    ):
        pqs = ParquetFile(
            id_=id_,
            file_path=results_file.file_path,
            file_name=results_file.file_name,
            data=results_file.data,
            file_created=results_file.file_created,
            search_tree=results_file.search_tree,
            type_=results_file.__class__.__name__,
            pardir=pardir,
            name=name,
            monitor=monitor,
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
            data=file_dir,
            type_=info["type"],
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
                    "type": self.type_,
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

    def store_file(self, results_file: ResultsFile, monitor: DefaultMonitor = None) -> int:
        """ Store results file as 'ParquetFile'. """
        if monitor:
            # number of steps is equal to number of parquet files
            n_steps = 0
            for tbl in results_file.data.tables.values():
                n = int(math.ceil(tbl.shape[1] / ParquetFrame.CHUNK_SIZE))
                n_steps += n

            monitor.reset_progress(new_max=n_steps)
            monitor.storing_started()

        id_gen = incremental_id_gen(checklist=list(self.files.keys()))
        id_ = next(id_gen)
        file = ParquetFile.from_results_file(
            id_=id_, results_file=results_file, pardir=self.workdir, name="", monitor=monitor
        )
        self.files[id_] = file

        if monitor:
            monitor.storing_finished()

        return id_

    def delete_file(self, id_: int) -> None:
        """ Delete file with given id. """
        shutil.rmtree(self.files[id_].workdir, ignore_errors=True)
        del self.files[id_]

    def save_as(self, dir_, name):
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

    def merge_with(self, storage_path: Union[str, List[str]]) -> None:
        """ Merge this storage with arbitrary number of other ones. """
        paths = storage_path if isinstance(storage_path, list) else [storage_path]
        id_gen = incremental_id_gen(start=0, checklist=list(self.files.keys()))
        for path in paths:
            pqs = ParquetStorage.load_storage(path)
            for id_, file in pqs.files.items():
                # create new identifiers in case that id already exists
                new_id = next(id_gen) if id_ in self.files.keys() else id_
                new_name = f"file-{new_id}"
                new_workdir = Path(self.workdir, new_name)

                # clone all the parquet data
                shutil.copytree(file.workdir, new_workdir)

                # update parquet frame root
                for table in file.data.tables.values():
                    table_name = table.name
                    table.workdir = Path(new_workdir, table_name)

                # assign updated attributes
                file.workdir = new_workdir
                file.id_ = new_id

                self.files[new_id] = file

            del pqs
