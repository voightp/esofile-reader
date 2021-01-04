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

from esofile_reader.abstractions.base_file import BaseFile
from esofile_reader.pqt.parquet_tables import ParquetFrame, ParquetTables, get_unique_workdir
from esofile_reader.processing.progress_logger import BaseLogger
from esofile_reader.search_tree import Tree
from esofile_reader.typehints import ResultsFileType, PathLike


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
        self.workdir = workdir.absolute()
        self.tables = tables
        super().__init__(file_path, file_name, file_created, tables, search_tree, file_type)

    def __copy__(self):
        new_workdir = get_unique_workdir(self.workdir)
        return self._copy(new_workdir)

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
    def predict_number_of_parquets(cls, results_file: ResultsFileType) -> int:
        """ Calculate future number of parquets for given Results file. """
        n = 0
        for df in results_file.tables.values():
            n += ParquetFrame.predict_n_parquets(df)
        return n

    @classmethod
    def from_results_file(
        cls,
        id_: int,
        results_file: ResultsFileType,
        pardir: PathLike = "",
        logger: BaseLogger = None,
    ) -> "ParquetFile":
        workdir = Path(pardir, f"file-{id_}")
        workdir.mkdir()
        tables = ParquetTables.from_dftables(results_file.tables, workdir, logger)
        pqf = ParquetFile(
            id_=id_,
            file_path=results_file.file_path,
            file_name=results_file.file_name,
            tables=tables,
            file_created=results_file.file_created,
            search_tree=copy(results_file.search_tree),
            file_type=results_file.file_type,
            workdir=workdir,
        )
        return pqf

    @classmethod
    def _read_json_from_zip(cls, zf: ZipFile) -> Dict[str, Any]:
        """ Get content of info json from given zip. """
        with tempfile.TemporaryDirectory() as tempdir:
            tempson = zf.extract(cls.INFO_JSON, path=tempdir)
            with open(Path(tempson), "r") as f:
                content = json.load(f)
        return content

    @classmethod
    def _unzip_source_file(
        cls, source: Union[Path, io.BytesIO], dest_dir: PathLike
    ) -> Tuple[Path, Dict[str, Any]]:
        """ Extract content of given zip into destination. """
        with ZipFile(source, "r") as zf:
            # extract info to find out dir name
            info = cls._read_json_from_zip(zf)
            file_dir = Path(dest_dir, f"{info['name']}")
            file_dir.mkdir()
            zf.extractall(file_dir)
        return file_dir, info

    @classmethod
    def from_file_system(
        cls, source: PathLike, dest_dir: PathLike = "", logger: BaseLogger = None
    ) -> "ParquetFile":
        """ Create parquet file instance from filesystem files. """
        source = Path(source)
        if source.suffix == cls.EXT:
            workdir, info = cls._unzip_source_file(source, dest_dir)
        elif source.is_dir():
            with open(Path(source, cls.INFO_JSON), "r") as f:
                info = json.load(f)
                workdir = source
        else:
            raise IOError(f"Invalid file type. Only '{cls.EXT}' files are allowed")

        tables = ParquetTables.from_fs(workdir)
        tree = Tree.from_header_dict(tables.get_all_variables_dct())
        pqf = ParquetFile(
            id_=info["id"],
            file_path=Path(info["file_path"]),
            file_name=info["file_name"],
            file_created=datetime.fromtimestamp(info["file_created"]),
            tables=tables,
            file_type=info["file_type"],
            workdir=workdir,
            search_tree=tree,
        )
        pqf.info_json_path.unlink()
        return pqf

    @property
    def info_json_path(self) -> Path:
        return Path(self.workdir, self.INFO_JSON)

    def clean_up(self) -> None:
        shutil.rmtree(self.workdir, ignore_errors=True)

    @contextlib.contextmanager
    def temporary_attribute_json(self) -> Path:
        with open(str(self.info_json_path), "w") as f:
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
        try:
            yield self.info_json_path
        finally:
            self.info_json_path.unlink()

    def count_parquets(self):
        """ Count all child parquets. """
        return sum(pqf.parquet_count for pqf in self.tables.values())

    def save_file_to_zip(self, zf: ZipFile, relative_to: Path, logger: BaseLogger = None):
        with self.temporary_attribute_json() as f:
            zf.write(f, arcname=f.relative_to(relative_to))
        for pqt_frame in self.tables.values():
            pqt_frame.save_frame_to_zip(zf, relative_to, logger)

    def save_as(self, dir_: PathLike, name: str) -> Path:
        """ Save parquet storage into given location. """
        device = Path(dir_, f"{name}{self.EXT}")
        with ZipFile(device, mode="w") as zf:
            self.save_file_to_zip(zf, self.workdir)
        return device
