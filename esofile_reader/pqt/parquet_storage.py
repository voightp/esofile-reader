import shutil
import tempfile
from enum import Enum
from pathlib import Path
from typing import Type
from zipfile import ZipFile

from esofile_reader.abstractions.base_frame import get_unique_workdir, BaseParquetFrame
from esofile_reader.abstractions.base_storage import BaseStorage
from esofile_reader.id_generator import incremental_id_gen, get_unique_name
from esofile_reader.pqt.parquet_file import ParquetFile
from esofile_reader.pqt.parquet_tables import (
    VirtualParquetTables,
    ParquetTables,
    DfParquetTables,
    get_conversion_n_steps,
    convert_tables,
)
from esofile_reader.processing.progress_logger import BaseLogger
from esofile_reader.typehints import ResultsFileType, PathLike


class StorageType(Enum):
    PARQUET = ParquetTables
    VIRTUAL = VirtualParquetTables
    DF = DfParquetTables


class ParquetStorage(BaseStorage):
    def __init__(
        self, workdir: PathLike = None, storage_type: StorageType = StorageType.PARQUET
    ):
        super().__init__()
        self.files = {}
        self.path = None
        self._storage_type = storage_type
        if workdir:
            self.workdir = Path(workdir)
            self.workdir.mkdir()
        else:
            self.workdir = Path(tempfile.mkdtemp(prefix="pqs-"))

    def __del__(self):
        shutil.rmtree(self.workdir, ignore_errors=True)

    def __copy__(self):
        return self.copy_to(get_unique_workdir(self.workdir))

    @property
    def _tables_class(self) -> Type[ParquetTables]:
        return self._storage_type.value

    def copy_to(self, new_workdir: Path):
        pqs = ParquetStorage(new_workdir)
        for id_, file in self.files.items():
            new_file = file.copy_to(new_workdir)
            pqs.files[id_] = new_file
        return pqs

    @classmethod
    def _load_storage(
        cls, path: Path, storage_type: StorageType, logger: BaseLogger
    ) -> "ParquetStorage":
        if path.suffix != cls.EXT:
            raise IOError(f"Invalid file type loaded. Only '{cls.EXT}' files are allowed")
        pqs = ParquetStorage(storage_type=storage_type)
        pqs.path = path

        logger.log_section("unzipping files")
        with ZipFile(path, "r") as zf:
            zf.extractall(pqs.workdir)

        logger.log_section("creating parquet instances")
        for dir_ in [d for d in pqs.workdir.iterdir() if d.is_dir()]:
            pqf = ParquetFile.from_file_system(dir_, "", tables_class=storage_type.value)
            pqs.files[pqf.id_] = pqf
        return pqs

    @classmethod
    def load_storage(
        cls,
        path: PathLike,
        storage_type: StorageType = StorageType.PARQUET,
        logger: BaseLogger = None,
    ) -> "ParquetStorage":
        path = path if isinstance(path, Path) else Path(path)
        logger = logger if logger else BaseLogger(path.name)
        with logger.log_task("Load storage"):
            return cls._load_storage(path, storage_type, logger)

    def change_storage_type(self, storage_type: Type[StorageType], logger: BaseLogger) -> None:
        """ Update current tables class. """
        logger = logger if logger else BaseLogger(self.workdir.name)
        n = sum(
            [get_conversion_n_steps(f.tables, storage_type.value) for f in self.files.values()]
        )
        with logger.log_task("convert table type"):
            logger.set_maximum_progress(n)
            for file in self.files.values():
                file.tables = convert_tables(file.tables, storage_type.value, logger)

    def calculate_n_steps_saving(self) -> int:
        """ Count all child parquets. """
        return sum(pqf.calculate_n_steps_saving() for pqf in self.files.values())

    def save_as(self, dir_: PathLike, name: str, logger: BaseLogger = None) -> Path:
        logger = logger if logger else BaseLogger(self.workdir.name)
        with logger.log_task("save storage"):
            logger.set_maximum_progress(self.calculate_n_steps_saving())
            path = Path(dir_, f"{name}{self.EXT}")
            with ZipFile(path, mode="w") as zf:
                for pqf in self.files.values():
                    pqf.save_file_to_zip(zf, self.workdir, logger)
            self.path = path
        return path

    def save(self, logger: BaseLogger = None) -> Path:
        if not self.path:
            raise FileNotFoundError("Path not defined! Call 'save_as' first.")
        dir_ = self.path.parent
        name = self.path.with_suffix("").name
        return self.save_as(dir_, name, logger)

    def merge_with(self, storage_path: PathLike, logger: BaseLogger = None) -> None:
        logger = logger if logger else BaseLogger(self.workdir.name)
        storage_path = Path(storage_path)
        if not logger:
            logger = BaseLogger(storage_path.name)
        with logger.log_task(f"merge storage with {storage_path.name}"):
            id_gen = incremental_id_gen(start=1, checklist=set(self.files.keys()))
            temporary_storage = ParquetStorage.load_storage(
                storage_path, self._storage_type, logger
            )
            for id_, file in dict(sorted(temporary_storage.files.items())).items():
                # create new identifiers in case that id already exists
                new_id = next(id_gen) if id_ in self.files.keys() else id_
                new_name = get_unique_name(file.file_name, self.get_all_file_names())
                file.rename(new_name)
                new_file = file.copy_to(self.workdir, new_id=new_id)
                self.files[new_id] = new_file
            del temporary_storage

    def store_file(self, results_file: ResultsFileType, logger: BaseLogger = None) -> int:
        logger = logger if logger else BaseLogger(self.workdir.name)
        with logger.log_task(f"Store file {results_file.file_name}"):
            logger.log_section("calculating number of parquets")
            n = self._tables_class.predict_n_chunks(results_file.tables)
            logger.set_maximum_progress(n)
            id_gen = incremental_id_gen(checklist=set(self.files.keys()))
            id_ = next(id_gen)

            logger.log_section("writing parquets")
            file = ParquetFile.from_results_file(
                id_=id_,
                results_file=results_file,
                pardir=self.workdir,
                logger=logger,
                tables_class=self._tables_class,
            )
            self.files[id_] = file
        return id_

    def delete_file(self, id_: int, logger: BaseLogger = None) -> None:
        logger = logger if logger else BaseLogger(self.workdir.name)
        with logger.log_task(f"Delete file: {self.files[id_].file_name}"):
            shutil.rmtree(self.files[id_].workdir, ignore_errors=True)
            del self.files[id_]
