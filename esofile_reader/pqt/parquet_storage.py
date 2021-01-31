import shutil
import tempfile
from pathlib import Path
from typing import Type
from zipfile import ZipFile

from esofile_reader.abstractions.base_frame import get_unique_workdir
from esofile_reader.abstractions.base_storage import BaseStorage
from esofile_reader.id_generator import incremental_id_gen, get_unique_name
from esofile_reader.pqt.parquet_file import ParquetFile
from esofile_reader.pqt.parquet_tables import (
    VirtualParquetTables,
    ParquetTables,
    get_conversion_n_steps,
    convert_tables,
)
from esofile_reader.processing.progress_logger import BaseLogger
from esofile_reader.typehints import ResultsFileType, PathLike


class ParquetStorage(BaseStorage):
    def __init__(
        self, workdir: PathLike = None, tables_class: Type[ParquetTables] = ParquetTables
    ):
        super().__init__()
        self.files = {}
        self.path = None
        self._tables_class = tables_class
        if workdir:
            self.workdir = Path(workdir)
            self.workdir.mkdir()
        else:
            self.workdir = Path(tempfile.mkdtemp(prefix="pqs-"))

    def __del__(self):
        shutil.rmtree(self.workdir, ignore_errors=True)

    def __copy__(self):
        return self.copy_to(get_unique_workdir(self.workdir))

    def copy_to(self, new_workdir: Path):
        pqs = ParquetStorage(new_workdir)
        for id_, file in self.files.items():
            new_file = file.copy_to(new_workdir)
            pqs.files[id_] = new_file
        return pqs

    @classmethod
    def _load_storage(
        cls, path: Path, tables_class: Type[ParquetTables], logger: BaseLogger
    ) -> "ParquetStorage":
        if path.suffix != cls.EXT:
            raise IOError(f"Invalid file type loaded. Only '{cls.EXT}' files are allowed")
        pqs = ParquetStorage(tables_class=tables_class)
        pqs.path = path

        logger.log_section("unzipping files")
        with ZipFile(path, "r") as zf:
            zf.extractall(pqs.workdir)

        logger.log_section("creating parquet instances")
        for dir_ in [d for d in pqs.workdir.iterdir() if d.is_dir()]:
            pqf = ParquetFile.from_file_system(dir_, "", tables_class=tables_class)
            pqs.files[pqf.id_] = pqf
        return pqs

    @classmethod
    def load_storage(
        cls,
        path: PathLike,
        tables_class: Type[ParquetTables] = ParquetTables,
        logger: BaseLogger = None,
    ) -> "ParquetStorage":
        path = path if isinstance(path, Path) else Path(path)
        logger = logger if logger else BaseLogger(path.name)
        with logger.log_task("Load storage"):
            return cls._load_storage(path, tables_class, logger)

    def change_tables_class(
        self, tables_class: Type[ParquetTables], logger: BaseLogger
    ) -> None:
        """ Update current tables class. """
        logger = logger if logger else BaseLogger(self.workdir.name)
        n = sum([get_conversion_n_steps(f.tables, tables_class) for f in self.files.values()])
        with logger.log_task("convert table type"):
            logger.set_maximum_progress(n)
            for file in self.files.values():
                file.tables = convert_tables(file.tables, tables_class, logger)

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
                storage_path, self._tables_class, logger
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
