import shutil
import tempfile
from pathlib import Path
from zipfile import ZipFile

from esofile_reader.df.df_storage import DFStorage
from esofile_reader.id_generator import incremental_id_gen, get_unique_name
from esofile_reader.typehints import ResultsFileType, PathLike
from esofile_reader.pqt.parquet_file import ParquetFile
from esofile_reader.processing.progress_logger import BaseLogger


class ParquetStorage(DFStorage):
    EXT = ".cfs"

    def __init__(self, path: PathLike = None):
        super().__init__()
        self.files = {}
        self.path = None
        if path:
            self.workdir = Path(path)
            self.workdir.mkdir()
        else:
            self.workdir = Path(tempfile.mkdtemp(prefix="pqs-"))

    def __del__(self):
        shutil.rmtree(self.workdir)

    @classmethod
    def _load_storage(cls, path: Path, logger: BaseLogger) -> "ParquetStorage":
        if path.suffix != cls.EXT:
            raise IOError(f"Invalid file type loaded. Only '{cls.EXT}' files are allowed")
        pqs = ParquetStorage()
        pqs.path = path

        logger.log_section("unzipping files")
        with ZipFile(path, "r") as zf:
            zf.extractall(pqs.workdir)

        logger.log_section("creating parquet instances")
        for dir_ in [d for d in pqs.workdir.iterdir() if d.is_dir()]:
            pqf = ParquetFile.from_file_system(dir_)
            pqs.files[pqf.id_] = pqf
        return pqs

    @classmethod
    def load_storage(cls, path: PathLike, logger: BaseLogger = None) -> "ParquetStorage":
        """ Load ParquetStorage from filesystem. """
        path = path if isinstance(path, Path) else Path(path)
        logger = logger if logger else BaseLogger(path.name)
        with logger.log_task("Load storage"):
            return cls._load_storage(path, logger)

    def store_file(self, results_file: ResultsFileType, logger: BaseLogger = None) -> int:
        """ Store results file as persistent 'ParquetFile'. """
        logger = logger if logger else BaseLogger(self.workdir.name)
        with logger.log_task(f"Store file {results_file.file_name}"):
            logger.log_section("calculating number of parquets")
            n = ParquetFile.predict_number_of_parquets(results_file)
            logger.set_maximum_progress(n)
            id_gen = incremental_id_gen(checklist=set(self.files.keys()))
            id_ = next(id_gen)

            logger.log_section("writing parquets")
            file = ParquetFile.from_results_file(
                id_=id_, results_file=results_file, pardir=self.workdir, logger=logger,
            )
            self.files[id_] = file
        return id_

    def delete_file(self, id_: int, logger: BaseLogger = None) -> None:
        """ Delete file with given id. """
        logger = logger if logger else BaseLogger(self.workdir.name)
        with logger.log_task(f"Delete file: {self.files[id_].file_name}"):
            shutil.rmtree(self.files[id_].workdir, ignore_errors=True)
            del self.files[id_]

    def save_as(self, dir_: PathLike, name: str, logger: BaseLogger = None) -> Path:
        """ Save parquet storage into given location. """
        logger = logger if logger else BaseLogger(self.workdir.name)
        with logger.log_task("save storage"):
            path = Path(dir_, f"{name}{self.EXT}")
            with ZipFile(path, mode="w") as zf:
                for pqf in self.files.values():
                    pqf.save_file_to_zip(zf, self.workdir)
            self.path = path
        return path

    def save(self, logger: BaseLogger = None) -> Path:
        """ Save parquet storage. """
        if not self.path:
            raise FileNotFoundError("Path not defined! Call 'save_as' first.")
        dir_ = self.path.parent
        name = self.path.with_suffix("").name
        return self.save_as(dir_, name, logger)

    def merge_with(self, storage_path: PathLike, logger: BaseLogger = None) -> None:
        """ Merge this storage with arbitrary number of other ones. """
        logger = logger if logger else BaseLogger(self.workdir.name)
        storage_path = Path(storage_path)
        if not logger:
            logger = BaseLogger(storage_path.name)
        with logger.log_task(f"merge storage with {storage_path.name}"):
            id_gen = incremental_id_gen(start=1, checklist=set(self.files.keys()))
            temporary_storage = ParquetStorage._load_storage(storage_path, logger)
            for id_, file in dict(sorted(temporary_storage.files.items())).items():
                # create new identifiers in case that id already exists
                new_id = next(id_gen) if id_ in self.files.keys() else id_
                new_name = get_unique_name(file.file_name, self.get_all_file_names())
                file.rename(new_name)
                new_file = file.copy_to(self.workdir, new_id=new_id)
                self.files[new_id] = new_file
            del temporary_storage
