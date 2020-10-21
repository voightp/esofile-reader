import shutil
import tempfile
from pathlib import Path
from zipfile import ZipFile

from esofile_reader.id_generator import incremental_id_gen, get_str_identifier
from esofile_reader.mini_classes import ResultsFileType, PathLike
from esofile_reader.processing.progress_logger import GenericProgressLogger
from esofile_reader.df.df_storage import DFStorage
from esofile_reader.pqt.parquet_tables import ParquetFrame
from esofile_reader.pqt.parquet_file import ParquetFile


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

    def save_as(self, dir_: PathLike, name: str) -> Path:
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

    def save(self) -> Path:
        """ Save parquet storage. """
        if not self.path:
            raise FileNotFoundError("Path not defined! Call 'save_as' first.")
        dir_ = self.path.parent
        name = self.path.with_suffix("").name
        return self.save_as(dir_, name)

    def merge_with(self, storage_path: PathLike) -> None:
        """ Merge this storage with arbitrary number of other ones. """
        id_gen = incremental_id_gen(start=0, checklist=list(self.files.keys()))
        pqs = ParquetStorage.load_storage(storage_path)
        for id_, file in dict(sorted(pqs.files.items())).items():
            # create new identifiers in case that id already exists
            new_id = next(id_gen) if id_ in self.files.keys() else id_
            new_name = get_str_identifier(file.file_name, self.get_all_file_names())
            file.rename(new_name)
            new_file = file.copy_to(self.workdir, new_id=new_id)
            self.files[new_id] = new_file
        del pqs
