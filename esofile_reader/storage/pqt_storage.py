import math
import shutil
import tempfile
from pathlib import Path
from typing import Union, List
from zipfile import ZipFile

from esofile_reader.data.pqt_data import ParquetFrame
from esofile_reader.mini_classes import ResultsFile
from esofile_reader.processor.monitor import DefaultMonitor
from esofile_reader.storage.df_storage import DFStorage
from esofile_reader.storage.storage_files import ParquetFile


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

        id_ = self._id_generator()
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

        for path in paths:
            pqs = ParquetStorage.load_storage(path)
            for id_, file in pqs.files.items():
                # create new identifiers in case that id already exists
                new_id = self._id_generator() if id_ in self.files.keys() else id_
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
