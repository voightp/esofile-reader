import shutil
import tempfile
import math
from pathlib import Path
from typing import Union
from zipfile import ZipFile

from esofile_reader.data.pqt_data import ParquetFrame
from esofile_reader.storage.df_storage import DFStorage
from esofile_reader.storage.storage_files import ParquetFile
from esofile_reader.totals_file import TotalsFile
from esofile_reader.utils.mini_classes import ResultsFile
from esofile_reader.processor.monitor import DefaultMonitor


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
    # @profile(entries=10, sort="time")
    def load(cls, path: Union[str, Path]):
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
        file = ParquetFile(
            id_=id_,
            file_path=results_file.file_path,
            file_name=results_file.file_name,
            data=results_file.data,
            file_created=results_file.file_created,
            search_tree=results_file.search_tree,
            totals=isinstance(results_file, TotalsFile),
            pardir=self.workdir,
            monitor=monitor,
        )
        self.files[id_] = file

        if monitor:
            monitor.storing_finished()

        return id_

    def delete_file(self, id_: int) -> None:
        """ Delete file with given id. """
        shutil.rmtree(self.files[id_].workdir, ignore_errors=True)
        del self.files[id_]

    # @profile(entries=10, sort="time")
    def save_as(self, dir_, name):
        """ Save parquet storage into given location. """
        self.path = str(Path(dir_, f"{name}{self.EXT}"))

        # save all files
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
