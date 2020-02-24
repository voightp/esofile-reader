import contextlib
import os
import shutil
import tempfile
from pathlib import Path
from typing import Union

from esofile_reader.storage.df_storage import DFStorage
from esofile_reader.storage.storage_files import ParquetFile
from esofile_reader.totals_file import TotalsFile
from esofile_reader.utils.mini_classes import ResultsFile


class ParquetStorage(DFStorage):
    EXT = ".cfy"

    def __init__(self, path=None):
        super().__init__()
        self.files = {}
        self.path = Path(path) if path else path
        self.temp_dir = tempfile.mkdtemp(prefix="chartify-")

    def __del__(self):
        print("REMOVING PARQUET STORAGE " + str(self.temp_dir))
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @classmethod
    def load(cls, path: Union[str, Path]):
        """ Load ParquetStorage from filesystem. """
        path = path if isinstance(path, Path) else Path(path)
        pqs = ParquetStorage(path)
        files = [Path(path, d) for d in path.iterdir() if d.suffix == ParquetFile.EXT]
        for f in files:
            pqf = ParquetFile.load_file(f, pqs.temp_dir)
            pqs.files[pqf.id_] = pqf
        return pqs

    def store_file(self, results_file: ResultsFile) -> int:
        """ Store results file as 'ParquetFile'. """
        id_ = self._id_generator()
        file = ParquetFile(
            id_=id_,
            file_path=results_file.file_path,
            file_name=results_file.file_name,
            data=results_file.data,
            file_created=results_file.file_created,
            search_tree=results_file.search_tree,
            totals=isinstance(results_file, TotalsFile),
            pardir=self.temp_dir,
        )
        self.files[id_] = file
        return id_

    def delete_file(self, id_: int) -> None:
        """ Delete file with given id. """
        shutil.rmtree(self.files[id_].path, ignore_errors=True)
        del self.files[id_]

    def save_as(self, dir_, name):
        """ Save parquet storage into given location. . """
        # store json summary file
        for f in self.files.values():
            f.save_meta()

        # store all the tempdir content
        zf = shutil.make_archive(str(Path(dir_, f"{name}")), "zip", self.temp_dir)

        # change zip to custom extension
        p = Path(zf)
        path = p.with_suffix(self.EXT)
        with contextlib.suppress(FileNotFoundError):
            os.remove(path)
        p.rename(path)
        self.path = path

    def save(self):
        """ Save parquet storage. """
        if not self.path:
            raise FileNotFoundError("Path not defined! Call 'save_as' first.")
        dir_ = self.path.parent
        name = self.path.with_suffix("").name
        self.save_as(dir_, name)
